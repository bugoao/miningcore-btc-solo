using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using MiningcoreBtcSolo.Metrics;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.Submit;

/// <summary>
/// Solo block submission (submit-first, persist-on-failure):
/// 1) Enqueue in memory and submitblock ASAP (no disk on the happy path)
/// 2) Single consumer with retries
/// 3) Inconclusive results persist immediately; transport failures persist after retries
/// 4) Consensus rejects → archive to failed-blocks/
/// 5) Startup re-queues any pending files from prior failures
/// </summary>
public sealed class BlockSubmitQueue
{
    private readonly BitcoinRpcClient _rpc;
    private readonly MetricsStore _metrics;
    private readonly string _pendingDir;
    private readonly string _failedDir;
    private readonly Channel<PendingBlock> _channel = Channel.CreateUnbounded<PendingBlock>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    private readonly object _chainStateLock = new();
    private TaskCompletionSource _chainStateChanged = NewChainStateSignal();
    private long _chainStateVersion;
    private Func<CancellationToken, Task>? _onAccepted;
    private readonly object _lifecycleLock = new();
    private Task? _runTask;
    private CancellationTokenSource? _runCts;
    private Task? _stopTask;
    private int _started;
    private int _stopping;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public BlockSubmitQueue(AppConfig cfg, BitcoinRpcClient rpc, MetricsStore metrics)
    {
        _rpc = rpc;
        _metrics = metrics;
        var root = ResolveDataDir(cfg.Runtime.DataDir);
        _pendingDir = Path.Combine(root, "pending-blocks");
        _failedDir = Path.Combine(root, "failed-blocks");
        Directory.CreateDirectory(_pendingDir);
        Directory.CreateDirectory(_failedDir);
    }

    public string PendingDir => _pendingDir;
    internal bool IsStopped => _stopTask?.IsCompletedSuccessfully == true;

    /// <summary>Optional hook after submitblock accepted (e.g. refresh GBT).</summary>
    public void SetOnAccepted(Func<CancellationToken, Task> onAccepted) => _onAccepted = onAccepted;

    /// <summary>
    /// Wake a block waiting for its P2P-announced parent after Core publishes an
    /// authoritative template on the new chain tip.
    /// </summary>
    public void NotifyChainStateChanged()
    {
        TaskCompletionSource signal;
        lock (_chainStateLock)
        {
            signal = _chainStateChanged;
            _chainStateChanged = NewChainStateSignal();
            _chainStateVersion++;
        }
        signal.TrySetResult();
    }

    public Task StartAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (Interlocked.Exchange(ref _started, 1) != 0)
            return Task.CompletedTask;

        RecoverPendingIntoChannel();
        _runCts = new CancellationTokenSource();
        _runTask = Task.Run(() => RunLoopAsync(_runCts.Token), CancellationToken.None);
        SoloLog.Info("block submit queue ready",
            ("mode", "submit_first_persist_on_fail"),
            ("pending_dir", _pendingDir),
            ("failed_dir", _failedDir));
        return Task.CompletedTask;
    }

    /// <summary>
    /// Stop accepting new blocks, drain queued submissions, and only then return.
    /// Normal operation remains submit-first; the timeout is a final shutdown escape
    /// hatch that persists the in-flight and queued blocks before process exit.
    /// </summary>
    public Task StopAsync(TimeSpan? drainTimeout = null)
    {
        lock (_lifecycleLock)
        {
            _stopTask ??= StopCoreAsync(drainTimeout ?? TimeSpan.FromMinutes(2));
            return _stopTask;
        }
    }

    /// <summary>
    /// Memory enqueue only (no disk). Returns immediately after the work is queued.
    /// RPC submit runs on the background consumer; disk persist only if submit still fails.
    /// </summary>
    public Task EnqueueFoundBlockAsync(string blockHex, string blockHashHex, uint height)
    {
        if (string.IsNullOrWhiteSpace(blockHex) || string.IsNullOrWhiteSpace(blockHashHex))
            throw new ArgumentException("block hex/hash required");

        var hash = blockHashHex.ToLowerInvariant();
        // Block hex from ShareValidator/Hex.Encode is already lowercase; avoid a full-string copy.
        var pending = new PendingBlock
        {
            Height = height,
            Hash = hash,
            BlockHex = blockHex,
            CreatedAtUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            FromDisk = false
        };

        SoloLog.Alert("block queued for submit (memory; no disk yet)",
            ("height", height),
            ("hash", hash));

        lock (_lifecycleLock)
        {
            if (Volatile.Read(ref _stopping) != 0)
                throw new InvalidOperationException("block submit queue is stopping");
            if (!_channel.Writer.TryWrite(pending))
                throw new InvalidOperationException("block submit queue is unavailable");
        }

        return Task.CompletedTask;
    }

    private void RecoverPendingIntoChannel()
    {
        string[] files;
        try
        {
            files = Directory.GetFiles(_pendingDir, "*.json");
        }
        catch (Exception ex)
        {
            SoloLog.Alert("failed to scan pending blocks", ("dir", _pendingDir), ("error", ex.Message));
            return;
        }

        if (files.Length == 0)
            return;

        Array.Sort(files, StringComparer.Ordinal);
        SoloLog.Alert("recovering pending blocks from disk", ("count", files.Length), ("dir", _pendingDir));

        foreach (var file in files)
        {
            try
            {
                var json = File.ReadAllText(file);
                var pending = JsonSerializer.Deserialize<PendingBlock>(json, JsonOpts);
                if (pending == null || string.IsNullOrWhiteSpace(pending.BlockHex) || string.IsNullOrWhiteSpace(pending.Hash))
                {
                    SoloLog.Alert("skip corrupt pending block file", ("path", file));
                    continue;
                }

                pending.Hash = pending.Hash.ToLowerInvariant();
                pending.BlockHex = pending.BlockHex.ToLowerInvariant();
                pending.FromDisk = true;
                if (!_channel.Writer.TryWrite(pending))
                    SoloLog.Alert("could not re-queue pending block", ("hash", pending.Hash));
                else
                    SoloLog.Alert("re-queued pending block", ("height", pending.Height), ("hash", pending.Hash));
            }
            catch (Exception ex)
            {
                SoloLog.Alert("failed to load pending block", ("path", file), ("error", ex.Message));
            }
        }
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var pending in _channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    await SubmitOnceWithRetriesAsync(pending, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    // A bounded graceful shutdown may cancel an in-flight RPC. Preserve
                    // that block before leaving; queued items are drained by StopCoreAsync.
                    try
                    {
                        await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        SoloLog.Alert("failed to persist block during submit queue shutdown",
                            ("hash", pending.Hash),
                            ("height", pending.Height),
                            ("error", ex.Message));
                    }
                    break;
                }
                catch (Exception ex)
                {
                    // Best effort: ensure hex is on disk, then re-queue.
                    SoloLog.Alert("submit loop error; will persist and retry",
                        ("hash", pending.Hash),
                        ("height", pending.Height),
                        ("error", ex.Message));
                    try
                    {
                        await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception pex)
                    {
                        SoloLog.Alert("persist after loop error failed",
                            ("hash", pending.Hash),
                            ("error", pex.Message));
                    }

                    try
                    {
                        await Task.Delay(5_000, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        break;
                    }

                    _channel.Writer.TryWrite(pending);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // shutdown
        }
    }

    private async Task SubmitOnceWithRetriesAsync(PendingBlock pending, CancellationToken ct)
    {
        // Fast first attempts for propagation; longer delays if RPC is unhealthy.
        var delaysMs = new[] { 0, 200, 500, 1000, 2000, 4000, 8000, 15_000, 30_000 };
        Exception? last = null;
        var waitingForParent = pending.WaitingForParent;
        var parentWaitVersion = GetChainStateVersion();

        foreach (var delay in delaysMs)
        {
            if (delay > 0)
            {
                if (waitingForParent)
                    await WaitForChainStateOrDelayAsync(delay, parentWaitVersion, ct).ConfigureAwait(false);
                else
                    await Task.Delay(delay, ct).ConfigureAwait(false);
            }

            try
            {
                var chainStateBeforeSubmit = GetChainStateVersion();
                var result = await _rpc
                    .SubmitBlockAsync(pending.BlockHex, ct)
                    .ConfigureAwait(false);

                if (result == null)
                {
                    _metrics.RecordBlock(pending.Height, pending.Hash, "submitted");
                    SoloLog.Info("submitblock accepted", ("height", pending.Height), ("hash", pending.Hash));
                    DeletePending(pending.Hash);
                    await InvokeOnAcceptedAsync(ct).ConfigureAwait(false);
                    return;
                }

                if (string.Equals(result, "duplicate", StringComparison.OrdinalIgnoreCase))
                {
                    _metrics.RecordBlock(pending.Height, pending.Hash, "duplicate");
                    SoloLog.Info("submitblock duplicate", ("height", pending.Height), ("hash", pending.Hash));
                    DeletePending(pending.Hash);
                    return;
                }

                if (IsMissingParentResult(result))
                {
                    waitingForParent = true;
                    pending.WaitingForParent = true;
                    parentWaitVersion = chainStateBeforeSubmit;
                    last = new InvalidOperationException($"submitblock deferred: {result}");
                    SoloLog.Warn("submitblock waiting for parent block",
                        ("height", pending.Height),
                        ("hash", pending.Hash),
                        ("reason", result));
                    continue;
                }

                if (IsInconclusiveResult(result))
                {
                    last = new InvalidOperationException($"submitblock inconclusive: {result}");

                    // Core has not made a conclusive validity decision. Keep the full
                    // candidate durable before retrying; it must never enter failed-blocks.
                    await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
                    if (!pending.CountedInconclusive)
                    {
                        _metrics.RecordBlock(pending.Height, pending.Hash, "inconclusive");
                        pending.CountedInconclusive = true;
                    }
                    SoloLog.Warn("submitblock inconclusive; persisted and will retry",
                        ("height", pending.Height),
                        ("hash", pending.Hash),
                        ("reason", result));
                    continue;
                }

                // Consensus / policy rejection — do not spin forever; archive for forensics.
                _metrics.RecordBlock(pending.Height, pending.Hash, "rejected");
                SoloLog.Alert("submitblock rejected (archived)",
                    ("height", pending.Height),
                    ("hash", pending.Hash),
                    ("reason", result));
                ArchiveFailed(pending, result);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                last = ex;
                SoloLog.Warn("submitblock attempt failed",
                    ("hash", pending.Hash),
                    ("height", pending.Height),
                    ("error", ex.Message));
            }
        }

        // Transport / node still failing — only now pay for disk I/O so a crash can recover.
        // Record metrics once per "wave" (first failure only) so re-queues do not inflate counters.
        if (!pending.CountedSubmitFailed)
        {
            _metrics.RecordBlock(pending.Height, pending.Hash, "submit_failed");
            pending.CountedSubmitFailed = true;
        }

        try
        {
            await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
            SoloLog.Alert("submitblock exhausted retries; persisted for recovery",
                ("height", pending.Height),
                ("hash", pending.Hash),
                ("path", PathForPending(pending.Hash)),
                ("error", last?.Message ?? "unknown"));
        }
        catch (Exception ex)
        {
            SoloLog.Alert("submitblock exhausted retries AND persist failed — block only in memory until re-queue",
                ("height", pending.Height),
                ("hash", pending.Hash),
                ("submit_error", last?.Message ?? "unknown"),
                ("persist_error", ex.Message));
        }

        // Keep trying after RPC may recover while the process is running. During a
        // graceful stop, the block is already persisted and must not be re-queued.
        if (Volatile.Read(ref _stopping) != 0)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                if (pending.WaitingForParent)
                    await WaitForChainStateOrDelayAsync(
                        60_000, parentWaitVersion, CancellationToken.None).ConfigureAwait(false);
                else
                    await Task.Delay(60_000, CancellationToken.None).ConfigureAwait(false);
                pending.FromDisk = File.Exists(PathForPending(pending.Hash));
                _channel.Writer.TryWrite(pending);
            }
            catch
            {
                // ignore
            }
        }, CancellationToken.None);
    }

    private async Task WaitForChainStateOrDelayAsync(
        int delayMs, long observedVersion, CancellationToken ct)
    {
        Task signalTask;
        lock (_chainStateLock)
        {
            if (_chainStateVersion != observedVersion)
                return;
            signalTask = _chainStateChanged.Task;
        }

        try
        {
            await signalTask
                .WaitAsync(TimeSpan.FromMilliseconds(delayMs), ct)
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // Periodic retry remains the fallback if no authoritative GBT arrives.
        }
    }

    private static TaskCompletionSource NewChainStateSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private long GetChainStateVersion()
    {
        lock (_chainStateLock)
            return _chainStateVersion;
    }

    internal static bool IsMissingParentResult(string? result) =>
        string.Equals(result, "prev-blk-not-found", StringComparison.OrdinalIgnoreCase);

    internal static bool IsInconclusiveResult(string? result) =>
        string.Equals(result, "inconclusive", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(result, "duplicate-inconclusive", StringComparison.OrdinalIgnoreCase);

    private async Task StopCoreAsync(TimeSpan drainTimeout)
    {
        if (Volatile.Read(ref _started) == 0)
            return;
        if (drainTimeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(drainTimeout));

        Interlocked.Exchange(ref _stopping, 1);
        _channel.Writer.TryComplete();

        var runTask = _runTask;
        if (runTask == null)
            return;

        var completed = await Task.WhenAny(runTask, Task.Delay(drainTimeout)).ConfigureAwait(false);
        if (completed != runTask)
        {
            SoloLog.Warn("submit queue drain timeout; persisting remaining blocks",
                ("timeout_secs", drainTimeout.TotalSeconds));
            _runCts?.Cancel();
        }

        try
        {
            await runTask.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SoloLog.Alert("submit queue stopped with an error",
                ("error", ex.Message));
        }

        while (_channel.Reader.TryRead(out var pending))
        {
            try
            {
                await PersistPendingAsync(pending, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SoloLog.Alert("failed to persist queued block during shutdown",
                    ("hash", pending.Hash),
                    ("height", pending.Height),
                    ("error", ex.Message));
            }
        }

        _runCts?.Dispose();
        _runCts = null;
    }

    private async Task PersistPendingAsync(PendingBlock pending, CancellationToken ct)
    {
        var path = PathForPending(pending.Hash);
        if (File.Exists(path))
        {
            pending.FromDisk = true;
            return;
        }

        await WritePendingAtomicAsync(path, pending, ct).ConfigureAwait(false);
        pending.FromDisk = true;
        SoloLog.Alert("block persisted for submit recovery",
            ("height", pending.Height),
            ("hash", pending.Hash),
            ("path", path));
    }

    private async Task InvokeOnAcceptedAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _stopping) != 0)
            return;
        var hook = _onAccepted;
        if (hook == null)
            return;
        try
        {
            await hook(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SoloLog.Warn("post-submit hook failed", ("error", ex.Message));
        }
    }

    private string PathForPending(string hash) => Path.Combine(_pendingDir, $"{hash.ToLowerInvariant()}.json");

    private static async Task WritePendingAtomicAsync(string path, PendingBlock pending, CancellationToken ct)
    {
        var dir = Path.GetDirectoryName(path)!;
        Directory.CreateDirectory(dir);
        var tmp = path + ".tmp";
        // Do not serialize FromDisk (runtime-only).
        var wire = new PendingBlockWire
        {
            Height = pending.Height,
            Hash = pending.Hash,
            BlockHex = pending.BlockHex,
            CreatedAtUnix = pending.CreatedAtUnix
        };
        var json = JsonSerializer.Serialize(wire, JsonOpts);
        await File.WriteAllTextAsync(tmp, json, ct).ConfigureAwait(false);

        try
        {
            await using var fs = new FileStream(tmp, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            fs.Flush(flushToDisk: true);
        }
        catch
        {
            // Some filesystems ignore Flush(true); file is still renamed below.
        }

        File.Move(tmp, path, overwrite: true);
    }

    private void DeletePending(string hash)
    {
        var path = PathForPending(hash);
        try
        {
            if (File.Exists(path))
                File.Delete(path);
            var tmp = path + ".tmp";
            if (File.Exists(tmp))
                File.Delete(tmp);
        }
        catch (Exception ex)
        {
            SoloLog.Warn("failed to delete pending block file", ("path", path), ("error", ex.Message));
        }
    }

    private void ArchiveFailed(PendingBlock pending, string reason)
    {
        var pendingPath = PathForPending(pending.Hash);
        var failedPath = Path.Combine(_failedDir, $"{pending.Hash}.json");
        try
        {
            var archived = new FailedBlock
            {
                Height = pending.Height,
                Hash = pending.Hash,
                BlockHex = pending.BlockHex,
                CreatedAtUnix = pending.CreatedAtUnix,
                FailedAtUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                Reason = reason
            };
            File.WriteAllText(failedPath, JsonSerializer.Serialize(archived, JsonOpts));
            if (File.Exists(pendingPath))
                File.Delete(pendingPath);
        }
        catch (Exception ex)
        {
            SoloLog.Alert("failed to archive rejected block",
                ("hash", pending.Hash),
                ("error", ex.Message),
                ("pending", pendingPath));
        }
    }

    private static string ResolveDataDir(string dataDir)
    {
        if (string.IsNullOrWhiteSpace(dataDir))
            dataDir = "data";
        if (Path.IsPathRooted(dataDir))
            return dataDir;
        return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), dataDir));
    }

    private sealed class PendingBlock
    {
        public uint Height { get; set; }
        public string Hash { get; set; } = "";
        public string BlockHex { get; set; } = "";
        public long CreatedAtUnix { get; set; }
        [JsonIgnore] public bool FromDisk { get; set; }
        /// <summary>Runtime: avoid double-counting submit_failed on re-queue waves.</summary>
        [JsonIgnore] public bool CountedSubmitFailed { get; set; }
        /// <summary>Runtime: record an inconclusive result once while retries continue.</summary>
        [JsonIgnore] public bool CountedInconclusive { get; set; }
        /// <summary>Runtime: retry promptly when Core learns a P2P-announced parent.</summary>
        [JsonIgnore] public bool WaitingForParent { get; set; }
    }

    /// <summary>On-disk shape (no runtime flags).</summary>
    private sealed class PendingBlockWire
    {
        public uint Height { get; set; }
        public string Hash { get; set; } = "";
        public string BlockHex { get; set; } = "";
        public long CreatedAtUnix { get; set; }
    }

    private sealed class FailedBlock
    {
        public uint Height { get; set; }
        public string Hash { get; set; } = "";
        public string BlockHex { get; set; } = "";
        public long CreatedAtUnix { get; set; }
        public long FailedAtUnix { get; set; }
        public string Reason { get; set; } = "";
    }
}
