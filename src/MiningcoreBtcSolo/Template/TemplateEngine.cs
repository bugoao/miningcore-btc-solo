using System.Threading.Channels;
using MiningcoreBtcSolo.Metrics;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Submit;
using MiningcoreBtcSolo.Util;
using NetMQ;
using NetMQ.Sockets;

namespace MiningcoreBtcSolo.Template;

/// <summary>
/// Solo template policy:
/// - P2P headers/cmpctblock → empty clean job only (no GBT on success path).
/// - ZMQ/direct GBT may overlap longpoll; their short apply phases remain serialized.
/// - Identical template key → skip, no stratum job notify.
/// - No rebroadcast / same-tip fee timer.
/// </summary>
public sealed class TemplateEngine
{
    private readonly AppConfig _cfg;
    private readonly BitcoinRpcClient _rpc;
    private readonly JobBuilder _builder;
    private readonly MetricsStore _metrics;
    private readonly BlockSubmitQueue _submitQueue;
    private readonly SemaphoreSlim _refreshSem = new(1, 1);
    private readonly SemaphoreSlim _directGbtSem = new(1, 1);
    // Protects latest-state notification coalescing. It is never held while fan-out
    // or socket I/O runs, so P2P-fast/ZMQ/longpoll publication cannot be stalled by a miner.
    private readonly object _publicationLock = new();
    private readonly object _jobLock = new();
    private readonly List<JobRegistration> _jobs = new();
    private readonly Dictionary<ulong, JobRegistration> _jobsByKey = new();
    private readonly Dictionary<string, DateTimeOffset> _expiredJobIds = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, DateTimeOffset> _expiredJobKeys = new();
    private long _retainedTransactionBytes;
    // Active work may briefly be a P2P empty-fast job. Authoritative work is
    // always the latest full GBT and is never overwritten by the fast path.
    private JobTemplate _active = JobTemplate.Empty();
    private JobTemplate _authoritative = JobTemplate.Empty();
    private ChainTip? _tip;
    private string? _longpollId;
    private string? _lastTemplateKey;
    private long _lastZmqMs;
    private string? _lastAppliedGbtLongpollId;
    private long _jobEpoch;
    // Latest-state notification: at most one JobTemplate (which may retain a full
    // block transaction set) waits for broadcast. Superseding updates merge the
    // clean flag so a required old-job invalidation can never be lost.
    private readonly Channel<bool> _notifySignal = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropWrite,
            AllowSynchronousContinuations = false
        });
    private JobNotify? _pendingNotify;

    public TemplateEngine(AppConfig cfg, BitcoinRpcClient rpc, MetricsStore metrics, BlockSubmitQueue submitQueue)
    {
        _cfg = cfg;
        _rpc = rpc;
        _metrics = metrics;
        _submitQueue = submitQueue;
        _builder = new JobBuilder(cfg);
        _submitQueue.SetOnAccepted(ct => RefreshDirectAsync(TemplateSource.PostSubmit, ct));
    }

    public async Task DispatchNotificationsAsync(Action<JobNotify> dispatch, CancellationToken ct)
    {
        while (await _notifySignal.Reader.WaitToReadAsync(ct))
        {
            while (_notifySignal.Reader.TryRead(out _)) { }
            JobNotify? pending;
            lock (_publicationLock)
            {
                pending = _pendingNotify;
                _pendingNotify = null;
            }

            if (pending.HasValue)
                dispatch(pending.Value);
        }
    }

    public JobTemplate AuthoritativeJob
    {
        get { lock (_jobLock) return _authoritative; }
    }

    public JobTemplate ActiveMiningJob
    {
        get { lock (_jobLock) return _active; }
    }

    public bool TryUseActiveMiningJob(Action<JobTemplate> action)
    {
        lock (_jobLock)
        {
            if (!_active.Ready || !_jobs.Any(x => ReferenceEquals(x.Job, _active)))
                return false;

            action(_active);
            return true;
        }
    }

    public bool TryUseAuthoritativeJob(Action<JobTemplate> action)
    {
        lock (_jobLock)
        {
            if (!_authoritative.Ready ||
                !ReferenceEquals(_active, _authoritative) ||
                !_jobs.Any(x => ReferenceEquals(x.Job, _authoritative)))
                return false;

            action(_authoritative);
            return true;
        }
    }

    public JobLookupResult FindJob(string jobId)
    {
        lock (_jobLock)
        {
            JobRegistration? registration = null;
            for (var i = 0; i < _jobs.Count; i++)
            {
                var candidate = _jobs[i];
                if (!string.Equals(candidate.Job.JobId, jobId, StringComparison.Ordinal))
                    continue;
                registration = candidate;
                break;
            }
            if (registration != null)
            {
                return new JobLookupResult(
                    registration.RetiredByEpoch.HasValue
                        ? JobLookupStatus.RetiredWithinGrace
                        : JobLookupStatus.Available,
                    registration.Job);
            }

            var now = DateTimeOffset.UtcNow;
            if (_expiredJobIds.TryGetValue(jobId, out var tombstoneUntil))
            {
                if (tombstoneUntil > now)
                    return new JobLookupResult(JobLookupStatus.Expired, null);
                _expiredJobIds.Remove(jobId);
            }

            return new JobLookupResult(JobLookupStatus.Unknown, null);
        }
    }

    public JobLookupResult FindJob(ulong jobKey)
    {
        lock (_jobLock)
        {
            if (jobKey != 0 && _jobsByKey.TryGetValue(jobKey, out var registration))
            {
                return new JobLookupResult(
                    registration.RetiredByEpoch.HasValue
                        ? JobLookupStatus.RetiredWithinGrace
                        : JobLookupStatus.Available,
                    registration.Job);
            }

            var now = DateTimeOffset.UtcNow;
            if (jobKey != 0 && _expiredJobKeys.TryGetValue(jobKey, out var tombstoneUntil))
            {
                if (tombstoneUntil > now)
                    return new JobLookupResult(JobLookupStatus.Expired, null);
                _expiredJobKeys.Remove(jobKey);
            }

            return new JobLookupResult(JobLookupStatus.Unknown, null);
        }
    }

    /// <summary>
    /// Marks every job retired by this or an earlier clean epoch as delivered to the
    /// live-client snapshot. Actual reclamation remains delayed for in-flight shares.
    /// A later epoch safely satisfies an earlier barrier after latest-wins coalescing.
    /// </summary>
    public void MarkCleanBroadcastComplete(long epoch, DateTimeOffset completedAt)
    {
        lock (_jobLock)
        {
            foreach (var entry in _jobs)
            {
                if (entry.RetiredByEpoch.HasValue && entry.RetiredByEpoch.Value <= epoch &&
                    !entry.BroadcastCompletedAt.HasValue)
                    entry.BroadcastCompletedAt = completedAt;
            }
            ReclaimJobsLocked(completedAt);
        }
    }

    public void ReclaimRetiredJobs(DateTimeOffset now)
    {
        lock (_jobLock)
            ReclaimJobsLocked(now);
    }

    public long MinimumRetainedEpoch
    {
        get
        {
            lock (_jobLock)
                return _jobs.Count == 0 ? Volatile.Read(ref _jobEpoch) : _jobs.Min(x => x.Job.Epoch);
        }
    }

    internal bool TryEnterPublicationForTest() => _refreshSem.Wait(0);

    internal void ExitPublicationForTest() => _refreshSem.Release();

    public async Task StartAsync(CancellationToken ct)
    {
        await RefreshDirectAsync(TemplateSource.Startup, ct);
        _ = Task.Run(() => LongpollLoop(ct), ct);
        if (_cfg.Bitcoind.ZmqBlockUrls.Count > 0)
            _ = Task.Run(() => ZmqLoop(ct), ct);
    }

    /// <summary>
    /// Submit-first: memory-enqueue for immediate submitblock. Disk persist only if
    /// RPC still fails after retries (crash recovery via runtime.data_dir/pending-blocks).
    /// </summary>
    public Task SubmitBlockAsync(string blockHex, string blockHashHex, uint height)
        => _submitQueue.EnqueueFoundBlockAsync(blockHex, blockHashHex, height);

    /// <summary>
    /// Solo: early empty clean job from P2P headers/cmpctblock only.
    /// Does not pull full GBT — ZMQ / longpoll win the full-template race.
    /// </summary>
    public Task HandleP2pFastAnnouncementAsync(
        string prevhashHex,
        string blockHashHex,
        uint blockTime,
        uint blockHeight,
        uint blockNbits,
        CancellationToken ct)
        => TryPublishEmptyFastAsync(
            prevhashHex, blockHashHex, blockTime, blockHeight, blockNbits, ct);

    private async Task TryPublishEmptyFastAsync(
        string prevhashHex,
        string blockHashHex,
        uint blockTime,
        uint blockHeight,
        uint blockNbits,
        CancellationToken ct)
    {
        const TemplateSource source = TemplateSource.P2pFast;

        // Never discard a valid fast-path candidate merely because a full template is
        // in its short synchronous apply phase. Direct/longpoll RPC I/O does not hold
        // this semaphore; after the current apply completes we re-read and revalidate
        // the tip, then either publish the fast child or reject it as superseded.
        await _refreshSem.WaitAsync(ct);

        try
        {
            ChainTip? tip;
            JobTemplate current;
            lock (_jobLock)
            {
                tip = _tip;
                current = _active;
            }
            if (tip == null)
            {
                SoloLog.Debug("skip_job", ("reason", "no_tip"), ("source", SoloLog.SourceName(source)));
                return;
            }

            if (!tip.HashHex.Equals(prevhashHex, StringComparison.OrdinalIgnoreCase))
            {
                SoloLog.Debug("skip_job",
                    ("reason", "prevhash_mismatch"),
                    ("source", SoloLog.SourceName(source)),
                    ("tip", tip.HashHex),
                    ("ann_prev", prevhashHex.ToLowerInvariant()),
                    ("hash", blockHashHex));
                // Solo: no GBT from P2P — ZMQ / longpoll own full-template refresh.
                return;
            }
            if (blockHeight != tip.Height + 1)
            {
                SoloLog.Debug("skip_job",
                    ("reason", "height_mismatch"),
                    ("source", SoloLog.SourceName(source)),
                    ("tip_height", tip.Height),
                    ("ann_height", blockHeight),
                    ("hash", blockHashHex));
                return;
            }
            if (blockNbits != tip.Nbits)
            {
                SoloLog.Debug("skip_job",
                    ("reason", "nbits_mismatch"),
                    ("source", SoloLog.SourceName(source)),
                    ("tip_nbits", tip.Nbits.ToString("x8")),
                    ("ann_nbits", blockNbits.ToString("x8")));
                return;
            }
            if (!BlockHashMeetsTarget(blockHashHex, blockNbits))
            {
                SoloLog.Warn("reject empty-fast block: invalid pow",
                    ("source", SoloLog.SourceName(source)), ("hash", blockHashHex));
                return;
            }

            var nextHeight = blockHeight + 1;
            if (!ShouldPublishFastEmpty(nextHeight))
            {
                SoloLog.Debug("skip_job",
                    ("reason", "empty_fast_guards"),
                    ("source", SoloLog.SourceName(source)),
                    ("height", nextHeight),
                    ("hash", blockHashHex));
                return;
            }

            // Already advanced tip to this announcement — no re-notify.
            if (current.Ready &&
                string.Equals(current.PrevhashBe, blockHashHex, StringComparison.OrdinalIgnoreCase) &&
                current.Height == nextHeight)
            {
                SoloLog.Debug("skip_job",
                    ("reason", "already_on_tip"),
                    ("source", SoloLog.SourceName(source)),
                    ("height", nextHeight),
                    ("hash", blockHashHex));
                return;
            }

            var estimatedMtp = Math.Max(tip.MedianTimePast, blockTime);
            var job = _builder.BuildEmptyFast(tip, blockHashHex, nextHeight, estimatedMtp, source);
            if (string.Equals(_lastTemplateKey, job.TemplateKey, StringComparison.Ordinal))
            {
                SoloLog.Debug("skip_job",
                    ("reason", "identical_template"),
                    ("source", SoloLog.SourceName(source)),
                    ("height", job.Height));
                return;
            }

            PublishFastJob(job);
            lock (_jobLock)
            {
                _tip = new ChainTip
                {
                    HashHex = blockHashHex.ToLowerInvariant(),
                    Height = blockHeight,
                    MedianTimePast = estimatedMtp,
                    Nbits = tip.Nbits,
                    Version = tip.Version,
                    Vbrequired = tip.Vbrequired,
                    TargetLe = tip.TargetLe.ToArray(),
                    NetworkDifficulty = tip.NetworkDifficulty
                };
            }
            SoloLog.Info(SoloLog.FormatTemplateEvent(
                "new block", source, job.Height, 0, job.CoinbaseValue, job.Nbits, cleanJobs: true));
            // Solo: do not schedule GBT here — ZMQ / longpoll own full-template refresh.
        }
        finally
        {
            _refreshSem.Release();
        }
    }

    private bool ShouldPublishFastEmpty(uint nextHeight)
    {
        // mainnet only; skip retarget boundary
        if (!_cfg.NetworkName.Equals("mainnet", StringComparison.OrdinalIgnoreCase) &&
            !_cfg.NetworkName.Equals("bitcoin", StringComparison.OrdinalIgnoreCase))
            return false;
        if (nextHeight % 2016 == 0)
            return false;
        lock (_jobLock)
        {
            if (_active.Ready && _active.Vbrequired != 0)
                return false;
        }
        return true;
    }

    private async Task LongpollLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var param = BuildGbtParams(_longpollId);
                var gbt = await _rpc.CallLongpollAsync<GbtResponse>("getblocktemplate", param, ct);

                // Longpoll and one direct GBT may overlap. Only the short apply phase
                // is serialized, so ZMQ remains an independent recovery path.
                await _refreshSem.WaitAsync(ct);
                try
                {
                    ApplyGbt(gbt, TemplateSource.Longpoll);
                }
                finally
                {
                    _refreshSem.Release();
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                ForgetAppliedGbtRevision();
                MarkRefreshFailed();
                SoloLog.Warn("longpoll error; falling back to direct GBT", ("error", ex.Message));
                try
                {
                    await RefreshDirectAsync(TemplateSource.LongpollFallback, ct);
                }
                catch (Exception ex2)
                {
                    SoloLog.Warn("direct GBT fallback failed", ("error", ex2.Message));
                    await Task.Delay(3000, ct);
                }
            }
        }
    }

    private void ZmqLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var sub = new SubscriberSocket();
                sub.Options.Linger = TimeSpan.Zero;
                foreach (var url in _cfg.Bitcoind.ZmqBlockUrls.Where(u => !string.IsNullOrWhiteSpace(u)))
                {
                    sub.Connect(url);
                    SoloLog.Info("ZMQ connected", ("endpoint", url));
                }
                sub.Subscribe("hashblock");
                sub.Subscribe("rawblock");

                while (!ct.IsCancellationRequested)
                {
                    if (!sub.TryReceiveFrameString(TimeSpan.FromSeconds(1), out var topic))
                        continue;

                    // Multipart: [topic][body][sequence...]
                    byte[]? body = null;
                    if (sub.HasIn)
                        sub.TryReceiveFrameBytes(out body);
                    while (sub.HasIn)
                        sub.TryReceiveFrameBytes(out _);

                    var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    if (now - Interlocked.Read(ref _lastZmqMs) < 10)
                        continue;
                    Interlocked.Exchange(ref _lastZmqMs, now);

                    // Solo: ZMQ only schedules full GBT (race with longpoll). Empty clean is P2P-only.
                    var source = topic == "rawblock" ? TemplateSource.ZmqRawblock : TemplateSource.ZmqHashblock;
                    string? hashHex = null;
                    if (topic == "hashblock" && body is { Length: 32 })
                    {
                        var hashBe = Hex.ReverseCopy(body);
                        hashHex = Hex.Encode(hashBe);
                    }

                    SoloLog.Info("ZMQ block notification received",
                        ("source", SoloLog.SourceName(source)),
                        ("hash", hashHex ?? ""),
                        ("action", "scheduling template refresh"));
                    _ = RefreshFromZmqAsync(source, ct);
                }
            }
            catch (Exception ex)
            {
                SoloLog.Warn("ZMQ loop error; reconnecting", ("error", ex.Message));
                Thread.Sleep(2000);
            }
        }
    }

    public async Task RefreshDirectAsync(TemplateSource source, CancellationToken ct)
    {
        // Coalesce direct callers while allowing the standing longpoll request to run.
        if (!await _directGbtSem.WaitAsync(TimeSpan.FromSeconds(15), ct))
        {
            SoloLog.Debug("skip_job",
                ("reason", "direct_gbt_in_flight"),
                ("source", SoloLog.SourceName(source)));
            return;
        }

        try
        {
            // Do not hold the publication semaphore across the RPC. A slow direct
            // getblocktemplate must not prevent a validated P2P announcement from
            // publishing its empty clean job immediately. The raw GBT is reclassified
            // against the then-current active tip after we acquire the apply lock.
            var gbt = await _rpc.CallAsync<GbtResponse>("getblocktemplate", BuildGbtParams(null), ct);
            await _refreshSem.WaitAsync(ct);
            try
            {
                ApplyGbt(gbt, source);
            }
            finally
            {
                _refreshSem.Release();
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            ForgetAppliedGbtRevision();
            MarkRefreshFailed();
            throw;
        }
        finally
        {
            _directGbtSem.Release();
        }
    }

    private async Task RefreshFromZmqAsync(TemplateSource source, CancellationToken ct)
    {
        try
        {
            await RefreshDirectAsync(source, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // normal shutdown
        }
        catch (Exception ex)
        {
            SoloLog.Warn("ZMQ template refresh failed",
                ("source", SoloLog.SourceName(source)),
                ("error", ex.Message));
        }
    }

    private void ApplyGbt(GbtResponse gbt, TemplateSource source)
    {
        var gbtPrev = gbt.PreviousBlockhash ?? "";
        var gbtHeight = gbt.Height;
        var txCount = gbt.Transactions?.Length ?? 0;

        // Cheap tip checks on raw GBT fields — avoid full FromGbt (coinbase/merkle/tx lists).
        lock (_jobLock)
        {
            if (IsSupersededGbtResponse(gbt, _authoritative, _lastAppliedGbtLongpollId))
            {
                var responseRevision = ParseGbtLongpollRevision(gbt.LongPollId);
                var appliedRevision = ParseGbtLongpollRevision(_lastAppliedGbtLongpollId);
                SoloLog.Info("skip_job",
                    ("reason", "older_same_tip_gbt_revision"),
                    ("source", SoloLog.SourceName(source)),
                    ("gbt_height", gbtHeight),
                    ("prevhash", gbtPrev),
                    ("response_revision", responseRevision?.TransactionsUpdated),
                    ("applied_revision", appliedRevision?.TransactionsUpdated));
                MarkRefreshOk();
                return;
            }

            var tipRelation = ClassifyGbtTip(_active, gbtPrev, gbtHeight);
            if (tipRelation == GbtTipRelation.Behind)
            {
                SoloLog.Debug("skip_job",
                    ("reason", "behind_tip"),
                    ("source", SoloLog.SourceName(source)),
                    ("gbt_height", gbtHeight),
                    ("current_height", _active.Height));
                MarkRefreshOk();
                return;
            }

            if (tipRelation == GbtTipRelation.Reorg)
            {
                SoloLog.Info("same-height chain reorg detected",
                    ("source", SoloLog.SourceName(source)),
                    ("height", gbtHeight),
                    ("old_prev", _active.PrevhashBe),
                    ("new_prev", gbtPrev.ToLowerInvariant()));
            }
        }

        // A superseded/behind response must not roll the next longpoll back to an
        // obsolete id. Apply phases are serialized, so this assignment is ordered.
        if (!string.IsNullOrEmpty(gbt.LongPollId))
            _longpollId = gbt.LongPollId;

        // Identical template (ZMQ/longpoll race loser): fingerprint txids only — no job build.
        // On miss, reuse LE txid leaves in FromGbt (no second full txid decode).
        TemplateKeyParts? keyParts = null;
        try
        {
            keyParts = _builder.ComputeTemplateKeyParts(gbt);
        }
        catch (Exception ex)
        {
            SoloLog.Warn("template key compute failed; falling through to FromGbt",
                ("source", SoloLog.SourceName(source)),
                ("error", ex.Message));
        }

        if (keyParts != null)
        {
            lock (_jobLock)
            {
                if (string.Equals(_lastTemplateKey, keyParts.Value.Key, StringComparison.Ordinal))
                {
                    RecordAppliedGbtRevision(gbt.LongPollId);
                    MarkRefreshOk();
                    SoloLog.Debug("skip_job",
                        ("reason", "identical_template"),
                        ("source", SoloLog.SourceName(source)),
                        ("height", gbtHeight),
                        ("txs", txCount));
                    return;
                }
            }
        }

        var job = keyParts != null
            ? _builder.FromGbt(gbt, source, keyParts.Value.Key, keyParts.Value.TxHashesLe)
            : _builder.FromGbt(gbt, source);

        // Defense in depth if key was unavailable above, or race with concurrent publish.
        lock (_jobLock)
        {
            if (string.Equals(_lastTemplateKey, job.TemplateKey, StringComparison.Ordinal))
            {
                RecordAppliedGbtRevision(gbt.LongPollId);
                MarkRefreshOk();
                SoloLog.Debug("skip_job",
                    ("reason", "identical_template"),
                    ("source", SoloLog.SourceName(source)),
                    ("height", job.Height),
                    ("txs", job.TransactionCount));
                return;
            }
        }

        bool clean;
        lock (_jobLock)
            clean = ShouldCleanGbtUpdate(_active, job);

        // Publish first so miners see the job ASAP; network hashrate is dashboard-only.
        PublishAuthoritativeJob(job, clean);
        // A P2P-fast child may already be queued while RPC Core is still learning its
        // parent. Applying an authoritative GBT means Core's chain state advanced, so
        // wake deferred submitblock attempts immediately.
        _submitQueue.NotifyChainStateChanged();
        lock (_jobLock)
        {
            RecordAppliedGbtRevision(gbt.LongPollId);
            _tip = new ChainTip
            {
                HashHex = job.PrevhashBe,
                Height = job.Height > 0 ? job.Height - 1 : 0,
                MedianTimePast = job.Mintime > 0 ? job.Mintime - 1 : job.Ntime,
                Nbits = job.Nbits,
                Version = job.Version,
                Vbrequired = job.Vbrequired,
                TargetLe = job.TargetLe.ToArray(),
                NetworkDifficulty = job.NetworkDifficulty
            };
        }

        var eventName = clean ? "new block" : "template update";
        SoloLog.Info(SoloLog.FormatTemplateEvent(
            eventName, source, job.Height, job.TransactionCount, job.CoinbaseValue, job.Nbits, clean));

        // Do not await: job notify must not wait on getnetworkhashps.
        _ = RefreshNetworkHashrateAsync(CancellationToken.None);
    }

    private void RecordAppliedGbtRevision(string? longpollId)
    {
        _lastAppliedGbtLongpollId = ParseGbtLongpollRevision(longpollId).HasValue
            ? longpollId
            : null;
    }

    private void ForgetAppliedGbtRevision()
    {
        // Bitcoin Core's mempool update counter restarts with the node process.
        // A failed outstanding GBT is the process-boundary signal available here.
        lock (_jobLock)
            _lastAppliedGbtLongpollId = null;
    }

    internal static bool IsSupersededGbtResponse(
        GbtResponse response,
        JobTemplate authoritative,
        string? lastAppliedLongpollId)
    {
        if (!authoritative.Ready || response.Height != authoritative.Height ||
            !string.Equals(
                response.PreviousBlockhash,
                authoritative.PrevhashBe,
                StringComparison.OrdinalIgnoreCase))
            return false;

        var responseRevision = ParseGbtLongpollRevision(response.LongPollId);
        var appliedRevision = ParseGbtLongpollRevision(lastAppliedLongpollId);
        return responseRevision.HasValue && appliedRevision.HasValue &&
            string.Equals(
                responseRevision.Value.TipHash,
                response.PreviousBlockhash,
                StringComparison.OrdinalIgnoreCase) &&
            string.Equals(
                appliedRevision.Value.TipHash,
                responseRevision.Value.TipHash,
                StringComparison.OrdinalIgnoreCase) &&
            responseRevision.Value.TransactionsUpdated < appliedRevision.Value.TransactionsUpdated;
    }

    internal static GbtLongpollRevision? ParseGbtLongpollRevision(string? longpollId)
    {
        // Bitcoin Core format: <64-char best-chain hash><mempool update counter>.
        if (string.IsNullOrWhiteSpace(longpollId) || longpollId.Length <= 64)
            return null;

        var tipHash = longpollId[..64];
        if (!Hex.TryDecode(tipHash, out var tipBytes) || tipBytes.Length != 32 ||
            !ulong.TryParse(
                longpollId.AsSpan(64),
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out var transactionsUpdated))
            return null;

        return new GbtLongpollRevision(tipHash.ToLowerInvariant(), transactionsUpdated);
    }

    private void MarkRefreshOk()
    {
        _metrics.LastRefreshOk = true;
        _metrics.LastRefreshMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    private void MarkRefreshFailed()
    {
        _metrics.LastRefreshOk = false;
    }

    /// <summary>Best-effort dashboard metric; must not delay stratum job notify.</summary>
    private async Task RefreshNetworkHashrateAsync(CancellationToken ct)
    {
        try
        {
            var nh = await _rpc.CallAsync<double>("getnetworkhashps", Array.Empty<object>(), ct);
            _metrics.NetworkHashrateHps = nh;
        }
        catch
        {
            // ignore — stale network hashrate is acceptable
        }
    }

    private void PublishFastJob(JobTemplate job) =>
        PublishJob(job, cleanJobs: true, authoritative: false);

    private void PublishAuthoritativeJob(JobTemplate job, bool cleanJobs) =>
        PublishJob(job, cleanJobs, authoritative: true);

    private void PublishJob(JobTemplate job, bool cleanJobs, bool authoritative)
    {
        JobNotify notify;
        lock (_publicationLock)
        {
            // A pending clean flag follows the latest coalesced notification. This is
            // required when a P2P-fast clean job is superseded by its same-tip full GBT
            // before the Stratum broadcaster observes the fast notification.
            var clean = cleanJobs || (_pendingNotify?.CleanJobs ?? false);
            var epoch = Interlocked.Increment(ref _jobEpoch);
            job.Epoch = epoch;
            lock (_jobLock)
            {
                _active = job;
                if (authoritative)
                    _authoritative = job;
                _lastTemplateKey = job.TemplateKey;
                if (clean)
                {
                    foreach (var entry in _jobs)
                    {
                        if (!entry.RetiredByEpoch.HasValue)
                        {
                            entry.RetiredByEpoch = epoch;
                            entry.RetiredAt = DateTimeOffset.UtcNow;
                        }
                    }
                }
                var registration = new JobRegistration(job);
                _jobs.Insert(0, registration);
                _retainedTransactionBytes = checked(
                    _retainedTransactionBytes + job.TransactionBytes);
                if (job.JobKey != 0)
                    _jobsByKey[job.JobKey] = registration;
                TrimNonRetiredJobsLocked();
                ReclaimJobsLocked(DateTimeOffset.UtcNow);
            }

            if (authoritative)
                MarkRefreshOk();
            notify = new JobNotify(epoch, job, clean);
            _pendingNotify = notify;
        }
        _notifySignal.Writer.TryWrite(true);
    }

    private void TrimNonRetiredJobsLocked()
    {
        var keep = Math.Max(1, _cfg.Runtime.KeepOldJobs + 1);
        var nonRetired = 0;
        for (var i = 0; i < _jobs.Count;)
        {
            var entry = _jobs[i];
            if (entry.RetiredByEpoch.HasValue)
            {
                i++;
                continue;
            }

            nonRetired++;
            if (nonRetired <= keep || ReferenceEquals(entry.Job, _active))
            {
                i++;
                continue;
            }

            RemoveJobAtLocked(i, DateTimeOffset.UtcNow);
        }
    }

    private void ReclaimJobsLocked(DateTimeOffset now)
    {
        var grace = TimeSpan.FromMilliseconds(_cfg.Stratum.LateShareGraceMs);
        var maxAge = TimeSpan.FromSeconds(_cfg.Runtime.RetiredJobMaxAgeSecs);
        var retiredCount = _jobs.Count(x => x.RetiredByEpoch.HasValue);

        for (var i = _jobs.Count - 1; i >= 0; i--)
        {
            var entry = _jobs[i];
            if (ReferenceEquals(entry.Job, _active))
                continue;

            // The byte budget covers every retained transaction set, regardless of
            // clean/retired state. Walk oldest-first and preserve only the active job.
            if (_retainedTransactionBytes > _cfg.Runtime.MaxRetainedTransactionBytes)
            {
                RemoveJobAtLocked(i, now);
                if (entry.RetiredByEpoch.HasValue)
                    retiredCount--;
                continue;
            }
            if (!entry.RetiredByEpoch.HasValue)
                continue;

            var barrierAndGraceComplete = entry.BroadcastCompletedAt.HasValue &&
                now - entry.BroadcastCompletedAt.Value >= grace;
            var hardAgeExceeded = entry.RetiredAt.HasValue && now - entry.RetiredAt.Value >= maxAge;
            var overMemoryBound = retiredCount > _cfg.Runtime.MaxRetiredJobs;
            if (!barrierAndGraceComplete && !hardAgeExceeded && !overMemoryBound)
                continue;

            RemoveJobAtLocked(i, now);
            retiredCount--;
        }

        var tombstoneCutoff = now;
        foreach (var key in _expiredJobIds.Where(x => x.Value <= tombstoneCutoff).Select(x => x.Key).ToArray())
            _expiredJobIds.Remove(key);
        foreach (var key in _expiredJobKeys.Where(x => x.Value <= tombstoneCutoff).Select(x => x.Key).ToArray())
            _expiredJobKeys.Remove(key);
    }

    private void RemoveJobAtLocked(int index, DateTimeOffset now)
    {
        var job = _jobs[index].Job;
        RememberExpiredLocked(job, now);
        if (job.JobKey != 0)
            _jobsByKey.Remove(job.JobKey);
        _retainedTransactionBytes -= job.TransactionBytes;
        if (_retainedTransactionBytes < 0)
            _retainedTransactionBytes = 0;
        _jobs.RemoveAt(index);
    }

    private void RememberExpiredLocked(JobTemplate job, DateTimeOffset now)
    {
        var tombstoneLifetime = TimeSpan.FromSeconds(
            Math.Max(_cfg.Runtime.RetiredJobMaxAgeSecs, 5));
        var expiresAt = now + tombstoneLifetime;
        _expiredJobIds[job.JobId] = expiresAt;
        if (job.JobKey != 0)
            _expiredJobKeys[job.JobKey] = expiresAt;
    }

    internal static bool ShouldCleanGbtUpdate(JobTemplate active, JobTemplate next)
    {
        if (!active.Ready)
            return true;

        var chainChanged = active.Height != next.Height ||
            !string.Equals(active.PrevhashBe, next.PrevhashBe, StringComparison.OrdinalIgnoreCase);
        if (chainChanged)
            return true;

        // submitold describes replacement of Core's preceding GBT. A P2P-fast
        // job already mines on this exact tip, so confirming it must preserve
        // late fast-job shares and take over with clean=false.
        return !next.SubmitOld && active.Source != TemplateSource.P2pFast;
    }

    private static object[] BuildGbtParams(string? longpollId)
    {
        var rules = new Dictionary<string, object>
        {
            ["rules"] = new[] { "segwit" }
        };
        if (!string.IsNullOrEmpty(longpollId))
            rules["longpollid"] = longpollId;
        return new object[] { rules };
    }

    private static bool BlockHashMeetsTarget(string hashHex, uint nbits)
    {
        try
        {
            var hashBe = Hex.Decode(hashHex);
            if (hashBe.Length != 32) return false;
            var hashLe = Hex.ReverseCopy(hashBe);
            var targetLe = BitcoinEncoding.CompactTargetToLe(nbits);
            return BitcoinEncoding.LeqLe256(hashLe, targetLe);
        }
        catch { return false; }
    }

    internal static GbtTipRelation ClassifyGbtTip(JobTemplate current, string gbtPrev, uint gbtHeight)
    {
        // Job.Height is the block being mined, while PrevhashBe is the active tip.
        // A P2P-fast announcement for block H publishes a speculative job for H+1.
        // Therefore an older Core GBT still mining H is Behind, not a same-height
        // reorg. Core confirming H on another hash produces an H+1 GBT and is the
        // genuine same-height reorg case.
        if (!current.Ready)
            return GbtTipRelation.Ahead;
        if (gbtHeight < current.Height)
            return GbtTipRelation.Behind;
        if (gbtHeight > current.Height)
            return GbtTipRelation.Ahead;
        return string.Equals(current.PrevhashBe, gbtPrev, StringComparison.OrdinalIgnoreCase)
            ? GbtTipRelation.SameTip
            : GbtTipRelation.Reorg;
    }
}

public readonly record struct JobNotify(long Epoch, JobTemplate Job, bool CleanJobs);

internal readonly record struct GbtLongpollRevision(string TipHash, ulong TransactionsUpdated);

public readonly record struct JobLookupResult(JobLookupStatus Status, JobTemplate? Job);

public enum JobLookupStatus
{
    Available,
    RetiredWithinGrace,
    Expired,
    Unknown
}

internal sealed class JobRegistration(JobTemplate job)
{
    public JobTemplate Job { get; } = job;
    public long? RetiredByEpoch { get; set; }
    public DateTimeOffset? RetiredAt { get; set; }
    public DateTimeOffset? BroadcastCompletedAt { get; set; }
}

internal enum GbtTipRelation
{
    Behind,
    SameTip,
    Reorg,
    Ahead
}
