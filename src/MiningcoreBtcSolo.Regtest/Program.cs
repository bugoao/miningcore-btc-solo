using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using MiningcoreBtcSolo;
using MiningcoreBtcSolo.Metrics;
using MiningcoreBtcSolo.Rpc;
using MiningcoreBtcSolo.Share;
using MiningcoreBtcSolo.Stratum;
using MiningcoreBtcSolo.Submit;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;
using NBitcoin;

// Pre-mainnet validation harness (regtest):
// share validation → full block assembly → submitblock → active-chain confirmation
//
// Modes:
//   direct   – empty-block + mempool multi-tx library paths
//   stratum  — gateway + Stratum V1 (with mempool txs when possible)
//   mempool  — multi-tx library path only
//   vardiff  — offline deterministic VarDiff regression checks (no bitcoind required)
//   encoding — offline encoding/reorg/P2P-fast boundary checks (no bitcoind required)
//   shutdown — offline submit-queue shutdown persistence check (no bitcoind required)
//   safety   — offline review-fix regression checks (no bitcoind required)
//   synthetic-gbt — offline 10k/20k transaction GBT parse/build stress checks
//   large-mempool — real bitcoind + 10,001 independent mempool transactions
//   stress   — synthetic-gbt + large-mempool
//   p2p-fast — real header + coinbase-only fast job + mined active-chain block
//   lifecycle — real bitcoind + multi-miner clean/late/stale burst checks
//   all      — empty + mempool + stratum (default)

var mode = args.FirstOrDefault(a => !a.StartsWith('-')) ?? "all";
if (mode == "vardiff")
{
    RunVarDiffChecks();
    return;
}
if (mode == "encoding")
{
    RunEncodingChecks();
    return;
}
if (mode == "shutdown")
{
    await RunSubmitQueueShutdownCheckAsync();
    return;
}
if (mode == "safety")
{
    await RunSafetyRegressionChecksAsync();
    return;
}
if (mode is "synthetic-gbt" or "stress")
{
    await RunSyntheticGbtStressChecksAsync();
    if (mode == "synthetic-gbt")
        return;
}

var configuredWorkDir = Environment.GetEnvironmentVariable("REGTEST_WORK_DIR");
var ownsWorkDir = string.IsNullOrWhiteSpace(configuredWorkDir);
var workDir = ownsWorkDir
    ? Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-regtest-" + Guid.NewGuid().ToString("N"))
    : Path.GetFullPath(configuredWorkDir!);
var datadir = Env("REGTEST_DATADIR", Path.Combine(workDir, "bitcoind"));
var rpcUrl = Env("REGTEST_RPC_URL", "http://127.0.0.1:18443");
var rpcUser = Env("REGTEST_RPC_USER", "regtest");
var rpcPass = Env("REGTEST_RPC_PASSWORD", "regtestpass");
var stratumHost = Env("REGTEST_STRATUM_HOST", "127.0.0.1");
var stratumPort = int.Parse(Env("REGTEST_STRATUM_PORT", "3333"));
var apiPort = int.Parse(Env("REGTEST_API_PORT", "7152"));
var keepBitcoind = args.Any(a => a is "--keep-bitcoind" or "-k");

Console.WriteLine("=== miningcore-btc-solo regtest validation ===");
Console.WriteLine($"mode={mode} datadir={datadir}");

Process? bitcoind = null;
Process? gateway = null;
var failures = new List<string>();
var passed = new List<string>();

try
{
    EnsureBitcoind(ref bitcoind, datadir, rpcUrl, rpcUser, rpcPass);
    var rpc = new BitcoinRpcClient(rpcUrl, rpcUser, rpcPass);

    WaitForRpc(rpc, TimeSpan.FromSeconds(60));
    var payout = await EnsureWalletAndPayoutAsync(rpc);
    Console.WriteLine($"payout address (regtest): {payout}");

    // Mature coinbase funds / advance chain so GBT is healthy
    await EnsureChainReadyAsync(rpc);

    if (mode == "p2p-fast")
    {
        Console.WriteLine();
        Console.WriteLine("--- P2P-FAST: real header -> coinbase-only job -> mine -> submitblock -> active chain ---");
        try
        {
            await RunRealP2pFastPolicyPathAsync(rpc, payout, workDir);
            passed.Add("p2p-fast coinbase-only block");
            Console.WriteLine("PASS P2P-fast coinbase-only block");
        }
        catch (Exception ex)
        {
            failures.Add($"p2p-fast: {ex.Message}");
            Console.Error.WriteLine($"FAIL P2P-fast: {ex}");
        }
    }

    if (mode is "large-mempool" or "stress")
    {
        Console.WriteLine();
        Console.WriteLine("--- LARGE MEMPOOL: 10,001 tx GBT -> full block -> submitblock -> active chain ---");
        try
        {
            await RunLargeMempoolPathAsync(
                rpc, payout, rpcUrl, rpcUser, rpcPass, transactionCount: 10_001);
            passed.Add("large mempool 10001 tx end-to-end");
            Console.WriteLine("PASS large mempool 10,001 tx end-to-end");
        }
        catch (Exception ex)
        {
            failures.Add($"large-mempool: {ex.Message}");
            Console.Error.WriteLine($"FAIL large mempool: {ex}");
        }
    }

    if (mode is "direct" or "all")
    {
        Console.WriteLine();
        Console.WriteLine("--- [1/3] DIRECT empty: GBT → mine → ShareValidator → submitblock → getblock ---");
        try
        {
            await RunDirectPathAsync(rpc, payout, requireTxCount: 0, label: "empty");
            passed.Add("direct empty share→submitblock→chain");
            Console.WriteLine("PASS direct empty");
        }
        catch (Exception ex)
        {
            failures.Add($"direct-empty: {ex.Message}");
            Console.Error.WriteLine($"FAIL direct empty: {ex}");
        }
    }

    if (mode is "direct" or "mempool" or "all")
    {
        Console.WriteLine();
        Console.WriteLine("--- [2/3] DIRECT mempool: seed txs → GBT(txs>0) → mine full block → submitblock → verify txids ---");
        try
        {
            var mempoolTxids = await SeedMempoolAsync(rpc, count: 3);
            await RunDirectPathAsync(rpc, payout, requireTxCount: 1, label: "mempool", expectedTxids: mempoolTxids);
            passed.Add("direct mempool multi-tx share→submitblock→chain");
            Console.WriteLine("PASS direct mempool multi-tx");
        }
        catch (Exception ex)
        {
            failures.Add($"direct-mempool: {ex.Message}");
            Console.Error.WriteLine($"FAIL direct mempool: {ex}");
        }
    }

    if (mode is "stratum" or "all" or "lifecycle")
    {
        Console.WriteLine();
        Console.WriteLine(mode == "lifecycle"
            ? "--- LIFECYCLE: 16 miners + 3-block burst + clean/late/stale checks ---"
            : "--- [3/3] STRATUM mempool: seed txs → gateway mining.submit → chain tip + txids ---");
        try
        {
            var mempoolTxids = mode == "lifecycle"
                ? new List<string>()
                : await SeedMempoolAsync(rpc, count: 3);
            if (mode == "lifecycle")
            {
                await RunRealP2pFastPolicyPathAsync(rpc, payout, workDir);
                passed.Add("p2p-fast coinbase-only block");
            }
            var configPath = await WriteRuntimeConfigAsync(
                workDir, payout, rpcUrl, rpcUser, rpcPass, stratumPort, apiPort,
                lifecycleMode: mode == "lifecycle");
            gateway = StartGateway(configPath);
            await WaitHttpAsync($"http://127.0.0.1:{apiPort}/healthz", TimeSpan.FromSeconds(45));
            if (mode == "lifecycle")
            {
                await RunStratumLifecyclePathAsync(
                    rpc, stratumHost, stratumPort, apiPort, payout, minerCount: 16);
                passed.Add("stratum clean lifecycle burst");
                Console.WriteLine("PASS stratum clean lifecycle burst");
            }
            else
            {
                await RunStratumPathAsync(rpc, stratumHost, stratumPort, apiPort, expectedTxids: mempoolTxids);
                passed.Add("stratum mempool multi-tx share→submitblock→chain");
                Console.WriteLine("PASS stratum mempool multi-tx");
            }
        }
        catch (Exception ex)
        {
            failures.Add($"stratum: {ex.Message}");
            Console.Error.WriteLine($"FAIL stratum: {ex}");
        }
    }

    Console.WriteLine();
    if (mode is "large-mempool" or "stress")
    {
        Console.WriteLine("=== large-mempool regtest evidence ===");
        Console.WriteLine(
            $"  [{(passed.Any(p => p.Contains("large mempool", StringComparison.Ordinal)) ? "x" : " ")}] " +
            "10,001 mempool tx -> GBT -> assembled block -> active chain");
    }
    else
    {
        Console.WriteLine("=== pre-mainnet checklist (regtest evidence) ===");
        PrintChecklist(passed, failures);
    }
}
finally
{
    TryKill(gateway);
    if (!keepBitcoind)
        TryKill(bitcoind);
    else
        Console.WriteLine("leaving bitcoind running (--keep-bitcoind)");
    if (ownsWorkDir && (bitcoind == null || !keepBitcoind))
    {
        try { Directory.Delete(workDir, recursive: true); } catch { }
    }
}

if (failures.Count > 0)
{
    Console.Error.WriteLine($"FAILED ({failures.Count}): {string.Join("; ", failures)}");
    Environment.Exit(1);
}

Console.WriteLine("ALL REGTEST CHECKS PASSED — safe to proceed with mainnet config review.");
return;

// ---------------------------------------------------------------------------

static void RunVarDiffChecks()
{
    var config = new DifficultyConfig
    {
        Min = 1,
        Max = 1_000_000,
        TargetTimeSecs = 5,
        RetargetTimeSecs = 30,
        RetargetShareBurst = 8,
        VariancePercent = 30,
        RetargetSmoothing = 0.25,
        MaxStepUp = 2,
        MaxStepUpBurst = 32,
        MaxStepDown = 0.5
    };

    var beforeWindow = VarDiffCalculator.Evaluate(config, 1024, 0, 0, 29.9, false);
    AssertVarDiff(!beforeWindow.ResetWindow, "zero-share window fired early");

    var silence = VarDiffCalculator.Evaluate(config, 1024, 0, 0, 30, false);
    AssertVarDiff(silence.ResetWindow && silence.ApplyDifficulty, "zero-share window did not retarget");
    AssertNear(silence.NextDifficulty, 512, "zero-share step-down");

    var luckyPair = VarDiffCalculator.Evaluate(config, 1, 2, 2, 0.1, true);
    AssertVarDiff(!luckyPair.ResetWindow, "two-share sample bypassed retarget_share_burst");

    var realBurst = VarDiffCalculator.Evaluate(config, 1, 8, 8, 0.8, true);
    AssertVarDiff(realBurst.ApplyDifficulty && realBurst.BurstUp, "configured burst did not raise difficulty");
    AssertNear(realBurst.NextDifficulty, 32, "burst max-step cap");

    // Eight old-difficulty shares represent eight units of work. With current=32,
    // weighted estimation moves toward 50, not current*50 (the old compounding bug).
    var graceShares = VarDiffCalculator.Evaluate(config, 32, 8, 8, 0.8, true);
    AssertVarDiff(graceShares.ApplyDifficulty && graceShares.BurstUp, "weighted grace samples did not retarget");
    AssertNear(graceShares.NextDifficulty, 50, "grace-share weighted estimate");

    // With six expected shares/window, five and eight shares are normal variance.
    var stableLow = VarDiffCalculator.Evaluate(config, 100, 5, 500, 30, false);
    AssertVarDiff(stableLow.ResetWindow && !stableLow.ApplyDifficulty,
        "stable five-share window changed difficulty");
    var stableHigh = VarDiffCalculator.Evaluate(config, 100, 8, 800, 30, false);
    AssertVarDiff(stableHigh.ResetWindow && !stableHigh.ApplyDifficulty,
        "stable eight-share window changed difficulty");
    var stableEarlyHigh = VarDiffCalculator.Evaluate(config, 100, 8, 800, 29, true);
    AssertVarDiff(!stableEarlyHigh.ResetWindow && !stableEarlyHigh.ApplyDifficulty,
        "stable eighth share incorrectly triggered the burst path");

    var stableRatio = 1.0;
    var stableShares = new[] { 5, 7, 4, 8, 6, 5 };
    for (var windowIndex = 0; windowIndex < 24; windowIndex++)
    {
        var shares = stableShares[windowIndex % stableShares.Length];
        var stable = VarDiffCalculator.Evaluate(
            config, 100, shares, shares * 100, 30, false, stableRatio);
        AssertVarDiff(stable.ResetWindow && !stable.ApplyDifficulty,
            $"stable multi-window sequence retargeted at window {windowIndex}");
        stableRatio = stable.SmoothedRatio;
    }

    // A single noisy window updates the EWMA; persistence confirms a hashrate change.
    var sparseFirst = VarDiffCalculator.Evaluate(config, 100, 1, 100, 30, false);
    AssertVarDiff(sparseFirst.ResetWindow && !sparseFirst.ApplyDifficulty,
        "single sparse window changed difficulty");
    var sparseSecond = VarDiffCalculator.Evaluate(
        config, 100, 1, 100, 30, false, sparseFirst.SmoothedRatio);
    AssertVarDiff(sparseSecond.ResetWindow && sparseSecond.ApplyDifficulty,
        "persistent sparse windows did not lower difficulty");
    AssertNear(sparseSecond.NextDifficulty, 63.54166666666667,
        "smoothed sparse-window step-down");

    var fastFirst = VarDiffCalculator.Evaluate(config, 100, 12, 1200, 30, false);
    AssertVarDiff(fastFirst.ResetWindow && !fastFirst.ApplyDifficulty,
        "single fast window changed difficulty");
    var fastSecond = VarDiffCalculator.Evaluate(
        config, 100, 12, 1200, 30, false, fastFirst.SmoothedRatio);
    AssertVarDiff(fastSecond.ResetWindow && fastSecond.ApplyDifficulty,
        "persistent fast windows did not raise difficulty");
    AssertNear(fastSecond.NextDifficulty, 143.75, "smoothed fast-window step-up");

    var atMinimum = VarDiffCalculator.Evaluate(config, 1, 0, 0, 30, false);
    AssertVarDiff(atMinimum.ResetWindow && !atMinimum.ApplyDifficulty,
        "minimum difficulty should consume the silent window without reapplying");

    Console.WriteLine("PASS vardiff offline checks");
}

static void AssertVarDiff(bool condition, string message)
{
    if (!condition)
        throw new InvalidOperationException(message);
}

static void AssertNear(double actual, double expected, string message)
{
    var tolerance = Math.Max(1e-9, Math.Abs(expected) * 1e-9);
    if (Math.Abs(actual - expected) > tolerance)
        throw new InvalidOperationException($"{message}: expected={expected:G17} actual={actual:G17}");
}

static void RunEncodingChecks()
{
    var compactSmall = BitcoinEncoding.CompactTargetToLe(0x0300ffff);
    AssertBytes(compactSmall, new byte[] { 0xff, 0xff, 0x00, 0x00 }, 4,
        "compact target exponent=3 little-endian placement");

    var compactTiny = BitcoinEncoding.CompactTargetToLe(0x02008000);
    AssertBytes(compactTiny, new byte[] { 0x80, 0x00, 0x00 }, 3,
        "compact target exponent=2 little-endian placement");

    var compactExponent33 = BitcoinEncoding.CompactTargetToLe(0x210000ff);
    if (compactExponent33[30] != 0xff || compactExponent33.Where((_, i) => i != 30).Any(x => x != 0))
        throw new InvalidOperationException(
            $"compact target exponent=33 boundary mismatch: {Hex.Encode(compactExponent33)}");
    if (BitcoinEncoding.CompactTargetToLe(0x21010000).Any(x => x != 0))
        throw new InvalidOperationException("overflowing compact target exponent=33 was not rejected");
    var compactExponent34 = BitcoinEncoding.CompactTargetToLe(0x220000ff);
    if (compactExponent34[31] != 0xff || compactExponent34.Take(31).Any(x => x != 0))
        throw new InvalidOperationException(
            $"compact target exponent=34 boundary mismatch: {Hex.Encode(compactExponent34)}");

    AssertBytes(BitcoinEncoding.EncodeCoinbaseHeight(0x007fffff),
        new byte[] { 0x03, 0xff, 0xff, 0x7f }, 4,
        "BIP34 height without sign byte");
    AssertBytes(BitcoinEncoding.EncodeCoinbaseHeight(1),
        new byte[] { 0x51 }, 1,
        "BIP34 small integer OP_1 encoding");
    AssertBytes(BitcoinEncoding.EncodeCoinbaseHeight(16),
        new byte[] { 0x60 }, 1,
        "BIP34 small integer OP_16 encoding");
    AssertBytes(BitcoinEncoding.EncodeCoinbaseHeight(17),
        new byte[] { 0x01, 0x11 }, 2,
        "BIP34 height 17 data-push encoding");
    AssertBytes(BitcoinEncoding.EncodeCoinbaseHeight(0x00800000),
        new byte[] { 0x04, 0x00, 0x00, 0x80, 0x00 }, 5,
        "BIP34 height with sign byte");

    var current = new JobTemplate
    {
        Ready = true,
        Source = TemplateSource.Longpoll,
        Height = 900_001,
        PrevhashBe = new string('a', 64)
    };
    if (TemplateEngine.ClassifyGbtTip(current, new string('b', 64), current.Height) != GbtTipRelation.Reorg)
        throw new InvalidOperationException("same-height different-prev template was not classified as a reorg");
    if (TemplateEngine.ClassifyGbtTip(current, current.PrevhashBe, current.Height) != GbtTipRelation.SameTip)
        throw new InvalidOperationException("same-height same-prev template was not classified as SameTip");
    if (TemplateEngine.ClassifyGbtTip(current, current.PrevhashBe, current.Height - 1) != GbtTipRelation.Behind)
        throw new InvalidOperationException("lower-height template was not classified as Behind");

    // P2P announces block B at H and publishes a speculative job mining H+1 on B.
    // A Core GBT still mining H on A is stale relative to that job, not a reorg.
    var fast = new JobTemplate
    {
        Ready = true,
        Source = TemplateSource.P2pFast,
        Height = current.Height + 1,
        PrevhashBe = new string('b', 64)
    };
    if (TemplateEngine.ClassifyGbtTip(fast, current.PrevhashBe, current.Height) != GbtTipRelation.Behind)
        throw new InvalidOperationException("pre-fast Core GBT was not classified as Behind");

    // Core confirms B: the full H+1 template replaces the empty fast template
    // without a clean-chain switch.
    if (TemplateEngine.ClassifyGbtTip(fast, fast.PrevhashBe, fast.Height) != GbtTipRelation.SameTip)
        throw new InvalidOperationException("Core confirmation of the P2P-fast tip was not classified as SameTip");

    // Core confirms competing block C at H: its H+1 GBT is authoritative reorg.
    if (TemplateEngine.ClassifyGbtTip(fast, new string('c', 64), fast.Height) != GbtTipRelation.Reorg)
        throw new InvalidOperationException("competing Core tip after P2P-fast was not classified as Reorg");

    var confirmingGbt = new JobTemplate
    {
        Ready = true,
        Source = TemplateSource.Longpoll,
        Height = fast.Height,
        PrevhashBe = fast.PrevhashBe,
        SubmitOld = false
    };
    if (TemplateEngine.ShouldCleanGbtUpdate(fast, confirmingGbt))
        throw new InvalidOperationException("confirming GBT incorrectly invalidated same-tip P2P-fast work");

    var precedingGbt = new JobTemplate
    {
        Ready = true,
        Source = TemplateSource.Longpoll,
        Height = confirmingGbt.Height,
        PrevhashBe = confirmingGbt.PrevhashBe
    };
    if (!TemplateEngine.ShouldCleanGbtUpdate(precedingGbt, confirmingGbt))
        throw new InvalidOperationException("submitold=false did not invalidate the preceding authoritative GBT");

    var competingGbt = new JobTemplate
    {
        Ready = true,
        Source = TemplateSource.Longpoll,
        Height = fast.Height,
        PrevhashBe = new string('c', 64),
        SubmitOld = true
    };
    if (!TemplateEngine.ShouldCleanGbtUpdate(fast, competingGbt))
        throw new InvalidOperationException("competing GBT did not clean P2P-fast work");

    Console.WriteLine("PASS encoding/reorg/P2P-fast boundary checks");
}

static void AssertBytes(byte[] actual, byte[] expectedPrefix, int length, string message)
{
    if (actual.Length != 32 && message.StartsWith("compact", StringComparison.Ordinal))
        throw new InvalidOperationException($"{message}: target length={actual.Length}");
    if (actual.Length < length || !actual.AsSpan(0, length).SequenceEqual(expectedPrefix))
        throw new InvalidOperationException(
            $"{message}: expected prefix={Hex.Encode(expectedPrefix)} actual={Hex.Encode(actual.AsSpan(0, Math.Min(actual.Length, length)))}");
}

static async Task RunSubmitQueueShutdownCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-shutdown-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = new AppConfig
        {
            Runtime = new RuntimeConfig { DataDir = root }
        };
        using var rpc = new BitcoinRpcClient(
            "http://127.0.0.1:1", "unused", "unused", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        await queue.StartAsync(CancellationToken.None);
        await queue.EnqueueFoundBlockAsync("00", new string('1', 64), 900_001);
        await queue.StopAsync(TimeSpan.FromMilliseconds(50));

        var pending = Directory.GetFiles(queue.PendingDir, "*.json");
        if (pending.Length != 1)
            throw new InvalidOperationException($"graceful shutdown persisted {pending.Length} pending blocks, expected 1");
        if (!queue.IsStopped)
            throw new InvalidOperationException("submit queue stop task did not complete successfully");

        Console.WriteLine("PASS submit queue graceful-shutdown persistence check");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunSafetyRegressionChecksAsync()
{
    RunDifficultyBoundaryChecks();
    RunNetworkDifficultyClampChecks();
    RunAcceptedShareTrackingIntegrationCheck();
    RunSubmitParserChecks();
    RunPooledResponseFrameChecks();
    RunGbtHexConverterChecks();
    RunTemplateFingerprintAllocationCheck();
    RunTemplateKeyIdentityCheck();
    RunMerkleRootCacheChecks();
    RunMetricsWindowChecks();
    await RunSubmitBlockContentChecksAsync();
    await RunInconclusiveRetryCheckAsync();
    await RunMissingParentRetryCheckAsync();
    RunDuplicateShareChecks();
    await RunServiceSupervisorChecksAsync();
    await RunRefreshFailureReadinessCheckAsync();
    await RunBlockOwnershipBeforeResponseCheckAsync();
    await RunDirectGbtDoesNotBlockFastPublishCheckAsync();
    RunGbtResponseOrderingChecks();
    await RunLongpollDoesNotBlockDirectGbtCheckAsync();
    await RunCleanJobDispatchOrderingCheckAsync();
    RunLatestJobQueueCoalescingCheck();
    await RunClientJobWriteProgressCheckAsync();
    await RunPooledWriterFailureChecksAsync();
    RunRetainedTransactionBudgetCheck();
    await RunCrossSourceCleanLifecycleCheckAsync();
    Console.WriteLine("PASS safety regression checks");
}

static void RunSubmitParserChecks()
{
    var json = Encoding.UTF8.GetBytes(
        "{\"id\":17,\"method\":\"mining.submit\",\"params\":[\"worker\",\"1a\",\"00000001\",\"65abcdef\",\"89abcdef\",\"20000000\"]}");
    var sequence = TestSequenceSegment.Create(
        json.AsMemory(0, 7),
        json.AsMemory(7, 19),
        json.AsMemory(26, 31),
        json.AsMemory(57));
    if (!StratumServer.TryParseSubmit(sequence, out var submit) ||
        !submit.HasRequiredParams || submit.Id.Kind != StratumRequestIdKind.Int64 ||
        submit.Id.Signed != 17 || submit.JobKey != 0x1a ||
        submit.Extranonce2 != 1 || submit.Extranonce2HexLength != 8 ||
        submit.Ntime != 0x65abcdef || submit.Nonce != 0x89abcdef ||
        !submit.HasVersion || submit.Version != 0x20000000)
        throw new InvalidOperationException("segmented UTF-8 submit parsing changed");

    var stringId = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"method\":\"mining.submit\",\"id\":\"request-7\",\"params\":[null,\"2\",\"0x01\",\"1\",\"2\"]}"));
    if (!StratumServer.TryParseSubmit(stringId, out var stringSubmit) ||
        stringSubmit.Id.Kind != StratumRequestIdKind.String || stringSubmit.Id.Text != "request-7" ||
        stringSubmit.Extranonce2 != 1 || stringSubmit.Extranonce2HexLength != 2)
        throw new InvalidOperationException("string-id submit compatibility changed");

    var nullId = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"id\":null,\"method\":\"mining.submit\",\"params\":[null,\"3\",\"\",\"1\",\"2\"]}"));
    if (!StratumServer.TryParseSubmit(nullId, out var nullSubmit) ||
        nullSubmit.Id.Kind != StratumRequestIdKind.Null || !nullSubmit.Extranonce2Valid)
        throw new InvalidOperationException("null-id/empty-extranonce submit compatibility changed");

    var invalidJobId = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"id\":18,\"method\":\"mining.submit\",\"params\":[\"worker\",\"1g\",\"00000001\",\"65abcdef\",\"89abcdef\"]}"));
    if (!StratumServer.TryParseSubmit(invalidJobId, out var invalidJobSubmit) ||
        invalidJobSubmit.HasRequiredParams)
        throw new InvalidOperationException("malformed job id was accepted as submit params");

    var objectSubmitParams = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"params\":{\"nested\":[1,{\"x\":2}]},\"id\":19,\"method\":\"mining.submit\"}"));
    if (!StratumServer.TryParseSubmit(objectSubmitParams, out var objectSubmit) ||
        objectSubmit.HasRequiredParams || objectSubmit.Id.Kind != StratumRequestIdKind.Int64 ||
        objectSubmit.Id.Signed != 19)
        throw new InvalidOperationException("object submit params changed fast-parser compatibility");

    var objectNonSubmitParams = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
        "{\"params\":{\"nested\":[1,{\"x\":2}]},\"id\":20,\"method\":\"mining.subscribe\"}"));
    if (StratumServer.TryParseSubmit(objectNonSubmitParams, out _))
        throw new InvalidOperationException("non-submit object params were classified as a submit");

    var segmentedObjectBytes = Encoding.UTF8.GetBytes(
        "{\"params\":{\"nested\":[1,{\"x\":2}]},\"id\":21,\"method\":\"mining.submit\"}");
    var segmentedObjectParams = TestSequenceSegment.Create(
        segmentedObjectBytes.AsMemory(0, 13),
        segmentedObjectBytes.AsMemory(13, 17),
        segmentedObjectBytes.AsMemory(30));
    if (!StratumServer.TryParseSubmit(segmentedObjectParams, out var segmentedObjectSubmit) ||
        segmentedObjectSubmit.HasRequiredParams || segmentedObjectSubmit.Id.Signed != 21)
        throw new InvalidOperationException("segmented object submit params changed parser compatibility");

    static void RequireMalformedSubmitRejected(string malformed, string description)
    {
        try
        {
            StratumServer.TryParseSubmit(
                new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(malformed)), out _);
            throw new InvalidOperationException($"{description} was accepted");
        }
        catch (JsonException)
        {
            // expected
        }
    }

    RequireMalformedSubmitRejected(
        "{\"method\":\"mining.submit\",\"params\":[\"worker\",\"1\",\"1\",\"1\",\"1\"]} trailing-garbage",
        "submit JSON with trailing garbage");
    RequireMalformedSubmitRejected(
        "{\"method\":\"mining.submit\",\"params\":[\"worker\",\"1\",\"1\",\"1\",\"1\"]",
        "unterminated submit root object");
    RequireMalformedSubmitRejected(
        "{\"method\":\"mining.submit\",\"params\":{\"nested\":[1,2]}",
        "unterminated submit object params");

    var contiguous = new ReadOnlySequence<byte>(json);
    for (var i = 0; i < 32; i++)
        StratumServer.TryParseSubmit(contiguous, out _);
    var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    for (var i = 0; i < 10_000; i++)
    {
        if (!StratumServer.TryParseSubmit(contiguous, out _))
            throw new InvalidOperationException("submit parser stopped recognizing the hot-path request");
    }
    var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
    if (allocated != 0)
        throw new InvalidOperationException($"numeric submit parser allocated {allocated} bytes");
}

static void RunPooledResponseFrameChecks()
{
    static string Consume(PooledFrameBuffer frame)
    {
        try
        {
            return Encoding.UTF8.GetString(frame.Buffer, 0, frame.Length);
        }
        finally
        {
            frame.Return();
        }
    }

    var baseline = OutboundFrame.OutstandingPooledBufferCount;
    var numeric = Consume(StratumServer.BuildPooledStratumErrorFrame(
        StratumRequestId.FromInt64(17), 23, "Low difficulty share"));
    var text = Consume(StratumServer.BuildPooledOkTrueFrame(
        StratumRequestId.FromString("request-7")));
    var nullId = Consume(StratumServer.BuildPooledOkTrueFrame(StratumRequestId.Null));
    if (numeric != "{\"id\":17,\"result\":false,\"error\":[23,\"Low difficulty share\",null]}\n" ||
        text != "{\"id\":\"request-7\",\"result\":true,\"error\":null}\n" ||
        nullId != "{\"id\":null,\"result\":true,\"error\":null}\n")
        throw new InvalidOperationException("pooled submit response JSON semantics changed");

    for (var i = 0; i < 32; i++)
        StratumServer.BuildPooledStratumErrorFrame(
            StratumRequestId.FromInt64(i), 23, "Low difficulty share").Return();
    var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    for (var i = 0; i < 10_000; i++)
        StratumServer.BuildPooledStratumErrorFrame(
            StratumRequestId.FromInt64(i), 23, "Low difficulty share").Return();
    var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
    if (allocated != 0)
        throw new InvalidOperationException($"numeric pooled error builder allocated {allocated} bytes");
    if (OutboundFrame.OutstandingPooledBufferCount != baseline)
        throw new InvalidOperationException("response builder did not return all pooled buffers");
}

static void RunGbtHexConverterChecks()
{
    var txid = new string('1', 64);
    var ordered = JsonSerializer.Deserialize<GbtTx>(
        $"{{\"txid\":\"{txid}\",\"hash\":\"{txid}\",\"data\":\"00aBcd\"}}")
        ?? throw new InvalidOperationException("GBT transaction did not deserialize");
    if (!ordered.Data.SequenceEqual(new byte[] { 0x00, 0xab, 0xcd }) || ordered.TxId != txid)
        throw new InvalidOperationException("GBT transaction field-order/hex decoding changed");

    const int decodedLength = 100_000;
    var token = new byte[decodedLength * 2 + 2];
    token[0] = (byte)'\"';
    token[^1] = (byte)'\"';
    for (var i = 1; i < token.Length - 1; i += 2)
    {
        token[i] = (byte)'a';
        token[i + 1] = (byte)'5';
    }
    var sequence = TestSequenceSegment.Create(
        token.AsMemory(0, 8191),
        token.AsMemory(8191, 65537),
        token.AsMemory(73728));
    var reader = new Utf8JsonReader(sequence, isFinalBlock: true, state: default);
    if (!reader.Read())
        throw new InvalidOperationException("large GBT hex fixture was empty");
    var converter = new HexByteArrayJsonConverter();
    var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    var decoded = converter.Read(ref reader, typeof(byte[]), new JsonSerializerOptions());
    var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
    if (decoded.Length != decodedLength || decoded[0] != 0xa5 || decoded[^1] != 0xa5)
        throw new InvalidOperationException("segmented large GBT transaction decoded incorrectly");
    if (allocated > decodedLength + 32_768)
        throw new InvalidOperationException(
            $"large GBT transaction decode allocated {allocated} bytes for {decodedLength} output bytes");

    var malformed = new Utf8JsonReader("\"00xz\""u8, isFinalBlock: true, state: default);
    malformed.Read();
    try
    {
        converter.Read(ref malformed, typeof(byte[]), new JsonSerializerOptions());
        throw new InvalidOperationException("malformed GBT transaction hex was accepted");
    }
    catch (JsonException)
    {
        // expected
    }
}

static void RunTemplateFingerprintAllocationCheck()
{
    var hashes = new byte[4_000 * 32];
    for (var i = 0; i < 4_000; i++)
        for (var j = 0; j < 32; j++)
            hashes[i * 32 + j] = (byte)(i + j);
    JobBuilder.TxSetFingerprint(hashes);
    var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    var fingerprint = JobBuilder.TxSetFingerprint(hashes);
    var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
    if (fingerprint.Length != 16)
        throw new InvalidOperationException("transaction-set fingerprint length changed");
    if (allocated >= 85_000)
        throw new InvalidOperationException($"transaction-set fingerprint created a LOH allocation ({allocated} bytes)");
}

static void RunTemplateKeyIdentityCheck()
{
    var gbt = new GbtResponse
    {
        Version = 0x20000000,
        PreviousBlockhash = new string('1', 64),
        CoinbaseValue = 5_000_000_000,
        Target = "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        CurTime = 1_700_000_000,
        Bits = "207fffff",
        Height = 1_000_000,
        Transactions = new[]
        {
            new GbtTx { Data = new byte[] { 1 }, TxId = new string('2', 64), Hash = new string('2', 64) },
            new GbtTx { Data = new byte[] { 2 }, TxId = new string('3', 64), Hash = new string('3', 64) }
        }
    };
    var builder = new JobBuilder(OfflineConfig(Path.GetTempPath()));
    var expected = builder.ComputeTemplateKeyParts(gbt).Key;
    var direct = builder.FromGbt(gbt, TemplateSource.Startup);
    if (!string.Equals(direct.TemplateKey, expected, StringComparison.Ordinal))
        throw new InvalidOperationException("Merkle reduction changed the transaction-set template key");

    static GbtResponse Copy(GbtResponse source) => new()
    {
        Version = source.Version,
        PreviousBlockhash = source.PreviousBlockhash,
        CoinbaseValue = source.CoinbaseValue,
        Target = source.Target,
        CurTime = source.CurTime,
        Bits = source.Bits,
        Height = source.Height,
        Transactions = source.Transactions,
        CoinbaseAux = source.CoinbaseAux == null
            ? null
            : new GbtCoinbaseAux { Flags = source.CoinbaseAux.Flags },
        DefaultWitnessCommitment = source.DefaultWitnessCommitment,
        LongPollId = source.LongPollId,
        Mintime = source.Mintime,
        Vbrequired = source.Vbrequired,
        SubmitOld = source.SubmitOld
    };

    var mutations = new (string Name, Action<GbtResponse> Apply)[]
    {
        ("version", value => value.Version++),
        ("vbrequired", value => value.Vbrequired = 1),
        ("target", value => value.Target = new string('6', 64)),
        ("curtime", value => value.CurTime++),
        ("mintime", value => value.Mintime = value.CurTime + 1),
        ("coinbaseaux.flags", value => value.CoinbaseAux = new GbtCoinbaseAux { Flags = "01aa" }),
        ("submitold", value => value.SubmitOld = false),
        ("witness commitment", value => value.DefaultWitnessCommitment = "6a24aa21a9ed" + new string('0', 64))
    };
    foreach (var mutation in mutations)
    {
        var changed = Copy(gbt);
        mutation.Apply(changed);
        var changedKey = builder.ComputeTemplateKeyParts(changed).Key;
        if (string.Equals(expected, changedKey, StringComparison.Ordinal))
            throw new InvalidOperationException($"template key ignored {mutation.Name}");
    }
}

static async Task RunSubmitBlockContentChecksAsync()
{
    var blockHex = string.Concat(Enumerable.Repeat("00abcdef", 1024));
    using (var content = SubmitBlockRpcContent.Create(42, blockHex))
    {
        var payload = await content.ReadAsByteArrayAsync();
        using var doc = JsonDocument.Parse(payload);
        var root = doc.RootElement;
        if (root.GetProperty("jsonrpc").GetString() != "1.0" ||
            root.GetProperty("id").GetString() != "42" ||
            root.GetProperty("method").GetString() != "submitblock" ||
            root.GetProperty("params")[0].GetString() != blockHex)
            throw new InvalidOperationException("submitblock UTF-8 payload changed JSON-RPC semantics");
        if (content.Headers.ContentLength != payload.Length)
            throw new InvalidOperationException("submitblock content length was not deterministic");
    }

    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    var serverTask = Task.Run(async () =>
    {
        var context = await listener.GetContextAsync();
        using var body = await JsonDocument.ParseAsync(context.Request.InputStream);
        if (body.RootElement.GetProperty("params")[0].GetString() != blockHex)
            throw new InvalidOperationException("submitblock HTTP body was truncated or changed");
        var response = Encoding.UTF8.GetBytes("{\"result\":null,\"error\":null,\"id\":\"1\"}");
        context.Response.ContentType = "application/json";
        context.Response.ContentLength64 = response.Length;
        await context.Response.OutputStream.WriteAsync(response);
        context.Response.Close();
    });

    using var rpc = new BitcoinRpcClient($"http://127.0.0.1:{port}", "user", "pass", requestTimeoutSecs: 15);
    var result = await rpc.SubmitBlockAsync(blockHex);
    await serverTask;
    listener.Stop();
    if (result != null)
        throw new InvalidOperationException("accepted submitblock response was not parsed as null");

    var malformedPort = GetFreeTcpPort();
    using var malformedListener = new HttpListener();
    malformedListener.Prefixes.Add($"http://127.0.0.1:{malformedPort}/");
    malformedListener.Start();
    var malformedServerTask = Task.Run(async () =>
    {
        var context = await malformedListener.GetContextAsync();
        var response = Encoding.UTF8.GetBytes("{\"error\":null,\"id\":\"2\"}");
        context.Response.ContentType = "application/json";
        context.Response.ContentLength64 = response.Length;
        await context.Response.OutputStream.WriteAsync(response);
        context.Response.Close();
    });

    using var malformedRpc = new BitcoinRpcClient(
        $"http://127.0.0.1:{malformedPort}", "user", "pass", requestTimeoutSecs: 15);
    var missingResultRejected = false;
    try
    {
        await malformedRpc.SubmitBlockAsync(blockHex);
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains("missing required 'result'", StringComparison.Ordinal))
    {
        missingResultRejected = true;
    }
    await malformedServerTask;
    malformedListener.Stop();
    if (!missingResultRejected)
        throw new InvalidOperationException("submitblock response without result was accepted");
}

static async Task RunMissingParentRetryCheckAsync()
{
    if (!BlockSubmitQueue.IsMissingParentResult("prev-blk-not-found") ||
        BlockSubmitQueue.IsMissingParentResult("bad-prevblk"))
        throw new InvalidOperationException("submitblock missing-parent classification is incorrect");

    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-parent-retry-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();

    var firstRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var secondRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var serverTask = Task.Run(async () =>
    {
        for (var attempt = 1; attempt <= 2; attempt++)
        {
            var context = await listener.GetContextAsync();
            using var body = await JsonDocument.ParseAsync(context.Request.InputStream);
            if (body.RootElement.GetProperty("method").GetString() != "submitblock")
                throw new InvalidOperationException("parent retry server received a non-submitblock request");

            var json = attempt == 1
                ? "{\"result\":\"prev-blk-not-found\",\"error\":null,\"id\":\"1\"}"
                : "{\"result\":null,\"error\":null,\"id\":\"2\"}";
            var response = Encoding.UTF8.GetBytes(json);
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = response.Length;
            await context.Response.OutputStream.WriteAsync(response);
            context.Response.Close();

            if (attempt == 1)
                firstRequest.TrySetResult();
            else
                secondRequest.TrySetResult();
        }
    });

    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var metrics = new MetricsStore();
        var queue = new BlockSubmitQueue(cfg, rpc, metrics);
        await queue.StartAsync(CancellationToken.None);
        await queue.EnqueueFoundBlockAsync("00", new string('2', 64), 900_002);

        await firstRequest.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var wakeLatency = Stopwatch.StartNew();
        queue.NotifyChainStateChanged();
        await secondRequest.Task.WaitAsync(TimeSpan.FromSeconds(2));
        wakeLatency.Stop();
        if (wakeLatency.ElapsedMilliseconds >= 180)
            throw new InvalidOperationException(
                $"chain-state signal did not wake submitblock promptly ({wakeLatency.ElapsedMilliseconds}ms)");
        await serverTask.WaitAsync(TimeSpan.FromSeconds(2));

        var acceptedDeadline = DateTime.UtcNow.AddSeconds(2);
        while (metrics.BlocksAccepted == 0 && DateTime.UtcNow < acceptedDeadline)
            await Task.Delay(10);
        if (metrics.BlocksAccepted != 1)
            throw new InvalidOperationException("missing-parent block was not accepted after chain-state wakeup");

        var failedDir = Path.Combine(root, "failed-blocks");
        if (Directory.Exists(failedDir) && Directory.GetFiles(failedDir, "*.json").Length != 0)
            throw new InvalidOperationException("missing-parent block was archived as a permanent rejection");

        await queue.StopAsync(TimeSpan.FromSeconds(2));
    }
    finally
    {
        listener.Stop();
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunInconclusiveRetryCheckAsync()
{
    if (!BlockSubmitQueue.IsInconclusiveResult("inconclusive") ||
        !BlockSubmitQueue.IsInconclusiveResult("DUPLICATE-INCONCLUSIVE") ||
        BlockSubmitQueue.IsInconclusiveResult("duplicate-invalid"))
        throw new InvalidOperationException("submitblock inconclusive classification is incorrect");

    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-inconclusive-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();

    var firstResponse = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var secondRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var allowAcceptance = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var serverTask = Task.Run(async () =>
    {
        for (var attempt = 1; attempt <= 2; attempt++)
        {
            var context = await listener.GetContextAsync();
            using var body = await JsonDocument.ParseAsync(context.Request.InputStream);
            if (body.RootElement.GetProperty("method").GetString() != "submitblock")
                throw new InvalidOperationException("inconclusive retry server received a non-submitblock request");

            if (attempt == 2)
            {
                secondRequest.TrySetResult();
                await allowAcceptance.Task;
            }

            var json = attempt == 1
                ? "{\"result\":\"inconclusive\",\"error\":null,\"id\":\"1\"}"
                : "{\"result\":null,\"error\":null,\"id\":\"2\"}";
            var response = Encoding.UTF8.GetBytes(json);
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = response.Length;
            await context.Response.OutputStream.WriteAsync(response);
            context.Response.Close();

            if (attempt == 1)
                firstResponse.TrySetResult();
        }
    });

    BlockSubmitQueue? queue = null;
    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var metrics = new MetricsStore();
        queue = new BlockSubmitQueue(cfg, rpc, metrics);
        await queue.StartAsync(CancellationToken.None);
        await queue.EnqueueFoundBlockAsync("00", new string('3', 64), 900_003);

        await firstResponse.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await secondRequest.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var pending = Directory.GetFiles(queue.PendingDir, "*.json");
        if (pending.Length != 1)
            throw new InvalidOperationException(
                $"inconclusive submit persisted {pending.Length} candidates, expected 1");
        var failedDir = Path.Combine(root, "failed-blocks");
        if (Directory.GetFiles(failedDir, "*.json").Length != 0)
            throw new InvalidOperationException("inconclusive submit was archived as a permanent rejection");
        if (!metrics.GetBlocks().Any(x => x.result == "inconclusive"))
            throw new InvalidOperationException("inconclusive submit status was not recorded");

        allowAcceptance.TrySetResult();
        await serverTask.WaitAsync(TimeSpan.FromSeconds(5));

        var acceptedDeadline = DateTime.UtcNow.AddSeconds(2);
        while (metrics.BlocksAccepted == 0 && DateTime.UtcNow < acceptedDeadline)
            await Task.Delay(10);
        if (metrics.BlocksAccepted != 1)
            throw new InvalidOperationException("inconclusive candidate was not accepted after retry");
        var finalBlockEvents = metrics.GetBlocks();
        if (metrics.BlocksSubmitted != 1 || finalBlockEvents.Count != 1 ||
            finalBlockEvents[0].result != "submitted")
            throw new InvalidOperationException(
                "inconclusive-to-accepted transition double-counted the candidate or kept a stale status");
        if (Directory.GetFiles(queue.PendingDir, "*.json").Length != 0)
            throw new InvalidOperationException("accepted inconclusive candidate remained pending");

        await queue.StopAsync(TimeSpan.FromSeconds(2));
    }
    finally
    {
        allowAcceptance.TrySetResult();
        if (queue != null && !queue.IsStopped)
            await queue.StopAsync(TimeSpan.FromSeconds(2));
        listener.Stop();
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static int GetFreeTcpPort()
{
    var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
    listener.Start();
    var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
}

static void RunMetricsWindowChecks()
{
    const int sampleCount = 50_017;
    var metrics = new MetricsStore();
    var identity = new WorkerIdentity
    {
        SessionId = "metrics-window",
        Name = "worker",
        UserAgent = "offline-check",
        Peer = "127.0.0.1:1",
        Extranonce1 = "00000001",
        IsNormalized = true
    };

    for (var i = 0; i < sampleCount; i++)
        metrics.RecordShare(identity, 1, 1, accepted: true, assignedDifficulty: 32);

    var expectedHps = sampleCount * 4294967296.0 / (MetricsStore.HashrateMinWindowMs / 1000.0);
    var totalHps = metrics.EstimateTotalHps();
    var workerAllocationBefore = GC.GetAllocatedBytesForCurrentThread();
    var workers = metrics.GetWorkers();
    var workerAllocation = GC.GetAllocatedBytesForCurrentThread() - workerAllocationBefore;
    if (Math.Abs(totalHps - expectedHps) / expectedHps > 1e-12)
        throw new InvalidOperationException($"metrics total hashrate changed: {totalHps} != {expectedHps}");
    if (workers.Count != 1 || Math.Abs(workers[0].hashrate_hps - expectedHps) / expectedHps > 1e-12)
        throw new InvalidOperationException("worker hashrate aggregation diverged from total hashrate");
    if (workerAllocation >= 85_000)
        throw new InvalidOperationException($"worker dashboard snapshot created a LOH allocation ({workerAllocation} bytes)");

    long boundaryNowMs = 1_000_004_999;
    var boundaryMetrics = new MetricsStore(() => boundaryNowMs);
    var boundaryIdentity = new WorkerIdentity
    {
        SessionId = "metrics-boundary",
        Name = "worker",
        UserAgent = "offline-check",
        Peer = "127.0.0.1:2",
        Extranonce1 = "00000002",
        IsNormalized = true
    };
    boundaryMetrics.RecordShare(
        boundaryIdentity, 1, 1, accepted: true, assignedDifficulty: 32);
    boundaryNowMs += MetricsStore.HashrateWindowMs;
    boundaryMetrics.RecordShare(
        boundaryIdentity, 2, 2, accepted: true, assignedDifficulty: 32);

    var expectedBoundaryHps = 3 * 4294967296.0 /
        (MetricsStore.HashrateWindowMs / 1000.0);
    var boundaryTotalHps = boundaryMetrics.EstimateTotalHps();
    var boundaryWorkers = boundaryMetrics.GetWorkers();
    if (Math.Abs(boundaryTotalHps - expectedBoundaryHps) / expectedBoundaryHps > 1e-12 ||
        boundaryWorkers.Count != 1 ||
        Math.Abs(boundaryWorkers[0].hashrate_hps - expectedBoundaryHps) /
            expectedBoundaryHps > 1e-12)
        throw new InvalidOperationException(
            "partially live hashrate boundary bucket was evicted early");

    boundaryNowMs += 1_001;
    var expectedAgedHps = 2 * 4294967296.0 /
        (MetricsStore.HashrateMinWindowMs / 1000.0);
    var agedTotalHps = boundaryMetrics.EstimateTotalHps();
    var agedWorkers = boundaryMetrics.GetWorkers();
    if (Math.Abs(agedTotalHps - expectedAgedHps) / expectedAgedHps > 1e-12 ||
        agedWorkers.Count != 1 ||
        Math.Abs(agedWorkers[0].hashrate_hps - expectedAgedHps) / expectedAgedHps > 1e-12)
        throw new InvalidOperationException("expired hashrate boundary bucket did not age out");

    metrics.RemoveSession(identity.SessionId);
    if (metrics.GetWorkers().Count != 0)
        throw new InvalidOperationException("disconnected worker remained in the dashboard snapshot");

    var oneMillisecond = Stopwatch.Frequency / 1000;
    metrics.RecordShareValidation(oneMillisecond);
    metrics.RecordShareValidation(oneMillisecond * 3);
    if (metrics.ShareValidationSamples != 2 ||
        Math.Abs(metrics.ShareValidationAverageMs - 2) > 0.01 ||
        Math.Abs(metrics.ShareValidationMaxMs - 3) > 0.01)
        throw new InvalidOperationException("share validation latency aggregation changed");
}

static void RunMerkleRootCacheChecks()
{
    var branches = Enumerable.Range(0, 5)
        .Select(i => Enumerable.Range(0, 32).Select(j => (byte)(i * 31 + j)).ToArray())
        .ToList();
    var job = new JobTemplate
    {
        Ready = true,
        JobId = "merkle-cache-a",
        Version = 0x20000000,
        Nbits = 0x1d00ffff,
        Ntime = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        PrevhashLe = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray(),
        TargetLe = new byte[32],
        Coinbase2 = Enumerable.Range(0, 32).Select(i => (byte)(255 - i)).ToArray(),
        MerkleBranchesLe = branches,
        Transactions = TransactionSet.Empty
    };
    var prefix = Enumerable.Range(0, 48).Select(i => (byte)i).ToArray();
    var en2a = new byte[] { 0, 0, 0, 7 };
    var en2b = new byte[] { 0, 0, 0, 8 };
    var target = Enumerable.Repeat((byte)0xff, 32).ToArray();
    Span<byte> rootA = stackalloc byte[32];
    Span<byte> rootB = stackalloc byte[32];
    ShareValidator.ComputeMerkleRoot(job, prefix, en2a, rootA);
    ShareValidator.ComputeMerkleRoot(job, prefix, en2b, rootB);
    if (rootA.SequenceEqual(rootB))
        throw new InvalidOperationException("merkle root did not change with extranonce2");

    var direct = ShareValidator.Validate(
        job, prefix, en2a, job.Ntime, nonce: 123, job.Version, target);
    var cached = ShareValidator.ValidateWithMerkleRoot(
        job, prefix, en2a, rootA, job.Ntime, nonce: 123, job.Version, target);
    if (direct.Accepted != cached.Accepted ||
        direct.IsBlock != cached.IsBlock ||
        direct.Hash != cached.Hash ||
        direct.BlockHex != cached.BlockHex ||
        direct.ActualDiff != cached.ActualDiff)
        throw new InvalidOperationException("cached merkle validation changed the share result");

    var cache = new MerkleRootCache();
    cache.Set(job.JobId, en2a, rootA);
    Span<byte> hit = stackalloc byte[32];
    if (!cache.TryGet(job.JobId, en2a, hit) || !hit.SequenceEqual(rootA))
        throw new InvalidOperationException("merkle cache missed the stored tuple");
    if (cache.TryGet(job.JobId, en2b, hit) || cache.TryGet("merkle-cache-b", en2a, hit))
        throw new InvalidOperationException("merkle cache ignored a key component");
    cache.Reset();
    if (cache.TryGet(job.JobId, en2a, hit))
        throw new InvalidOperationException("merkle cache did not reset");

    ShareValidator.ComputeMerkleRoot(job, prefix, en2a, hit);
    ShareValidator.ValidateWithMerkleRoot(
        job, prefix, en2a, hit, job.Ntime, 0, job.Version, job.TargetLe);
    var allocationBefore = GC.GetAllocatedBytesForCurrentThread();
    Span<byte> computed = stackalloc byte[32];
    for (uint nonce = 1; nonce <= 10_000; nonce++)
    {
        ShareValidator.ComputeMerkleRoot(job, prefix, en2a, computed);
        var rejected = ShareValidator.ValidateWithMerkleRoot(
            job, prefix, en2a, computed, job.Ntime, nonce, job.Version, job.TargetLe);
        if (rejected.Accepted || rejected.IsBlock)
            throw new InvalidOperationException("zero-target allocation check unexpectedly accepted a share");
    }
    var validationAllocated = GC.GetAllocatedBytesForCurrentThread() - allocationBefore;
    if (validationAllocated != 0)
        throw new InvalidOperationException(
            $"low-difficulty validation/Merkle miss allocated {validationAllocated} bytes");

    cache.Set(job.JobId, en2a, hit);
    allocationBefore = GC.GetAllocatedBytesForCurrentThread();
    Span<byte> cachedRoot = stackalloc byte[32];
    for (var i = 0; i < 10_000; i++)
    {
        if (!cache.TryGet(job.JobId, en2a, cachedRoot))
            throw new InvalidOperationException("allocation check lost the Merkle cache entry");
    }
    var cacheAllocated = GC.GetAllocatedBytesForCurrentThread() - allocationBefore;
    if (cacheAllocated != 0)
        throw new InvalidOperationException($"Merkle cache hit allocated {cacheAllocated} bytes");
}

static void RunAcceptedShareTrackingIntegrationCheck()
{
    var job = new JobTemplate
    {
        Ready = true,
        JobId = "accepted-share-integration",
        Version = 1,
        Nbits = 0x1d00ffff,
        Ntime = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        PrevhashLe = new byte[32],
        TargetLe = new byte[32],
        Coinbase2 = new byte[] { 1, 2, 3 },
        MerkleBranchesLe = new List<byte[]>(),
        Transactions = TransactionSet.Empty
    };
    var result = ShareValidator.Validate(
        job,
        new byte[] { 4, 5, 6 },
        new byte[] { 0, 0, 0, 1 },
        job.Ntime,
        nonce: 0,
        job.Version,
        Enumerable.Repeat((byte)0xff, 32).ToArray());

    if (!result.Accepted || result.IsBlock || result.Hash == default)
        throw new InvalidOperationException(
            $"ordinary accepted share did not expose its value-type header hash: " +
            $"accepted={result.Accepted} block={result.IsBlock} hash={result.Hash}");

    var tracker = new AcceptedShareTracker();
    if (tracker.TryRegister(1, result.Hash) != AcceptedShareRegistration.Added ||
        tracker.TryRegister(1, result.Hash) != AcceptedShareRegistration.Duplicate ||
        tracker.TryRegister(2, result.Hash) != AcceptedShareRegistration.Added)
        throw new InvalidOperationException("ordinary accepted share could not be duplicate-tracked");
}

static void RunDifficultyBoundaryChecks()
{
    var target79 = BitcoinEncoding.DiffToShareTargetLe(7.9e16);
    var target80 = BitcoinEncoding.DiffToShareTargetLe(8.0e16);
    var targetMax = BitcoinEncoding.DiffToShareTargetLe(BitcoinEncoding.MaxSupportedShareDifficulty);
    var value79 = new System.Numerics.BigInteger(target79, isUnsigned: true, isBigEndian: false);
    var value80 = new System.Numerics.BigInteger(target80, isUnsigned: true, isBigEndian: false);
    var valueMax = new System.Numerics.BigInteger(targetMax, isUnsigned: true, isBigEndian: false);
    if (value79 <= 0 || value80 <= 0 || valueMax <= 0 || value80 >= value79 || valueMax >= value80)
        throw new InvalidOperationException("high-difficulty targets are not positive and strictly decreasing");

    var cfg = OfflineConfig(Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-config-check"));
    cfg.Difficulty.Max = BitcoinEncoding.MaxSupportedShareDifficulty * 2;
    try
    {
        cfg.Validate();
        throw new InvalidOperationException("difficulty.max above the supported ceiling was accepted");
    }
    catch (InvalidOperationException ex) when (ex.Message == "invalid difficulty bounds")
    {
        // expected
    }
}

static void RunNetworkDifficultyClampChecks()
{
    const uint nbits = 0x207fffff;
    var networkTarget = BitcoinEncoding.CompactTargetToLe(nbits);
    var networkDifficulty = BitcoinEncoding.HashToDisplayDiff(networkTarget);
    var config = new DifficultyConfig
    {
        Min = 1,
        Max = BitcoinEncoding.MaxSupportedShareDifficulty,
        Default = 1024
    };
    var job = new JobTemplate
    {
        Ready = true,
        TargetLe = networkTarget,
        NetworkDifficulty = networkDifficulty
    };

    var clamped = StratumServer.ClampDifficultyForJob(config, config.Default, job);
    if (!(clamped > 0 && clamped < networkDifficulty))
        throw new InvalidOperationException("share difficulty was not clamped below network difficulty");
    var shareTarget = BitcoinEncoding.DiffToShareTargetLe(clamped);
    if (!BitcoinEncoding.LeqLe256(networkTarget, shareTarget))
        throw new InvalidOperationException("clamped share target is harder than the network target");

    var configuredTarget = BitcoinEncoding.DiffToShareTargetLe(config.Default);
    if (!StratumServer.RequiresDownwardDifficultyClampForJob(
            config, config.Default, configuredTarget, job) ||
        StratumServer.RequiresDownwardDifficultyClampForJob(
            config, clamped, shareTarget, job) ||
        StratumServer.RequiresDownwardDifficultyClampForJob(
            config, config.Default, configuredTarget, JobTemplate.Empty()))
        throw new InvalidOperationException("network difficulty clamp fast-path changed");
}

static void RunDuplicateShareChecks()
{
    var tracker = new AcceptedShareTracker();
    var prev = new byte[32];
    var merkle = new byte[32];
    var hashV1 = Hash256.FromLittleEndian(BitcoinEncoding.DoubleSha256(
        BitcoinEncoding.BuildHeader(1, prev, merkle, 1, 0x207fffff, 1)));
    var hashV2 = Hash256.FromLittleEndian(BitcoinEncoding.DoubleSha256(
        BitcoinEncoding.BuildHeader(2, prev, merkle, 1, 0x207fffff, 1)));

    if (tracker.TryRegister(10, hashV1) != AcceptedShareRegistration.Added ||
        tracker.TryRegister(10, hashV1) != AcceptedShareRegistration.Duplicate ||
        tracker.TryRegister(10, hashV2) != AcceptedShareRegistration.Added)
        throw new InvalidOperationException("duplicate tracking did not distinguish effective header versions");

    tracker.Reset();
    if (tracker.TryRegister(10, hashV1) != AcceptedShareRegistration.Added)
        throw new InvalidOperationException("duplicate tracking did not reset for a clean job");

    tracker.Remove(10, hashV1);
    if (tracker.TryRegister(10, hashV1) != AcceptedShareRegistration.Added)
        throw new InvalidOperationException("duplicate tracking did not allow retry after enqueue rollback");

    tracker.TryRegister(11, hashV2);
    tracker.PruneBefore(11);
    if (tracker.TryRegister(10, hashV1) != AcceptedShareRegistration.Added ||
        tracker.TryRegister(11, hashV2) != AcceptedShareRegistration.Duplicate)
        throw new InvalidOperationException("duplicate tracking did not prune by job epoch");

    var capacityTracker = new AcceptedShareTracker();
    for (var i = 0; i < AcceptedShareTracker.Capacity; i++)
    {
        if (capacityTracker.TryRegister(20, new Hash256((ulong)i, 1, 2, 3)) != AcceptedShareRegistration.Added)
            throw new InvalidOperationException("duplicate tracking reached capacity early");
    }

    var blockHash = new Hash256(ulong.MaxValue, ulong.MaxValue, ulong.MaxValue, ulong.MaxValue);
    if (capacityTracker.TryRegister(20, new Hash256(4, 5, 6, 7)) !=
        AcceptedShareRegistration.CapacityExceeded)
        throw new InvalidOperationException("ordinary share bypassed duplicate tracking capacity");
    if (capacityTracker.TryRegister(20, blockHash, isBlockCandidate: true) !=
            AcceptedShareRegistration.Added ||
        capacityTracker.TryRegister(20, blockHash, isBlockCandidate: true) !=
            AcceptedShareRegistration.Duplicate)
        throw new InvalidOperationException("network block candidate was blocked or not deduplicated at capacity");
}

static async Task RunServiceSupervisorChecksAsync()
{
    using var liveCts = new CancellationTokenSource();
    var socketFailure = new SocketException((int)SocketError.AddressAlreadyInUse);
    try
    {
        await ServiceTaskSupervisor.WaitForShutdownOrFailureAsync(
            new[] { Task.FromException(socketFailure) }, liveCts.Token);
        throw new InvalidOperationException("service supervisor swallowed a network failure");
    }
    catch (SocketException ex) when (ex.SocketErrorCode == SocketError.AddressAlreadyInUse)
    {
        // expected
    }

    try
    {
        await ServiceTaskSupervisor.WaitForShutdownOrFailureAsync(
            new[] { Task.CompletedTask }, liveCts.Token);
        throw new InvalidOperationException("service supervisor accepted an unexpected clean stop");
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains("stopped unexpectedly", StringComparison.Ordinal))
    {
        // expected
    }

    try
    {
        await ServiceTaskSupervisor.WaitForShutdownOrFailureAsync(
            new[] { Task.FromCanceled(new CancellationToken(canceled: true)) }, liveCts.Token);
        throw new InvalidOperationException("service supervisor swallowed an unexpected task cancellation");
    }
    catch (OperationCanceledException)
    {
        // expected: the process entry point only treats cancellation as shutdown
        // when its own shutdown token has been canceled.
    }
}

static async Task RunRefreshFailureReadinessCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-readiness-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused", requestTimeoutSecs: 15);
        var metrics = new MetricsStore { LastRefreshOk = true, LastRefreshMs = 123 };
        var queue = new BlockSubmitQueue(cfg, rpc, metrics);
        var engine = new TemplateEngine(cfg, rpc, metrics, queue);
        try
        {
            await engine.RefreshDirectAsync(TemplateSource.ZmqHashblock, CancellationToken.None);
            throw new InvalidOperationException("unreachable RPC unexpectedly refreshed a template");
        }
        catch (Exception ex) when (ex is HttpRequestException or TimeoutException)
        {
            // expected
        }

        if (metrics.LastRefreshOk)
            throw new InvalidOperationException("failed template refresh left readiness marked healthy");
        if (metrics.LastRefreshMs != 123)
            throw new InvalidOperationException("failed template refresh rewrote the last-success timestamp");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunBlockOwnershipBeforeResponseCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-ack-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        await queue.StartAsync(CancellationToken.None);

        await queue.EnqueueFoundBlockAsync("00", new string('1', 64), 900_001);
        try
        {
            throw new IOException("simulated client response queue failure");
        }
        catch (IOException)
        {
            // The response failure occurs after ownership transferred to the queue.
        }

        await queue.StopAsync(TimeSpan.FromMilliseconds(50));
        if (Directory.GetFiles(queue.PendingDir, "*.json").Length != 1)
            throw new InvalidOperationException("response failure lost the already-enqueued block");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunCleanJobDispatchOrderingCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-clean-order-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        cfg.Stratum.LateShareGraceMs = 0;
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        var publish = typeof(TemplateEngine).GetMethod(
            "PublishJob",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("TemplateEngine.PublishJob was not found");

        void Publish(JobTemplate job, bool clean) =>
            publish.Invoke(engine, new object[] { job, clean, false });

        async Task DispatchOneAsync(Action<JobNotify> inspect)
        {
            using var dispatchCts = new CancellationTokenSource();
            try
            {
                await engine.DispatchNotificationsAsync(notify =>
                {
                    inspect(notify);
                    dispatchCts.Cancel();
                }, dispatchCts.Token);
            }
            catch (OperationCanceledException) when (dispatchCts.IsCancellationRequested)
            {
                // The requested notification was dispatched and its cleanup completed.
            }
        }

        var oldJob = new JobTemplate { Ready = true, JobId = "clean-order-old" };
        Publish(oldJob, clean: false);
        await DispatchOneAsync(notify =>
        {
            if (!ReferenceEquals(notify.Job, oldJob) || notify.CleanJobs)
                throw new InvalidOperationException("initial non-clean job notification changed");
        });

        var replacement = new JobTemplate { Ready = true, JobId = "clean-order-new" };
        Publish(replacement, clean: true);
        if (engine.FindJob(oldJob.JobId).Job == null || engine.FindJob(replacement.JobId).Job == null)
            throw new InvalidOperationException("clean publish removed old work before notification dispatch");

        await DispatchOneAsync(notify =>
        {
            if (!ReferenceEquals(notify.Job, replacement) || !notify.CleanJobs)
                throw new InvalidOperationException("replacement clean notification changed");
            if (engine.FindJob(oldJob.JobId).Job == null)
                throw new InvalidOperationException("old work disappeared before clean notification was dispatched");
        });

        if (engine.FindJob(oldJob.JobId).Status != JobLookupStatus.RetiredWithinGrace)
            throw new InvalidOperationException("old work was not retained pending clean write completion");
        engine.MarkCleanBroadcastComplete(replacement.Epoch, DateTimeOffset.UtcNow);
        if (engine.FindJob(oldJob.JobId).Status != JobLookupStatus.Expired ||
            engine.FindJob(replacement.JobId).Job == null)
            throw new InvalidOperationException("old work was not reclaimed after clean write completion");

        // If a same-tip update supersedes a pending clean notification, the merged
        // notification must carry clean=true and cleanup must retain only the latest job.
        var superseding = new JobTemplate { Ready = true, JobId = "clean-order-latest" };
        var coalesced = new JobTemplate { Ready = true, JobId = "clean-order-coalesced" };
        Publish(superseding, clean: true);
        Publish(coalesced, clean: false);
        if (engine.FindJob(replacement.JobId).Job == null || engine.FindJob(superseding.JobId).Job == null)
            throw new InvalidOperationException("coalesced clean publish removed jobs before dispatch");

        await DispatchOneAsync(notify =>
        {
            if (!ReferenceEquals(notify.Job, coalesced) || !notify.CleanJobs)
                throw new InvalidOperationException("pending clean flag was lost during notification coalescing");
            if (engine.FindJob(replacement.JobId).Job == null || engine.FindJob(superseding.JobId).Job == null)
                throw new InvalidOperationException("coalesced old work disappeared before dispatch");
        });

        engine.MarkCleanBroadcastComplete(coalesced.Epoch, DateTimeOffset.UtcNow);
        if (engine.FindJob(replacement.JobId).Status != JobLookupStatus.Expired ||
            engine.FindJob(superseding.JobId).Status != JobLookupStatus.Expired ||
            engine.FindJob(coalesced.JobId).Job == null)
            throw new InvalidOperationException("coalesced clean cleanup retained the wrong jobs");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunDirectGbtDoesNotBlockFastPublishCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-gbt-lock-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    var requestReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var releaseResponse = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var serverTask = Task.Run(async () =>
    {
        var context = await listener.GetContextAsync();
        requestReceived.TrySetResult();
        await releaseResponse.Task;
        var response = Encoding.UTF8.GetBytes(
            "{\"result\":null,\"error\":{\"code\":-1,\"message\":\"offline stop\"},\"id\":\"1\"}");
        context.Response.ContentType = "application/json";
        context.Response.ContentLength64 = response.Length;
        await context.Response.OutputStream.WriteAsync(response);
        context.Response.Close();
    });

    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        var refresh = engine.RefreshDirectAsync(TemplateSource.ZmqHashblock, CancellationToken.None);
        await requestReceived.Task.WaitAsync(TimeSpan.FromSeconds(2));

        if (!engine.TryEnterPublicationForTest())
            throw new InvalidOperationException("direct GBT held the P2P publication lock during RPC I/O");
        engine.ExitPublicationForTest();

        releaseResponse.TrySetResult();
        try
        {
            await refresh.WaitAsync(TimeSpan.FromSeconds(2));
            throw new InvalidOperationException("offline GBT error response unexpectedly succeeded");
        }
        catch (InvalidOperationException)
        {
            // Expected: only semaphore ownership is under test.
        }
        await serverTask.WaitAsync(TimeSpan.FromSeconds(2));
    }
    finally
    {
        releaseResponse.TrySetResult();
        listener.Stop();
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static void RunGbtResponseOrderingChecks()
{
    var tipA = new string('a', 64);
    var tipB = new string('b', 64);
    var authoritative = new JobTemplate
    {
        Ready = true,
        Height = 100,
        PrevhashBe = tipA
    };

    GbtResponse Response(uint height, string tip, string? longpollId) => new()
    {
        Height = height,
        PreviousBlockhash = tip,
        LongPollId = longpollId
    };

    if (!TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipA, tipA + "9"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipA, tipA + "10"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipA, tipA + "11"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipB, tipB + "9"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(99, tipA, tipA + "9"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipA, "opaque-token"), authoritative, tipA + "10") ||
        TemplateEngine.IsSupersededGbtResponse(
            Response(100, tipA, tipA + "9"), JobTemplate.Empty(), tipA + "10"))
        throw new InvalidOperationException("GBT longpoll revision ordering changed");

    var parsed = TemplateEngine.ParseGbtLongpollRevision(tipA + "123");
    if (!parsed.HasValue || parsed.Value.TipHash != tipA ||
        parsed.Value.TransactionsUpdated != 123 ||
        TemplateEngine.ParseGbtLongpollRevision("opaque-token").HasValue)
        throw new InvalidOperationException("GBT longpoll revision parsing changed");
}

static async Task RunLongpollDoesNotBlockDirectGbtCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-gbt-race-" + Guid.NewGuid().ToString("N"));
    var port = GetFreeTcpPort();
    using var listener = new HttpListener();
    listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    listener.Start();
    using var testCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    var longpollReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var releaseLongpoll = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    Task? longpollResponseTask = null;
    var directGbtCount = 0;

    var now = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    var target = Hex.Encode(Hex.ReverseCopy(BitcoinEncoding.CompactTargetToLe(0x207fffff)));
    var gbtResult = new Dictionary<string, object?>
    {
        ["version"] = 0x20000000u,
        ["previousblockhash"] = new string('0', 64),
        ["coinbasevalue"] = 5_000_000_000L,
        ["target"] = target,
        ["curtime"] = now,
        ["bits"] = "207fffff",
        ["height"] = 1u,
        ["transactions"] = Array.Empty<object>(),
        ["coinbaseaux"] = new Dictionary<string, object?> { ["flags"] = "" },
        ["longpollid"] = "race-longpoll",
        ["mintime"] = now > 0 ? now - 1 : 0,
        ["vbrequired"] = 0u,
        ["submitold"] = true
    };

    static async Task RespondAsync(HttpListenerContext context, object? result)
    {
        var response = JsonSerializer.SerializeToUtf8Bytes(new { result, error = (object?)null, id = "1" });
        context.Response.ContentType = "application/json";
        context.Response.ContentLength64 = response.Length;
        await context.Response.OutputStream.WriteAsync(response);
        context.Response.Close();
    }

    var serverTask = Task.Run(async () =>
    {
        try
        {
            while (!testCts.IsCancellationRequested)
            {
                var context = await listener.GetContextAsync();
                string body;
                using (var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8))
                    body = await reader.ReadToEndAsync();
                using var request = JsonDocument.Parse(body);
                var method = request.RootElement.GetProperty("method").GetString();
                if (method == "getnetworkhashps")
                {
                    await RespondAsync(context, 0d);
                    continue;
                }

                var parameters = request.RootElement.GetProperty("params");
                var isLongpoll = parameters.GetArrayLength() > 0 &&
                    parameters[0].ValueKind == JsonValueKind.Object &&
                    parameters[0].TryGetProperty("longpollid", out _);
                if (isLongpoll)
                {
                    longpollReceived.TrySetResult();
                    longpollResponseTask = Task.Run(async () =>
                    {
                        await releaseLongpoll.Task;
                        try { await RespondAsync(context, gbtResult); }
                        catch { context.Response.Abort(); }
                    });
                    continue;
                }

                Interlocked.Increment(ref directGbtCount);
                await RespondAsync(context, gbtResult);
            }
        }
        catch (Exception ex) when (ex is HttpListenerException or ObjectDisposedException)
        {
            // listener stopped by test cleanup
        }
    });

    try
    {
        var cfg = OfflineConfig(root);
        cfg.Bitcoind.RpcUrl = $"http://127.0.0.1:{port}";
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "user", "pass", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        using var engineCts = new CancellationTokenSource();
        await engine.StartAsync(engineCts.Token);
        await longpollReceived.Task.WaitAsync(TimeSpan.FromSeconds(2));

        await engine.RefreshDirectAsync(TemplateSource.ZmqHashblock, engineCts.Token)
            .WaitAsync(TimeSpan.FromSeconds(2));
        if (Volatile.Read(ref directGbtCount) < 2)
            throw new InvalidOperationException("direct GBT did not run while longpoll was outstanding");

        engineCts.Cancel();
        releaseLongpoll.TrySetResult();
        if (longpollResponseTask != null)
            await longpollResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
    }
    finally
    {
        releaseLongpoll.TrySetResult();
        testCts.Cancel();
        listener.Stop();
        try { await serverTask.WaitAsync(TimeSpan.FromSeconds(2)); } catch { }
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static void RunLatestJobQueueCoalescingCheck()
{
    long nextSequence = 40;
    var dirtyA = new byte[] { 1 };
    var cleanA = new byte[] { 2 };
    var difficultyA = new byte[] { 7 };
    var first = JobOutboundFrame.ReplacePending(
        null,
        () => ++nextSequence,
        epoch: 100,
        cleanJobs: true,
        versionFrame: null,
        notifyFrame: dirtyA,
        cleanNotifyFrame: cleanA,
        difficultyFrame: difficultyA);
    if (!first.CleanJobs || !ReferenceEquals(first.Frame, cleanA) ||
        !ReferenceEquals(first.DifficultyFrame, difficultyA) || first.Sequence != 41)
        throw new InvalidOperationException("first clean job frame was not queued correctly");

    var dirtyB = new byte[] { 3 };
    var cleanB = new byte[] { 4 };
    var replacement = JobOutboundFrame.ReplacePending(
        first,
        () => ++nextSequence,
        epoch: 101,
        cleanJobs: false,
        versionFrame: null,
        notifyFrame: dirtyB,
        cleanNotifyFrame: cleanB);
    if (replacement.Epoch != 101 || !replacement.CleanJobs ||
        replacement.Sequence != first.Sequence ||
        !ReferenceEquals(replacement.Frame, cleanB) ||
        !ReferenceEquals(replacement.DifficultyFrame, difficultyA) || nextSequence != 41)
        throw new InvalidOperationException(
            "latest-wins replacement lost clean state or its ordered queue position");

    var dirtyC = new byte[] { 5 };
    var cleanC = new byte[] { 6 };
    var independent = JobOutboundFrame.ReplacePending(
        null,
        () => ++nextSequence,
        epoch: 102,
        cleanJobs: false,
        versionFrame: null,
        notifyFrame: dirtyC,
        cleanNotifyFrame: cleanC);
    if (independent.CleanJobs || !ReferenceEquals(independent.Frame, dirtyC) || independent.Sequence != 42)
        throw new InvalidOperationException("independent dirty job frame changed semantics");
}

static void RunRetainedTransactionBudgetCheck()
{
    var root = Path.Combine(
        Path.GetTempPath(), "miningcore-btc-solo-retained-budget-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        cfg.Runtime.MaxRetainedTransactionBytes = 100;
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused");
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var publish = typeof(TemplateEngine).GetMethod(
            "PublishJob",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("TemplateEngine.PublishJob was not found");

        var dirtyEngine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        var dirtyFirst = new JobTemplate
        {
            Ready = true,
            JobId = "63",
            JobKey = 0x63,
            Transactions = TransactionSet.CopyFrom(new[] { new byte[60] })
        };
        var dirtySecond = new JobTemplate
        {
            Ready = true,
            JobId = "64",
            JobKey = 0x64,
            Transactions = TransactionSet.CopyFrom(new[] { new byte[60] })
        };
        publish.Invoke(dirtyEngine, new object[] { dirtyFirst, false, true });
        publish.Invoke(dirtyEngine, new object[] { dirtySecond, false, true });
        if (dirtyEngine.FindJob(dirtyFirst.JobKey).Status != JobLookupStatus.Expired ||
            dirtyEngine.FindJob(dirtySecond.JobKey).Status != JobLookupStatus.Available)
            throw new InvalidOperationException(
                "retained transaction budget did not evict the oldest non-retired job");

        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);

        var first = new JobTemplate
        {
            Ready = true,
            JobId = "65",
            JobKey = 0x65,
            Transactions = TransactionSet.CopyFrom(new[] { new byte[80] })
        };
        var second = new JobTemplate
        {
            Ready = true,
            JobId = "66",
            JobKey = 0x66,
            Transactions = TransactionSet.CopyFrom(new[] { new byte[80] })
        };
        publish.Invoke(engine, new object[] { first, false, true });
        publish.Invoke(engine, new object[] { second, true, true });
        if (engine.FindJob(first.JobKey).Status != JobLookupStatus.Expired ||
            engine.FindJob(second.JobKey).Status != JobLookupStatus.Available)
            throw new InvalidOperationException("retained transaction budget did not evict the oldest retired job");

        var oversizedActive = new JobTemplate
        {
            Ready = true,
            JobId = "67",
            JobKey = 0x67,
            Transactions = TransactionSet.CopyFrom(new[] { new byte[200] })
        };
        publish.Invoke(engine, new object[] { oversizedActive, true, true });
        if (engine.FindJob(second.JobKey).Status != JobLookupStatus.Expired ||
            engine.FindJob(oversizedActive.JobKey).Status != JobLookupStatus.Available)
            throw new InvalidOperationException("transaction budget reclaimed the active job or lost its tombstone");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunClientJobWriteProgressCheckAsync()
{
    var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
    listener.Start();
    var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
    using var miner = new TcpClient();
    var connect = miner.ConnectAsync(System.Net.IPAddress.Loopback, port);
    using var serverTcp = await listener.AcceptTcpClientAsync();
    await connect;
    listener.Stop();

    var cfg = OfflineConfig(Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-writer-check"));
    cfg.Stratum.WriteTimeoutSecs = 2;
    cfg.Stratum.SendQueueCapacity = 4;
    var metrics = new MetricsStore();
    using var session = new ClientSession(serverTcp, cfg, metrics);
    var cleanNotify = System.Text.Encoding.UTF8.GetBytes("clean-latest\n");
    var dirtyNotify = System.Text.Encoding.UTF8.GetBytes("dirty-latest\n");
    var difficultyNotify = System.Text.Encoding.UTF8.GetBytes("difficulty\n");

    if (!session.TryQueueJob(
            epoch: 500,
            cleanJobs: true,
            versionFrame: null,
            notifyFrame: dirtyNotify,
            cleanNotifyFrame: cleanNotify,
            difficultyFrame: difficultyNotify))
        throw new InvalidOperationException("client writer rejected the clean job test frame");

    using var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
    var expected = "difficulty\nclean-latest\n";
    var buffer = new byte[512];
    var read = 0;
    while (read < expected.Length)
    {
        var count = await miner.GetStream().ReadAsync(buffer.AsMemory(read), readCts.Token);
        if (count == 0)
            throw new EndOfStreamException("client writer closed before the complete job frame");
        read += count;
    }
    var text = System.Text.Encoding.UTF8.GetString(buffer, 0, read);
    if (text != expected)
        throw new InvalidOperationException($"client writer emitted the wrong job frame: {text}");

    var deadline = DateTimeOffset.UtcNow.AddSeconds(2);
    while (session.LastWrittenJobEpoch < 500 && DateTimeOffset.UtcNow < deadline)
        await Task.Delay(5);
    if (session.LastWrittenJobEpoch != 500)
        throw new InvalidOperationException("job epoch advanced before or failed to advance after socket write");

    var acceptedStarted = Stopwatch.GetTimestamp();
    var pooledBaseline = OutboundFrame.OutstandingPooledBufferCount;
    const string acceptedText = "{\"id\":7,\"result\":true,\"error\":null}\n";
    if (!session.TryQueueAcceptedShareResponse(
            StratumServer.BuildPooledOkTrueFrame(StratumRequestId.FromInt64(7)), acceptedStarted))
        throw new InvalidOperationException("client writer rejected the accepted response test frame");
    if (metrics.AcceptedShareAckQueued != 1 || metrics.AcceptedShareAckWritten != 0)
        throw new InvalidOperationException("accepted response metrics advanced at the wrong queue stage");
    read = await miner.GetStream().ReadAsync(buffer, readCts.Token);
    text = Encoding.UTF8.GetString(buffer, 0, read);
    if (text != acceptedText)
        throw new InvalidOperationException($"client writer emitted the wrong accepted frame: {text}");
    deadline = DateTimeOffset.UtcNow.AddSeconds(2);
    while (metrics.AcceptedShareAckWritten < 1 && DateTimeOffset.UtcNow < deadline)
        await Task.Delay(5);
    if (metrics.AcceptedShareAckWritten != 1 || metrics.AcceptedShareAckWriteMaxMs < 0)
        throw new InvalidOperationException("accepted response socket-write metric did not advance");
    if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline)
        throw new InvalidOperationException("normally written accepted response was not returned to the pool");

    var outboundLock = typeof(ClientSession).GetField(
        "_outboundLock", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
        ?.GetValue(session) ?? throw new InvalidOperationException("ClientSession outbound lock was not found");
    lock (outboundLock)
    {
        for (var i = 0; i < cfg.Stratum.SendQueueCapacity; i++)
        {
            if (!session.TryQueuePooledWrite(StratumServer.BuildPooledStratumErrorFrame(
                    StratumRequestId.FromInt64(9), 23, "Low difficulty share")))
                throw new InvalidOperationException("pooled send queue filled before its configured capacity");
        }
        if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline + cfg.Stratum.SendQueueCapacity)
            throw new InvalidOperationException("queued pooled response ownership count changed");
        if (session.TryQueuePooledWrite(StratumServer.BuildPooledStratumErrorFrame(
                StratumRequestId.FromInt64(10), 23, "Low difficulty share")))
            throw new InvalidOperationException("full send queue accepted another pooled response");
        if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline + cfg.Stratum.SendQueueCapacity)
            throw new InvalidOperationException("queue-full path did not immediately return its pooled response");
    }

    var oneError = "{\"id\":9,\"result\":false,\"error\":[23,\"Low difficulty share\",null]}\n";
    var expectedErrors = string.Concat(Enumerable.Repeat(oneError, cfg.Stratum.SendQueueCapacity));
    read = 0;
    while (read < expectedErrors.Length)
    {
        var count = await miner.GetStream().ReadAsync(buffer.AsMemory(read), readCts.Token);
        if (count == 0)
            throw new EndOfStreamException("writer closed before draining pooled error frames");
        read += count;
    }
    text = Encoding.UTF8.GetString(buffer, 0, read);
    if (text != expectedErrors)
        throw new InvalidOperationException("pooled queue drain changed response ordering or bytes");
    deadline = DateTimeOffset.UtcNow.AddSeconds(2);
    while (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline && DateTimeOffset.UtcNow < deadline)
        await Task.Delay(5);
    if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline)
        throw new InvalidOperationException("drained queue retained pooled response buffers");

    await session.StopWriterAsync();
    var closedFrame = StratumServer.BuildPooledStratumErrorFrame(
        StratumRequestId.FromInt64(8), 23, "Low difficulty share");
    if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline + 1)
        throw new InvalidOperationException("closed-queue test frame did not acquire pool ownership");
    if (session.TryQueuePooledWrite(closedFrame))
        throw new InvalidOperationException("closed writer accepted a pooled response");
    if (OutboundFrame.OutstandingPooledBufferCount != pooledBaseline)
        throw new InvalidOperationException("closed writer did not return the rejected pooled response");
}

static async Task RunPooledWriterFailureChecksAsync()
{
    static async Task<(TcpClient Miner, TcpClient Server)> ConnectPairAsync()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var endpoint = (IPEndPoint)listener.LocalEndpoint;
        var miner = new TcpClient();
        var connect = miner.ConnectAsync(endpoint.Address, endpoint.Port);
        var server = await listener.AcceptTcpClientAsync();
        await connect;
        return (miner, server);
    }

    var baseline = OutboundFrame.OutstandingPooledBufferCount;
    var cancelPair = await ConnectPairAsync();
    using (cancelPair.Miner)
    using (cancelPair.Server)
    {
        var cfg = OfflineConfig(Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-writer-cancel"));
        cfg.Stratum.SendQueueCapacity = 4;
        cfg.Stratum.WriteTimeoutSecs = 1;
        using var session = new ClientSession(cancelPair.Server, cfg);
        var gate = typeof(ClientSession).GetField(
            "_outboundLock", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?.GetValue(session) ?? throw new InvalidOperationException("ClientSession outbound lock was not found");
        var writerCts = (CancellationTokenSource)(typeof(ClientSession).GetField(
            "_writerCts", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?.GetValue(session) ?? throw new InvalidOperationException("ClientSession writer CTS was not found"));
        lock (gate)
        {
            for (var i = 0; i < cfg.Stratum.SendQueueCapacity; i++)
            {
                if (!session.TryQueuePooledWrite(StratumServer.BuildPooledOkTrueFrame(
                        StratumRequestId.FromInt64(i))))
                    throw new InvalidOperationException("cancellation test could not fill the pooled queue");
            }
            writerCts.Cancel();
        }
        await session.StopWriterAsync();
        if (OutboundFrame.OutstandingPooledBufferCount != baseline)
            throw new InvalidOperationException("writer cancellation did not drain pooled response buffers");
    }

    var resetPair = await ConnectPairAsync();
    using (resetPair.Server)
    {
        var cfg = OfflineConfig(Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-writer-reset"));
        cfg.Stratum.WriteTimeoutSecs = 1;
        using var session = new ClientSession(resetPair.Server, cfg);
        resetPair.Miner.Client.LingerState = new LingerOption(enable: true, seconds: 0);
        resetPair.Miner.Close();
        await Task.Delay(25);
        for (var i = 0; i < 4; i++)
        {
            if (!session.TryQueuePooledWrite(StratumServer.BuildPooledOkTrueFrame(
                    StratumRequestId.FromInt64(i))))
                break;
        }
        var deadline = DateTimeOffset.UtcNow.AddSeconds(2);
        while ((!session.WriterUnavailable || OutboundFrame.OutstandingPooledBufferCount != baseline) &&
               DateTimeOffset.UtcNow < deadline)
            await Task.Delay(5);
        await session.StopWriterAsync();
        if (!session.WriterUnavailable || OutboundFrame.OutstandingPooledBufferCount != baseline)
            throw new InvalidOperationException("socket failure did not close the writer and return pooled buffers");
    }
}

static async Task RunCrossSourceCleanLifecycleCheckAsync()
{
    var root = Path.Combine(Path.GetTempPath(), "miningcore-btc-solo-source-lifecycle-" + Guid.NewGuid().ToString("N"));
    try
    {
        var cfg = OfflineConfig(root);
        cfg.Stratum.LateShareGraceMs = 0;
        using var rpc = new BitcoinRpcClient(cfg.Bitcoind.RpcUrl, "unused", "unused", requestTimeoutSecs: 15);
        var queue = new BlockSubmitQueue(cfg, rpc, new MetricsStore());
        var engine = new TemplateEngine(cfg, rpc, new MetricsStore(), queue);
        var publish = typeof(TemplateEngine).GetMethod(
            "PublishJob",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("TemplateEngine.PublishJob was not found");
        void Publish(JobTemplate job, bool clean, bool authoritative) =>
            publish.Invoke(engine, new object[] { job, clean, authoritative });

        async Task<JobNotify> DispatchOneAsync()
        {
            JobNotify captured = default;
            using var cts = new CancellationTokenSource();
            try
            {
                await engine.DispatchNotificationsAsync(notify =>
                {
                    captured = notify;
                    cts.Cancel();
                }, cts.Token);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
            return captured;
        }

        var longpoll = new JobTemplate
        {
            Ready = true,
            JobId = "source-longpoll",
            Source = TemplateSource.Longpoll,
            Height = 900_001,
            PrevhashBe = new string('a', 64)
        };
        Publish(longpoll, clean: false, authoritative: true);
        await DispatchOneAsync();

        var p2pFast = new JobTemplate
        {
            Ready = true,
            JobId = "source-p2p-fast",
            Source = TemplateSource.P2pFast,
            Height = 900_002,
            PrevhashBe = new string('b', 64)
        };
        Publish(p2pFast, clean: true, authoritative: false);

        var zmqFull = new JobTemplate
        {
            Ready = true,
            JobId = "source-zmq-full",
            Source = TemplateSource.ZmqHashblock,
            Height = p2pFast.Height,
            PrevhashBe = p2pFast.PrevhashBe,
            SubmitOld = false
        };
        if (TemplateEngine.ShouldCleanGbtUpdate(p2pFast, zmqFull))
            throw new InvalidOperationException("same-tip P2P-fast confirmation changed clean policy");
        Publish(zmqFull, clean: false, authoritative: true);

        var merged = await DispatchOneAsync();
        if (!ReferenceEquals(merged.Job, zmqFull) || !merged.CleanJobs || merged.Epoch != zmqFull.Epoch)
            throw new InvalidOperationException("P2P-fast clean was lost when ZMQ full GBT superseded it");
        if (engine.FindJob(longpoll.JobId).Status != JobLookupStatus.RetiredWithinGrace ||
            engine.FindJob(p2pFast.JobId).Status != JobLookupStatus.RetiredWithinGrace)
            throw new InvalidOperationException("cross-source old jobs were not retained for late shares");

        var nextLongpoll = new JobTemplate
        {
            Ready = true,
            JobId = "source-next-longpoll",
            Source = TemplateSource.Longpoll,
            Height = 900_003,
            PrevhashBe = new string('c', 64)
        };
        Publish(nextLongpoll, clean: true, authoritative: true);
        var nextNotify = await DispatchOneAsync();
        if (!nextNotify.CleanJobs || nextNotify.Epoch <= merged.Epoch)
            throw new InvalidOperationException("consecutive clean source epochs were not monotonic");

        // A newer actually-written epoch satisfies all earlier clean barriers.
        engine.MarkCleanBroadcastComplete(nextNotify.Epoch, DateTimeOffset.UtcNow);
        foreach (var expiredId in new[] { longpoll.JobId, p2pFast.JobId, zmqFull.JobId })
        {
            if (engine.FindJob(expiredId).Status != JobLookupStatus.Expired)
                throw new InvalidOperationException($"cross-source retired job was not reclaimed: {expiredId}");
        }
        if (engine.FindJob(nextLongpoll.JobId).Status != JobLookupStatus.Available)
            throw new InvalidOperationException("latest longpoll job was reclaimed by an older clean barrier");
    }
    finally
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }
}

static async Task RunSyntheticGbtStressChecksAsync()
{
    Console.WriteLine("=== synthetic GBT stress validation ===");
    await RunSyntheticGbtStressCaseAsync(10_000);
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    await RunSyntheticGbtStressCaseAsync(20_000);
    Console.WriteLine("PASS synthetic GBT 10k/20k stress checks");
}

static async Task RunSyntheticGbtStressCaseAsync(int transactionCount)
{
    const int rawTransactionBytes = 90;
    var transactions = new GbtTx[transactionCount];
    for (var i = 0; i < transactionCount; i++)
    {
        var seed = BitConverter.GetBytes(i);
        var hash = System.Security.Cryptography.SHA256.HashData(seed);
        var data = new byte[rawTransactionBytes];
        for (var offset = 0; offset < data.Length; offset++)
            data[offset] = hash[offset % hash.Length];
        BitConverter.TryWriteBytes(data.AsSpan(0, sizeof(int)), i);
        var txid = Hex.Encode(hash);
        transactions[i] = new GbtTx
        {
            Data = data,
            TxId = txid,
            Hash = txid
        };
    }

    GbtResponse? source = new()
    {
        Version = 0x20000000,
        PreviousBlockhash = new string('1', 64),
        CoinbaseValue = 5_000_000_000,
        Target = "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        CurTime = 1_700_000_000,
        Bits = "207fffff",
        Height = 1_000_000,
        Transactions = transactions,
        CoinbaseAux = new GbtCoinbaseAux { Flags = "062f503253482f" },
        DefaultWitnessCommitment = "6a24aa21a9ed" + new string('0', 64),
        Mintime = 1_700_000_000,
        SubmitOld = true
    };
    var jsonOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };
    var json = JsonSerializer.SerializeToUtf8Bytes(source, jsonOptions);
    source = null;
    transactions = null!;

    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
    var parseTimer = Stopwatch.StartNew();
    using var jsonStream = new MemoryStream(json, writable: false);
    var parsed = await JsonSerializer.DeserializeAsync<GbtResponse>(jsonStream, jsonOptions)
        ?? throw new InvalidOperationException("synthetic GBT deserialized to null");
    parseTimer.Stop();

    var cfg = OfflineConfig(Path.GetTempPath());
    var builder = new JobBuilder(cfg);
    var keyTimer = Stopwatch.StartNew();
    var keyParts = builder.ComputeTemplateKeyParts(parsed);
    keyTimer.Stop();
    var buildTimer = Stopwatch.StartNew();
    var job = builder.FromGbt(
        parsed, TemplateSource.Startup, keyParts.Key, keyParts.TxHashesLe);
    buildTimer.Stop();
    var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;

    var expectedTransactionBytes = (long)transactionCount * rawTransactionBytes;
    var expectedMerkleBranches = (int)Math.Ceiling(Math.Log2(transactionCount + 1));
    if (!job.Ready || job.TransactionCount != transactionCount ||
        job.TransactionBytes != expectedTransactionBytes ||
        keyParts.TxHashesLe.Count != transactionCount ||
        job.MerkleBranchesLe.Count != expectedMerkleBranches)
    {
        throw new InvalidOperationException(
            $"synthetic GBT {transactionCount:N0} invariant failed: " +
            $"job_txs={job.TransactionCount} tx_bytes={job.TransactionBytes} " +
            $"leaves={keyParts.TxHashesLe.Count} branches={job.MerkleBranchesLe.Count}");
    }
    var middle = transactionCount / 2;
    if (!job.Transactions.GetTransaction(middle).SequenceEqual(parsed.Transactions[middle].Data))
        throw new InvalidOperationException("synthetic GBT job changed transaction payload");

    var coinbasePrefix = new byte[job.Coinbase1.Length + 4];
    job.Coinbase1.CopyTo(coinbasePrefix, 0);
    var merkleRoot = new byte[32];
    ShareValidator.ComputeMerkleRoot(
        job, coinbasePrefix, new byte[4], merkleRoot);
    if (merkleRoot.All(value => value == 0))
        throw new InvalidOperationException("synthetic GBT produced an empty Merkle root");
    if (allocated > 512L * 1024 * 1024)
        throw new InvalidOperationException(
            $"synthetic GBT {transactionCount:N0} allocated {allocated / 1024d / 1024d:F1} MiB");

    Console.WriteLine(
        $"synthetic GBT txs={transactionCount:N0} json={json.Length / 1024d / 1024d:F2} MiB " +
        $"raw={job.TransactionBytes / 1024d / 1024d:F2} MiB branches={job.MerkleBranchesLe.Count} " +
        $"parse={parseTimer.Elapsed.TotalMilliseconds:F1} ms " +
        $"fingerprint={keyTimer.Elapsed.TotalMilliseconds:F1} ms " +
        $"build={buildTimer.Elapsed.TotalMilliseconds:F1} ms " +
        $"managed_alloc={allocated / 1024d / 1024d:F1} MiB");
}

static AppConfig OfflineConfig(string dataDir)
{
    var payout = new Key().PubKey.GetAddress(ScriptPubKeyType.Segwit, Network.RegTest).ToString();
    var cfg = new AppConfig
    {
        NetworkName = "regtest",
        Bitcoind = new BitcoindConfig
        {
            RpcUrl = "http://127.0.0.1:1",
            RpcUser = "unused",
            RpcPassword = "unused"
        },
        Coinbase = new CoinbaseConfig { Address = payout },
        Runtime = new RuntimeConfig { DataDir = dataDir },
        Difficulty = new DifficultyConfig { Min = 1, Max = 1e12, Default = 1 }
    };
    cfg.Validate();
    return cfg;
}

static async Task RunDirectPathAsync(
    BitcoinRpcClient rpc,
    string payout,
    int requireTxCount,
    string label,
    IReadOnlyList<string>? expectedTxids = null,
    bool requireAllExpectedTxids = false,
    TimeSpan? gbtTimeout = null)
{
    var cfg = BuildConfig(payout, "http://127.0.0.1:18443", "regtest", "regtestpass");
    var builder = new JobBuilder(cfg);

    var tipBefore = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var heightBefore = tipBefore.GetProperty("blocks").GetInt32();
    var bestBefore = tipBefore.GetProperty("bestblockhash").GetString()!;

    var gbt = await WaitForGbtAsync(
        rpc, requireTxCount, gbtTimeout ?? TimeSpan.FromSeconds(15));

    if (string.IsNullOrEmpty(gbt.DefaultWitnessCommitment) && gbt.Transactions.Length > 0)
        throw new InvalidOperationException("GBT missing default_witness_commitment with txs present");
    if (requireTxCount > 0 && string.IsNullOrEmpty(gbt.DefaultWitnessCommitment))
        throw new InvalidOperationException("multi-tx GBT must include default_witness_commitment (segwit)");

    // Confirm at least one GBT entry is a true witness tx when we seeded bech32 spends
    if (requireTxCount > 0)
    {
        var witnessTxs = gbt.Transactions.Count(t =>
            t.TxId != null && t.Hash != null &&
            !t.TxId.Equals(t.Hash, StringComparison.OrdinalIgnoreCase));
        Console.WriteLine($"GBT witness txs (txid!=wtxid): {witnessTxs}/{gbt.Transactions.Length}");
        if (witnessTxs == 0)
            throw new InvalidOperationException("expected at least one segwit mempool tx (txid != wtxid) in GBT");
    }

    var job = builder.FromGbt(gbt, TemplateSource.Startup);
    if (!job.Ready)
        throw new InvalidOperationException("job not ready");
    if (job.TransactionCount < requireTxCount)
        throw new InvalidOperationException($"job txs={job.TransactionCount} < required {requireTxCount}");

    Console.WriteLine(
        $"[{label}] job height={job.Height} nbits={job.NbitsHex} txs={job.TransactionCount} " +
        $"merkleBranches={job.MerkleBranchesLe.Count} witness={job.HasWitnessCommitment}");

    // Fixed extranonce for deterministic assembly
    var en1 = Hex.Decode("01020304");
    var en2 = "05060708";
    var prefix = new byte[job.Coinbase1.Length + en1.Length];
    Buffer.BlockCopy(job.Coinbase1, 0, prefix, 0, job.Coinbase1.Length);
    Buffer.BlockCopy(en1, 0, prefix, job.Coinbase1.Length, en1.Length);

    // Share target: accept anything (diff ~0) so we only care about block target for isBlock
    var shareTarget = Enumerable.Repeat((byte)0xff, 32).ToArray();

    var mined = MineBlock(job, prefix, en2, shareTarget, maxNonces: 50_000_000);
    if (mined == null)
        throw new InvalidOperationException("failed to mine block within nonce budget (regtest target unexpectedly hard?)");
    var minedValue = mined.Value;
    var minedHashHex = minedValue.Hash.ToHex();

    Console.WriteLine($"[{label}] mined hash={minedHashHex} actualDiff={minedValue.ActualDiff:F6} isBlock={minedValue.IsBlock}");
    if (!minedValue.IsBlock || minedValue.BlockHex == null)
        throw new InvalidOperationException("mined share is not a full block");

    // Sanity: assembled block hex must embed every GBT tx payload
    var transactionSearchOffset = 0;
    for (var txIndex = 0; txIndex < job.Transactions.Count; txIndex++)
    {
        var tx = job.Transactions.GetTransaction(txIndex);
        var txHex = Hex.Encode(tx);
        var transactionOffset = minedValue.BlockHex.IndexOf(
            txHex, transactionSearchOffset, StringComparison.OrdinalIgnoreCase);
        if (transactionOffset < 0)
            throw new InvalidOperationException(
                "assembled block hex missing or reordering a GBT transaction payload");
        transactionSearchOffset = transactionOffset + txHex.Length;
    }

    var submitResult = await rpc.SubmitBlockAsync(minedValue.BlockHex);
    if (submitResult != null && submitResult != "duplicate")
        throw new InvalidOperationException($"submitblock rejected: {submitResult}");

    Console.WriteLine($"[{label}] submitblock ok result={submitResult ?? "null(accepted)"}");

    // Confirm on active chain
    await Task.Delay(200);
    var tipAfter = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var heightAfter = tipAfter.GetProperty("blocks").GetInt32();
    var bestAfter = tipAfter.GetProperty("bestblockhash").GetString()!;

    if (heightAfter != heightBefore + 1)
        throw new InvalidOperationException($"height did not advance: {heightBefore} → {heightAfter}");
    if (!bestAfter.Equals(minedHashHex, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException($"bestblockhash mismatch: chain={bestAfter} mined={minedHashHex}");

    var block = await rpc.CallAsync<JsonElement>("getblock", new object[] { minedHashHex, 1 });
    var conf = block.GetProperty("confirmations").GetInt32();
    var blockWeight = block.GetProperty("weight").GetInt32();
    if (conf < 1)
        throw new InvalidOperationException($"block confirmations={conf}");
    if (blockWeight > 4_000_000)
        throw new InvalidOperationException($"block weight exceeds consensus limit: {blockWeight}");

    var blockTxids = block.GetProperty("tx").EnumerateArray().Select(x => x.GetString()!).ToList();
    if (blockTxids.Count != job.TransactionCount + 1)
        throw new InvalidOperationException(
            $"block tx count mismatch: chain={blockTxids.Count} expected={job.TransactionCount + 1}");

    AssertTxidsInBlock(
        blockTxids, expectedTxids, gbt, requireAllExpectedTxids);

    var coinbaseTxid = blockTxids[0];
    Console.WriteLine(
        $"[{label}] on-chain height={heightAfter} conf={conf} txs={blockTxids.Count} weight={blockWeight} " +
        $"coinbase={coinbaseTxid} prev={bestBefore}");
}

static async Task RunStratumPathAsync(
    BitcoinRpcClient rpc,
    string host,
    int port,
    int apiPort,
    IReadOnlyList<string>? expectedTxids = null)
{
    var tipBefore = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var heightBefore = tipBefore.GetProperty("blocks").GetInt32();

    // Confirm GBT still has mempool before mining (gateway should have built the same tip)
    var gbt = await WaitForGbtAsync(rpc, requireTxCount: expectedTxids is { Count: > 0 } ? 1 : 0, TimeSpan.FromSeconds(15));
    Console.WriteLine($"stratum pre-mine GBT txs={gbt.Transactions.Length} height={gbt.Height}");

    using var client = new StratumMinerClient();
    await client.ConnectAsync(host, port);
    var negotiatedMask = await client.ConfigureVersionRollingAsync("1fffe000");
    Console.WriteLine($"stratum version-rolling mask={negotiatedMask}");
    var (en1, _) = await client.SubscribeAsync("regtest-miner/1.0");
    await client.AuthorizeAsync("worker1", "x");
    var job = await client.WaitForJobAsync(TimeSpan.FromSeconds(30));

    if (expectedTxids is { Count: > 0 } && job.MerkleBranch.Count == 0)
        throw new InvalidOperationException(
            "stratum job has empty merkle branch but mempool txs were expected (gateway template missing txs)");

    Console.WriteLine(
        $"stratum job id={job.JobId} prev={job.PrevHash} nbits={job.NBits} en1={en1} " +
        $"merkleBranches={job.MerkleBranch.Count}");

    // Build coinbase + header using same rules as ShareValidator (via notify fields)
    var mined = MineStratumJob(job, en1, maxNonces: 50_000_000);
    if (mined == null)
        throw new InvalidOperationException("stratum mine failed within nonce budget");

    Console.WriteLine($"stratum mined ntime={mined.Value.Ntime} nonce={mined.Value.Nonce} hash={mined.Value.HashHex}");

    var accepted = await client.SubmitAsync(
        "worker1",
        job.JobId,
        mined.Value.Extranonce2,
        mined.Value.Ntime,
        mined.Value.Nonce,
        job.Version);

    if (!accepted)
        throw new InvalidOperationException("mining.submit not accepted by gateway");

    Console.WriteLine("mining.submit accepted");

    // Wait for gateway submitblock + chain advance
    var deadline = DateTime.UtcNow.AddSeconds(30);
    string? best = null;
    int heightAfter = heightBefore;
    while (DateTime.UtcNow < deadline)
    {
        var tip = await rpc.CallAsync<JsonElement>("getblockchaininfo");
        heightAfter = tip.GetProperty("blocks").GetInt32();
        best = tip.GetProperty("bestblockhash").GetString();
        if (heightAfter >= heightBefore + 1 &&
            best != null &&
            best.Equals(mined.Value.HashHex, StringComparison.OrdinalIgnoreCase))
            break;
        await Task.Delay(250);
    }

    if (heightAfter < heightBefore + 1)
        throw new InvalidOperationException($"chain height did not advance after stratum submit ({heightBefore}→{heightAfter})");
    if (best == null || !best.Equals(mined.Value.HashHex, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException($"tip hash mismatch after stratum: tip={best} mined={mined.Value.HashHex}");

    var block = await rpc.CallAsync<JsonElement>("getblock", new object[] { best, 1 });
    var blockTxids = block.GetProperty("tx").EnumerateArray().Select(x => x.GetString()!).ToList();
    if (expectedTxids is { Count: > 0 } && blockTxids.Count < 2)
        throw new InvalidOperationException($"stratum block has no mempool txs (txcount={blockTxids.Count})");
    AssertTxidsInBlock(blockTxids, expectedTxids, gbt);

    // Dashboard API must record the block
    using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
    var blocksJson = await http.GetStringAsync($"http://127.0.0.1:{apiPort}/api/blocks");
    if (!blocksJson.Contains(mined.Value.HashHex, StringComparison.OrdinalIgnoreCase) &&
        !blocksJson.Contains("submitted", StringComparison.OrdinalIgnoreCase) &&
        !blocksJson.Contains("duplicate", StringComparison.OrdinalIgnoreCase))
    {
        Console.WriteLine($"warn: /api/blocks may not yet list hash (payload={blocksJson})");
    }
    else
    {
        Console.WriteLine("dashboard /api/blocks observed submit");
    }

    var workersJson = await http.GetStringAsync($"http://127.0.0.1:{apiPort}/api/workers");
    using (var workersDoc = JsonDocument.Parse(workersJson))
    {
        var workerSeen = workersDoc.RootElement.EnumerateArray().Any(worker =>
            worker.TryGetProperty("extranonce1", out var en1El) &&
            string.Equals(en1El.GetString(), en1, StringComparison.OrdinalIgnoreCase) &&
            worker.TryGetProperty("assigned_difficulty", out var diffEl) &&
            diffEl.GetDouble() > 0);
        if (!workerSeen)
            throw new InvalidOperationException($"dashboard worker metrics missing en1={en1}: {workersJson}");
    }

    var sharesJson = await http.GetStringAsync($"http://127.0.0.1:{apiPort}/api/shares");
    using (var sharesDoc = JsonDocument.Parse(sharesJson))
    {
        var shareSeen = sharesDoc.RootElement.EnumerateArray().Any(share =>
            share.TryGetProperty("extranonce1", out var en1El) &&
            string.Equals(en1El.GetString(), en1, StringComparison.OrdinalIgnoreCase) &&
            share.TryGetProperty("actual_diff", out var diffEl) &&
            diffEl.GetDouble() > 0);
        if (!shareSeen)
            throw new InvalidOperationException($"dashboard share metrics missing en1={en1}: {sharesJson}");
    }
    Console.WriteLine("dashboard worker/share metrics observed accepted share");

    Console.WriteLine($"stratum on-chain height={heightAfter} hash={best} txs={blockTxids.Count}");
}

static async Task RunStratumLifecyclePathAsync(
    BitcoinRpcClient rpc,
    string host,
    int port,
    int apiPort,
    string payout,
    int minerCount)
{
    var clients = Enumerable.Range(0, minerCount).Select(_ => new StratumMinerClient()).ToArray();
    try
    {
        var setup = clients.Select(async (client, index) =>
        {
            await client.ConnectAsync(host, port);
            await client.ConfigureVersionRollingAsync("1fffe000");
            var (en1, _) = await client.SubscribeAsync($"regtest-lifecycle/{index}");
            await client.AuthorizeAsync($"worker{index}", "x");
            var job = await client.WaitForJobAsync(TimeSpan.FromSeconds(30));
            return (Index: index, Extranonce1: en1, Job: job);
        }).ToArray();
        var initial = await Task.WhenAll(setup);
        var firstJobId = initial[0].Job.JobId;
        if (initial.Any(x => x.Job.JobId != firstJobId || !x.Job.CleanJobs))
            throw new InvalidOperationException("miners did not start on the same clean job");

        // Lifecycle mode sets a target above uint256, so any header is a share. Pick
        // headers that deliberately miss the regtest network target to avoid creating
        // a competing block when exercising old-job acceptance.
        var lateShares = initial.Select(x =>
            MineNonBlockStratumShare(x.Job, x.Extranonce1, "00000001")).ToArray();
        var staleShare = MineNonBlockStratumShare(initial[0].Job, initial[0].Extranonce1, "00000002");

        var burstStarted = Stopwatch.GetTimestamp();
        var generated = await rpc.CallAsync<JsonElement>(
            "generatetoaddress", new object[] { 3, payout });
        var hashes = generated.EnumerateArray().Select(x => x.GetString()!).ToArray();
        if (hashes.Length != 3)
            throw new InvalidOperationException($"generatetoaddress returned {hashes.Length} hashes, expected 3");
        var finalNotifyPrevhash = ToStratumNotifyPrevhash(hashes[^1]);

        var finalJobs = clients.Select(async (client, index) =>
        {
            var job = await client.WaitForJobAsync(
                candidate => candidate.CleanJobs &&
                    candidate.PrevHash.Equals(finalNotifyPrevhash, StringComparison.OrdinalIgnoreCase),
                TimeSpan.FromSeconds(10));
            return (Index: index, Job: job);
        }).ToArray();

        // Once publication has retired the old job, submit work computed immediately
        // before the burst. This must be accepted during late_share_grace_ms.
        await Task.WhenAny(finalJobs);
        var lateSubmitStarted = Stopwatch.GetTimestamp();
        var lateSubmits = clients.Select(async (client, index) =>
        {
            var accepted = await client.SubmitAsync(
                $"worker{index}", firstJobId, lateShares[index].Extranonce2,
                lateShares[index].Ntime, lateShares[index].Nonce, initial[index].Job.Version);
            return (Accepted: accepted, CompletedTimestamp: Stopwatch.GetTimestamp());
        }).ToArray();
        var lateResults = await Task.WhenAll(lateSubmits);
        if (lateResults.Any(x => !x.Accepted))
            throw new InvalidOperationException("one or more old-job shares were rejected during clean grace");
        var lateAckFirstMs = StopwatchTicksToMilliseconds(
            lateResults.Min(x => x.CompletedTimestamp) - lateSubmitStarted);
        var lateAckLastMs = StopwatchTicksToMilliseconds(
            lateResults.Max(x => x.CompletedTimestamp) - lateSubmitStarted);

        var received = await Task.WhenAll(finalJobs);
        var arrivalTicks = received.Select(x => x.Job.ReceivedTimestamp).ToArray();
        var firstArrivalMs = StopwatchTicksToMilliseconds(arrivalTicks.Min() - burstStarted);
        var lastArrivalMs = StopwatchTicksToMilliseconds(arrivalTicks.Max() - burstStarted);
        var spreadMs = StopwatchTicksToMilliseconds(arrivalTicks.Max() - arrivalTicks.Min());
        if (spreadMs >= 1500)
            throw new InvalidOperationException($"clean fan-out spread {spreadMs:F3}ms exceeded 1500ms barrier");
        if (clients.Any(x => x.NotificationCount < 2 || x.CleanNotificationCount < 2))
            throw new InvalidOperationException("one or more miners missed the post-burst clean notification");

        Console.WriteLine(
            $"burst blocks=3 miners={minerCount} final_job={received[0].Job.JobId} " +
            $"first_clean_ms={firstArrivalMs:F3} last_clean_ms={lastArrivalMs:F3} spread_ms={spreadMs:F3} " +
            $"late_ack_first_ms={lateAckFirstMs:F3} late_ack_last_ms={lateAckLastMs:F3}");

        // Socket receipt is later than server WriteAsync completion, so grace is
        // guaranteed to have elapsed 2.4s after the last miner observes clean.
        await Task.Delay(2400);
        var staleError = await clients[0].SubmitExpectErrorAsync(
            "worker0", firstJobId, staleShare.Extranonce2,
            staleShare.Ntime, staleShare.Nonce, initial[0].Job.Version);
        if (!staleError.Contains("Stale job", StringComparison.OrdinalIgnoreCase) ||
            staleError.Contains("Job not found", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"retired job classification changed: {staleError}");

        var unknownError = await clients[0].SubmitExpectErrorAsync(
            "worker0", "ffffffffffffffff", staleShare.Extranonce2,
            staleShare.Ntime, staleShare.Nonce, initial[0].Job.Version);
        if (!unknownError.Contains("Job not found", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"unknown job classification changed: {unknownError}");

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var metricsDeadline = DateTime.UtcNow.AddSeconds(3);
        JsonElement stats = default;
        while (DateTime.UtcNow < metricsDeadline)
        {
            using var statsDoc = JsonDocument.Parse(
                await http.GetStringAsync($"http://127.0.0.1:{apiPort}/api/stats"));
            stats = statsDoc.RootElement.Clone();
            if (stats.GetProperty("shares_late").GetInt64() >= minerCount &&
                stats.GetProperty("shares_stale").GetInt64() >= 1 &&
                stats.GetProperty("shares_unknown_job").GetInt64() >= 1 &&
                stats.GetProperty("share_accepted_ack_written").GetInt64() >= minerCount)
                break;
            await Task.Delay(50);
        }

        if (stats.ValueKind != JsonValueKind.Object ||
            stats.GetProperty("shares_late").GetInt64() < minerCount ||
            stats.GetProperty("shares_stale").GetInt64() < 1 ||
            stats.GetProperty("shares_unknown_job").GetInt64() < 1 ||
            stats.GetProperty("clean_broadcast_client_timeouts").GetInt64() != 0 ||
            stats.GetProperty("share_accepted_ack_written").GetInt64() < minerCount)
            throw new InvalidOperationException($"lifecycle metrics did not converge: {stats}");

        Console.WriteLine(
            $"lifecycle metrics late={stats.GetProperty("shares_late").GetInt64()} " +
            $"stale={stats.GetProperty("shares_stale").GetInt64()} " +
            $"unknown={stats.GetProperty("shares_unknown_job").GetInt64()} " +
            $"clean={stats.GetProperty("clean_broadcasts").GetInt64()} " +
            $"timeouts={stats.GetProperty("clean_broadcast_client_timeouts").GetInt64()} " +
            $"validate_avg_ms={stats.GetProperty("share_validation_avg_ms").GetDouble():F3} " +
            $"ack_queue_avg_ms={stats.GetProperty("share_accepted_ack_queue_avg_ms").GetDouble():F3} " +
            $"ack_write_avg_ms={stats.GetProperty("share_accepted_ack_write_avg_ms").GetDouble():F3}");
    }
    finally
    {
        foreach (var client in clients)
            client.Dispose();
    }
}

static async Task RunRealP2pFastPolicyPathAsync(
    BitcoinRpcClient rpc,
    string regtestMiningAddress,
    string workDir)
{
    // ChainGuard correctly forbids starting a mainnet gateway against regtest. Test the
    // mainnet-only empty-fast policy in-process instead, using a genuine Core-mined
    // header and a genuine follow-up GBT from this isolated regtest node.
    var mainnetPayout = new Key().PubKey.GetAddress(ScriptPubKeyType.Segwit, Network.Main).ToString();
    var cfg = new AppConfig
    {
        NetworkName = "mainnet",
        Bitcoind = new BitcoindConfig
        {
            RpcUrl = "http://127.0.0.1:18443",
            RpcUser = "regtest",
            RpcPassword = "regtestpass"
        },
        Coinbase = new CoinbaseConfig { Address = mainnetPayout },
        Runtime = new RuntimeConfig { DataDir = Path.Combine(workDir, "p2p-fast-policy-data") },
        Difficulty = new DifficultyConfig { Min = 1, Max = 1e12, Default = 1 }
    };
    cfg.Validate();
    var metrics = new MetricsStore();
    var submitQueue = new BlockSubmitQueue(cfg, rpc, metrics);
    var engine = new TemplateEngine(cfg, rpc, metrics, submitQueue);

    async Task<JobNotify> DispatchOneAsync()
    {
        JobNotify captured = default;
        var received = false;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await engine.DispatchNotificationsAsync(notify =>
            {
                captured = notify;
                received = true;
                cts.Cancel();
            }, cts.Token);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
        if (!received)
            throw new TimeoutException("timed out waiting for the P2P-fast job notification");
        return captured;
    }

    await engine.RefreshDirectAsync(TemplateSource.Startup, CancellationToken.None);
    var initial = await DispatchOneAsync();
    var generated = await rpc.CallAsync<JsonElement>(
        "generatetoaddress", new object[] { 1, regtestMiningAddress });
    var blockHash = generated[0].GetString()
        ?? throw new InvalidOperationException("real P2P-fast policy block hash missing");
    var header = await rpc.CallAsync<JsonElement>("getblockheader", new object[] { blockHash, true });
    var prevhash = header.GetProperty("previousblockhash").GetString()!;
    var height = header.GetProperty("height").GetUInt32();
    var blockTime = header.GetProperty("time").GetUInt32();
    var nbits = Convert.ToUInt32(header.GetProperty("bits").GetString(), 16);

    await engine.HandleP2pFastAnnouncementAsync(
        prevhash, blockHash, blockTime, height, nbits, CancellationToken.None);
    var fast = engine.ActiveMiningJob;
    if (fast.Source != TemplateSource.P2pFast || fast.Height != height + 1 ||
        !fast.PrevhashBe.Equals(blockHash, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("real Core header did not publish mainnet P2P-fast work");

    // Do not dispatch the fast notify yet. The full Core GBT should coalesce over it,
    // retain clean=true, and become authoritative without exposing an intermediate job.
    await engine.RefreshDirectAsync(TemplateSource.ZmqHashblock, CancellationToken.None);
    var merged = await DispatchOneAsync();
    if (merged.Job.Source != TemplateSource.ZmqHashblock || !merged.CleanJobs ||
        merged.Job.Height != fast.Height ||
        !merged.Job.PrevhashBe.Equals(fast.PrevhashBe, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("real GBT did not safely supersede P2P-fast clean work");
    if (engine.FindJob(initial.Job.JobId).Status != JobLookupStatus.RetiredWithinGrace ||
        engine.FindJob(fast.JobId).Status != JobLookupStatus.RetiredWithinGrace)
        throw new InvalidOperationException("real P2P-fast/full-GBT merge lost retired jobs");

    Console.WriteLine(
        $"real P2P-fast policy header={blockHash} fast_job={fast.JobId} " +
        $"full_job={merged.Job.JobId} merged_clean={merged.CleanJobs}");

    // Advance Core once more, then mine the child directly from the P2P-fast
    // coinbase-only job before any full GBT can replace it.
    var generatedParent = await rpc.CallAsync<JsonElement>(
        "generatetoaddress", new object[] { 1, regtestMiningAddress });
    var parentHash = generatedParent[0].GetString()
        ?? throw new InvalidOperationException("P2P-fast mining parent hash missing");
    var parentHeader = await rpc.CallAsync<JsonElement>(
        "getblockheader", new object[] { parentHash, true });
    var parentPrevhash = parentHeader.GetProperty("previousblockhash").GetString()!;
    var parentHeight = parentHeader.GetProperty("height").GetUInt32();
    var parentTime = parentHeader.GetProperty("time").GetUInt32();
    var parentNbits = Convert.ToUInt32(parentHeader.GetProperty("bits").GetString(), 16);

    await engine.HandleP2pFastAnnouncementAsync(
        parentPrevhash, parentHash, parentTime, parentHeight, parentNbits,
        CancellationToken.None);
    var miningJob = engine.ActiveMiningJob;
    if (miningJob.Source != TemplateSource.P2pFast ||
        miningJob.Height != parentHeight + 1 ||
        !miningJob.PrevhashBe.Equals(parentHash, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("real parent header did not publish the mining P2P-fast job");
    if (miningJob.TransactionCount != 0 || miningJob.TransactionBytes != 0 ||
        miningJob.MerkleBranchesLe.Count != 0 || miningJob.MerkleBranchesHex.Count != 0 ||
        miningJob.HasWitnessCommitment || miningJob.WitnessCommitmentScriptHex != null)
        throw new InvalidOperationException("P2P-fast mining job was not coinbase-only");
    if (miningJob.CoinbaseValue != BitcoinEncoding.BlockSubsidySat(miningJob.Height))
        throw new InvalidOperationException("P2P-fast mining job coinbase subsidy changed");

    var fastNotify = await DispatchOneAsync();
    if (!fastNotify.CleanJobs || !ReferenceEquals(fastNotify.Job, miningJob))
        throw new InvalidOperationException("P2P-fast mining job was not dispatched as clean work");

    var extranonce1 = Hex.Decode("01020304");
    const string extranonce2 = "05060708";
    var coinbasePrefix = new byte[miningJob.Coinbase1.Length + extranonce1.Length];
    Buffer.BlockCopy(miningJob.Coinbase1, 0, coinbasePrefix, 0, miningJob.Coinbase1.Length);
    Buffer.BlockCopy(
        extranonce1, 0, coinbasePrefix, miningJob.Coinbase1.Length, extranonce1.Length);
    var shareTarget = Enumerable.Repeat((byte)0xff, 32).ToArray();
    var mined = MineBlock(
        miningJob, coinbasePrefix, extranonce2, shareTarget, maxNonces: 50_000_000)
        ?? throw new InvalidOperationException("failed to mine the P2P-fast coinbase-only job");
    if (!mined.IsBlock || mined.BlockHex == null)
        throw new InvalidOperationException("P2P-fast share did not assemble a full block");

    var minedHash = mined.Hash.ToHex();
    var submitResult = await rpc.SubmitBlockAsync(mined.BlockHex);
    if (submitResult != null)
        throw new InvalidOperationException($"P2P-fast submitblock rejected: {submitResult}");

    var chain = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    if (chain.GetProperty("blocks").GetUInt32() != miningJob.Height ||
        !chain.GetProperty("bestblockhash").GetString()!.Equals(
            minedHash, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("P2P-fast block did not become the active chain tip");

    var onChain = await rpc.CallAsync<JsonElement>(
        "getblock", new object[] { minedHash, 2 });
    if (onChain.GetProperty("height").GetUInt32() != miningJob.Height ||
        !onChain.GetProperty("previousblockhash").GetString()!.Equals(
            parentHash, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("P2P-fast on-chain block header changed");

    var transactions = onChain.GetProperty("tx");
    if (transactions.GetArrayLength() != 1)
        throw new InvalidOperationException(
            $"P2P-fast block contains {transactions.GetArrayLength()} transactions instead of only coinbase");
    var coinbase = transactions[0];
    if (!coinbase.GetProperty("vin")[0].TryGetProperty("coinbase", out _))
        throw new InvalidOperationException("P2P-fast block's only transaction is not coinbase");

    var expectedPayoutScript = Hex.Encode(
        BitcoinAddress.Create(mainnetPayout, Network.Main).ScriptPubKey.ToBytes(true));
    JsonElement payout = default;
    var foundPayout = false;
    foreach (var output in coinbase.GetProperty("vout").EnumerateArray())
    {
        var scriptHex = output.GetProperty("scriptPubKey").GetProperty("hex").GetString();
        if (!string.Equals(scriptHex, expectedPayoutScript, StringComparison.OrdinalIgnoreCase))
            continue;
        payout = output;
        foundPayout = true;
        break;
    }
    if (!foundPayout)
        throw new InvalidOperationException("P2P-fast coinbase does not pay the configured address script");
    var expectedPayoutBtc = miningJob.CoinbaseValue / 100_000_000m;
    if (payout.GetProperty("value").GetDecimal() != expectedPayoutBtc)
        throw new InvalidOperationException("P2P-fast coinbase payout amount changed");

    Console.WriteLine(
        $"P2P-fast coinbase-only block height={miningJob.Height} hash={minedHash} " +
        $"txs={transactions.GetArrayLength()} payout_btc={expectedPayoutBtc}");
}

static (string Extranonce2, string Ntime, string Nonce) MineNonBlockStratumShare(
    StratumJob job,
    string extranonce1,
    string extranonce2)
{
    var en2Bytes = Hex.Decode(extranonce2);
    var en1Bytes = Hex.Decode(extranonce1);
    var cb1 = Hex.Decode(job.Coinbase1);
    var cb2 = Hex.Decode(job.Coinbase2);
    var coinbase = new byte[cb1.Length + en1Bytes.Length + en2Bytes.Length + cb2.Length];
    var offset = 0;
    Buffer.BlockCopy(cb1, 0, coinbase, offset, cb1.Length); offset += cb1.Length;
    Buffer.BlockCopy(en1Bytes, 0, coinbase, offset, en1Bytes.Length); offset += en1Bytes.Length;
    Buffer.BlockCopy(en2Bytes, 0, coinbase, offset, en2Bytes.Length); offset += en2Bytes.Length;
    Buffer.BlockCopy(cb2, 0, coinbase, offset, cb2.Length);

    var merkle = BitcoinEncoding.DoubleSha256(coinbase);
    foreach (var branch in job.MerkleBranch)
        merkle = BitcoinEncoding.MerkleStep(merkle, Hex.Decode(branch));

    var prevNotify = Hex.Decode(job.PrevHash);
    var prevBe = new byte[32];
    for (var i = 0; i < 8; i++)
        Buffer.BlockCopy(prevNotify, i * 4, prevBe, (7 - i) * 4, 4);
    var prevLe = Hex.ReverseCopy(prevBe);
    var version = Convert.ToUInt32(job.Version, 16);
    var nbits = Convert.ToUInt32(job.NBits, 16);
    var ntime = Convert.ToUInt32(job.NTime, 16);
    var networkTarget = BitcoinEncoding.CompactTargetToLe(nbits);

    for (uint nonce = 0; nonce < 1_000_000; nonce++)
    {
        var header = BitcoinEncoding.BuildHeader(version, prevLe, merkle, ntime, nbits, nonce);
        var hashLe = BitcoinEncoding.DoubleSha256(header);
        if (!BitcoinEncoding.LeqLe256(hashLe, networkTarget))
            return (extranonce2, Hex.U32BeHex(ntime), Hex.U32BeHex(nonce));
    }
    throw new InvalidOperationException("failed to construct a non-block regtest share");
}

static string ToStratumNotifyPrevhash(string blockHashHex)
{
    var blockHashBe = Hex.Decode(blockHashHex);
    var notify = new byte[32];
    for (var i = 0; i < 8; i++)
        Buffer.BlockCopy(blockHashBe, i * 4, notify, (7 - i) * 4, 4);
    return Hex.Encode(notify);
}

static double StopwatchTicksToMilliseconds(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;

static async Task RunLargeMempoolPathAsync(
    BitcoinRpcClient rpc,
    string payout,
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    int transactionCount)
{
    var seedTimer = Stopwatch.StartNew();
    var txids = await SeedLargeIndependentMempoolAsync(
        rpc, payout, rpcUrl, rpcUser, rpcPass, transactionCount);
    seedTimer.Stop();

    var mempoolInfo = await rpc.CallAsync<JsonElement>("getmempoolinfo");
    var mempoolSize = mempoolInfo.GetProperty("size").GetInt32();
    if (mempoolSize != transactionCount)
        throw new InvalidOperationException(
            $"large mempool size={mempoolSize}, expected exactly {transactionCount}");

    Console.WriteLine(
        $"large mempool seeded txs={mempoolSize:N0} elapsed={seedTimer.Elapsed.TotalSeconds:F1}s");
    var pathTimer = Stopwatch.StartNew();
    await RunDirectPathAsync(
        rpc,
        payout,
        requireTxCount: transactionCount,
        label: "large-mempool",
        expectedTxids: txids,
        requireAllExpectedTxids: true,
        gbtTimeout: TimeSpan.FromMinutes(2));
    pathTimer.Stop();

    mempoolInfo = await rpc.CallAsync<JsonElement>("getmempoolinfo");
    var remaining = mempoolInfo.GetProperty("size").GetInt32();
    if (remaining != 0)
        throw new InvalidOperationException(
            $"large-mempool block left {remaining} transactions in mempool");
    Console.WriteLine(
        $"large mempool end-to-end txs={transactionCount:N0} " +
        $"template+mine+submit+confirm={pathTimer.Elapsed.TotalSeconds:F1}s");
}

static async Task<List<string>> SeedLargeIndependentMempoolAsync(
    BitcoinRpcClient rpc,
    string payout,
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    int transactionCount)
{
    const int outputsPerFundingTransaction = 2_000;
    const long fanoutValueSats = 100_000;

    var existing = await rpc.CallAsync<JsonElement>("getrawmempool");
    if (existing.ValueKind == JsonValueKind.Array && existing.GetArrayLength() > 0)
    {
        Console.WriteLine(
            $"clearing existing mempool txs={existing.GetArrayLength():N0} before large seed");
        await rpc.CallAsync<JsonElement>(
            "generatetoaddress", new object[] { 1, payout });
    }

    var fanoutKey = new Key();
    var fanoutAddress = fanoutKey.PubKey
        .GetAddress(ScriptPubKeyType.Segwit, Network.RegTest);
    var fanoutScript = fanoutAddress.ScriptPubKey;
    var coins = new List<Coin>(transactionCount);
    var fundingCount =
        (transactionCount + outputsPerFundingTransaction - 1) /
        outputsPerFundingTransaction;

    for (var fundingIndex = 0; fundingIndex < fundingCount; fundingIndex++)
    {
        var outputCount = Math.Min(
            outputsPerFundingTransaction, transactionCount - coins.Count);
        var requiredBtc = outputCount * fanoutValueSats / 100_000_000m + 0.01m;
        var unspent = await rpc.CallAsync<JsonElement>(
            "listunspent", new object[] { 1, 9_999_999 });
        var fundingInput = unspent.EnumerateArray().FirstOrDefault(item =>
            (!item.TryGetProperty("spendable", out var spendable) || spendable.GetBoolean()) &&
            item.GetProperty("amount").GetDecimal() >= requiredBtc);
        if (fundingInput.ValueKind != JsonValueKind.Object)
            throw new InvalidOperationException(
                $"wallet has no confirmed UTXO large enough for {requiredBtc} BTC fan-out");

        var unsignedFundingTransaction = Network.RegTest.CreateTransaction();
        unsignedFundingTransaction.Version = 2;
        unsignedFundingTransaction.Inputs.Add(new TxIn(new OutPoint(
            uint256.Parse(fundingInput.GetProperty("txid").GetString()),
            fundingInput.GetProperty("vout").GetUInt32())));
        for (var i = 0; i < outputCount; i++)
            unsignedFundingTransaction.Outputs.Add(
                new TxOut(Money.Satoshis(fanoutValueSats), fanoutScript));
        var unsignedHex = unsignedFundingTransaction.ToHex();
        var funded = await rpc.CallAsync<JsonElement>(
            "fundrawtransaction", new object[] { unsignedHex });
        var fundedHex = funded.GetProperty("hex").GetString()
            ?? throw new InvalidOperationException("fundrawtransaction returned no hex");
        var signed = await rpc.CallAsync<JsonElement>(
            "signrawtransactionwithwallet", new object[] { fundedHex });
        if (!signed.GetProperty("complete").GetBoolean())
            throw new InvalidOperationException("wallet did not fully sign fan-out transaction");
        var signedHex = signed.GetProperty("hex").GetString()
            ?? throw new InvalidOperationException("signrawtransactionwithwallet returned no hex");
        var fundingTransaction = Transaction.Parse(signedHex, Network.RegTest);
        var fundingTxid = fundingTransaction.GetHash().ToString();
        var rpcTxid = await rpc.CallAsync<string>(
            "sendrawtransaction", new object[] { signedHex });
        if (!rpcTxid.Equals(fundingTxid, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                $"fan-out txid mismatch local={fundingTxid} core={rpcTxid}");

        var matched = 0;
        for (var outputIndex = 0; outputIndex < fundingTransaction.Outputs.Count; outputIndex++)
        {
            var output = fundingTransaction.Outputs[outputIndex];
            if (output.Value.Satoshi != fanoutValueSats ||
                !output.ScriptPubKey.Equals(fanoutScript))
                continue;
            coins.Add(new Coin(
                new OutPoint(fundingTransaction.GetHash(), (uint)outputIndex), output));
            matched++;
        }
        if (matched != outputCount)
            throw new InvalidOperationException(
                $"fan-out parent {fundingIndex + 1} has {matched} matching outputs, expected {outputCount}");

        await rpc.CallAsync<JsonElement>(
            "generatetoaddress", new object[] { 1, payout });
        Console.WriteLine(
            $"confirmed fan-out parent {fundingIndex + 1}/{fundingCount} " +
            $"outputs={outputCount:N0} total_utxos={coins.Count:N0}");
    }

    if (coins.Count != transactionCount)
        throw new InvalidOperationException(
            $"fan-out created {coins.Count} UTXOs, expected {transactionCount}");

    // Four payload bytes keep stripped size above Core's minimum standard
    // transaction size while retaining enough block-weight room for >10k txs.
    var nullDataScript = TxNullDataTemplate.Instance.GenerateScriptPubKey(new byte[4]);
    var secret = fanoutKey.GetBitcoinSecret(Network.RegTest);
    var rawTransactions = new List<string>(transactionCount);
    var txids = new List<string>(transactionCount);
    long rawBytes = 0;
    for (var i = 0; i < coins.Count; i++)
    {
        var transaction = Network.RegTest.CreateTransaction();
        transaction.Version = 2;
        transaction.Inputs.Add(new TxIn(coins[i].Outpoint));
        transaction.Outputs.Add(new TxOut(Money.Zero, nullDataScript));
        transaction.Sign(secret, coins[i]);
        var rawHex = transaction.ToHex();
        rawBytes += rawHex.Length / 2;
        rawTransactions.Add(rawHex);
        txids.Add(transaction.GetHash().ToString());
    }

    Console.WriteLine(
        $"signed independent children txs={transactionCount:N0} " +
        $"raw={rawBytes / 1024d / 1024d:F2} MiB avg={rawBytes / (double)transactionCount:F1} bytes");
    await BroadcastRawTransactionBatchesAsync(
        rpcUrl, rpcUser, rpcPass, rawTransactions, batchSize: 200);

    var deadline = DateTime.UtcNow.AddMinutes(2);
    while (DateTime.UtcNow < deadline)
    {
        var info = await rpc.CallAsync<JsonElement>("getmempoolinfo");
        var size = info.GetProperty("size").GetInt32();
        if (size == transactionCount)
            return txids;
        await Task.Delay(250);
    }

    var finalInfo = await rpc.CallAsync<JsonElement>("getmempoolinfo");
    throw new TimeoutException(
        $"mempool size={finalInfo.GetProperty("size").GetInt32()} did not reach {transactionCount}");
}

static async Task BroadcastRawTransactionBatchesAsync(
    string rpcUrl,
    string rpcUser,
    string rpcPass,
    IReadOnlyList<string> rawTransactions,
    int batchSize)
{
    using var http = new HttpClient
    {
        Timeout = TimeSpan.FromMinutes(5)
    };
    http.DefaultRequestHeaders.Authorization =
        new System.Net.Http.Headers.AuthenticationHeaderValue(
            "Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes($"{rpcUser}:{rpcPass}")));

    for (var offset = 0; offset < rawTransactions.Count; offset += batchSize)
    {
        var count = Math.Min(batchSize, rawTransactions.Count - offset);
        var requests = new object[count];
        for (var i = 0; i < count; i++)
        {
            requests[i] = new
            {
                jsonrpc = "1.0",
                id = offset + i,
                method = "sendrawtransaction",
                @params = new object[] { rawTransactions[offset + i] }
            };
        }

        using var content = new StringContent(
            JsonSerializer.Serialize(requests), Encoding.UTF8, "application/json");
        using var response = await http.PostAsync(rpcUrl, content);
        response.EnsureSuccessStatusCode();
        await using var responseStream = await response.Content.ReadAsStreamAsync();
        using var document = await JsonDocument.ParseAsync(responseStream);
        if (document.RootElement.ValueKind != JsonValueKind.Array ||
            document.RootElement.GetArrayLength() != count)
            throw new InvalidOperationException(
                $"sendrawtransaction batch response count changed at offset {offset}");

        foreach (var item in document.RootElement.EnumerateArray())
        {
            if (item.TryGetProperty("error", out var error) &&
                error.ValueKind != JsonValueKind.Null)
                throw new InvalidOperationException(
                    $"sendrawtransaction batch failed at offset {offset}: {error}");
            if (!item.TryGetProperty("result", out var result) ||
                result.ValueKind != JsonValueKind.String)
                throw new InvalidOperationException(
                    $"sendrawtransaction batch returned no txid at offset {offset}");
        }

        var broadcast = offset + count;
        if (broadcast == rawTransactions.Count || broadcast % 1_000 == 0)
            Console.WriteLine(
                $"broadcast mempool transactions {broadcast:N0}/{rawTransactions.Count:N0}");
    }
}

/// <summary>
/// Create confirmed UTXOs if needed, then broadcast several bech32→bech32 spends into the mempool.
/// Returns wallet txids that must appear in the next mined block (when fees are high enough for GBT).
/// </summary>
static async Task<List<string>> SeedMempoolAsync(BitcoinRpcClient rpc, int count)
{
    await EnsureSpendableBalanceAsync(rpc, minBtc: count * 0.05 + 1.0);

    // Prefer emptying mempool so GBT is deterministic
    var existing = await rpc.CallAsync<JsonElement>("getrawmempool");
    if (existing.ValueKind == JsonValueKind.Array && existing.GetArrayLength() > 0)
    {
        Console.WriteLine($"mempool already has {existing.GetArrayLength()} tx(s); generating 1 block to clear");
        var sink = await NewBech32AddressAsync(rpc);
        await rpc.CallAsync<JsonElement>("generatetoaddress", new object[] { 1, sink });
    }

    var txids = new List<string>(count);
    for (var i = 0; i < count; i++)
    {
        // Mix address types: bech32 (witness) and p2sh-segwit for broader coverage
        var addrType = i % 2 == 0 ? "bech32" : "p2sh-segwit";
        string dest;
        try
        {
            dest = await rpc.CallAsync<string>("getnewaddress", new object[] { "", addrType });
        }
        catch
        {
            dest = await NewBech32AddressAsync(rpc);
        }

        // Small fixed amount; fallbackfee covers relay
        var amount = 0.01 + i * 0.001;
        var txid = await rpc.CallAsync<string>("sendtoaddress", new object[] { dest, amount });
        txids.Add(txid);
        Console.WriteLine($"seeded mempool tx[{i}] type={addrType} amount={amount} txid={txid}");
    }

    // Wait until mempool reflects broadcasts
    var deadline = DateTime.UtcNow.AddSeconds(10);
    while (DateTime.UtcNow < deadline)
    {
        var mem = await rpc.CallAsync<JsonElement>("getrawmempool");
        var n = mem.ValueKind == JsonValueKind.Array ? mem.GetArrayLength() : 0;
        if (n >= count)
        {
            Console.WriteLine($"mempool size={n} (seeded {count})");
            return txids;
        }
        await Task.Delay(200);
    }

    throw new TimeoutException($"mempool did not reach {count} txs after sendtoaddress");
}

static async Task EnsureSpendableBalanceAsync(BitcoinRpcClient rpc, double minBtc)
{
    for (var attempt = 0; attempt < 6; attempt++)
    {
        double bal;
        try
        {
            bal = await rpc.CallAsync<double>("getbalance", Array.Empty<object>());
        }
        catch
        {
            bal = 0;
        }

        if (bal >= minBtc)
        {
            Console.WriteLine($"wallet balance={bal} BTC (need>={minBtc})");
            return;
        }

        var sink = await NewBech32AddressAsync(rpc);
        Console.WriteLine($"balance {bal} < {minBtc}; mining 50 blocks to {sink}...");
        await rpc.CallAsync<JsonElement>("generatetoaddress", new object[] { 50, sink });
    }

    var finalBal = await rpc.CallAsync<double>("getbalance", Array.Empty<object>());
    if (finalBal < minBtc)
        throw new InvalidOperationException($"insufficient wallet balance {finalBal} BTC (need>={minBtc})");
}

static async Task<string> NewBech32AddressAsync(BitcoinRpcClient rpc)
{
    try
    {
        return await rpc.CallAsync<string>("getnewaddress", new object[] { "", "bech32" });
    }
    catch
    {
        return await rpc.CallAsync<string>("getnewaddress", Array.Empty<object>());
    }
}

static async Task<GbtResponse> WaitForGbtAsync(BitcoinRpcClient rpc, int requireTxCount, TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    GbtResponse? last = null;
    while (DateTime.UtcNow < deadline)
    {
        last = await rpc.CallAsync<GbtResponse>("getblocktemplate", new object[]
        {
            new Dictionary<string, object> { ["rules"] = new[] { "segwit" } }
        });
        if (last.Transactions.Length >= requireTxCount)
            return last;
        await Task.Delay(200);
    }

    throw new TimeoutException(
        $"GBT txs={last?.Transactions.Length ?? 0} < required {requireTxCount} within {timeout.TotalSeconds:0}s");
}

static void AssertTxidsInBlock(
    IReadOnlyList<string> blockTxids,
    IReadOnlyList<string>? expectedTxids,
    GbtResponse gbt,
    bool requireAllExpectedTxids = false)
{
    var blockTxidSet = new HashSet<string>(
        blockTxids, StringComparer.OrdinalIgnoreCase);

    // Every GBT txid must be in the block (order after coinbase follows template)
    foreach (var gbtTx in gbt.Transactions)
    {
        var id = gbtTx.TxId ?? gbtTx.Hash;
        if (string.IsNullOrEmpty(id))
            continue;
        if (!blockTxidSet.Contains(id))
            throw new InvalidOperationException($"block missing GBT txid {id}");
    }

    if (expectedTxids == null || expectedTxids.Count == 0)
        return;

    var missing = expectedTxids
        .Where(id => !blockTxidSet.Contains(id))
        .ToList();

    // sendtoaddress txids should be selected by GBT when mempool is only our seeds.
    // If Core omitted one (fee policy), require that at least one seeded tx landed AND all GBT txs did.
    if (missing.Count == expectedTxids.Count)
        throw new InvalidOperationException(
            "none of the seeded mempool txids appeared in the mined block");

    if (requireAllExpectedTxids && missing.Count > 0)
        throw new InvalidOperationException(
            $"block missing {missing.Count}/{expectedTxids.Count} required seeded txids: " +
            string.Join(",", missing.Take(5)));
    if (missing.Count > 0)
        Console.WriteLine(
            $"warn: {missing.Count}/{expectedTxids.Count} seeded txids not in block " +
            $"(GBT selected {gbt.Transactions.Length} txs): {string.Join(",", missing.Take(5))}");
    else
        Console.WriteLine($"all {expectedTxids.Count} seeded mempool txids confirmed in block");
}

static ShareResult? MineBlock(JobTemplate job, byte[] coinbasePrefix, string extranonce2, byte[] shareTargetLe, int maxNonces)
{
    var ntime = job.Ntime;
    if (job.Mintime != 0 && ntime < job.Mintime)
        ntime = job.Mintime;

    for (uint nonce = 0; nonce < (uint)maxNonces; nonce++)
    {
        var result = ShareValidator.Validate(job, coinbasePrefix, new ShareSubmit
        {
            Extranonce2 = extranonce2,
            Ntime = Hex.U32BeHex(ntime),
            Nonce = Hex.U32BeHex(nonce)
        }, shareTargetLe);

        if (result.IsBlock)
            return result;
    }

    return null;
}

static (string Extranonce2, string Ntime, string Nonce, string HashHex)? MineStratumJob(
    StratumJob job,
    string extranonce1,
    int maxNonces)
{
    // Reconstruct coinbase and header the same way the gateway does.
    // We only need a valid PoW header; submit uses en2/ntime/nonce.
    var en2 = "00000001";
    var en2Bytes = Hex.Decode(en2);
    var en1Bytes = Hex.Decode(extranonce1);
    var cb1 = Hex.Decode(job.Coinbase1);
    var cb2 = Hex.Decode(job.Coinbase2);

    var coinbase = new byte[cb1.Length + en1Bytes.Length + en2Bytes.Length + cb2.Length];
    var o = 0;
    Buffer.BlockCopy(cb1, 0, coinbase, o, cb1.Length); o += cb1.Length;
    Buffer.BlockCopy(en1Bytes, 0, coinbase, o, en1Bytes.Length); o += en1Bytes.Length;
    Buffer.BlockCopy(en2Bytes, 0, coinbase, o, en2Bytes.Length); o += en2Bytes.Length;
    Buffer.BlockCopy(cb2, 0, coinbase, o, cb2.Length);

    var coinbaseHash = BitcoinEncoding.DoubleSha256(coinbase);
    var merkle = coinbaseHash;
    foreach (var br in job.MerkleBranch)
    {
        var b = Hex.Decode(br);
        merkle = BitcoinEncoding.MerkleStep(merkle, b);
    }

    // prevhash in notify is word-swapped BE; convert to LE for header
    var prevNotify = Hex.Decode(job.PrevHash);
    var prevBe = new byte[32];
    for (var i = 0; i < 8; i++)
        Buffer.BlockCopy(prevNotify, i * 4, prevBe, (7 - i) * 4, 4);
    var prevLe = Hex.ReverseCopy(prevBe);

    var version = Convert.ToUInt32(job.Version, 16);
    var nbits = Convert.ToUInt32(job.NBits, 16);
    var ntime = Convert.ToUInt32(job.NTime, 16);
    var targetLe = BitcoinEncoding.CompactTargetToLe(nbits);

    for (uint nonce = 0; nonce < (uint)maxNonces; nonce++)
    {
        var header = BitcoinEncoding.BuildHeader(version, prevLe, merkle, ntime, nbits, nonce);
        var hashLe = BitcoinEncoding.DoubleSha256(header);
        if (!BitcoinEncoding.LeqLe256(hashLe, targetLe))
            continue;

        var hashHex = Hex.Encode(Hex.ReverseCopy(hashLe));
        return (en2, Hex.U32BeHex(ntime), Hex.U32BeHex(nonce), hashHex);
    }

    return null;
}

static AppConfig BuildConfig(string payout, string rpcUrl, string user, string pass)
{
    var cfg = new AppConfig
    {
        NetworkName = "regtest",
        Bitcoind = new BitcoindConfig
        {
            RpcUrl = rpcUrl,
            RpcUser = user,
            RpcPassword = pass
        },
        Coinbase = new CoinbaseConfig
        {
            Address = payout,
            Message = "regtest-e2e",
            SegwitCommitment = true
        },
        Stratum = new StratumConfig { Extranonce1Size = 4, Extranonce2Size = 4 },
        Difficulty = new DifficultyConfig { Min = 0.001, Max = 1, Default = 0.01 }
    };
    cfg.Validate();
    return cfg;
}

static async Task<string> WriteRuntimeConfigAsync(
    string workDir, string payout, string rpcUrl, string user, string pass, int stratumPort, int apiPort,
    bool lifecycleMode = false)
{
    var path = Path.Combine(workDir, "gateway.json");
    var cfg = new AppConfig
    {
        LogLevel = "Information",
        NetworkName = "regtest",
        Stratum = new StratumConfig
        {
            ListenAddr = "127.0.0.1",
            ListenPort = stratumPort,
            Extranonce1Size = 4,
            Extranonce2Size = 4,
            IdleTimeoutSecs = 3600,
            MaxConnections = 32
        },
        Bitcoind = new BitcoindConfig
        {
            RpcUrl = rpcUrl,
            RpcUser = user,
            RpcPassword = pass,
            ZmqBlockUrls = new List<string> { "tcp://127.0.0.1:28332" },
            P2pFastPeer = "127.0.0.1:18444"
        },
        Coinbase = new CoinbaseConfig
        {
            Address = payout,
            Message = "regtest-e2e",
            SegwitCommitment = true
        },
        Difficulty = new DifficultyConfig
        {
            Min = lifecycleMode ? 1e-12 : 0.001,
            Max = lifecycleMode ? 1e-6 : 1,
            Default = lifecycleMode ? 1e-12 : 0.01,
            TargetTimeSecs = 5,
            RetargetTimeSecs = 90
        },
        Runtime = new RuntimeConfig
        {
            KeepOldJobs = 8,
            DataDir = Path.Combine(workDir, "gateway-data")
        },
        Api = new ApiConfig
        {
            ListenAddr = "127.0.0.1",
            ListenPort = apiPort,
            Enabled = true
        }
    };
    Directory.CreateDirectory(Path.GetDirectoryName(path)!);
    await File.WriteAllTextAsync(path, JsonSerializer.Serialize(cfg, AppConfig.JsonOptions()));
    return path;
}

static Process StartGateway(string configPath)
{
    var repo = FindRepoRoot();
    var targetFramework = $"net{Environment.Version.Major}.0";
    var candidates = new[]
    {
        Path.Combine(repo, "build", "MiningcoreBtcSolo.exe"),
        Path.Combine(repo, "build", "MiningcoreBtcSolo"),
        Path.Combine(repo, "src", "MiningcoreBtcSolo", "bin", "Release", targetFramework, "MiningcoreBtcSolo.exe"),
        Path.Combine(repo, "src", "MiningcoreBtcSolo", "bin", "Debug", targetFramework, "MiningcoreBtcSolo.exe")
    };
    var exe = candidates.FirstOrDefault(File.Exists);
    ProcessStartInfo psi;
    if (exe != null)
    {
        psi = new ProcessStartInfo
        {
            FileName = exe,
            Arguments = $"--config \"{configPath}\"",
            WorkingDirectory = Path.GetDirectoryName(exe)!,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
    }
    else
    {
        var csproj = Path.Combine(repo, "src", "MiningcoreBtcSolo", "MiningcoreBtcSolo.csproj");
        psi = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"run --project \"{csproj}\" -c Release --no-build -- --config \"{configPath}\"",
            WorkingDirectory = repo,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
    }

    var p = Process.Start(psi) ?? throw new InvalidOperationException("failed to start gateway");
    p.OutputDataReceived += (_, e) => { if (e.Data != null) Console.WriteLine($"[gateway] {e.Data}"); };
    p.ErrorDataReceived += (_, e) => { if (e.Data != null) Console.Error.WriteLine($"[gateway] {e.Data}"); };
    p.BeginOutputReadLine();
    p.BeginErrorReadLine();
    return p;
}

static void EnsureBitcoind(ref Process? bitcoind, string datadir, string rpcUrl, string user, string pass)
{
    // Probe RPC first
    try
    {
        var probe = new BitcoinRpcClient(rpcUrl, user, pass);
        probe.CallAsync<JsonElement>("getblockchaininfo").GetAwaiter().GetResult();
        Console.WriteLine("bitcoind RPC already up");
        return;
    }
    catch
    {
        // start local
    }

    Directory.CreateDirectory(datadir);
    var bitcoindExe = FindBitcoind();
    Console.WriteLine($"starting bitcoind: {bitcoindExe}");

    bitcoind = Process.Start(new ProcessStartInfo
    {
        FileName = bitcoindExe,
        Arguments = $"-datadir=\"{datadir}\" -regtest -server=1 -txindex=1 -rpcuser={user} -rpcpassword={pass} -rpcport=18443 -rpcallowip=127.0.0.1 -rpcbind=127.0.0.1 -fallbackfee=0.0002 -acceptnonstdtxn=1 -zmqpubhashblock=tcp://127.0.0.1:28332 -zmqpubrawblock=tcp://127.0.0.1:28333",
        UseShellExecute = false,
        RedirectStandardOutput = true,
        RedirectStandardError = true
    }) ?? throw new InvalidOperationException("failed to start bitcoind");

    bitcoind.OutputDataReceived += (_, e) => { if (e.Data != null) Console.WriteLine($"[bitcoind] {e.Data}"); };
    bitcoind.ErrorDataReceived += (_, e) => { if (e.Data != null) Console.Error.WriteLine($"[bitcoind] {e.Data}"); };
    bitcoind.BeginOutputReadLine();
    bitcoind.BeginErrorReadLine();
}

static string FindBitcoind()
{
    var env = Environment.GetEnvironmentVariable("BITCOIND");
    if (!string.IsNullOrWhiteSpace(env) && File.Exists(env))
        return env;

    var pf = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles);
    var candidates = new[]
    {
        Path.Combine(pf, "Bitcoin", "daemon", "bitcoind.exe"),
        Path.Combine(pf, "Bitcoin", "bitcoind.exe"),
        "bitcoind"
    };
    foreach (var c in candidates)
    {
        if (c == "bitcoind")
            return c;
        if (File.Exists(c))
            return c;
    }
    throw new FileNotFoundException("bitcoind not found. Install Bitcoin Core or set BITCOIND.");
}

static void WaitForRpc(BitcoinRpcClient rpc, TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    Exception? last = null;
    while (DateTime.UtcNow < deadline)
    {
        try
        {
            rpc.CallAsync<JsonElement>("getblockchaininfo").GetAwaiter().GetResult();
            Console.WriteLine("RPC ready");
            return;
        }
        catch (Exception ex)
        {
            last = ex;
            Thread.Sleep(500);
        }
    }
    throw new TimeoutException($"RPC not ready: {last?.Message}");
}

static async Task WaitHttpAsync(string url, TimeSpan timeout)
{
    using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
    var deadline = DateTime.UtcNow + timeout;
    Exception? last = null;
    while (DateTime.UtcNow < deadline)
    {
        try
        {
            var resp = await http.GetAsync(url);
            if (resp.IsSuccessStatusCode)
            {
                Console.WriteLine($"HTTP ready {url}");
                return;
            }
        }
        catch (Exception ex)
        {
            last = ex;
        }
        await Task.Delay(400);
    }
    throw new TimeoutException($"HTTP not ready {url}: {last?.Message}");
}

static async Task<string> EnsureWalletAndPayoutAsync(BitcoinRpcClient rpc)
{
    const string wallet = "regtest_solo";

    // Load or create named wallet (descriptor wallets are default on Core 26+)
    var loaded = false;
    try
    {
        var wallets = await rpc.CallAsync<JsonElement>("listwallets", Array.Empty<object>());
        if (wallets.ValueKind == JsonValueKind.Array &&
            wallets.EnumerateArray().Any(w => w.GetString() == wallet))
            loaded = true;
    }
    catch { /* ignore */ }

    if (!loaded)
    {
        try
        {
            await rpc.CallAsync<JsonElement>("loadwallet", new object[] { wallet });
            loaded = true;
            Console.WriteLine($"loaded wallet {wallet}");
        }
        catch (Exception loadEx)
        {
            try
            {
                await rpc.CallAsync<JsonElement>("createwallet", new object[] { wallet });
                loaded = true;
                Console.WriteLine($"created wallet {wallet}");
            }
            catch (Exception createEx)
            {
                // Already exists on disk but not loaded, or race — try load once more
                try
                {
                    await rpc.CallAsync<JsonElement>("loadwallet", new object[] { wallet });
                    loaded = true;
                    Console.WriteLine($"loaded wallet {wallet} after create conflict");
                }
                catch
                {
                    throw new InvalidOperationException(
                        $"unable to create/load wallet '{wallet}': load={loadEx.Message}; create={createEx.Message}");
                }
            }
        }
    }

    if (!loaded)
        throw new InvalidOperationException($"wallet '{wallet}' not available");

    // Prefer bech32 (mainnet-like coinbase script)
    string address;
    try
    {
        address = await rpc.CallAsync<string>("getnewaddress", new object[] { "", "bech32" });
    }
    catch
    {
        address = await rpc.CallAsync<string>("getnewaddress", Array.Empty<object>());
    }

    // Validate with NBitcoin RegTest
    _ = BitcoinAddress.Create(address, Network.RegTest);
    return address;
}

static async Task EnsureChainReadyAsync(BitcoinRpcClient rpc)
{
    var info = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    var blocks = info.GetProperty("blocks").GetInt32();
    if (blocks < 101)
    {
        // generate to a disposable address so GBT height >= 101 (coinbase maturity)
        string sink;
        try { sink = await rpc.CallAsync<string>("getnewaddress", new object[] { "", "bech32" }); }
        catch { sink = await rpc.CallAsync<string>("getnewaddress", Array.Empty<object>()); }

        var need = 101 - blocks;
        Console.WriteLine($"generatetoaddress {need} blocks for mature chain...");
        await rpc.CallAsync<JsonElement>("generatetoaddress", new object[] { need, sink });
    }

    // One more empty tip so our mined block is cleanly next
    info = await rpc.CallAsync<JsonElement>("getblockchaininfo");
    Console.WriteLine($"chain ready height={info.GetProperty("blocks").GetInt32()} chain={info.GetProperty("chain").GetString()}");
}

static void PrintChecklist(List<string> passed, List<string> failures)
{
    var items = new (string key, bool ok)[]
    {
        ("Regtest empty-block path (GBT → submitblock → tip)", passed.Any(p => p.Contains("empty"))),
        ("Mempool multi-tx GBT (witness txs + commitment)", passed.Any(p => p.Contains("mempool"))),
        ("Multi-tx full block assembly + submitblock accepted", passed.Any(p => p.Contains("mempool"))),
        ("Seeded mempool txids present in active-chain block", passed.Any(p => p.Contains("mempool"))),
        ("Stratum V1 path with multi-tx template", passed.Any(p => p.Contains("stratum mempool"))),
        ("Gateway submitblock after mining.submit (multi-tx)", passed.Any(p => p.Contains("stratum mempool"))),
        ("P2P-fast coinbase-only job mined into the active chain",
            passed.Any(p => p.Contains("p2p-fast coinbase-only"))),
        ("Rapid clean fan-out + concurrent late shares", passed.Any(p => p.Contains("clean lifecycle")))
    };

    foreach (var (key, ok) in items)
        Console.WriteLine($"  [{(ok ? "x" : " ")}] {key}");

    Console.WriteLine();
    Console.WriteLine("Before mainnet go-live, still verify manually:");
    Console.WriteLine("  [ ] config.json network=mainnet");
    Console.WriteLine("  [ ] coinbase.address is YOUR mainnet payout (bech32 recommended)");
    Console.WriteLine("  [ ] bitcoind RPC auth + firewall (no public RPC)");
    Console.WriteLine("  [ ] zmq_block_urls / p2p_fast_peer point at your node");
    Console.WriteLine("  [ ] difficulty.min/default sized for your hashrate");
    Console.WriteLine("  [ ] /healthz and /readyz monitored");
    Console.WriteLine("  [ ] sole miner points at this gateway (not a pool)");

    if (failures.Count > 0)
        Console.WriteLine("FAILED items: " + string.Join(" | ", failures));
}

static void TryKill(Process? p)
{
    if (p == null) return;
    try
    {
        if (!p.HasExited)
        {
            p.Kill(entireProcessTree: true);
            p.WaitForExit(5000);
        }
    }
    catch { /* ignore */ }
}

static string FindRepoRoot()
{
    var dir = new DirectoryInfo(AppContext.BaseDirectory);
    while (dir != null)
    {
        if (File.Exists(Path.Combine(dir.FullName, "MiningcoreBtcSolo.sln")))
            return dir.FullName;
        if (File.Exists(Path.Combine(dir.FullName, "config.json")) &&
            Directory.Exists(Path.Combine(dir.FullName, "src")))
            return dir.FullName;
        dir = dir.Parent;
    }
    return Directory.GetCurrentDirectory();
}

static string Env(string key, string fallback)
    => Environment.GetEnvironmentVariable(key) is { Length: > 0 } v ? v : fallback;

// ---------------------------------------------------------------------------
// Minimal Stratum V1 client
// ---------------------------------------------------------------------------

sealed class StratumMinerClient : IDisposable
{
    private TcpClient? _tcp;
    private StreamReader? _reader;
    private StreamWriter? _writer;
    private int _id;
    private readonly Dictionary<int, TaskCompletionSource<JsonElement>> _pending = new();
    private readonly object _gate = new();
    private CancellationTokenSource? _cts;
    private readonly Channel<StratumJob> _jobs = Channel.CreateUnbounded<StratumJob>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    private int _notificationCount;
    private int _cleanNotificationCount;

    public int NotificationCount => Volatile.Read(ref _notificationCount);
    public int CleanNotificationCount => Volatile.Read(ref _cleanNotificationCount);

    public async Task ConnectAsync(string host, int port)
    {
        _tcp = new TcpClient();
        await _tcp.ConnectAsync(host, port);
        _tcp.NoDelay = true;
        var stream = _tcp.GetStream();
        _reader = new StreamReader(stream, Encoding.UTF8);
        _writer = new StreamWriter(stream, new UTF8Encoding(false)) { NewLine = "\n", AutoFlush = true };
        _cts = new CancellationTokenSource();
        _ = Task.Run(() => ReadLoop(_cts.Token));
    }

    public async Task<(string Extranonce1, int Extranonce2Size)> SubscribeAsync(string ua)
    {
        var result = await RequestAsync("mining.subscribe", new object[] { ua });
        // [[notifications], extranonce1, extranonce2_size]
        var en1 = result[1].GetString() ?? throw new InvalidOperationException("no extranonce1");
        var en2Size = result[2].GetInt32();
        return (en1, en2Size);
    }

    public async Task AuthorizeAsync(string user, string pass)
    {
        var result = await RequestAsync("mining.authorize", new object[] { user, pass });
        if (result.ValueKind == JsonValueKind.False)
            throw new InvalidOperationException("authorize rejected");
    }

    public async Task<string> ConfigureVersionRollingAsync(string requestedMask)
    {
        var result = await RequestAsync("mining.configure", new object[]
        {
            new[] { "version-rolling" },
            new Dictionary<string, string> { ["version-rolling.mask"] = requestedMask }
        });
        if (result.ValueKind != JsonValueKind.Object ||
            !result.TryGetProperty("version-rolling", out var enabled) ||
            enabled.ValueKind != JsonValueKind.True ||
            !result.TryGetProperty("version-rolling.mask", out var mask))
        {
            throw new InvalidOperationException($"version-rolling negotiation failed: {result}");
        }
        return mask.GetString() ?? throw new InvalidOperationException("version-rolling mask missing");
    }

    public async Task<StratumJob> WaitForJobAsync(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        return await _jobs.Reader.ReadAsync(cts.Token);
    }

    public async Task<StratumJob> WaitForJobAsync(Func<StratumJob, bool> predicate, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (await _jobs.Reader.WaitToReadAsync(cts.Token))
        {
            while (_jobs.Reader.TryRead(out var job))
            {
                if (predicate(job))
                    return job;
            }
        }
        throw new TimeoutException("mining.notify");
    }

    public async Task<bool> SubmitAsync(
        string worker,
        string jobId,
        string en2,
        string ntime,
        string nonce,
        string? versionBits = null)
    {
        object[] parameters = versionBits == null
            ? new object[] { worker, jobId, en2, ntime, nonce }
            : new object[] { worker, jobId, en2, ntime, nonce, versionBits };
        var result = await RequestAsync("mining.submit", parameters);
        return result.ValueKind != JsonValueKind.False;
    }

    public async Task<string> SubmitExpectErrorAsync(
        string worker,
        string jobId,
        string en2,
        string ntime,
        string nonce,
        string? versionBits = null)
    {
        try
        {
            await SubmitAsync(worker, jobId, en2, ntime, nonce, versionBits);
            throw new InvalidOperationException("mining.submit unexpectedly succeeded");
        }
        catch (InvalidOperationException ex) when (!ex.Message.Contains(
                   "unexpectedly succeeded", StringComparison.Ordinal))
        {
            return ex.Message;
        }
    }

    private async Task<JsonElement> RequestAsync(string method, object[] parameters)
    {
        var id = Interlocked.Increment(ref _id);
        var tcs = new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate) _pending[id] = tcs;

        var payload = JsonSerializer.Serialize(new
        {
            id,
            method,
            @params = parameters
        });
        await _writer!.WriteLineAsync(payload);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var reg = cts.Token.Register(() => tcs.TrySetException(new TimeoutException(method)));
        return await tcs.Task;
    }

    private async Task ReadLoop(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var line = await _reader!.ReadLineAsync(ct);
                if (line == null) break;
                if (string.IsNullOrWhiteSpace(line)) continue;

                using var doc = JsonDocument.Parse(line);
                var root = doc.RootElement;

                if (root.TryGetProperty("method", out var methodEl) && methodEl.GetString() == "mining.notify")
                {
                    var p = root.GetProperty("params");
                    var job = new StratumJob
                    {
                        JobId = p[0].GetString()!,
                        PrevHash = p[1].GetString()!,
                        Coinbase1 = p[2].GetString()!,
                        Coinbase2 = p[3].GetString()!,
                        MerkleBranch = p[4].EnumerateArray().Select(x => x.GetString()!).ToList(),
                        Version = p[5].GetString()!,
                        NBits = p[6].GetString()!,
                        NTime = p[7].GetString()!,
                        CleanJobs = p.GetArrayLength() > 8 && p[8].GetBoolean(),
                        ReceivedTimestamp = Stopwatch.GetTimestamp()
                    };
                    Interlocked.Increment(ref _notificationCount);
                    if (job.CleanJobs)
                        Interlocked.Increment(ref _cleanNotificationCount);
                    _jobs.Writer.TryWrite(job);
                    continue;
                }

                if (root.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.Number)
                {
                    var id = idEl.GetInt32();
                    TaskCompletionSource<JsonElement>? tcs;
                    lock (_gate) _pending.Remove(id, out tcs);
                    if (tcs == null) continue;

                    if (root.TryGetProperty("error", out var err) && err.ValueKind != JsonValueKind.Null)
                    {
                        tcs.TrySetException(new InvalidOperationException($"stratum error: {err}"));
                    }
                    else if (root.TryGetProperty("result", out var result))
                    {
                        tcs.TrySetResult(result.Clone());
                    }
                    else
                    {
                        tcs.TrySetException(new InvalidOperationException("stratum response missing result"));
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"stratum read loop: {ex.Message}");
        }
    }

    public void Dispose()
    {
        try { _cts?.Cancel(); } catch { }
        try { _tcp?.Close(); } catch { }
        _reader?.Dispose();
        _writer?.Dispose();
        _tcp?.Dispose();
        _cts?.Dispose();
    }
}

sealed class StratumJob
{
    public string JobId { get; init; } = "";
    public string PrevHash { get; init; } = "";
    public string Coinbase1 { get; init; } = "";
    public string Coinbase2 { get; init; } = "";
    public List<string> MerkleBranch { get; init; } = new();
    public string Version { get; init; } = "";
    public string NBits { get; init; } = "";
    public string NTime { get; init; } = "";
    public bool CleanJobs { get; init; }
    public long ReceivedTimestamp { get; init; }
}

sealed class TestSequenceSegment : ReadOnlySequenceSegment<byte>
{
    private TestSequenceSegment(ReadOnlyMemory<byte> memory)
    {
        Memory = memory;
    }

    public static ReadOnlySequence<byte> Create(params ReadOnlyMemory<byte>[] segments)
    {
        if (segments.Length == 0)
            return ReadOnlySequence<byte>.Empty;

        var first = new TestSequenceSegment(segments[0]);
        var last = first;
        for (var i = 1; i < segments.Length; i++)
        {
            var next = new TestSequenceSegment(segments[i])
            {
                RunningIndex = last.RunningIndex + last.Memory.Length
            };
            last.Next = next;
            last = next;
        }
        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }
}
