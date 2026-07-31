# Pre-mainnet Validation Report

Status: **PASS**

Validation date: 2026-07-31 (Asia/Shanghai)

Environment:

- Windows 10.0.26200 x64
- .NET SDK 10.0.302, runtime 10.0.10
- Bitcoin Core v28.1.0
- Release configuration

## Build and dependency checks

`dotnet restore MiningcoreBtcSolo.sln` and
`dotnet build MiningcoreBtcSolo.sln -c Release --no-restore` completed with
0 warnings and 0 errors. `dotnet list MiningcoreBtcSolo.sln package --vulnerable
--include-transitive` reported no known vulnerable packages.

## Regtest harness

All 13 modes were run sequentially in isolated temporary datadirs. Every mode
returned exit code 0.

| Mode | Result | Evidence |
|---|---|---|
| `all` | PASS | Empty block, 3-transaction SegWit GBT, and Stratum `mining.submit` all reached the active chain |
| `direct` | PASS | Empty block and multi-transaction direct assembly/submission passed |
| `mempool` | PASS | All 3 seeded transaction IDs were confirmed in a 4-transaction block |
| `stratum` | PASS | Accepted Stratum share, `submitblock`, dashboard block/worker/share metrics, and 4-transaction active-chain block |
| `vardiff` | PASS | Deterministic silent-window, burst, weighted grace, smoothing, and boundary checks |
| `encoding` | PASS | Compact target, BIP34, Merkle, reorg, and P2P-fast boundary checks |
| `shutdown` | PASS | Undrained candidate persisted for restart recovery |
| `safety` | PASS | Inconclusive/missing-parent retry, block ownership, duplicate tracking, parser, metrics, publication lock, job retirement, and difficulty checks |
| `synthetic-gbt` | PASS | 10,000 and 20,000 transaction parse/fingerprint/Merkle/build cases |
| `large-mempool` | PASS | All 10,001 independent transactions entered a 10,002-transaction, 3.73 MB active-chain block |
| `stress` | PASS | Synthetic 10k/20k followed by a second complete 10,001-transaction real-Core run |
| `p2p-fast` | PASS | Coinbase-only P2P-fast block accepted at height 104 with the configured 50 BTC payout |
| `lifecycle` | PASS | 16 miners, 4 clean broadcasts, 16 late shares, stale/unknown classification, ZMQ/longpoll interaction, 0 client timeouts |

The 20,000-transaction synthetic case retained a 1.72 MiB contiguous raw
transaction payload and measured 11.9 MiB managed allocation. In `stress`, the
10,001-transaction template was assembled, mined, submitted, and confirmed in
about 0.5 seconds after mempool construction.

## Reproduction

Run the full suite from the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-regtest-validation.ps1 suite
```

The suite starts a local regtest node when needed and removes its temporary
datadir after each mode. Do not run real-Core modes concurrently unless each
process is assigned distinct RPC, P2P, ZMQ, Stratum, and API ports.
