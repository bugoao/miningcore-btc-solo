# MiningcoreBtcSolo

BTC-only **true solo** Stratum V1 gateway for Bitcoin Core.

It pulls templates from `bitcoind` (`getblocktemplate` + longpoll), optionally refreshes via ZMQ / P2P, validates shares locally, assembles full blocks, and calls `submitblock`. Coinbase always pays a **fixed address** from config — no pool accounts, no database, no SV2, no multi-coin.

## Scope

**Good fit when you:**

- Already run Bitcoin Core and want a local solo Stratum entrypoint
- Want share validation and block submission on your machine (not an upstream pool)
- Need a small .NET 10 process with an embedded read-only dashboard

**Does not:**

| Feature | Status |
|---------|--------|
| Pool payout / PPLNS / accounts | Not implemented |
| Username → payout address | Username is worker label only |
| Stratum V2 | Not implemented |
| Multi-coin | BTC only |
| Persistent DB | In-memory metrics only (pending blocks on disk only if submit fails) |

## Features

- `getblocktemplate` + **longpoll** (primary steady-state path)
- Optional **ZMQ** `hashblock` / `rawblock`
- Optional **P2P fast peer** empty-block clean jobs (mainnet only, with safety guards)
- **Chain guard** at startup: refuses to run if `getblockchaininfo.chain` ≠ `network`
- Local share + network-target checks; full block hex assembly before `submitblock`
- **Submit-first queue**: memory enqueue → `submitblock` ASAP; `null` / `duplicate` = success; `inconclusive` is persisted immediately and retried; `prev-blk-not-found` waits for authoritative GBT / chain advance and retries; transport failures are persisted after retry exhaustion (restart recovery); permanent consensus rejects → `data/failed-blocks/`
- Stratum V1: subscribe, authorize, submit, configure (version-rolling), suggest_difficulty, extranonce.subscribe, ping
- Version rolling mask capped at `1fffe000`; `vbrequired` bits never rolled
- Segwit coinbase commitment when GBT provides `default_witness_commitment`
- Time-driven VarDiff with zero-share recovery and bounded burst catch-up for multi-TH/PH miners
- Dashboard + JSON API: `/`, `/api/stats`, `/api/workers`, `/api/shares`, `/api/blocks`, `/healthz`, `/readyz`
- Greppable `ALERT` logs for found blocks and submit failures
- Networks: `mainnet` (`bitcoin` alias), `testnet`, `regtest`, `signet` (limited — see below)
- Regtest harness: empty block + multi-tx mempool + full Stratum path

## Requirements

| Component | Notes |
|-----------|--------|
| .NET 10 SDK | Local build / publish |
| Bitcoin Core | RPC required; ZMQ recommended; P2P optional |
| Linux/Windows | Gateway is pure managed code (+ NetMQ for ZMQ) |

## Quick start

### 1. Configure

Edit [`config.json`](config.json):

```json
{
  "network": "mainnet",
  "bitcoind": {
    "rpc_url": "http://127.0.0.1:8332",
    "rpc_user": "your-rpc-user",
    "rpc_password": "your-rpc-password",
    "zmq_block_urls": ["tcp://127.0.0.1:28332"],
    "p2p_fast_peer": "127.0.0.1:8333"
  },
  "coinbase": {
    "address": "bc1q...your-mainnet-payout...",
    "message": "mcore-solo",
    "segwit_commitment": true
  },
  "difficulty": {
    "min": 1024,
    "max": 1000000000000,
    "default": 8192,
    "target_time_secs": 5
  },
  "runtime": {
    "max_retained_transaction_bytes": 67108864,
    "data_dir": "data"
  }
}
```

Replace RPC credentials and `coinbase.address` before production. Address must match `network` (validated at load).

### 2. Bitcoin Core

```ini
server=1
rpcuser=your-rpc-user
rpcpassword=your-rpc-password
rpcallowip=127.0.0.1
zmqpubhashblock=tcp://127.0.0.1:28332
zmqpubrawblock=tcp://127.0.0.1:28333
```

Keep RPC **private**. ZMQ is optional but recommended for faster tip switch.

### 3a. Run from source

```bash
dotnet publish src/MiningcoreBtcSolo/MiningcoreBtcSolo.csproj -c Release -o build --framework net10.0
./build/MiningcoreBtcSolo --config config.json
```

### 3b. Docker Compose

```bash
# config.json is injected at runtime and is excluded from the image build context
docker compose up -d --build
```

Default ports: **Stratum `3333`**, **Dashboard `7152`**.

Compose mounts `./config.json` read-only at runtime; the image contains no configuration file or RPC credentials. It uses the Docker-managed `miningcore-btc-solo-data` volume at `/app/data` for rare submit-failure recovery, with ownership initialized for the image's non-root UID 10001. On Linux, compose uses `network_mode: host` so the gateway can reach `bitcoind` on localhost. On Docker Desktop (Windows/macOS), switch to bridge networking and publish ports (see comments in `docker-compose.yml`).

### 3c. Prebuilt image (GHCR)

After publishing via GitHub Actions:

```bash
docker pull ghcr.io/<owner>/<repo>:latest
docker volume create miningcore-btc-solo-data
docker run --rm --network host \
  -v "$PWD/config.json:/app/config.json:ro" \
  -v "miningcore-btc-solo-data:/app/data" \
  -e SOLO_CONFIG_PATH=/app/config.json \
  ghcr.io/<owner>/<repo>:latest
```

Tags: `latest`, `sha-<short>`, and version tags from `v*` releases.

## Configuration reference

| Key | Description |
|-----|-------------|
| `network` | `mainnet` \| `bitcoin` \| `testnet` \| `regtest` \| `signet` |
| `log_level` | Console log level (default `Information`) |
| `stratum.listen_addr` / `listen_port` | Stratum bind (default `0.0.0.0:3333`) |
| `stratum.extranonce1_size` / `extranonce2_size` | 1–4 / 1–8 bytes |
| `stratum.idle_timeout_secs` | Drop connections that do not finish subscribe/authorize; authorized miners are exempt (default 3600) |
| `stratum.max_connections` | Max concurrent miners (default 256) |
| `stratum.max_message_bytes` | Maximum UTF-8 bytes in one newline-delimited request (default 65536) |
| `stratum.send_queue_capacity` | Bounded outbound frames per miner; full queues disconnect slow clients (default 64) |
| `stratum.write_timeout_secs` | Per-frame socket write timeout before disconnect (default 10) |
| `stratum.clean_broadcast_timeout_ms` | Deadline for a clean job to reach every live miner's TCP stack; laggards disconnect (default 1500) |
| `stratum.late_share_grace_ms` | Retain clean-retired jobs after delivery for shares already in flight (default 2000) |
| `bitcoind.rpc_*` | JSON-RPC endpoint and Basic auth |
| `bitcoind.zmq_block_urls` | List of ZMQ endpoints (optional) |
| `bitcoind.p2p_fast_peer` | `host` or `host:port` (optional) |
| `coinbase.address` | Fixed payout; must match `network` |
| `coinbase.message` | Coinbase push message (ASCII) |
| `coinbase.segwit_commitment` | Include BIP141 commitment from GBT |
| `difficulty.min` / `max` / `default` | Share difficulty bounds (raise `max` for multi-TH/PH); the effective value is always clamped below the current network difficulty |
| `difficulty.target_time_secs` | Desired seconds between accepted shares per connection |
| `difficulty.retarget_time_secs` | Steady VarDiff interval; zero-share windows lower difficulty (default 30) |
| `difficulty.retarget_share_burst` | Early upward retarget after N accepted shares; work estimate is difficulty-weighted (default 8) |
| `difficulty.variance_percent` | Stable share-interval band that does not retarget (default 30%) |
| `difficulty.retarget_smoothing` | New-window EWMA weight; lower values reduce difficulty oscillation (default 0.25) |
| `difficulty.max_step_up` / `max_step_up_burst` | Steady vs flood max × multiplier (2 / 32) |
| `difficulty.max_step_down` | Max downward × step (default 0.5) |
| `runtime.keep_old_jobs` | Job cache for late submits |
| `runtime.max_retired_jobs` | Hard memory bound for clean-retired full templates (default 8) |
| `runtime.retired_job_max_age_secs` | Hard retirement age if delivery/grace tracking cannot complete (default 15) |
| `runtime.max_retained_transaction_bytes` | Transaction byte budget for retained templates; active job is never reclaimed (default 64 MiB) |
| `runtime.data_dir` | Root for `pending-blocks/` + `failed-blocks/` (default `data/`) |
| `api.*` | Dashboard bind / enable |

Environment: `SOLO_CONFIG_PATH` overrides config path. CLI: `--config` / `-c`.

### Networks

| Value | Address validation | Default P2P port | P2P empty clean job |
|-------|--------------------|------------------|---------------------|
| `mainnet` / `bitcoin` | Main | 8333 | Yes (guarded) |
| `testnet` | TestNet | 18333 | No |
| `regtest` | RegTest | 18444 | No |
| `signet` | TestNet scripts (approx.) | 38333 | No |

P2P empty-job guards (mainnet): skip retarget height `% 2016 == 0`, skip when `vbrequired != 0`, require matching nbits + valid PoW on announcement.

Startup maps `network` → Core `chain` (`main` / `test` / `regtest` / `signet`) and **exits** on mismatch. IBD or headers-ahead-of-blocks logs a warning but does not exit.

## How it works

```
bitcoind ──RPC──► TemplateEngine ──notify──► Stratum V1 miners
          │            ▲
          ├── ZMQ ─────┤
          └── P2P ─────┘ (optional empty clean job)

coinbase.scriptPubKey = config.coinbase.address (fixed)
Block found ──► BlockSubmitQueue (memory) ──submitblock──► bitcoind
                     │ (inconclusive immediately; transport failure after retries)
                     └── fsync data/pending-blocks/  (restart recovery)
```

1. **ChainGuard**: `getblockchaininfo.chain` must match `network` (fail closed).
2. Startup GBT + longpoll for tip / mempool template updates.
3. **P2P** (optional): empty clean job only (`clean_jobs=true`) on headers/cmpctblock when safe.
4. **ZMQ** and **longpoll** both schedule full GBT; first successful apply wins; identical template key → skip (key includes ordered-txid fingerprint).
5. P2P fast work is active mining work only; dashboard state and VarDiff job re-pushes use the latest authoritative full GBT.
6. On `mining.submit`, rebuild coinbase + merkle + header; check share and block targets.
7. If the header meets the network target: ack `mining.submit`, enqueue **submitblock ASAP** (no disk on the happy path). `inconclusive` is durable and retried; `prev-blk-not-found` is deferred and retried immediately when authoritative GBT confirms a chain advance, with timed/disk recovery as fallback. Permanent consensus rejects are archived to `failed-blocks/`. Grep logs for `ALERT`.
8. Metrics and recent shares/blocks stay in memory for the dashboard.

### Template refresh priority (solo)

1. **P2P** empty clean job (mainnet, when safe) — no GBT on success path
2. **ZMQ** → full GBT (race with longpoll)
3. **Longpoll** → full GBT (race with ZMQ)
4. Unchanged template key → `skip_job` (no stratum notify)

### Block submit outcomes

| Core result | Gateway behavior |
|-------------|------------------|
| `null` (accepted) | Success; drop pending file if any; refresh template |
| `duplicate` | Success (already on chain / known) |
| `inconclusive` / `duplicate-inconclusive` | Uncertain; persist candidate and retry; never archive as a consensus rejection |
| Other string | Rejected; archive hex under `failed-blocks/` |
| RPC / network error | Retry with backoff; persist to `pending-blocks/` if still failing |

## Regtest validation (pre-mainnet)

Prove **share → full block (incl. mempool txs) → submitblock → active chain** before pointing real hashrate at mainnet.

**Prerequisites:** .NET 10 SDK, Bitcoin Core (`bitcoind` on `PATH` or under `Program Files\Bitcoin\daemon`, or set `BITCOIND`).

```bash
# Windows
scripts\run-regtest-validation.cmd

# any OS
dotnet publish src/MiningcoreBtcSolo/MiningcoreBtcSolo.csproj -c Release -o build --framework net10.0
dotnet run --project src/MiningcoreBtcSolo.Regtest -c Release -- all
```

To execute the complete 13-mode pre-mainnet suite (including the two large-mempool
stress modes), run `powershell -ExecutionPolicy Bypass -File scripts/run-regtest-validation.ps1 suite`.

| Mode | What it runs |
|------|----------------|
| `all` (default) | Empty block + mempool multi-tx + Stratum multi-tx |
| `direct` | Empty + mempool library paths |
| `mempool` | Multi-tx library path only |
| `stratum` | Gateway + Stratum V1 with mempool txs |
| `vardiff` | Offline VarDiff regression checks; no bitcoind required |
| `encoding` | Offline encoding, reorg, and P2P-fast boundary checks |
| `shutdown` | Offline submit-queue shutdown persistence check |
| `safety` | Offline block-ownership, difficulty, service-failure, readiness, and duplicate-share checks |
| `synthetic-gbt` | Offline 10k/20k transaction GBT JSON parse, fingerprint, Merkle, and job-build stress checks |
| `large-mempool` | Real bitcoind + 10,001 independent mempool txs + full-block submit/active-chain confirmation |
| `stress` | `synthetic-gbt` followed by `large-mempool` |
| `p2p-fast` | Real Core header + coinbase-only P2P-fast job + mined block submit/active-chain confirmation |
| `lifecycle` | `p2p-fast` block validation + 16 Stratum miners + rapid clean/late/stale/ZMQ/longpoll lifecycle checks |

The `suite` script runner executes all 13 modes in the table in a fresh process for each mode.
See [VALIDATION.md](VALIDATION.md) for the latest complete validation evidence.

Modes `vardiff`, `encoding`, `shutdown`, `safety`, and `synthetic-gbt` are offline. Other modes start a local `bitcoind -regtest` if RPC is not already up, seed bech32 / p2sh-segwit spends, mine via share assembly (and Stratum for mode `all` / `stratum`), then confirm tip hash + txids on the active chain. The `large-mempool` mode creates confirmed fan-out UTXOs, broadcasts 10,001 independent P2WPKH spends, and requires every txid in the submitted block.

The harness creates an isolated temporary bitcoind datadir, gateway config, and submit directory, then removes them on exit. Set `REGTEST_WORK_DIR` to retain them for debugging.

Build artifacts are **gitignored** (`build/`, `bin/`, `obj/`, `data/`). Clean with:

```bash
git clean -fdX
```

**Mainnet go-live checklist** (after regtest is green):

1. `network: "mainnet"` and a real `coinbase.address` (bech32 recommended)
2. RPC credentials + firewall (RPC/API/Stratum not public); gateway refuses start if bitcoind `chain` ≠ config
3. ZMQ / optional `p2p_fast_peer` pointed at your node
4. `difficulty.min` / `default` / `max` sized for your hashrate
5. Persist `runtime.data_dir` (compose volume `miningcore-btc-solo-data`) for rare submit-failure recovery; monitor `ALERT` and `/api/blocks`
6. Miners connect **only** to this gateway

## API (read-only)

| Path | Purpose |
|------|---------|
| `GET /` | Dashboard HTML |
| `GET /api/stats` | Height, hashrate (`hashrate_hps`), share/block counters, best share |
| `GET /api/workers` | Connected workers (hashrate, difficulty, shares) |
| `GET /api/shares` | Recent accepted/rejected shares |
| `GET /api/blocks` | Recent submit results (`submitted` / `duplicate` / `inconclusive` / `rejected`) |
| `GET /healthz` | Liveness |
| `GET /readyz` | Template ready + last refresh OK (`503` when not ready) |

`/api/stats` exposes SI hashrate as `hashrate_hps` (preferred). `hashrate_th_s` is kept for older clients (`hps / 1e12`).
It also separates job-lifecycle outcomes as `shares_late` (accepted during grace),
`shares_stale` (known but already reclaimed), and `shares_unknown_job`. Clean fan-out
health is exposed as `clean_broadcasts` and `clean_broadcast_client_timeouts`.
Share response timing is split into `share_validation_avg_ms` / `max_ms`, accepted ACK
queue latency, and accepted ACK socket-write latency. `share_accepted_ack_written` only
advances after `NetworkStream.WriteAsync` hands the response to the TCP stack; comparing
it with `share_accepted_ack_queued` reveals responses lost to slow or closed connections.

## CI / container images

| Workflow | Trigger | Action |
|----------|---------|--------|
| [`.github/workflows/ghcr-build.yml`](.github/workflows/ghcr-build.yml) | `workflow_dispatch`, tags `v*` | Build `linux/amd64` image → **GHCR** |

Image name: `ghcr.io/<lowercase-owner>/<lowercase-repo>`

| Tag | When |
|-----|------|
| `latest` | Every successful GHCR build |
| `sha-<short>` | Every successful GHCR build |
| `v1.2.3` / `1.2.3` / `1.2` | Push git tag `v1.2.3` (semver) |

**Publish an image:**

1. Push code to GitHub (enable Actions for the repo).
2. **Actions** → **Build And Push GHCR Image** → **Run workflow**, **or** create and push a tag:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```
3. Package appears under **Packages** on the repo / org page.

Package visibility: GitHub → Packages → package settings (public/private as needed). First pull of a private package requires:

```bash
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin
```

`GITHUB_TOKEN` is provided automatically to the workflow; no extra secret is required for same-repo GHCR push (needs `packages: write`).

## Repository layout

```
├── config.json                 # mainnet template (edit before production)
├── config.regtest.json         # regtest template
├── config.umbrel.json          # Umbrel-oriented template
├── scripts/                    # regtest one-shot helpers
├── docker-compose.yml
├── docker-compose.umbrel.yml
├── Dockerfile
├── MiningcoreBtcSolo.sln
├── .github/workflows/          # GHCR publish
└── src/
    ├── MiningcoreBtcSolo/      # gateway
    │   ├── Api/                # dashboard + JSON API
    │   ├── Metrics/
    │   ├── P2p/
    │   ├── Rpc/                # BitcoinRpcClient, ChainGuard
    │   ├── Share/
    │   ├── Stratum/
    │   ├── Submit/             # BlockSubmitQueue
    │   ├── Template/
    │   └── Util/
    └── MiningcoreBtcSolo.Regtest/   # pre-mainnet harness
```

## License

MIT
