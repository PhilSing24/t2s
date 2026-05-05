# Tick to Signal

A real-time Binance market data pipeline built with C++ and KDB-X. Dual feed handlers (trades + L5 order book) stream into a tickerplant-fanout architecture with batched analytics, RSI-based signal generation, simulated P&L tracking, and durability-log replay (WDB recovers from disconnects). A historical data store (`hdb_binancedata/`) supports offline research and feature-engineering primitives drawn from López de Prado's *Advances in Financial Machine Learning*.

Architecture patterns originally inspired by *Building Real-Time Event-Driven KDB-X Systems* by Data Intellect; extended with TLS-verified WS/REST, TCP keepalive and dead-connection detection, per-stream sequence-gap surfacing, async snapshot fetching, replay-on-reconnect, and a multi-language test suite.

## Architecture

```
Binance Trade Stream ──WS──► Trade FH ──┐
                                        ├──IPC──► TP:5010 ──► durability log
Binance Depth Stream ──WS──► Quote FH ──┘            │
            │                                        │
            └──REST──► (snapshot)                    │
                                                     │
              ┌──────────────────┬──────────────────┬┴────────────────┐
              ▼                  ▼                  ▼                 ▼
           WDB:5011          SIG:5012            MLE:5032          CTP:5014
           (HDB writer)      (RSI signals)       (ML features)     (1s batches)
                                │ positions                            │
                                └──────────────────────────────────────┤
                                                                       │
                                  ┌─────────────┬─────────────┬────────┤
                                  ▼             ▼             ▼        ▼
                               RDB:5017     RTE:5015      TEL:5016  PNL:5018
                               (queries)    (analytics)   (latency)  (P&L)
```

Each downstream process auto-reconnects with exponential backoff. The TP writes a durability log per day. **WDB persists a checkpoint and replays missed data from the durability log on reconnect** (Phase 4); other subscribers (CTP, RDB, RTE, TEL, SIG, PNL) are best-effort live analytics that may have gaps after disconnect. Feed handler TLS connections to Binance are fully verified (peer cert + hostname), use TCP keepalive plus a 30s WebSocket idle timeout to detect dead connections within ~90s, and TP tracks per-stream sequence-number gaps so missed messages are surfaced rather than silent.

## Components

| Component | Port  | Subscribes to | Role                                                   |
|-----------|-------|---------------|--------------------------------------------------------|
| Trade FH  | —     | Binance WS    | Trade feed handler (C++)                               |
| Quote FH  | —     | Binance WS    | L5 order book feed handler (C++) with REST snapshots   |
| TP        | 5010  | FHs           | Tickerplant — pub/sub hub with daily durability log    |
| CTP       | 5014  | TP            | Chained tickerplant — 1s batching for downstream fanout|
| WDB       | 5011  | TP            | Write-only DB — buffers and writes to HDB at EOD       |
| RDB       | 5017  | CTP           | Real-time DB — in-memory queries, 60min retention      |
| RTE       | 5015  | CTP           | Analytics — VWAP, realized vol, OBI (EMA-smoothed)     |
| TEL       | 5016  | CTP           | Telemetry — feed handler latency aggregation           |
| SIG       | 5012  | TP, CTP       | RSI signal generator — publishes positions to CTP      |
| PNL       | 5018  | CTP           | P&L and position monitoring                            |
| MLE       | 5032  | TP            | ML engine (research, not auto-started) — dollar-imbalance bars, threshold adaptation |

## Project Layout

```
t2s/
├── cpp/                      # Feed handlers (C++)
│   ├── include/              # Public headers
│   │   ├── config.hpp                # JSON config loader for FH binaries
│   │   ├── logger.hpp                # spdlog setup helper
│   │   ├── order_book_manager.hpp    # L5 book reconstruction + state machine
│   │   ├── quote_feed_handler.hpp    # Quote FH class declaration
│   │   ├── rest_client.hpp           # HTTPS client for Binance REST (snapshots)
│   │   ├── snapshot_worker.hpp       # Async snapshot fetcher (worker thread + bounded queue)
│   │   ├── socket_utils.hpp          # TCP keepalive helper
│   │   └── trade_feed_handler.hpp    # Trade FH class declaration
│   ├── src/                  # Implementations + main entry points
│   │   ├── quote_feed_handler.cpp    # Quote FH class implementation
│   │   ├── quote_fh_main.cpp         # Quote FH binary entry point
│   │   ├── trade_feed_handler.cpp    # Trade FH class implementation
│   │   └── trade_fh_main.cpp         # Trade FH binary entry point
│   └── third_party/
│       ├── catch2/                   # Vendored Catch2 v3 amalgamation (C++ tests)
│       └── kdb/                      # k.h and c.o for kdb+ IPC
├── kdb/
│   ├── schemas.q             # Shared table schemas (single source of truth)
│   ├── tick/                 # Tickerplant chain
│   │   ├── tp.q              # Primary tickerplant (durability log)
│   │   ├── chained_tp.q      # Batched fanout to downstream
│   │   ├── rdb.q             # Real-time DB
│   │   └── wdb.q             # Write-only DB → HDB
│   ├── analytics/            # Real-time analytics
│   │   ├── rte.q             # VWAP, vol, OBI
│   │   ├── tel.q             # Latency telemetry
│   │   ├── sig.q             # RSI signals
│   │   ├── pnl.q             # P&L tracking
│   │   └── mle.q             # ML engine (dollar-imbalance bars)
│   ├── ml/                   # ML feature pipeline (in progress)
│   │   ├── afml.q            # AFML primitives
│   │   └── features.q        # Feature engineering
│   ├── pubsub/               # KDB-X di.pubsub module
│   │   ├── init.q            # Module bootstrap: defines subscribable tables, fetches schemas
│   │   └── pubsub.q          # subscribe/publish primitives + utilities (sub clear, EOD/EOP broadcast)
│   └── utils/                # Operational tooling
│       ├── tradeLoader.q     # Historical trade loader (single-date or date range, interactive or scripted)
│       ├── fundingLoader.q   # Funding rate loader (incremental, paginated, splayed)
│       ├── hdbUtils.q        # HDB switching, queries
│       └── logmgr.q          # Durability log management
├── tests/                    # Test suite (bash + q + C++)
│   ├── run_tests.sh          # Test runner - dispatches .q, .sh, and build/test_* binaries
│   ├── t_lib.q               # Shared assertion + sandbox helpers
│   ├── test_schemas.q        # Schema integrity assertions
│   ├── test_smoke.sh         # Per-process load + .health[] smoke test
│   ├── test_wdb_eod.sh       # End-to-end WDB EOD persistence test
│   ├── wdb_eod_body.q        # Q assertions invoked by test_wdb_eod.sh
│   ├── test_order_book.cpp   # C++ unit tests for OrderBookManager (Catch2)
│   └── test_snapshot_worker.cpp  # C++ unit tests for SnapshotWorker (Catch2)
├── config/                   # Feed handler JSON configs
├── dashboards/               # KX Dashboards (Analytics, DataFlow, FH, Trades/Quotes)
├── hdb/                      # Live HDB partitions (gitignored, populated at EOD)
├── tmp/                      # WDB intraday writedown directory (gitignored)
├── hdb_binancedata/          # Historical research HDB (gitignored)
├── notebooks/                # Jupyter research notebooks
├── markdown_docs/            # Design notes, guides
├── CMakeLists.txt
├── install_kdb.sh
├── check_eod.sh              # Post-midnight verification script (HDB partition + WDB logs)
├── start.sh                  # Start all (tmux)
└── stop.sh                   # Stop all
```

## Prerequisites

- kdb+ 4.x or KDB-X (with `di.pubsub` module)
- C++17 compiler, CMake 3.16+
- Boost (Beast, Asio), OpenSSL, RapidJSON, spdlog

A helper script `install_kdb.sh` is provided for kdb+ setup.

## Build

```bash
cmake -S . -B build
cmake --build build
```

This produces two binaries: `trade_feed_handler` and `quote_feed_handler`.

## Run

Start everything via tmux:
```bash
./start.sh
```

Stop everything:
```bash
./stop.sh
```

`start.sh` brings up TP, CTP, WDB, RDB, RTE, TEL, SIG, PNL plus both feed handlers. MLE is research-only and is started manually when needed:
```bash
q kdb/analytics/mle.q
```

Individual processes can also be started manually. From the project root:
```bash
q kdb/tick/tp.q
q kdb/tick/chained_tp.q
q kdb/tick/wdb.q
q kdb/tick/rdb.q
q kdb/analytics/rte.q
q kdb/analytics/tel.q
q kdb/analytics/sig.q
q kdb/analytics/pnl.q
./build/trade_feed_handler config/trade_feed_handler.json
./build/quote_feed_handler config/quote_feed_handler.json
```

## Configuration

Feed handler runtime config lives in `config/`:

- `trade_feed_handler.json` — symbols, TP host/port, reconnect backoff, log level/file
- `quote_feed_handler.json` — same fields, used by the L5 quote handler

Each q process has its own config block at the top of its file (e.g. `.tp.cfg`, `.rdb.cfg`). Edit and reload to change ports, retention, batch intervals, etc.

Pipeline-wide table schemas live in `kdb/schemas.q` and are loaded by every q process. Adding or modifying a column there propagates everywhere on the next restart; field indices used by TP gap detection, TEL latency parsing, and RTE analytics are all derived from the schema (no magic numbers).

## Tests

Run the full suite from the project root:
```bash
./tests/run_tests.sh
```

The runner discovers `tests/test_*.q`, `tests/test_*.sh`, and any compiled binaries at `build/test_*`, then reports pass/fail per file. Current coverage:

- **`test_schemas.q`** — schemas.q column counts, types, and derived index positions. Catches accidental schema changes that would break the rest of the pipeline.
- **`test_smoke.sh`** — starts each q process (tp, ctp, rdb, wdb, sig, pnl, rte, tel) in isolation against test ports, asserts `.health[]` returns a sane response. Catches load-time errors and missing `.health[]` interface.
- **`test_wdb_eod.sh`** — full TP→WDB integration test: publishes synthetic data, forces EOD, verifies a partition lands in the sandbox HDB with correct row counts. Validates the EOD persistence path end-to-end.
- **`build/test_order_book`** — C++ unit tests (Catch2) for `OrderBookManager`: state machine (INIT→SYNCING→VALID→INVALID), snapshot truncation/padding, delta semantics (insert/update/delete via qty=0), sequence-gap detection, multi-symbol independence. Built when `cpp/third_party/catch2/catch_amalgamated.{hpp,cpp}` are present (download from https://github.com/catchorg/Catch2/releases).

Tests run on isolated ports (production + 10000) so they're safe to run while the live pipeline is up. Sandbox state goes under `tests/sandbox/` and is auto-cleaned on success, preserved on failure for inspection.

## Query Interfaces

Connect with `q -p` or any kdb+ client. A few examples:

```q
// RDB (port 5017) — recent trades and quotes
select from trade_binance where sym=`BTCUSDT
.rdb.tradeSummary[]
.rdb.lastQuotes[10]

// RTE (port 5015) — analytics
.rte.getVwap[]
.rte.getOBI[`smooth]
.rte.getOBIHistory[`BTCUSDT;30]
.rte.getVolComparison[]

// TEL (port 5016) — feed handler latency
.tel.vsFhStatus[]

// TP (port 5010) — durability log status, sequence tracking, replay
.tp.statusDict[]              / counters: gaps, dups, tpSeqNo, log chunks
.tp.lastAccepted[`trade]      / highest fhSeqNo accepted from trade FH
.tp.currentSeqNo[]            / current monotonic tpSeqNo
.tp.replayFrom[`trade_binance; fromSeq]   / replay slice from log

// WDB (port 5011) — Phase 4 replay state
.wdb.replayStatus[]           / lastTpSeqNo, replayMode, replay counters

// PNL (port 5018) — positions and P&L
// (see kdb/analytics/pnl.q for the query interface)

// All processes — standardized health check
.health[]
```

WDB persists its replay checkpoint to `$T2S_TMP_DIR/wdb.lastTpSeqNo` after every successful flush. On restart, it loads this and asks TP to replay everything since, so disk-persisted data is recoverable across WDB or TP restarts.

## Dashboards

Four KX Dashboard JSONs are provided in `dashboards/`:

- `TradesQuotes.json` — live trade and quote tables
- `Analytics.json` — VWAP, volatility, OBI charts
- `FeedhandlerMonitoring.json` — FH connection state and message rates
- `DataFlowMonitoring.json` — per-process throughput across the pipeline

Import into KX Dashboards and point each panel at the appropriate process port.

## Research Workflow

The historical side of the project supports offline analysis and ML research against partitioned trade data.

**Loading historical data.** `kdb/utils/tradeLoader.q` downloads daily trade ZIPs from `data.binance.vision` and loads them into a date-partitioned HDB at the path defined by `HDB_BINANCE_DIR`. Paths come from `BINANCE_DOWNLOAD_DIR` and `HDB_BINANCE_DIR` (set in the Runtime Paths section above) with relative-path fallbacks. Two ways to use it:

```bash
# Interactive: defines functions, drops into REPL
q kdb/utils/tradeLoader.q
```

```q
downloadAndLoad[2026.01.17; `BTCUSDT`ETHUSDT`SOLUSDT]
downloadAndLoadRange[2026.01.10; 2026.01.20; `BTCUSDT`ETHUSDT]
```

```bash
# Scripted: runs the range non-interactively, exits when done
q kdb/utils/tradeLoader.q -range 2026.01.10 2026.01.20 BTCUSDT,ETHUSDT
```

Range mode skips dates whose partition already exists (so backfills are idempotent), polite-sleeps between downloads to respect Binance rate limits, continues on per-date failures, and prints a summary of loaded/skipped/failed dates at the end.

**Querying the HDB.** `kdb/utils/hdbUtils.q` provides switch-and-query helpers:

```q
\l kdb/utils/hdbUtils.q
.hdb.use[`:hdb_binancedata]
.hdb.tables[]
.hdb.dateRange[]
.hdb.rowCounts[`trade; 2026.01.14; 2026.01.20]
.hdb.loadBySym[`trade; `BTCUSDT; 2026.01.14; 2026.01.20]
```

**ML feature pipeline (in progress).** `kdb/ml/` contains an in-progress implementation of feature engineering primitives from López de Prado's *Advances in Financial Machine Learning*. Currently includes dollar-imbalance bars (`afml.q`, `features.q`) — see `markdown_docs/dollar_imbalance_bars_guide.md` for design notes. Expect breaking changes.

**Notebooks.** Jupyter notebooks for ad-hoc analysis live in `notebooks/`. They connect to the running processes or directly to the HDB.

## Documentation

- `markdown_docs/` — design notes, intraday writedown patterns, compression notes, dollar-imbalance bars guide
- Architecture Decision Records and the project white paper: [tick-to-signal-docs](https://github.com/PhilSing24/tick-to-signal-docs)
- Inline ADR references in C++ headers (`@see docs/decisions/adr-NNN-*.md`) point to the docs repo above

## Known Gaps

The ML pipeline (`kdb/ml/`) is actively developed and APIs may change. The live tick pipeline is the stable, primary deliverable.

C++ unit tests currently cover `OrderBookManager` (17 cases, 73 assertions) and `SnapshotWorker` (13 cases, 51 assertions). Feed handler classes don't yet have isolated tests; they're exercised end-to-end via the live pipeline.

WDB replay-on-reconnect (Phase 4) reads only the current day's durability log. Disconnects spanning midnight UTC will not catch up data from the prior day — operationally rare on a single machine, but a known limitation.

## License

MIT — see [LICENSE](LICENSE) file.
