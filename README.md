# Tick to Signal

A real-time Binance market data pipeline built with C++ and kdb+/KDB-X. Dual feed handlers (trades + L5 order book) stream into a tickerplant-fanout architecture with batched analytics, real-time signals, P&L tracking, and durability-log replay. A historical data store (`hdb_binancedata/`) supports offline research and an in-progress ML feature pipeline based on López de Prado's *Advances in Financial Machine Learning*.

Inspired by *Building Real-Time Event-Driven KDB-X Systems* by Data Intellect.

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
                                └────────────────────────────► ────────┤
                                                                       │
                                  ┌─────────────┬─────────────┬────────┘
                                  ▼             ▼             ▼          ▼
                               RDB:5017     RTE:5015      TEL:5016   PNL:5018
                               (queries)    (analytics)   (latency)  (P&L)
```

Each downstream process auto-reconnects with exponential backoff. The TP writes a durability log per day; subscribers can replay it on restart.

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
| MLE       | 5032  | TP            | ML engine — dollar-imbalance bars, threshold adaptation|

## Project Layout

```
t2s/
├── cpp/                      # Feed handlers (C++)
│   ├── include/              # Public headers
│   ├── src/                  # Implementations + main()s
│   └── third_party/kdb/      # k.h
├── kdb/
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
│   │   ├── init.q
│   │   └── pubsub.q
│   └── utils/                # Operational tooling
│       ├── binanceLoader.q   # Historical data loader
│       ├── hdbUtils.q        # HDB switching, queries
│       └── logmgr.q          # Durability log management
├── config/                   # Feed handler JSON configs
├── dashboards/               # KX Dashboards (Analytics, DataFlow, FH, Trades/Quotes)
├── hdb_binancedata/          # Partitioned historical trades (gitignored)
├── notebooks/                # Jupyter research notebooks
├── markdown_docs/            # Design notes, guides
├── CMakeLists.txt
├── install_kdb.sh
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

Individual processes can be started manually. From the project root:
```bash
q kdb/tick/tp.q
q kdb/tick/chained_tp.q
q kdb/tick/wdb.q
q kdb/tick/rdb.q
q kdb/analytics/rte.q
q kdb/analytics/tel.q
q kdb/analytics/sig.q
q kdb/analytics/pnl.q
q kdb/analytics/mle.q
./build/trade_feed_handler config/trade_feed_handler.json
./build/quote_feed_handler config/quote_feed_handler.json
```

## Configuration

Feed handler runtime config lives in `config/`:

- `trade_feed_handler.json` — symbols, TP host/port, reconnect backoff, log level/file
- `quote_feed_handler.json` — same fields, used by the L5 quote handler

Each q process has its own config block at the top of its file (e.g. `.tp.cfg`, `.rdb.cfg`). Edit and reload to change ports, retention, batch intervals, etc.

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

// PNL (port 5018) — positions and P&L
// (see kdb/analytics/pnl.q for the query interface)

// All processes — standardized health check
.health[]
```

## Dashboards

Four KX Dashboard JSONs are provided in `dashboards/`:

- `TradesQuotes.json` — live trade and quote tables
- `Analytics.json` — VWAP, volatility, OBI charts
- `FeedhandlerMonitoring.json` — FH connection state and message rates
- `DataFlowMonitoring.json` — per-process throughput across the pipeline

Import into KX Dashboards and point each panel at the appropriate process port.

## Research Workflow

The historical side of the project supports offline analysis and ML research against partitioned trade data.

**Loading historical data.** `kdb/utils/binanceLoader.q` downloads daily trade ZIPs from `data.binance.vision` and loads them into a date-partitioned HDB at `hdb_binancedata/`. Edit the `.cfg` block at the top to set paths and symbols, then run:

```bash
q kdb/utils/binanceLoader.q
```

**Querying the HDB.** `kdb/utils/hdbUtils.q` provides switch-and-query helpers:

```q
\l kdb/utils/hdbUtils.q
.hdb.use[`:hdb_binancedata]
.hdb.tables[]
.hdb.dateRange[]
.hdb.rowCounts[`trade; 2026.01.14; 2026.01.20]
```

**ML feature pipeline (in progress).** `kdb/ml/` contains an in-progress implementation of feature engineering primitives from López de Prado's *Advances in Financial Machine Learning*. Currently includes dollar-imbalance bars (`afml.q`, `features.q`) — see `markdown_docs/dollar_imbalance_bars_guide.md` for design notes. Expect breaking changes.

**Notebooks.** Jupyter notebooks for ad-hoc analysis live in `notebooks/`. They connect to the running processes or directly to the HDB.

## Documentation

- `markdown_docs/` — design notes, intraday writedown patterns, compression notes, dollar-imbalance bars guide
- Architecture Decision Records and the project white paper: [tick-to-signal-docs](https://github.com/PhilSing24/tick-to-signal-docs)
- Inline ADR references in C++ headers (`@see docs/decisions/adr-NNN-*.md`) point to the docs repo above

## Known Gaps

The ML pipeline (`kdb/ml/`) is actively developed and APIs may change. The live tick pipeline is the stable, primary deliverable.

## License

Private repository.
