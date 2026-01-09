# CONTEXT.md - Project Status for Claude Sessions

*Last updated: 2026-01-07*

## Project Overview

Real-time Binance market data → C++ Feed Handlers → kdb+ pipeline (TP → RDB → RTE → TEL)

## Architecture
```
Binance WebSocket ──► Trade FH (C++) ──┬──► TP:5010 ──┬──► RDB:5011 (storage)
                                       │              ├──► RTE:5012 (analytics)
Binance WebSocket ──► Quote FH (C++) ──┘              ├──► TEL:5013 (telemetry)
                                                      └──► LOG:5014 (log manager)

Control: CTL:5000 (dashboard control)
Log files: logs/*.trade.log, logs/*.quote.log (auto-replay on startup)
```

## Component Status

| File | Purpose | Status |
|------|---------|--------|
| `cpp/src/trade_feed_handler.cpp` | Trade WebSocket, JSON parse, IPC publish | ✓ Complete |
| `cpp/src/quote_feed_handler.cpp` | L5 quote handler with snapshot reconciliation | ✓ Complete |
| `cpp/include/trade_feed_handler.hpp` | Trade handler class definition | ✓ Complete |
| `cpp/include/quote_feed_handler.hpp` | Quote handler class definition | ✓ Complete |
| `cpp/include/order_book_manager.hpp` | Flat-array L5 order book, state machine | ✓ Complete |
| `cpp/include/rest_client.hpp` | REST snapshot client | ✓ Complete |
| `cpp/include/config.hpp` | JSON config loader | ✓ Complete |
| `cpp/include/logger.hpp` | spdlog wrapper | ✓ Complete |
| `kdb/tp.q` | Tickerplant with u.q pub/sub, -11! compatible logging | ✓ Complete |
| `kdb/rdb.q` | Trade/quote storage, auto-replay on startup | ✓ Complete |
| `kdb/rte.q` | VWAP (bucketed) + imbalance, auto-replay | ✓ Complete |
| `kdb/tel.q` | Telemetry aggregation, persistent IPC handles | ✓ Complete |
| `kdb/logmgr.q` | Log cleanup and diagnostics | ✓ Complete |
| `kdb/ctl.q` | Dashboard control process | ✓ Complete |
| `kdb/u.q` | Standard tick.q pub/sub | ✓ Complete |

## Project Structure
```
binance_feed_handler/
├── cpp/
│   ├── include/
│   │   ├── trade_feed_handler.hpp
│   │   ├── quote_feed_handler.hpp
│   │   ├── order_book_manager.hpp
│   │   ├── rest_client.hpp
│   │   ├── config.hpp
│   │   └── logger.hpp
│   ├── src/
│   │   ├── trade_feed_handler.cpp
│   │   └── quote_feed_handler.cpp
│   └── third_party/kdb/
│       ├── k.h
│       └── c.o
├── config/
│   ├── trade_feed_handler.json
│   └── quote_feed_handler.json
├── kdb/
│   ├── tp.q
│   ├── rdb.q
│   ├── rte.q
│   ├── tel.q
│   ├── logmgr.q
│   ├── ctl.q
│   └── u.q
├── docs/decisions/
│   └── adr-001 to adr-010
├── logs/
│   └── processes/
├── CMakeLists.txt
├── start.sh
├── start_bg.sh
└── stop.sh
```

## Recent Changes

- **2026-01-07**: Log replay and LOG manager
  - TP logs now `-11!` compatible (proper `set ()` initialization)
  - RDB/RTE auto-replay on startup (zero data loss on restart)
  - LOG manager process (port 5014) for cleanup and diagnostics
  - TEL persistent IPC handles (eliminates connection overhead)
  - Updated ctl.q, start.sh, start_bg.sh, stop.sh for LOG process
  - ADR-006 updated, ADR-010 created

- **2026-01-06**: L5 order book and TEL process
  - Quote handler upgraded from L1 to L5 (5 levels bid/ask)
  - OrderBookManager with flat-array architecture
  - Dedicated TEL process for telemetry (moved from RDB)
  - Health metrics flow: FH → TP → TEL
  - ADR-009 updated for L5

- **2026-01-05**: Standard tick.q migration
  - Migrated to standard u.q pub/sub
  - TP uses `.u.pub`, `.u.sub`, `.u.end`
  - Separate log files per data type

- **2026-01-02**: Production hardening
  - JSON configuration files
  - Structured logging via spdlog
  - Health metrics every 5 seconds
  - Automatic reconnection with exponential backoff
  - Graceful shutdown on SIGINT/SIGTERM

## Tables

| Table | Fields | Location |
|-------|--------|----------|
| `trade_binance` | 14 fields | TP, RDB |
| `quote_binance` | 30 fields (L5) | TP, RDB |
| `health_feed_handler` | 10 fields | TP, TEL |
| `vwapBuckets` | 5 fields (keyed) | RTE |
| `telemetry_latency_fh` | 11 fields | TEL |
| `telemetry_latency_e2e` | 11 fields | TEL |
| `telemetry_system` | 4 fields | TEL |

## Process Ports

| Process | Port | Purpose |
|---------|------|---------|
| CTL | 5000 | Dashboard control |
| TP | 5010 | Tickerplant (pub/sub, logging) |
| RDB | 5011 | Storage (trades, quotes) |
| RTE | 5012 | Analytics (VWAP, imbalance) |
| TEL | 5013 | Telemetry aggregation |
| LOG | 5014 | Log management |

## Key Features

### Feed Handlers (C++)
- **Reconnection**: Exponential backoff (1s → 8s max)
- **Signal handling**: Graceful shutdown on SIGINT/SIGTERM
- **Configuration**: JSON files for symbols, ports, logging
- **Logging**: spdlog with levels (info/debug)
- **Health**: Uptime, message counts, connection state every 5s
- **Quote handler**: L5 order book, snapshot + delta reconciliation

### kdb+ Stack
- **TP**: Standard u.q pub/sub, separate log files, `-11!` compatible
- **RDB**: Auto-replay on startup, 14-field trades, 30-field quotes
- **RTE**: VWAP (1s buckets, 10min retention), L5 imbalance
- **TEL**: Persistent IPC handles, 5s buckets, 15min retention
- **LOG**: List, verify, cleanup log files
- **CTL**: Start/stop all processes, health checks

## Useful Commands

```bash
# Build
cmake -S cpp -B cpp/build && cmake --build cpp/build

# Start all (tmux)
./start.sh

# Start all (background, for dashboard)
./start_bg.sh

# Stop all
./stop.sh

# Control via CTL
q kdb/ctl.q
.ctl.start[]
.ctl.stop[]
.ctl.healthCheck[]
.ctl.statusTable[]

# Query RDB
select count i by sym from trade_binance
select count i by sym from quote_binance

# Query RTE
.rte.getVwap[`BTCUSDT; 5]
.rte.getImbalance[`BTCUSDT]

# Query TEL
.tel.handleStatus[]
.tel.fhStatusTable[]

# Query LOG
.log.list[]
.log.summary[]
.log.verifyDate[.z.D]
.log.cleanup[7]
```

## Key Decisions Summary

| Decision | Choice | ADR |
|----------|--------|-----|
| Trade ingestion | Tick-by-tick, async IPC | ADR-002 |
| Quote ingestion | L5 snapshot + delta reconciliation | ADR-009 |
| Durability | TP logging (separate files, -11! format) | ADR-003 |
| Recovery | Auto-replay on RDB/RTE startup | ADR-006 |
| Analytics | VWAP (1s buckets), imbalance (latest) | ADR-004 |
| Telemetry | TEL process, persistent handles, 5s buckets | ADR-005 |
| Log management | LOG process, retention policies | ADR-010 |
| Error handling | Fail-fast with logging, reconnection | ADR-008 |

## Open Items

1. [ ] KX Dashboards visualization
2. [ ] Historical Database (HDB)
3. [ ] Log compression/archival
4. [ ] Cross-day replay
5. [ ] Alerting integration
