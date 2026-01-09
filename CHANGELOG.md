# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0] - 2026-01-07

Major update: L5 order book support, production-grade logging with replay, dedicated telemetry process, and log management.

### Added

#### Quote Feed Handler (C++)
- New dedicated process for L5 order book data
- WebSocket connection to Binance depth stream
- REST client for snapshot fetching
- OrderBookManager with flat-array architecture (~16KB for 100 symbols)
- State machine: INIT → SYNCING → VALID ↔ INVALID
- Sequence validation and gap detection
- L5 depth: 5 levels of bid/ask (22 price/qty fields)
- Full instrumentation matching trade handler:
  - `fhParseUs` (includes book update + L5 extraction)
  - `fhSendUs` (L5 snapshot build + IPC)
  - `fhSeqNo`, `fhRecvTimeUtcNs`
- Health metrics published every 5 seconds

#### Tickerplant Enhancements
- Separate log files per data type (trade.log, quote.log)
- `-11!` compatible log format (proper initialization with `set ()`)
- Message counting per log type
- Log status query: `.tp.logStatus[]`
- Standard tick.q pub/sub via u.q

#### RDB Enhancements
- **Automatic log replay on startup**
- Quote table: 30 fields (L5 depth + metadata)
- Trade table: 14 fields (unchanged)
- Replay query interface: `.rdb.replay[date]`

#### RTE Enhancements
- **Automatic log replay on startup** with cleanup
- VWAP: Time-bucketed aggregation (1-second buckets)
  - 100x memory reduction vs per-trade storage
  - O(1) insert, O(buckets) query
- Order book imbalance from L5 quotes
- Configurable retention (default 10 minutes)
- Query interface: `.rte.getVwap[sym;mins]`, `.rte.getImbalance[sym]`

#### Telemetry Process (TEL) - New
- Dedicated process for all monitoring (port 5013)
- Subscribes to health via TP
- Queries RDB/RTE for metrics
- **Persistent IPC handles** (eliminates connection overhead)
- Tables:
  - `telemetry_latency_fh` (unified trade + quote)
  - `telemetry_latency_e2e` (trades only)
  - `telemetry_system` (memory per process)
  - `health_feed_handler` (from FH subscription)
- 5-second buckets, 15-minute retention

#### Log Manager (LOG) - New
- Dedicated process for log lifecycle (port 5014)
- Discovery: `.log.list[]`
- Diagnostics: `.log.summary[]`, `.log.verifyDate[date]`
- Cleanup: `.log.cleanup[days]`
- Integrity verification

#### Control Process (CTL) - New
- Dashboard control (port 5000)
- Start/stop all processes
- Health checks: `.ctl.healthCheck[]`
- Status table: `.ctl.statusTable[]`
- Native process control (PID files)

#### Infrastructure
- `start_bg.sh` - Background startup for dashboard control
- Updated `start.sh` - 7 tmux windows (TP, RDB, RTE, TEL, LOG, trade-fh, quote-fh)
- Updated `stop.sh` - Handles all processes including LOG

#### Documentation
- ADR-009: L5 Order Book Architecture
- ADR-010: Log Management and Lifecycle
- Updated ADR-005: Persistent IPC handles
- Updated ADR-006: Implemented auto-replay
- Updated README with all new components

### Changed

- Telemetry moved from RDB to dedicated TEL process
- VWAP changed from per-trade storage to time-bucketed aggregation
- RTE recovery changed from RDB query to log replay
- TP logging now uses proper `-11!` format
- Health metrics now flow through TP (FH → TP → TEL)

### Removed

- `replay.q` - Replaced by automatic replay in RDB/RTE
- `test_u_migration.q` - Migration complete
- Telemetry tables from RDB (moved to TEL)

### Fixed

- Log file format now compatible with `-11!` streaming replay
- RDB/RTE no longer lose data on restart (auto-replay)
- TEL connection overhead eliminated (persistent handles)

---

## [0.1.0] - 2025-12-18

Initial release of the real-time event-driven market data system.

### Added

#### Feed Handler (C++)
- WebSocket connection to Binance using Boost.Beast/Asio
- TLS support via OpenSSL
- JSON parsing with RapidJSON
- Multi-symbol support (BTCUSDT, ETHUSDT) via combined stream
- Full instrumentation per ADR-001:
  - Wall-clock timestamp (`fhRecvTimeUtcNs`)
  - Monotonic durations (`fhParseUs`, `fhSendUs`)
  - Sequence number (`fhSeqNo`) for gap detection
- Async IPC to Tickerplant via kdb+ C API
- Tick-by-tick publishing (no batching)

#### Tickerplant (kdb+)
- Pub/sub infrastructure (`.u.sub`, `.u.pub`)
- Subscriber management (`.u.w` registry)
- Graceful disconnect handling (`.z.pc`)
- TP receive timestamp capture (`tpRecvTimeUtcNs`)
- Fan-out to multiple subscribers (RDB, RTE)

#### Real-Time Database (kdb+)
- Trade storage with 14-field schema
- RDB apply timestamp (`rdbApplyTimeUtcNs`)
- Telemetry aggregation (1-second buckets):
  - FH latency percentiles (p50/p95/p99)
  - E2E latency percentiles
  - Throughput metrics per symbol
- 15-minute telemetry retention with automatic cleanup
- Query interface for dashboards (port 5011)

#### Real-Time Engine (kdb+)
- Rolling analytics (5-minute window)
- Per-symbol state management
- Tick-by-tick processing
- Lazy eviction of stale entries
- Analytics output:
  - `lastPrice`, `avgPrice5m`, `tradeCount5m`
  - Validity tracking (`isValid`, `fillPct`)
- Query interface for dashboards (port 5012)

#### Visualization
- KX Dashboards integration
- Polling-based queries (1-second intervals)
- Market data display (prices, analytics)
- Latency monitoring panels
- Validity indicators

#### Infrastructure
- `start.sh` - tmux-based pipeline launcher
- `stop.sh` - clean shutdown script
- CMake build system

#### Documentation
- 8 Architecture Decision Records (ADRs):
  - ADR-001: Timestamps and Latency Measurement
  - ADR-002: Feed Handler to kdb Ingestion Path
  - ADR-003: Tickerplant Logging and Durability Strategy
  - ADR-004: Real-Time Rolling Analytics Computation
  - ADR-005: Telemetry and Metrics Aggregation Strategy
  - ADR-006: Recovery and Replay Strategy
  - ADR-007: Visualisation and Consumption Strategy
  - ADR-008: Error Handling Strategy
- Architecture reference (from Data Intellect paper)
- Measurement notes (latency definitions, clock trust model)
- Binance API specification
- Canonical trades schema (v2.0)
- Comprehensive code comments (FH, TP, RDB, RTE)

### Design Decisions

- **Ephemeral data**: No TP logging, no HDB (ADR-003)
- **No recovery**: Data loss accepted; focus on real-time behaviour (ADR-006)
- **Tick-by-tick**: Lowest latency, clearest measurement (ADR-002, ADR-004)
- **Fail-fast**: Simple error handling with logging (ADR-008)
- **Polling dashboards**: 1-second refresh via KX Dashboards (ADR-007)

### Known Limitations

- No automatic reconnection (FH to Binance, RDB/RTE to TP)
- No persistent storage (all data lost on restart)
- No gap recovery (gaps are detected but not filled)
- Manual process management (no supervisor/systemd)
- Single-host deployment only

---

## Future Considerations

Not implemented; captured for potential future phases:

| Feature | Relevant ADR |
|---------|--------------|
| Historical Database (HDB) | ADR-003 |
| Gap recovery via REST API | ADR-006 |
| Streaming dashboards | ADR-007 |
| Multi-host deployment | - |
| Alerting integration | ADR-005, ADR-008 |
| Log compression/archival | ADR-010 |
| Cross-day replay | ADR-006 |
