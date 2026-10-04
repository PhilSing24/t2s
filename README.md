# Tick to Signal

A real-time Binance market data pipeline built with C++ and KDB-X. Three feed handlers (spot trades, USD-M futures aggTrades, L5 order book) stream into a tickerplant-fanout architecture with batched analytics, RSI-based signal generation, simulated P&L tracking, and durability-log replay (WDB recovers from disconnects). A historical data store (`hdb_binancedata/`) supports offline research and feature-engineering primitives drawn from López de Prado's *Advances in Financial Machine Learning*.

Architecture patterns originally inspired by *Building Real-Time Event-Driven KDB-X Systems* by Data Intellect; extended with TLS-verified WS/REST, TCP keepalive and dead-connection detection, per-stream sequence-gap surfacing, Binance-spec-correct order book reconciliation, async snapshot fetching, replay-on-reconnect, RAII-managed kdb+ IPC, drop-and-count JSON parsing, and a multi-language test suite. A read-only MCP server (`mcp/`) additionally exposes the live analytics for natural-language querying through Claude Desktop.

## Architecture

![Architecture Overview](images/DiagramArchitectureOverview.jpg)

Each downstream process auto-reconnects with exponential backoff. The TP writes a durability log per day. **WDB persists a checkpoint and replays missed data from the durability log on reconnect**; other subscribers (CTP, RDB, RTE, TEL, SIG, PNL) are best-effort live analytics that may have gaps after disconnect. Feed handler TLS connections to Binance are fully verified (peer cert + hostname), use TCP keepalive plus a 30s WebSocket idle timeout to detect dead connections within ~90s, and TP tracks per-stream sequence-number gaps so missed messages are surfaced rather than silent.

## Components

| Component    | Port  | Subscribes to | Role                                                   |
|--------------|-------|---------------|--------------------------------------------------------|
| Trade FH     | —     | Binance WS    | Spot trade feed handler (C++)                          |
| Trade FH Fut | —     | Binance WS    | USD-M futures aggTrade feed handler (C++)              |
| Quote FH     | —     | Binance WS    | Spot order book feed handler (C++) with REST snapshots |
| Quote FH Fut | —     | Binance WS    | USD-M futures order book feed handler (same class)     |
| TP           | 5010  | FHs           | Tickerplant — pub/sub hub with daily durability log    |
| WDB          | 5011  | TP            | Write-only DB — buffers and writes to HDB at EOD       |

The table above describes the live ingestion pipeline. Research-side code (`kdb/framework/` — backtest framework, `kdb/strategies/` — strategy files, `kdb/ml/` — feature engineering) is run on-demand against the historical HDB, not as long-running processes. See **Backtest Framework** and **Research Workflow** below.

## The System in Action

**System Health Monitoring** — Health status of main processes.

![System Health Monitoring](images/SystemHealthView.jpg)

**Feed Handler Monitoring** — Feedhandler latency metrics for trade and quote ingestion.

![Feed Handler Monitoring](images/FeedhandlerMonitoringView.png)

**Dataflow Monitoring** — Data volume, system resources, and end-to-end latency breakdown.

![Dataflow Monitoring](images/DataFlowMonitoringView.png)

**Analytics** — VWAP, Volatility, Order Book Imbalance.

![Analytics](images/RealTimeAnalyticsView.jpg)

**Trades & Quotes** — Live order books and OHLC charts per symbol.

![Trades & Quotes](images/TradesQuotesView.png)

## Project Layout

```
t2s/
├── cpp/                      # Feed handlers (C++)
│   ├── include/              # Public headers
│   │   ├── config.hpp                # JSON config loader for FH binaries
│   │   ├── json_reader.hpp           # Safe accessors for rapidjson (no-throw, type-checked)
│   │   ├── k_object.hpp              # RAII wrappers for kdb+ K objects (KOwned/KBorrowed)
│   │   ├── logger.hpp                # spdlog setup helper
│   │   ├── market_config.hpp         # Market shape (host, port, stream suffix, schema) shared by spot + futures trade FH
│   │   ├── fh_stats.hpp              # Sends a handler's counters to TP (.tp.fhStats)
│   │   ├── order_book_manager.hpp    # Full-depth book, spot/futures sync rules, horizon + refresh, top-N quote
│   │   ├── quote_fh_main_common.hpp  # main() shared by the two quote binaries
│   │   ├── quote_row.hpp             # Quote row for TP, generated from the configured depth
│   │   ├── quote_feed_handler.hpp    # Quote FH class declaration
│   │   ├── rest_client.hpp           # HTTPS client for Binance REST (snapshots)
│   │   ├── snapshot_scheduler.hpp    # When a snapshot may be requested: backoff, weight budget, 429/418 pauses
│   │   ├── snapshot_worker.hpp       # Async snapshot fetcher (worker thread + bounded queue)
│   │   ├── socket_utils.hpp          # TCP keepalive helper
│   │   ├── trade_feed_handler.hpp    # Trade FH class declaration (schema-branched: spot trade / futures aggTrade)
│   │   └── trade_row.hpp             # buildTradeRow helper (schema-driven K-object construction)
│   ├── src/                  # Implementations + main entry points
│   │   ├── quote_feed_handler.cpp    # Quote FH class implementation
│   │   ├── quote_fh_main.cpp         # Spot quote FH binary entry point
│   │   ├── quote_fh_fut_main.cpp     # USD-M futures quote FH binary entry point
│   │   ├── trade_feed_handler.cpp    # Trade FH class implementation (both schemas)
│   │   ├── trade_fh_main.cpp         # Spot trade FH binary entry point
│   │   └── trade_fh_fut_main.cpp     # Futures aggTrade FH binary entry point
│   └── third_party/
│       ├── catch2/                   # Vendored Catch2 v3 amalgamation (C++ tests)
│       └── kdb/                      # k.h and c.o for kdb+ IPC
├── kdb/
│   ├── schemas.q             # Shared table schemas (single source of truth, includes futures aggTrade)
│   ├── tick/                 # Tickerplant and writedown
│   │   ├── tp.q              # Primary tickerplant (durability log)
│   │   └── wdb.q             # Write-only DB → HDB
│   ├── ml/                   # ML feature pipeline (in progress)
│   │   ├── afml.q            # AFML primitives (López de Prado)
│   │   ├── features.q        # Feature engineering (dollar-imbalance bars, etc.)
│   │   └── labels.q          # Labeling primitives for supervised learning
│   ├── framework/            # Backtest framework (v0.1) — see Backtest Framework section
│   │   ├── backtest.q        # Top-level orchestrator: loads strategy + replays history, prints report
│   │   ├── framework.q       # Event loop, callback dispatch, virtual clock, intent routing
│   │   ├── pretrade.q        # Pre-trade gate: position-size cap, kill-switch enforcement
│   │   ├── execution.q       # Paper-fill simulator: synthetic spread + Binance USDS-M fees
│   │   ├── position.q        # Position tracking, P&L, funding application, auto-exit policies
│   │   └── replay.q          # Historical replay driver: HDB → framework events
│   ├── strategies/           # Strategy files that plug into the framework
│   │   └── momentum.q        # Example: fast/slow EMA crossover, long-flat, no embedded stops
│   ├── pubsub/               # KDB-X di.pubsub module
│   │   ├── init.q            # Module bootstrap: defines subscribable tables, fetches schemas
│   │   └── pubsub.q          # subscribe/publish primitives + utilities (sub clear, EOD/EOP broadcast)
│   └── utils/                # Operational tooling
│       ├── tradeLoader.q          # Historical spot trade loader (single-date or date range, interactive or scripted)
│       ├── aggTradeLoaderFut.q    # Historical USD-M futures aggTrade loader (mirrors tradeLoader.q)
│       ├── tradeLoaderFut.q       # Historical USD-M futures per-fill trade loader (finer granularity than aggTrade)
│       ├── fundingLoader.q        # Funding rate loader (incremental, paginated, splayed)
│       ├── hdbUtils.q             # HDB switching, queries
│       └── logmgr.q               # Durability log management
├── tests/                    # Test suite (bash + q + C++)
│   ├── run_tests.sh                     # Test runner - dispatches .q, .sh, and build/test_* binaries
│   ├── t_lib.q                          # Shared assertion + sandbox helpers
│   ├── test_schemas.q                   # Schema integrity assertions
│   ├── test_afml.q                      # Q tests for kdb/ml/afml.q primitives
│   ├── test_labels.q                    # Q tests for kdb/ml/labels.q primitives
│   ├── test_smoke.sh                    # Per-process load + .health[] smoke test
│   ├── test_wdb_eod.sh                  # End-to-end WDB EOD persistence test
│   ├── wdb_eod_body.q                   # Q assertions invoked by test_wdb_eod.sh
│   ├── test_order_book.cpp              # C++ unit tests for OrderBookManager (Catch2)
│   ├── test_snapshot_worker.cpp         # C++ unit tests for SnapshotWorker (Catch2)
│   ├── test_json_reader.cpp             # C++ unit tests for JsonReader + parseLevelPair (Catch2)
│   ├── test_trade_fh_row_construction.cpp   # C++ unit tests for buildTradeRow (both schemas)
│   ├── test_stream_path.cpp             # C++ unit tests for buildStreamPath (combined-stream URL builder)
│   └── test_aggtrade_parse.cpp          # C++ unit tests for futures aggTrade JSON parsing
├── config/                   # Feed handler JSON configs
│   ├── trade_feed_handler.json       # Spot trade FH
│   ├── trade_feed_handler_fut.json   # USD-M futures aggTrade FH
│   ├── quote_feed_handler.json       # Spot quote FH
│   ├── quote_feed_handler_fut.json   # USD-M futures quote FH
│   └── shared.json                   # Symbols and quote depth for all four handlers and the q schemas
├── hdb/                      # Live HDB partitions (gitignored, populated at EOD)
├── tmp/                      # WDB intraday writedown directory (gitignored)
├── hdb_binancedata/          # Historical research HDB (gitignored)
├── markdown_docs/            # Design notes, guides
├── run/                      # Runtime status files written by start.sh (gitignored)
├── CMakeLists.txt
├── install_kdb.sh
├── check_eod.sh              # Post-midnight verification script (HDB partition + WDB logs)
├── start.sh                  # Start all (tmux) — supports --markets {spot|futures|spot,futures}
└── stop.sh                   # Stop all
```

## Operating it day to day

- `./start.sh --markets spot,futures` starts everything in the tmux session `t2s` and only returns once TP is healthy, WDB is connected with its replay complete, and every handler has registered with TP; `--headless` returns without attaching. `./stop.sh` stops handlers, then WDB (graceful flush and checkpoint), then TP, then tmux. `./status.sh` shows processes, health, rows today, the counters that matter, disk, pending tmp dirs and the clock, and exits non-zero when something needs attention. `./check_eod.sh [date]` confirms a closed day against its log.
- The pipeline runs unattended as systemd user services, restarted on failure, with daily jobs on systemd timers and a Windows task that boots WSL at startup. Setup, what happens on a crash, `wsl --shutdown`, a Windows restart or laptop sleep, and the clock fix are in [ops/RUNNING.md](ops/RUNNING.md). `./start.sh --tmux` keeps the old tmux mode; the two never run together.
- Rebuilding a day's partition from its TP log: `q kdb/utils/rebuild_day.q -date D` reports, `-build` writes and verifies `hdb/.rebuild/D`, `-swap` moves the old partition to `hdb/.rebuild/D.bak.<stamp>` and the rebuilt one in. Rebuilt rows carry a null `wdbRecvTimeUtcNs`: null means "rebuilt from the log, not received live"; live rows always have it set.
- Resources: the pipeline uses well under 100 MB in steady state; the maintenance tools (retention scan, a day's rebuild, a long replay) need 1 to 7 GB one at a time. Measurements and a proposed `.wslconfig` are in [ops/RUNNING.md](ops/RUNNING.md).
- Clock: WSL2's clock can fall behind after sleep, and partitions are dated by it. `./status.sh` warns on drift versus the Windows clock, TP's `.health[]` reports `clockSkewMs` against exchange time. Fix: `sudo hwclock -s`. Optional, via `sudo visudo`, to make the fix passwordless and limited to that one command: `philippe ALL=(root) NOPASSWD: /usr/sbin/hwclock -s` (not installed by the repo).

## Prerequisites

- kdb+ 4.x or KDB-X (with `di.pubsub` module)
- C++17 compiler, CMake 3.16+
- Boost (Beast, Asio), OpenSSL, RapidJSON, spdlog

On Ubuntu/WSL the system libraries can be installed with:
```bash
sudo apt update
sudo apt install -y build-essential cmake \
    libboost-system-dev libssl-dev libspdlog-dev rapidjson-dev
```

A helper script `install_kdb.sh` is provided for kdb+ setup.

Catch2 is optional (only used to build the C++ test binaries). If `cpp/third_party/catch2/catch_amalgamated.{hpp,cpp}` is present, the test targets are built; otherwise CMake prints a notice and skips them. Download the amalgamated headers from https://github.com/catchorg/Catch2/releases (v3.x).

## Build

```bash
cmake -S . -B build
cmake --build build
```

This produces four binaries, two per market: `trade_feed_handler` and `quote_feed_handler` (spot), `trade_feed_handler_fut` and `quote_feed_handler_fut` (USD-M futures).

## Run

Start everything (through systemd once `ops/systemd/install.sh` has been run, otherwise in tmux):
```bash
./start.sh                          # spot: trade + quote handlers (default)
./start.sh --markets spot           # the same, explicit
./start.sh --markets futures        # USD-M futures: trade + quote handlers only
./start.sh --markets spot,futures   # all four handlers
```

Stop everything:
```bash
./stop.sh
```

`start.sh` brings up TP and WDB plus the feed handlers selected by `--markets`. Each market has a trade handler and a quote handler, and each handler owns one table:

| Handler | Stream | Table |
|---|---|---|
| `trade_feed_handler` | spot `@trade` | `trade_binance` |
| `quote_feed_handler` | spot `@depth@100ms` | `quote_binance` |
| `trade_feed_handler_fut` | futures `@aggTrade` on `/market` | `trade_binance_fut` |
| `quote_feed_handler_fut` | futures `@depth@100ms` on `/public` | `quote_binance_fut` |

Individual processes can also be started manually. From the project root:
```bash
q kdb/tick/tp.q
q kdb/tick/wdb.q
./build/trade_feed_handler config/trade_feed_handler.json
./build/trade_feed_handler_fut config/trade_feed_handler_fut.json
./build/quote_feed_handler config/quote_feed_handler.json
./build/quote_feed_handler_fut config/quote_feed_handler_fut.json
```

The feed handler binaries take a config file path as their only argument. If invoked with no argument (as `start.sh` does), each falls back to `config/<binary_name>.json` relative to the working directory, which is why `start.sh` runs them without an explicit path after `cd $BASEDIR`. To run from elsewhere, pass the config explicitly as shown above.

## Configuration

Feed handler runtime config lives in `config/`:

- `shared.json` — the symbols and the quote depth, read by all four handlers and by the q schemas (see **Symbols and quote depth** below)
- `trade_feed_handler.json` — spot trade FH: TP host/port, reconnect backoff, log level/file
- `trade_feed_handler_fut.json` — USD-M futures aggTrade FH: same shape, but with `host=fstream.binance.com`, `port=443`, `stream_suffix=@aggTrade`, `tp_table=trade_binance_fut`, `schema=futures_agg_trade`
- `quote_feed_handler.json` — spot quote FH. Its `market` block is required and states the WebSocket host/port/path prefix/stream suffix, the REST snapshot host and path, the snapshot limit with its request weight and the exchange's weight limit per minute, the TP table and `schema=spot_depth`
- `quote_feed_handler_fut.json` — USD-M futures quote FH: `fstream.binance.com` with `path_prefix=/public`, snapshots from `fapi.binance.com/fapi/v1/depth` (weight 20 of 2400/min), `tp_table=quote_binance_fut`, `schema=futures_depth`. Each quote binary refuses the other market's config

Each q process has its own config block at the top of its file (e.g. `.tp.cfg`, `.wdb.cfg`). Edit and reload to change ports, retention, batch intervals, etc.

Paths and ports can also be set per process through environment variables, which is how the test suite sandboxes TP and WDB without editing source. Unset variables fall back to the defaults below.

| Variable             | Process | Default                     | Meaning                                  |
|----------------------|---------|-----------------------------|------------------------------------------|
| `T2S_TP_PORT`        | TP      | `5010`                      | Listen port                              |
| `T2S_TP_LOG_DIR`     | TP      | `logs` (relative to cwd)    | Durability log directory                 |
| `T2S_WDB_PORT`       | WDB     | `5011`                      | Listen port                              |
| `T2S_WDB_TP_PORT`    | WDB     | `5010`                      | Port of the TP to subscribe to           |
| `T2S_HDB_DIR`        | WDB     | `../hdb` (relative to cwd)  | HDB root for EOD partitions and sym file |
| `T2S_TMP_DIR`        | WDB     | `../` (relative to cwd)     | Parent of the intraday `tmp.<date>` dirs |
| `T2S_WDB_CHECKPOINT` | WDB     | `$T2S_TMP_DIR/wdb.lastTpSeqNo` | Replay checkpoint file                |
| `T2S_TP_SEQ_FILE`    | TP      | `$T2S_TP_LOG_DIR/tp.tpSeqNo` | tpSeqNo reservation file (see below)     |
| `T2S_TP_MIN_FREE_MB` | TP      | `5120`                      | `.health[]` degrades when the log dir's filesystem has less free space |
| `T2S_SHARED_CONFIG`  | all     | `config/shared.json`        | Symbols and quote depth. Handlers look next to their own config file; q walks up from the script (or current) directory |
| `T2S_ALERT_WINDOW_SEC` | TP, WDB | `3600` | How long a problem stays flagged in `.health[]` and `status.sh` |
| `T2S_TP_SESSION_FILE` | TP | `$T2S_TP_LOG_DIR/tp.sessions` | Session, trade-id and open-gap state carried across TP restarts |
| `T2S_LOG_RETENTION_DAYS`, `T2S_LOG_PROTECTED` | logmgr | `7`, unset (no protected dates) | Retention policy inputs (see below) |
| `T2S_WDB_MAXROWS`, `T2S_WDB_ROLL_GRACE_SEC`, `T2S_WDB_ROLL_FALLBACK_SEC`, `T2S_WDB_REPLAY_DELAY_MS`, `T2S_TP_INDEX_EVERY`, `T2S_TP_FAKE_DATE`, `T2S_WDB_FAKE_DATE` | both | unset | Test hooks only. The fake dates fix the process clock; `start.sh` refuses to run with either set, and the test guard only allows them inside `tests/sandbox`. |

**Symbols and quote depth.** `config/shared.json` holds two values for the whole pipeline:

```json
{ "symbols": ["btcusdt", "ethusdt", "solusdt"], "quote_depth": 5 }
```

- `symbols` is the one list all four handlers subscribe to. A handler config with its own `symbols` list is refused. Changing the list needs a restart of the handlers only.
- `quote_depth` (1 to 50) is the number of book levels per side that the quote handlers publish. It drives the published row (`cpp/include/quote_row.hpp`), the width each quote handler announces to TP, and the generated schemas `.schema.quote` and `.schema.quoteFut` in `kdb/schemas.q`.

**A different depth is a different table layout.** `quote_binance` has `10 + 4*depth` feed-handler columns (`bidPrice1..N`, `bidQty1..N`, `askPrice1..N`, `askQty1..N`), `quote_binance_fut` two more. Two layouts cannot share one table in a partitioned HDB, so the change is guarded rather than applied silently:

- TP refuses a quote handler whose row width differs from its schema; the handler exits with code 2 before it connects to the exchange.
- TP and WDB refuse to start if any HDB date partition or any `tmp.<date>` directory holds a quote table of another depth. They name the directories and modify nothing. The same guard covers columns: see **Schema changes** below.

To change the depth: stop the pipeline, let the day roll or move `tmp.<date>` away, move the existing HDB partitions that contain `quote_binance` or `quote_binance_fut` to another directory (or accept starting a new HDB), edit `quote_depth`, and start again. Past quote data stays at its old depth where you moved it. `tests/test_depth_config.sh` exercises all of this at depth 3.

**Quote handlers.** Both quote handlers run the same class and keep a local order book per symbol from the diff depth stream plus REST snapshots.

- *Sync rule.* Spot: update ids are consecutive; the first event after a snapshot must satisfy `U <= lastUpdateId+1 <= u`. USD-M futures: ids are not consecutive; events with `u < lastUpdateId` are dropped, the first processed event has `U <= lastUpdateId <= u`, and afterwards each event's `pu` must equal the previous event's `u`. A break is a gap: one invalid row is published (`isValid=0b`, no levels) so the hole is visible downstream, and the book is rebuilt from a new snapshot. The doc pages are cited in `order_book_manager.hpp`.
- *Snapshots can never storm.* Every snapshot request goes through `SnapshotScheduler`: exponential backoff per symbol after a failure (1 s doubling to 60 s, with jitter), a budget of 10% of the exchange's request-weight limit shared by all symbols (twelve depth-1000 snapshots a minute on either market), and a pause of all requests on HTTP 429 or 418 for the server's `Retry-After` (at least 60 s and 300 s), or for 60 s when the used-weight header reaches half the limit.
- *Bounded buffer.* While a book waits for its snapshot, deltas are buffered up to 1000 per symbol; on overflow the oldest is dropped and counted. A snapshot that then cannot bridge to the remaining deltas is rejected by the sync rule and retried under backoff.
- *Full-depth book.* The book keeps every level it knows, so deleting a top level promotes the next real one instead of leaving an empty slot. A snapshot returns at most 1000 levels per side; its worst price is the side's *horizon*, and levels beyond it are unknown until they change. When fewer than 100 known levels remain on a side, the handler fetches a new snapshot in the background and swaps it in without publishing an invalid row. If a side ever has fewer known levels than the published depth, the quote is invalid rather than possibly wrong.
- *Counters.* Each handler reports its counters to TP every 5 s (`.tp.fhStatus[]`, `fhStats` in `.health[]`, one `FH` line per table in `./status.sh`): `bookGaps`, `resyncs`, `snapshotRequests`, `snapshotFailures`, `rateLimitPauses`, `bufferOverflows`, `depthRefreshes`, `refreshFailures`, `depthExhausted`, `wsReconnects` for the quote handlers; `exchGaps`, `exchMissed`, `exchOutOfOrder`, `exchDuplicates`, `wsReconnects` for the trade handlers. They are cumulative since the handler started; `status.sh` raises attention on their recent increase, not on the totals. `quote_binance_fut` also stores `exchTransactTimeMs`, the futures event's transaction time `T`.

**Exchange update ids.** Every valid quote row stores the range of exchange depth events applied since the previous published row of its symbol: `exchFirstUpdateId` (`U` of the first event), `exchUpdateId` (`u` of the last, the update id the book is at) and, on futures, `exchPrevUpdateId` (`pu` of the first event). A heartbeat row repeats the previous ids; an invalid row has nulls. Two uses:

- *Exact comparison with REST.* A depth snapshot whose `lastUpdateId` equals a row's `exchUpdateId` must show exactly that row's levels. One whose `lastUpdateId` falls inside a row's range is an intermediate state between that row and the one before.
- *Continuity from stored data alone.* `q kdb/utils/check_quote_seq.q -date 2026.10.04` (or `-dir tmp/tmp.<today>`) checks per symbol that each row continues the previous one (spot: `exchFirstUpdateId <= previous exchUpdateId + 1`; futures: `exchPrevUpdateId = previous exchUpdateId`). Each break is listed with its time and classified: *marked* when the handler published an invalid row there (sequence gap, lost WebSocket connection, exhausted depth) or was restarted, *UNMARKED* when nothing in the data explains it. It exits 1 on any unmarked break; there should never be one.

`trade_binance_fut.qtyExRpi` is the aggTrade event's `nq`, which Binance defines as the "normal quantity without the trades involving RPI orders" (Retail Price Improvement), next to `qty`, the "quantity with all the market trades". It has been in the stream since 2025-12-31 ([change log](https://developers.binance.com/docs/derivatives/change-log), [Aggregate Trade Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams)). An event without it is stored with a null and counted as `nqMissing`.

**Schema changes.** `kdb/schemas.q` is the single source of the stored layout. When a column is added to a live table, data written earlier lacks it; a partitioned HDB takes the column list from its newest partition, so HDB-wide queries on the new column would fail on older dates, and WDB could not append to an older `tmp.<date>` dir. TP and WDB therefore refuse to start while any HDB partition or tmp dir differs from the schema, and name the fix:

```bash
q kdb/utils/hdb_migrate.q          # dry run: what would be done
q kdb/utils/hdb_migrate.q -apply   # do it (pipeline stopped)
```

The tool adds each missing column as a file of typed nulls and rewrites the table's `.d` column list; a live table missing from an HDB partition gets an empty splay. It never rewrites, moves or deletes an existing column file, and it refuses (changing nothing) a table it cannot fix that way, such as another quote depth. Rows from before a column existed read as null in it. TP logs written with the old layout cannot be replayed or rebuilt into the new one; once the day is verified in the HDB (`check_eod.sh`) they are of no further use.

**Replay seeks and never runs inside TP.** TP writes a seek index next to each daily log (`<date>.idx`, one entry per 10,000 rows: tpSeqNo, byte offset, chunk). On reconnect WDB asks TP only for `.tp.replayInfo[]`, the log directory, today's committed length and the cutoff, then reads the logs itself: it starts in the log that holds its checkpoint, seeks via the index, reads every later log in date order, and reads today's log only up to the committed length, so a disconnect spanning midnight is replayed across both days' logs. TP's live path is never involved. Replayed rows are staged and merged only after every segment has been read and validated; a corrupt or short segment fails the replay explicitly, counts it (`replayFailures`), shows `status=error` in `.health[]` while WDB is disconnected, leaves the checkpoint untouched, and WDB retries on its timer. The startup roll of past-date tmp dirs waits for the first successful replay, so yesterday's rows arriving by replay land in yesterday's partition rather than being counted late.

**Log retention.** `q kdb/utils/logmgr.q -retention` prints, for every log in the log dir, its size, row count, whether every logged row is present in the HDB, and whether it would be deleted and why. A log is deleted only when the HDB partition for its date exists, every tpSeqNo in the log for every table is found in the partitions for that date and its two neighbours, and the log is older than `T2S_LOG_RETENTION_DAYS`. Today's log, dates in `T2S_LOG_PROTECTED` and `tp.tpSeqNo` are never deleted; the `.idx` goes with its log. The default is a dry run; add `-apply` to delete. `-summary` prints the completeness table alone, which shows which past days are safe in the HDB and which could only be rebuilt from their logs.

**tpSeqNo is monotonic and durable.** TP stamps every row with a sequence number that never goes backwards: across TP restarts, across midnight, and whether or not today's log exists. It keeps a small reservation file, `tp.tpSeqNo` in the log directory, and hands out a number only after a reservation covering it has been written atomically, so a crash can skip up to 10,000 numbers but never reuse one. On the first start without that file TP seeds the counter from the newest log in the log directory, whatever its date, and warns if the seed would be below WDB's persisted checkpoint. **Log retention must never delete `tp.tpSeqNo`.** If TP ever hands out a number below WDB's checkpoint anyway, WDB halts with `status=error` and writes nothing until an operator intervenes.

**No silent loss.** Every way a row can go missing is either repaired or recorded in the data.

| Where rows can be lost | What happens |
|---|---|
| Handler to TP (TP restart, dropped connection) | Each handler keeps its last 4096 published rows. On reconnect TP replies with the last `fhSeqNo` it has logged for the session and the handler resends everything after it. `missed` stays 0. If the ring were ever too short, the rows it could not resend arrive as a jump and TP counts them as `missed`. |
| TP to WDB | WDB replays from TP's logs from its checkpoint (see below). |
| Exchange to trade handler (reconnect, handler crash or restart) | Binance numbers trades per symbol without holes. A jump in the id is recorded in `trade_gap` and the missing trades are fetched over REST and published. A restarted handler first asks TP for the last id it logged per symbol (`.tp.tradeState`), so the trades its downtime left are covered too. |
| Exchange to quote handler | A book cannot be backfilled. The hole is marked by one invalid row per symbol, and `check_quote_seq.q` proves from the stored update ids that there is no unmarked break. |

*`trade_gap`.* One row per status change of a gap: `time`, `sym`, `srcTable` (`trade_binance` or `trade_binance_fut`), `firstMissingId`, `lastMissingId`, `missing`, `status`, `recovered`, `recoveredThroughId`, `reason`. The status goes `detected`, then `partial` after each backfilled page, then `recovered`, or `unrecoverable` with a reason. The current state of every gap is its latest row:

```q
select by srcTable, sym, firstMissingId from trade_gap where date = 2026.10.04     / or .schema.gapLatest
```

`reason` on a `detected` row is `handlerRestart` when the gap is what a handler's downtime left. On an `unrecoverable` row it is one of:

| Reason | Meaning |
|---|---|
| `tooLarge` | more missing ids than `backfill.max_gap_ids` in the handler's config (default 500,000). Larger holes can be filled later from the Binance daily archive with the loaders in `kdb/utils/` |
| `tooOld` | futures only: the endpoint serves the past two days |
| `notServed` | the exchange no longer returns these ids |
| `restFailed` | the request kept failing (10 attempts with backoff) |
| `backfillDisabled` | the handler's config has no `backfill` block |

*Backfilled rows.* Spot trades come from `GET /api/v3/historicalTrades`, futures aggTrades from `GET /fapi/v1/aggTrades`, both by id and without an API key. A backfilled row is an ordinary row of its table, with two things to know:

- `exchEventTimeMs` is null, because REST does not return the event time. That null is the marker: `select from trade_binance where null exchEventTimeMs` lists the backfilled trades.
- `time` is the handler's receive time, as for every row, so here it is the moment the REST reply arrived, and it decides the partition. The trade's own time is `exchTradeTimeMs`. Analytics that need trades in trade order should sort on `exchTradeTimeMs` or the trade id, not on `time`. `fhParseUs` and `fhSendUs` are 0.

Requests use the same scheduler as the quote snapshots: a tenth of the exchange's weight limit, backoff after a failure, a pause on HTTP 429 or 418. TP derives each gap's progress from the backfilled rows it logs, so a handler that dies mid-backfill resumes exactly where the log ends, with no id fetched or published twice.

*Files next to the TP logs.* `tp.tpSeqNo` (the tpSeqNo reservation) and `tp.sessions` (session id and last `fhSeqNo` per table, last trade id per symbol, open gaps). Neither is ever a retention candidate; do not delete them.

*Recent problems, not old totals.* `.health[]` and `./status.sh` flag what is wrong now (a process down, WDB halted, disk, clock, a handler that stopped reporting, a trade gap open for more than ten minutes) and what went wrong within the last `T2S_ALERT_WINDOW_SEC` seconds (default 3600). Counters since start are still printed, as totals. `./status.sh` shows a `GAPS` line and a `RECENT` line; `.tp.incidents[]` lists the recent incidents.


**Feed handler sessions.** Every handler connection starts with a synchronous `.tp.registerSession[table; sessionId; nextFhSeqNo; rowWidth]` call. The session id is the handler's start time, so a new id is a restart and the same id on a new connection is a reconnect; TP logs and counts both, and counts the rows missed in between as `missed`. The row width is checked against `kdb/schemas.q` at registration: a mismatched binary is refused and exits with the reason in its own log. TP never drops a row on a guess: a backward fhSeqNo inside a session is accepted and counted as `outOfOrder`, rows from an unregistered publisher are accepted and counted, and rows with the wrong width are rejected and counted as `schemaMismatch`. See `.health[]` and `.tp.status[]`.

**Partition date.** WDB routes every row to the HDB partition for the date of its own `time` column, which is the feed handler's UTC receive timestamp, for all three tables. Rows are never assigned a date by when an end-of-day message arrived, and WDB rolls on its own clock, so a missed or late end-of-day cannot mix two days into one partition. Trade-off: Binance's archive files are split by exchange time, so a live partition and an archive day differ by the handful of rows whose exchange timestamp falls on one side of midnight and whose receive timestamp falls on the other. Comparing the two needs those few rows from the neighbouring partition.

Pipeline-wide table schemas live in `kdb/schemas.q` and are loaded by every q process, TP included. Adding or modifying a column there propagates everywhere on the next restart; the field indices TP and WDB use (fhSeqNo, tpSeqNo, expected feed-handler row width) are derived from the schema per table, and a handler whose row width disagrees with the schema is refused at registration.

## Tests

Run the full suite from the project root:
```bash
./tests/run_tests.sh
```

The runner discovers `tests/test_*.q`, `tests/test_*.sh`, and any compiled binaries at `build/test_*`, then reports pass/fail per file. Current coverage:

- **`test_schemas.q`** — schemas.q column counts, types, and derived index positions. Catches accidental schema changes that would break the rest of the pipeline.
- **`test_afml.q`** — Q tests for AFML primitives in `kdb/ml/afml.q`.
- **`test_labels.q`** — Q tests for labeling primitives in `kdb/ml/labels.q`.
- **`test_depth_config.sh`** — the shared symbols/depth config: depth 3 end to end (schema, widths, registration, the real quote handler refused with exit code 2), TP and WDB refusing to start over partitions or tmp dirs of another depth, bad shared configs, each quote binary refusing the other market's config.
- **`test_hdb_migrate.sh`** — the layout guard and `hdb_migrate.q` on an old-layout sandbox HDB: refusal to start, dry run, byte-identical old files after `-apply`, null columns, HDB-wide queries, and what the tool refuses.
- **`test_quote_seq.sh`** — `check_quote_seq.q` on synthetic partitions: clean chains on both markets, marked and unmarked breaks with their times, rows without ids.
- **`test_resend.sh`** — `build/sim_trade_publisher` (synthetic trades through the handlers' own `TpPublisher`) with TP killed by `kill -9` and by SIGTERM mid-stream: `missed` stays 0 and the logged `fhSeqNo` is exactly 1..N; with a ring of one row, what cannot be resent equals TP's `missed`.
- **`test_trade_gap.sh`** — trade-id gaps: recorded in `trade_gap`, kept while TP is down, detected across a handler restart (also with TP killed, and on a new day), backfilled from a fake exchange with every missing id on disk exactly once, the unrecoverable reasons, and a handler killed mid-backfill resuming without duplicates.
- **`test_recent_status.sh`** — problems are flagged while within the alert window and clear afterwards, while totals remain.
- **`build/test_resend_ring`**, **`build/test_trade_gap`**, **`build/test_trade_backfill`** — C++ unit tests (Catch2) for the resend ring, gap detection and the gap row, and the backfill with a fake fetcher (paging, rate budget, failures, every unrecoverable reason, resume, the REST reply parsers).
- **`test_smoke.sh`** — starts each q process (tp, wdb) in isolation against test ports, asserts `.health[]` returns a sane response. Catches load-time errors and missing `.health[]` interface.
- **`test_wdb_eod.sh`** — full TP→WDB integration test: publishes synthetic data, forces EOD, verifies a partition lands in the sandbox HDB with correct row counts. Validates the EOD persistence path end-to-end.
- **`build/test_order_book`** — C++ unit tests (Catch2) for `OrderBookManager`: state machine (INIT→SYNCING→VALID→INVALID), full-depth storage, horizon and background refresh, spot and USD-M futures sync rules, configurable depth, delta semantics (insert/update/delete via qty=0), sequence-gap detection, multi-symbol independence, and Binance-spec compliance for overlapping deltas, boundary cases, and entirely-stale events.
- **`build/test_snapshot_scheduler`** — C++ unit tests (Catch2) for `SnapshotScheduler`: backoff schedule and cap, jitter, weight budget for both markets, 429/418 and used-weight pauses, and an hour-long simulated REST outage that must stay under the request bound.
- **`build/test_quote_row`** — C++ unit tests (Catch2) for the quote row sent to TP: width and column positions for several depths, with and without the futures transaction time.
- **`build/test_snapshot_worker`** — C++ unit tests (Catch2) for `SnapshotWorker`: bounded-queue semantics, drop-oldest on overflow, worker thread lifecycle, request-id stale-result discard, shutdown signalling.
- **`build/test_json_reader`** — C++ unit tests (Catch2) for `JsonReader` and `parseLevelPair`: missing keys, wrong types, malformed numeric strings, nested-object error propagation, first-error-wins semantics, and level-array edge cases (wrong shape, non-string elements, unparseable content, future-compat with extra elements).
- **`build/test_trade_fh_row_construction`** — C++ unit tests (Catch2) for `buildTradeRow`: schema-driven K-object construction for both spot trade and futures aggTrade payloads, FH observation-stamp population, type correctness across all columns.
- **`build/test_stream_path`** — C++ unit tests (Catch2) for `buildStreamPath`: combined-stream URL assembly for one or many symbols with arbitrary stream suffixes.
- **`build/test_aggtrade_parse`** — C++ unit tests (Catch2) for futures aggTrade JSON parsing: required-field extraction (`a`/`f`/`l`/`p`/`q`/`T`/`m`), missing-field handling, type-mismatch behaviour.

C++ tests are built when `cpp/third_party/catch2/catch_amalgamated.{hpp,cpp}` are present (download from https://github.com/catchorg/Catch2/releases).

Tests run on isolated ports (production + 10000) so they're safe to run while the live pipeline is up. Sandbox state goes under `tests/sandbox/` and is auto-cleaned on success, preserved on failure for inspection.

## Query Interfaces

Connect with `q -p` or any kdb+ client. A few examples:

```q
// TP (port 5010) — durability log status, sequence tracking, replay
.tp.statusDict[]              / counters: gaps, dups, tpSeqNo, log chunks
.tp.lastAccepted[`trade]      / highest fhSeqNo accepted from trade FH
.tp.currentSeqNo[]            / current monotonic tpSeqNo
.tp.replayFrom[`trade_binance; fromSeq]   / replay slice from log

// WDB (port 5011) — Phase 4 replay state
.wdb.replayStatus[]           / lastTpSeqNo, replayMode, replay counters

// All processes — standardized health check
.health[]
```

WDB persists its replay checkpoint to `$T2S_TMP_DIR/wdb.lastTpSeqNo` after every successful flush. On restart, it loads this and asks TP to replay everything since, so disk-persisted data is recoverable across WDB or TP restarts.

## Backtest Framework

A research tool that replays historical market data through a user-written strategy under realistic cost assumptions. Lives in `kdb/framework/` (the engine, six files) and `kdb/strategies/` (individual strategy files). Designed so strategy authors write only alpha logic; the framework owns position tracking, risk, and execution simulation.

**Four-layer architecture:**

1. **Strategy** (in `kdb/strategies/`) — pure alpha. Reacts to market events via callbacks, emits trading *intents*. Does not place orders directly, does not track position, does not implement stops.
2. **Pre-trade checks** (`pretrade.q`) — validates every intent before it becomes a fill. v0.1 rules: max position size per symbol, kill-switch enforcement.
3. **Execution** (`execution.q`) — paper-fill simulator. Synthesizes bid/ask from last trade price plus a configurable half-spread. Applies Binance USDS-M Regular fees (0.05% taker, 0.02% maker, optional 10% BNB discount).
4. **Position tracking** (`position.q`) — single source of truth for current state: net position, cost basis, realized/unrealized P&L, fees paid, funding paid. Applies funding charges at each 8h boundary (00:00, 08:00, 16:00 UTC) from the loaded funding history. Runs auto-exit policies (stop-loss, kill switch) which emit forced-exit intents back through the same pipeline.

The replay driver (`replay.q`) loads splayed trade data + funding events from `hdb_binancedata/` and dispatches them to the framework in time order; the framework's virtual clock injects funding events at the right moments. In a future live runner, a CTP subscription would take `replay.q`'s place — same framework, different driver.

**Strategy contract.** A strategy is a q file that defines callbacks in its own namespace:

```q
.strat.myStrategy.init     [cfg]                -> state                   // required
.strat.myStrategy.onTrade  [state; tradeEvent]  -> (newState; intents)     // required
.strat.myStrategy.onFill   [state; fillEvent]   -> newState                // optional
.strat.myStrategy.onTimer  [state; ts]          -> (newState; intents)     // optional
.strat.myStrategy.onFunding[state; fundingEv]   -> newState                // optional
```

State is whatever shape the strategy wants (the framework holds it and passes it back). Intents are tagged dicts: `` `action`sym`qty`source!(`buy|`sell|`flatten; `BTCUSDT; 0.1; `strategy) ``.

**Running a backtest:**

```bash
# Default: BTCUSDT, 2026-06-06, aggTrade_fut, momentum strategy
q kdb/framework/backtest.q

# With strategy parameter overrides (any unknown flag is forwarded to the strategy's cfg)
q kdb/framework/backtest.q -fastN 10000 -slowN 50000 -tradeQty 0.1

# Different symbol / date / data granularity
q kdb/framework/backtest.q -sym BTCUSDT -start 2026.06.01 -end 2026.06.07 -table trade_fut
```

The runner loads the framework, calls the strategy's `init`, dispatches every event in the historical range, and prints a position report with gross P&L, fees, funding, and net.

**v0.1 limitations** worth knowing before designing strategies against this:

- Single strategy per run (architecture supports multi-strategy; example doesn't exercise it)
- Single-symbol example (architecture supports multi-symbol)
- Market orders only — no limit-order support
- No size impact in fills (any size fills at the same synthetic price)
- No partial fills
- Kill switch is one-shot (resets only on process restart)
- Equity-curve output is final-only; no per-tick time series saved

## Research Workflow

The historical side of the project supports offline analysis and ML research against partitioned trade data.

**Loading historical data.** Three loaders live in `kdb/utils/` and share the same shape (interactive REPL or `-range` scripted mode, `BINANCE_DOWNLOAD_DIR` / `HDB_BINANCE_DIR` env vars, idempotent on existing partitions, polite-sleep between dates).

- **`tradeLoader.q`** downloads spot trade dailies from `data.binance.vision/data/spot/daily/trades/` into a `trade` table.
- **`aggTradeLoaderFut.q`** downloads USD-M futures aggTrade dailies from `data.binance.vision/data/futures/um/daily/aggTrades/` into an `aggTrade_fut` table. Matches the live pipeline's granularity (the `@aggTrade` WebSocket stream).
- **`tradeLoaderFut.q`** downloads USD-M futures per-fill trades from `data.binance.vision/data/futures/um/daily/trades/` into a `trade_fut` table. Finer granularity than the live stream provides — there is no `@trade` endpoint for futures — but useful for microstructure research where individual fill timing matters. Files are 3–10× larger than the equivalent aggTrade archive.

Both futures loaders write under `hdb_binancedata/<date>/` as sibling tables (`aggTrade_fut/`, `trade_fut/`), so a single date can host both granularities side by side. Schemas in the research HDB deliberately omit observation columns (`fhRecvTimeUtcNs`, `tpSeqNo`, etc.) that exist only in the live HDB.

Two ways to use any loader:

```bash
# Interactive: defines functions, drops into REPL
q kdb/utils/tradeLoader.q
q kdb/utils/aggTradeLoaderFut.q
q kdb/utils/tradeLoaderFut.q
```

```q
downloadAndLoad[2026.01.17; `BTCUSDT`ETHUSDT`SOLUSDT]
downloadAndLoadRange[2026.01.10; 2026.01.20; `BTCUSDT`ETHUSDT]
```

```bash
# Scripted: runs the range non-interactively, exits when done
q kdb/utils/tradeLoader.q       -range 2026.01.10 2026.01.20 BTCUSDT,ETHUSDT
q kdb/utils/aggTradeLoaderFut.q -range 2026.06.01 2026.06.07 BTCUSDT
q kdb/utils/tradeLoaderFut.q    -range 2026.06.01 2026.06.07 BTCUSDT
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

## Documentation

- `markdown_docs/` — design notes, intraday writedown patterns, compression notes, dollar-imbalance bars guide
- Architecture Decision Records and the project white paper: [tick-to-signal-docs](https://github.com/PhilSing24/tick-to-signal-docs)
- Inline ADR references in C++ headers (`@see docs/decisions/adr-NNN-*.md`) point to the docs repo above

## Known Gaps

The ML pipeline (`kdb/ml/`) is actively developed and APIs may change. The live tick pipeline is the stable, primary deliverable.

C++ unit tests cover `OrderBookManager` (both sync rules, depth, horizon, refresh, a randomised market), `SnapshotScheduler`, `SnapshotWorker`, `JsonReader`, the quote row, plus narrower units of the trade FH path: `buildTradeRow` (both schemas), `buildStreamPath`, and futures aggTrade parsing. The end-to-end FH classes themselves are still exercised via the live pipeline rather than in isolated tests. Trade output was end-to-end validated against the Binance Vision archive on 2026-05-09 (1,002,373 BTCUSDT trades, byte-identical modulo µs/ms timestamp resolution — Binance Vision archives carry microsecond precision, the WebSocket stream publishes milliseconds). Futures aggTrade output was validated against the Binance Vision archive on 2026-06-06 (2,274,464 BTCUSDT aggTrades, full UTC day span).

WDB replay reads across daily logs, so a disconnect spanning midnight UTC is caught up in full. A TP restart no longer loses the rows that were in flight: handlers resend them (see **No silent loss**).

The quote book is exact inside the snapshot horizon only (1000 levels per side, the futures maximum). A fast move through all known levels on one side makes that side's quotes invalid until the background refresh lands; this is counted as `depthExhausted`, never published as a valid row.

The futures aggTrade and depth events carry a field `st` whose meaning I could not confirm in Binance's documentation; it is not stored. The depth event's `ps` (pair) is not stored either.

Rows stored before 2026-10-04 have nulls in the update-id columns and in `qtyExRpi`, and tables that did not exist on a date (`quote_binance_fut` before 2026-10-04, `trade_binance_fut` in the oldest partitions) are empty for it. `hdb_migrate.q` added those columns and empty tables, so HDB-wide queries on all four live tables work for every date.

## License

MIT — see [LICENSE](LICENSE) file.
