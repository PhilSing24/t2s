# Repository review (2026-09-16)

Four-pass review of the `t2s` repository as it stood at commit `755f9f8` on
`main`. Written as background for the follow-up work that starts with the
`slim-pipeline` branch. File references are to that commit; several of the
files named here are removed in later steps.

Method: every q, C++, shell, config and test file was read. The schema test
and the six C++ test binaries were run and pass. The two shell integration
tests were not run because of finding 1 in pass 2. q semantics behind the
findings were confirmed in a scratch q session.

---

## Pass 1: Structure

### What the pieces are

- **Ingest.** Three C++ binaries from one static library. The trade handler
  takes a market config so spot and futures share one class. The quote
  handler rebuilds a five-level book from the depth stream, with REST
  snapshots fetched on a worker thread. All three publish row by row to TP
  over async IPC.
- **Hub.** `kdb/tick/tp.q` is the only durable point. It stamps a monotonic
  tpSeqNo, checks each handler's fhSeqNo for gaps, appends every row to a
  daily log, and fans out through the KDB-X pubsub module. EOD is a
  one-second timer watching the date.
- **Two fan-out tiers.** TP feeds WDB, SIG and MLE with raw rows.
  `kdb/tick/chained_tp.q` batches into one-second tables for RDB, RTE, TEL
  and PNL. SIG publishes positions back into CTP, which relays them to PNL.
- **Persistence.** `kdb/tick/wdb.q` is a w.q-style write-only RDB. It
  flushes at 50k rows into a per-day tmp dir, keeps a per-table tpSeqNo
  checkpoint, replays from TP on connect, recovers orphan dirs at start,
  and sorts then moves at EOD.
- **Research.** Three near-identical archive loaders plus a funding loader
  into a second HDB, the AFML bar builder, features, triple-barrier labels,
  and a five-file backtest framework with a runner and four strategy files.
- **Ops.** A tmux start script ordered by sleeps, a stop script, a
  post-midnight check, an on-demand log manager, an MCP sidecar, and four
  dashboards.

### What grew rather than was designed

1. **Schema truth is split.** `tp.q` hardcodes all four schemas instead of
   loading `kdb/schemas.q`, and derives its gap-detection indices from that
   local copy. The README says every process loads the shared file. A
   change there leaves TP silently on the old layout.
2. **Connection handling is pasted nine times.** The backoff cast bug was
   fixed with an explanatory comment in RDB, RTE and WDB. The same bug is
   still live in CTP, SIG, PNL, TEL and MLE. The two sequences differ in q:
   `1000*1.5 xexp n` cast after gives 1000 1500 2250 3375 5063 7594; cast
   before gives 1000 2000 2000 3000 5000 8000.
3. **Two message shapes downstream.** TP subscribers receive lists and CTP
   subscribers receive tables. Each consumer carries its own type switch,
   and RTE and TEL unroll every batch back into per-row calls. SIG only
   understands rows.
4. **Three loaders that are one loader.** A diff of the futures loaders
   shows the same skeleton. The shell-pipe fix and header stripping live
   only in the futures copies. All three plus the funding loader own the
   same `.cfg` namespace, so no two can be loaded in one session.
5. **Three notions of position, two of imbalance bars.** SIG and PNL
   positions, MLE positions, and the framework book have different schemas.
   `mle.q` signs trades by tick rule while `afml.q` signs by buyerIsMaker.
   Same name, different quantity. The RSI code is duplicated between
   `sig.q` and `features.q`.
6. **Demo scaffolding beside production code.** SIG fires one RSI signal
   about five minutes after start and never re-evaluates. PNL just marks
   it. They sit in the component table next to TP and WDB with no
   distinction.
7. **Dead and drifted surfaces.** RTE has a log-replay feature that reads a
   logs directory relative to its own cwd, where no TP log exists.
   `dashboards/Analytics.json` calls three RTE functions that do not exist:
   getSummary with two arguments, getImbalanceAll, and getCorrelation.
   `check_eod.sh` hardcodes the home path. `stop.sh` reads PID files
   nothing writes.
8. **Path and cwd coupling.** Every q process must start from its own
   directory for the relative schema load. The backtest runner and
   strategies hardcode repo-root paths. Configuration is q assignments at
   the top of each file, and the tests patch them with sed.
9. **No log retention.** `kdb/tick/logs` holds 9.9 GB and nothing prunes
   it automatically.
10. **Namespace collision waiting.** Strategies define `.sig.imbalance.*`
    while the live signal process owns `.sig.*`.

The TP, WDB and feed handler core is the designed part and reads that way.
The ring around it grew by copy.

---

## Pass 2: Correctness

Ranked by severity.

1. **The test suite touches production state.** `tests/test_wdb_eod.sh`
   patches the sandbox WDB's HDB path but not its tmp dir or checkpoint
   file, both of which read `T2S_TMP_DIR`. With that variable exported, the
   sandbox WDB writes into the real `tmp/`, and on forced EOD it moves
   `tmp/tmp.<today>` into the sandbox HDB, which the script deletes on
   success. If production WDB is running, its intraday partition is moved
   out from under it and destroyed. The test also overwrites the production
   checkpoint with test sequence numbers, so the next real start replays
   the whole log and appends duplicates. The smoke test's WDB runs orphan
   recovery against the production tmp dir, so it would move
   `tmp/tmp.2026.06.08` into the HDB. The smoke test also only patches
   `tpPort`, so the RDB, RTE, TEL and PNL sandboxes subscribe to the live
   CTP. The README says the suite is safe alongside the live pipeline. It
   is not.

2. **Every clean stop duplicates rows.** `.z.exit` in `wdb.q` flushes
   buffers to the tmp partition but does not advance the checkpoint. On
   restart, replay re-delivers everything since the old checkpoint and
   appends it again. Nothing on the disk path dedupes on tpSeqNo. `stop.sh`
   sends SIGTERM, so this is the ordinary restart, not a crash.

3. **Replay blocks TP and can lose rows.** `.tp.replayFrom` rescans the
   entire day's log with `-11!` inside TP, once per table, so three full
   passes over a multi-GB file while TP handles nothing. Feed handlers back
   up, Binance drops the socket, and the reconnect meant to fill a gap
   creates one. The result is materialized fully in TP memory. If the scan
   errors midway the trap prints and returns partial rows, after which WDB
   drops every live-buffered row below the cutoff as a duplicate.

4. **tpSeqNo is not monotonic across a TP restart on a new day.** Recovery
   reads only today's log. A restart after midnight before any message
   resets the counter to zero while WDB's checkpoint holds yesterday's
   value. WDB reports nothing to replay, its checkpoint freezes until TP
   catches up, and on each reconnect the rows between subscribe and cutoff
   are dropped as duplicates. This is a boot-order scenario, not an exotic
   one.

5. **A missed EOD corrupts partitions.** WDB takes the partition date from
   its own clock when the endofday message arrives, and only connected
   subscribers receive it. If WDB is disconnected at midnight or TP is
   down, nothing fires. Day N+1 rows keep appending to the day N tmp dir,
   and the next EOD moves two days into one partition. Nothing checks that
   the time column matches the partition date. The stranded
   `tmp/tmp.2026.06.08`, with no matching HDB partition, looks like this
   class.

6. **One invalid quote poisons OBI for the day.** On an invalid book row
   `.rte.updOBI` computes a null raw OBI, the EMA absorbs the null, and
   every later smoothed value is null because the previous smoothed value
   is null. It stays that way until EOD. The dashboard and the MCP path
   both read this.

7. **Silent drop in the sequence heuristic.** A backward fhSeqNo jump under
   1000 is classified as a duplicate and dropped with no log line. Handlers
   never resend, so the only effect of that branch is to discard the first
   messages of a handler that restarted shortly after TP did. Exchange-side
   gaps are visible only as tradeId warnings in the handler's own log. TP's
   gap counters cover the IPC hop, not the exchange hop.

8. **Quote handler REST storm.** A failed snapshot invalidates the book,
   the next delta resets and re-requests, and the worker fails again.
   During a REST outage that is up to ten depth-1000 requests per second
   per symbol at weight 50 each, which is IP-ban territory. There is no
   backoff on snapshot failure, and the delta buffer cap constant is
   declared but never enforced.

9. **Book depth after deletes.** Levels beyond the top five are discarded
   at snapshot time, so deleting a top level leaves a zero slot until a new
   insert lands in range. The published L5 and the OBI computed from it are
   biased after any deletion.

10. **Backtest type trap.** Passing `-tradeQty 1` produces a long, which
    fails with a type error when upserted into the float book column.
    Confirmed in q. The runner infers types and the strategies do not cast.

11. **Smaller items.** RTE log replay never finds a file. CTP's EOD deletes
    from tables it never inserts into. Filtered pubsub subscriptions would
    fail on single-row publishes. The three phantom dashboard calls error
    at render.

Verified passing: the schema test and all six C++ test binaries.

---

## Pass 3: What to do differently, ranked by cost of leaving it

1. **Make the disk path idempotent on tpSeqNo.** Dedupe by table and
   tpSeqNo before write, and persist the checkpoint in the same step as
   every flush, including the exit flush. Cost of leaving: every restart
   quietly poisons the HDB used for research.
2. **Isolate tests by construction.** Pass every path and port as an
   argument or one env prefix, and have the test scripts set all of them.
   Cost: one test run at the wrong moment deletes a day.
3. **Make tpSeqNo durable independently of the daily log, and let replay
   seek.** Persist the counter, keep a small index of sequence ranges per
   log file, and read only the needed range. Cost: the recovery feature is
   the project's headline and it fails on the ordinary restart after
   midnight.
4. **Give WDB its own clock and partition by the data.** Roll on WDB's own
   date check and route rows by the time column rather than by when a
   message arrived. Cost: mixed-day partitions that are painful to unpick.
5. **One schema file, loaded by TP too**, with a startup assertion that the
   handler row width matches. Cost: a silent gap-detection break on the
   next schema change.
6. **One connection module and one loader.** Cost: five copies of every
   future fix.
7. **Log retention plus a disk-space health flag.** Cost: a full disk takes
   the whole pipeline down at once.
8. **Snapshot backoff and an enforced buffer cap** in the quote handler.
   Cost: a REST blip becomes a ban.
9. **Quarantine SIG and PNL under a demo label** so readers do not mistake
   them for the trading path. Cost: mostly reputational, though they also
   emit a position stream nothing real consumes.

---

## Pass 4: Trading-system specifics

### Look-ahead in the signal path

- The replay driver sorts by exchange trade time and dispatches one row at
  a time. The strategy sees only its state and the current trade, and
  execution uses the last price seen. No forward peeking was found in
  framework, execution, position or the four strategies.
- Two subtler leaks remain. The stop-loss fires on the tick that breached
  it and fills at that tick's price plus half a spread, which is zero
  latency between observation and fill. Funding and timer events are
  injected only when the next trade arrives after the boundary, so in
  quiet periods they fire late in virtual time. The runner leaves the timer
  interval at zero, so onTimer strategies never tick.
- Labels use future highs and lows by design. The sigma at bar i includes
  bar i's own close, which is fine for a label but must be shifted before
  use as a feature.
- Research bars and live bars sign trades differently, buyerIsMaker versus
  tick rule. A model trained on one and run on the other will show
  unexplained live decay.
- RTE's vol clock is handler receive time, so a replay of the same day from
  the HDB on exchange time will not reproduce it.

### Do backtest and live share code?

No. The framework has no live driver. The README says a CTP subscription
would take the replay driver's place, but the research HDB and the live
tables use different column names, so even a driver needs an adapter. The
live signal producers are SIG and MLE, and neither uses the framework,
pretrade gate, execution model or position book. Nothing that produces a
live position passes through the risk gate. The only code shared across
research and live is the RSI arithmetic, and that is a copy.

### Fill and latency assumptions in simulated P&L

- Fill price is last trade plus or minus a synthetic half spread of 0.5 bp.
  That is wider than the usual perp spread, so conservative on spread, but
  there is no queue, no impact and no partial fill.
- Latency is zero. The fill happens inside the event that produced the
  intent.
- Every fill is a taker at 5 bp. There is no maker path.
- Marks are last trade rather than Binance mark price, so unrealized P&L
  and the stop-loss do not see the price liquidation uses.
- Live PNL uses last trade too, with no fees or funding at all.

### Does WDB replay reconstruct identical or approximate state?

- Payload: identical. Replayed rows carry the same tpSeqNo and every
  handler and TP column from the log.
- Observation: approximate. The WDB receive stamp is set at replay time and
  no flag marks replayed rows, so latency analysis on the HDB will show
  phantom spikes.
- Ordering: partitions are sorted by symbol at EOD, not by time, so
  within-symbol order is insertion order. Replayed rows land after live
  rows that arrived during the replay window. Within a symbol the disk
  order can disagree with tpSeqNo order across a reconnect.
- Duplicates: possible through the two paths in pass 2.
- Downstream: only WDB replays. RTE, PNL and SIG state after a disconnect
  is whatever it was plus new data. Vol windows, VWAP and OBI history carry
  holes with no marker.

### What happens on a gap it can't recover

- **Disconnect across midnight.** The prior day's rows are unreachable, and
  EOD may never fire, which is the mixed-partition case above.
- **TP restart on a fresh day.** Counter reset, checkpoint ahead of TP,
  replay reports nothing, live rows dropped as duplicates on every
  reconnect until the counter catches up. No error is raised anywhere.
- **Exchange-side gap.** The handler logs a tradeId gap and moves on.
  Nothing backfills from REST or the archive, and nothing records the gap
  in a queryable table. The HDB is silently missing trades. The futures
  archive loader could backfill a date, but no tooling compares a live
  partition against the archive.
- **Quote gap.** The book publishes one invalid row and rebuilds. Consumers
  see isValid false, and RTE's OBI goes null until EOD as described in
  pass 2.

---

## Overall

The ingestion core and the durability design are stronger than most hobby
pipelines, and the C++ side is genuinely careful. The weak points cluster
in three places. The restart and test paths corrupt the very HDB the
research side depends on. The replay guarantee is narrower than the README
claims. And the research framework is a good design that nothing live uses
yet, so the backtest results do not yet say anything about what the live
system would do.
