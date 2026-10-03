/ wdb.q - Write-only RDB with intraday writedown
/ Based on the w.q pattern - writes to disk when MAXROWS exceeded.
/ -
/ Durability model (see markdown_docs/review.md, pass 3 items 1 and 4):
/   - Every row is routed to a partition by ITS OWN `time` column (the feed
/     handler receive timestamp, UTC), never by when an end-of-day message
/     happened to arrive. One in-memory buffer can therefore feed two
/     tmp.<date> directories around midnight.
/   - WDB rolls on its own clock: when its date advances, it flushes, sorts
/     and moves every tmp.<date> with date < today into the HDB. TP's
/     endofday message is only a redundant trigger for the same roll, and
/     startup orphan recovery is the same roll too.
/   - Every flush to disk is followed, in the same step, by an atomic write
/     of the per-table tpSeqNo checkpoint (temp file + rename). This holds
/     for the interval flush, the roll, the manual flush, the graceful
/     shutdown and the SIGTERM exit flush.
/   - Every row received (live, replayed, or drained from the reconnect
/     buffer) is dropped if its tpSeqNo is at or below the table's
/     checkpoint, so a replay or a restart can never append duplicates.
/   - Nothing is dropped silently: duplicates, late rows and unexpected
/     dates are logged and counted; counters are exposed in .health[] and
/     .wdb.replayStatus[].

/ -------------------------------------------------------
/ Configuration
/ -------------------------------------------------------

/ Ports can be overridden by environment variables so a test harness can
/ sandbox the process without editing this file. Defaults are production.
.wdb.cfg.port:$[count v:getenv `T2S_WDB_PORT; "J"$v; 5011];
.wdb.cfg.tpPort:$[count v:getenv `T2S_WDB_TP_PORT; "J"$v; 5010];
/ HDB directory: read from env var, fall back to a relative path.
/ Override at launch with T2S_HDB_DIR=/path/to/hdb (recommended: absolute path).
.wdb.cfg.hdbDir:hsym `$ $[count v:getenv `T2S_HDB_DIR; v; "../hdb"];
/ Rows buffered per table before an interval flush. T2S_WDB_MAXROWS is a
/ test hook so a sandbox can force frequent flushes; production is 50000.
.wdb.cfg.maxRows:$[count v:getenv `T2S_WDB_MAXROWS; "J"$v; 50000];
/ Seconds to wait after the date advances before rolling the previous day,
/ so rows still in flight with yesterday's timestamp land before the move.
/ T2S_WDB_ROLL_GRACE_SEC is a test hook; production is 5.
.wdb.cfg.rollGraceSec:$[count v:getenv `T2S_WDB_ROLL_GRACE_SEC; "J"$v; 5];
/ A row dated more than this many days before today is "unexpected" (it is
/ still written, to tmp.<date>, but counted and logged).
.wdb.cfg.maxPastDays:7;

/ Enable compression for HDB writes
/ zstd, 2^17 block, level 1
.z.zd:(17;5;1);

/ Connection resilience
.wdb.conn.handle:0N;                   / TP connection handle
.wdb.conn.state:`disconnected;         / `disconnected`connecting`connected
.wdb.conn.lastAttempt:0Np;             / Last connection attempt time
.wdb.conn.retryCount:0;                / Consecutive failed attempts
.wdb.conn.cfg.baseDelayMs:1000;        / Initial retry delay (1 sec)
.wdb.conn.cfg.maxDelayMs:30000;        / Max retry delay (30 sec)
.wdb.conn.cfg.backoffMultiplier:1.5;   / Exponential backoff factor

/ Timer interval for reconnection and roll checks
.wdb.cfg.timerMs:5000;

system "g 0";

.wdb.epochOffset:neg "j"$1970.01.01D0;
.proc.startTime:.z.p;

/ Daily statistics (reset at roll)
.wdb.stats.flushCount:0j;
.wdb.stats.rowsWritten:0j;
.wdb.stats.tradesReceived:0j;
.wdb.stats.aggTradesReceived:0j;
.wdb.stats.quotesReceived:0j;

/ Durability counters (NOT reset at roll - they describe anomalies the
/ operator should look at)
.wdb.stats.duplicatesDropped:0j;     / rows with tpSeqNo <= checkpoint, dropped
.wdb.stats.lateRows:0j;              / rows dated for a day already moved to HDB
.wdb.stats.unexpectedDateRows:0j;    / rows dated in the future or far in the past
.wdb.stats.counterResets:0j;         / TP tpSeqNo seen below our checkpoint (previous-day checkpoint)
.wdb.stats.haltedRowsDropped:0j;     / rows dropped while halted (see .wdb.halted)
.wdb.stats.checkpointBehindDisk:0j;  / startups where tmp.* held tpSeqNo above the checkpoint
.wdb.lastRollDate:0Nd;

/ -------------------------------------------------------
/ Clock
/ -------------------------------------------------------
/ WDB decides "today" itself. .wdb.clock.fixed is a TEST HOOK: when non-null
/ it replaces .z.d so a test can move the process across midnight. It is
/ settable at start via T2S_WDB_FAKE_DATE and at runtime via .wdb.clock.set.
.wdb.clock.fixed:$[count v:getenv `T2S_WDB_FAKE_DATE; "D"$v; 0Nd];
.wdb.today:{[] $[null .wdb.clock.fixed; .z.d; .wdb.clock.fixed]};
.wdb.clock.set:{[d]
  .wdb.clock.fixed:d;
  -1 "WDB: clock override -> ",string d;
  .wdb.checkRoll[];
 };

/ Date WDB currently believes it is; when .wdb.today[] passes it, a roll is
/ scheduled after the grace period.
.wdb.currentDate:.wdb.today[];
.wdb.rollDueAt:0Np;

/ -------------------------------------------------------
/ Halt state (counter-reset safeguard)
/ -------------------------------------------------------
/ If TP's tpSeqNo is below a checkpoint that was written TODAY, something
/ is wrong that replay cannot fix (two TPs? a hand-edited checkpoint?).
/ WDB then refuses to write anything until an operator intervenes: rows
/ are dropped (counted, rate-limited log) and .health[] reports `error.
.wdb.halted:0b;
.wdb.haltReason:"";

/ -------------------------------------------------------
/ Phase 4: Replay-on-reconnect state
/ -------------------------------------------------------
/ WDB persists the highest tpSeqNo it has successfully flushed to disk,
/ PER TABLE, together with the date the checkpoint was written. On
/ reconnect, it asks TP to replay everything since that point.

/ Checkpoint file. T2S_WDB_CHECKPOINT overrides the full path; otherwise it
/ sits in T2S_TMP_DIR (or ../ relative to cwd).
.wdb.cfg.checkpointFile:hsym `$ $[count v:getenv `T2S_WDB_CHECKPOINT; v; raze ($[count v:getenv `T2S_TMP_DIR; v; "../"]; "wdb.lastTpSeqNo")];

/ Highest tpSeqNo successfully flushed to disk PER TABLE. A global cursor
/ would advance past unflushed rows of the other tables, so it is per table.
.wdb.lastTpSeqNo:`trade_binance`trade_binance_fut`quote_binance ! 0 0 0j;
/ Date (WDB clock) at which the checkpoint was last written. Null means a
/ legacy checkpoint without a date, treated as "a previous day".
.wdb.checkpointDate:0Nd;

/ Replay stats (reset at roll)
.wdb.stats.replayRowsApplied:0j;
.wdb.stats.replayDuplicatesFiltered:0j;

/ Buffers used during the reconnect window: between subscribing live and
/ finishing replay, incoming live messages accumulate here.
.wdb.replayLiveBuffer.trade_binance:();
.wdb.replayLiveBuffer.trade_binance_fut:();
.wdb.replayLiveBuffer.quote_binance:();
.wdb.replayMode:0b;
.wdb.replayCutoff:0j;

/ -------------------------------------------------------
/ Temp directory for intraday writes: one tmp.<date> per data date
/ -------------------------------------------------------
.wdb.tmpDir:$[count v:getenv `T2S_TMP_DIR; v; "../"];
.wdb.tmpPath:{[d] `$":",.wdb.tmpDir,"tmp.",string d};
.wdb.parseTmpDate:{[entryStr]
  if[14 <> count entryStr; :0Nd];
  if[not "tmp." ~ 4#entryStr; :0Nd];
  "D"$ 4_ entryStr
 };

/ -------------------------------------------------------
/ Table schemas (loaded from shared definition)
/ WDB receives data from TP with TP's stamp, then appends its own.
/ -------------------------------------------------------

\l ../schemas.q

trade_binance:.schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo`wdbRecvTimeUtcNs];
trade_binance_fut:.schema.extend[.schema.aggTrade; `tpRecvTimeUtcNs`tpSeqNo`wdbRecvTimeUtcNs];
quote_binance:.schema.extend[.schema.quote; `tpRecvTimeUtcNs`tpSeqNo`wdbRecvTimeUtcNs];

.wdb.tables:`trade_binance`trade_binance_fut`quote_binance;

/ Position of tpSeqNo in the incoming row, PER TABLE. The three schemas have
/ different widths (12, 14 and 28 feed-handler columns), so the index differs:
/ 13 for trade_binance, 15 for trade_binance_fut, 29 for quote_binance. A
/ single index taken from the trade schema read askPrice2 for quotes and
/ fhSeqNo for futures and dropped live rows as duplicates once the
/ checkpoint was non-zero (found in the first live run of this code).
.wdb.idx.tpSeqNo:.wdb.tables ! {[t] (cols value t)?`tpSeqNo} each .wdb.tables;

/ -------------------------------------------------------
/ Utility Functions
/ -------------------------------------------------------

.wdb.tsToNs:{[ts] .wdb.epochOffset+"j"$ts};
.wdb.msg:{[pieces] "WDB: ",raze pieces};

/ -------------------------------------------------------
/ Checkpoint persistence
/ -------------------------------------------------------

/ Load checkpoint from disk. Accepts three on-disk shapes:
/   - current: `seq`date!(per-table dict; date)
/   - legacy per-table dict without date (2 or 3 tables)
/   - legacy scalar long (applied to trade + quote, futures 0)
/ Returns (seqDict; date). Legacy shapes yield a null date, which the
/ counter-reset rule treats as "a previous day".
.wdb.loadCheckpoint:{[]
  defaults:(.wdb.tables ! 0 0 0j; 0Nd);
  if[() ~ key .wdb.cfg.checkpointFile;
    -1 "WDB: no checkpoint file, starting fresh (trade=0, aggTrade=0, quote=0)";
    :defaults
  ];
  v:@[get; .wdb.cfg.checkpointFile; {[err]
    -1 raze ("WDB: ERROR reading checkpoint - "; err);
    `error}];
  if[v ~ `error; :defaults];
  seq:defaults 0; d:0Nd;
  $[-7h = type v;
      [-1 raze ("WDB: migrating legacy scalar checkpoint "; string v; " -> per-table dict");
       seq:.wdb.tables ! (v;0j;v)];
    (99h = type v) and `seq in key v;
      [seq:v `seq; d:v `date];
    99h = type v;
      seq:v;
      -1 "WDB: unrecognised checkpoint shape - starting fresh"];
  / Pre-ADR-013 dict had only trade_binance + quote_binance.
  if[not `trade_binance_fut in key seq;
    -1 "WDB: migrating legacy 2-table checkpoint -> 3-table dict (trade_binance_fut=0)";
    seq:seq, (enlist `trade_binance_fut)!enlist 0j];
  seq:.wdb.tables # seq;
  -1 raze ("WDB: loaded checkpoint - trade="; string seq`trade_binance;
           " aggTrade="; string seq`trade_binance_fut;
           " quote="; string seq`quote_binance;
           " date="; string d);
  (seq; d)
 };

/ Persist the checkpoint atomically: write a temp file, then rename over
/ the real one so a crash mid-write cannot corrupt it. `d` is the date to
/ stamp: today's (WDB clock) for every flush; the previously loaded date
/ when the startup disk reconciliation merely raises the floor, so the
/ counter-reset rule still knows which day the checkpoint came from.
/ Returns 1b on success.
.wdb.saveCheckpoint:{[d]
  tmpStr:1 _ string .wdb.cfg.checkpointFile;
  tmpFile:hsym `$ raze (tmpStr; ".tmp");
  payload:`seq`date!(.wdb.lastTpSeqNo; d);
  result:.[set; (tmpFile; payload); {[err]
    -1 raze ("WDB: ERROR writing checkpoint tmp - "; err);
    `error}];
  if[result ~ `error; :0b];
  cmd:raze ("mv "; tmpStr; ".tmp "; tmpStr);
  ok:@[{[c] system c; 1b}; cmd; {[err] -1 raze ("WDB: ERROR renaming checkpoint - "; err); 0b}];
  if[ok; .wdb.checkpointDate:d];
  ok
 };

/ -------------------------------------------------------
/ Startup reconciliation: the disk is the truth, the checkpoint may lag it
/ -------------------------------------------------------
/ A crash between a tmp write and the checkpoint save leaves rows on disk
/ with tpSeqNo above the checkpoint. Replaying from the checkpoint would
/ re-deliver them, so before connecting WDB scans the tpSeqNo column of
/ every tmp.<date>/<table> and raises each table's floor to the maximum
/ found. Only tmp.* is scanned: rows reach an HDB partition only after a
/ successful checkpoint (the roll aborts otherwise), and HDB partitions may
/ hold rows from an earlier TP counter epoch whose sequence numbers would
/ wrongly raise the floor.
.wdb.diskMaxSeq:{[t]
  tmpRoot:hsym `$ .wdb.tmpDir;
  entries:@[key; tmpRoot; {[err] `symbol$()}];
  dates:.wdb.parseTmpDate each string entries;
  dates:dates where not null dates;
  if[0 = count dates; :0j];
  paths:{[t;d] ` sv (.wdb.tmpPath d), t, `tpSeqNo}[t] each dates;
  vals:{[p] $[() ~ key p; 0Nj; @[{max get x}; p; {[e] 0Nj}]]} each paths;
  0j | max `long$vals
 };

.wdb.reconcileCheckpointWithDisk:{[]
  diskMax:.wdb.tables ! .wdb.diskMaxSeq each .wdb.tables;
  behind:where diskMax > .wdb.lastTpSeqNo;
  if[0 = count behind; -1 "WDB: checkpoint consistent with tmp.* on disk"; :()];
  .wdb.stats.checkpointBehindDisk+:1;
  -1 raze ("WDB: checkpoint BEHIND disk for "; ", " sv string behind;
           " - checkpoint "; .Q.s1 .wdb.lastTpSeqNo behind; " disk "; .Q.s1 diskMax behind;
           " (crash between write and checkpoint?) -> raising checkpoint to disk");
  .wdb.lastTpSeqNo[behind]:diskMax behind;
  .wdb.saveCheckpoint[.wdb.checkpointDate];
 };

/ -------------------------------------------------------
/ Dedupe on receipt
/ -------------------------------------------------------
/ Returns 1b if the row (pre-WDB-stamp list) should be accepted, else logs,
/ counts and returns 0b. `source` is `live, `replay or `drain for the log.
.wdb.acceptRow:{[tbl;row;source]
  seq:row .wdb.idx.tpSeqNo[tbl];
  if[seq <= .wdb.lastTpSeqNo[tbl];
    .wdb.stats.duplicatesDropped+:1;
    -1 raze ("WDB: DUPLICATE dropped - "; string tbl; " tpSeqNo="; string seq;
             " checkpoint="; string .wdb.lastTpSeqNo[tbl]; " source="; string source);
    :0b
  ];
  1b
 };

/ -------------------------------------------------------
/ Flush path - the ONLY code that writes rows to tmp.<date>
/ -------------------------------------------------------

/ Classify a data date relative to today. Returns `ok, `late (partition
/ already in HDB) or `unexpected (future, or older than maxPastDays).
.wdb.classifyDate:{[d]
  today:.wdb.today[];
  if[d > today; :`unexpected];
  if[d < today - .wdb.cfg.maxPastDays; :`unexpected];
  if[(d < today) and not () ~ key .Q.par[.wdb.cfg.hdbDir; d; `]; :`late];
  `ok
 };

/ Append rows (a table) for one table and one data date to tmp.<date>.
/ Returns 1b on success.
.wdb.writeRows:{[t;d;rows]
  kind:.wdb.classifyDate d;
  if[kind = `late;
    .wdb.stats.lateRows+:count rows;
    -1 raze ("WDB: LATE rows - "; string count rows; " "; string t; " rows dated "; string d;
             " but that partition is already in the HDB; writing to "; string .wdb.tmpPath d;
             " for manual review")];
  if[kind = `unexpected;
    .wdb.stats.unexpectedDateRows+:count rows;
    -1 raze ("WDB: UNEXPECTED DATE - "; string count rows; " "; string t; " rows dated "; string d;
             " (today="; string .wdb.today[]; "); writing to "; string .wdb.tmpPath d;
             " for manual review")];
  path:` sv (.wdb.tmpPath d), t, `;
  .[{[p;hdb;r] .[p; (); ,; .Q.en[hdb] r]; 1b}; (path; .wdb.cfg.hdbDir; rows);
    {[t;d;err] -1 raze ("WDB: ERROR writing "; string t; " for "; string d; " - "; err); 0b}[t;d]]
 };

/ Flush one table's buffer to disk, routed by the `time` column's date, then
/ advance and persist the checkpoint. Returns 1b if the whole buffer was
/ written and the checkpoint saved.
/ -
/ Dates are written in ascending order. On a write failure the rows already
/ written are removed from the buffer and the checkpoint advances only if
/ every written tpSeqNo is below every remaining one (true when rows arrive
/ in time order); the failed and later dates stay buffered for retry.
.wdb.flushTable:{[t;reason]
  tbl:value t;
  if[0 = count tbl; :1b];
  / Defensive dedupe at flush time (receipt-time dedupe should make this a no-op)
  cp:.wdb.lastTpSeqNo[t];
  dupIdx:where tbl[`tpSeqNo] <= cp;
  if[count dupIdx;
    .wdb.stats.duplicatesDropped+:count dupIdx;
    -1 raze ("WDB: DUPLICATE dropped at flush - "; string count dupIdx; " "; string t;
             " rows with tpSeqNo <= "; string cp);
    tbl:tbl where tbl[`tpSeqNo] > cp];
  if[0 = count tbl; @[`.; t; 0#]; :1b];
  dates:`date$tbl`time;
  dlist:asc distinct dates;
  -1 raze ("WDB: flushing "; string t; " ("; string count tbl; " rows, "; string count dlist;
           " date(s): "; " " sv string dlist; ") - "; reason);
  written:0#tbl; remaining:tbl; ok:1b;
  i:0;
  while[(i < count dlist) and ok;
    d:dlist i;
    rows:tbl where dates = d;
    ok:.wdb.writeRows[t; d; rows];
    if[ok; written,:rows; remaining:tbl where dates > d];
    i+:1];
  if[0 = count written; :0b];
  .wdb.stats.flushCount+:1;
  .wdb.stats.rowsWritten+:count written;
  / Rebuild the buffer with whatever was not written
  @[`.; t; :; remaining];
  maxWritten:max written`tpSeqNo;
  advance:$[0 = count remaining; 1b; maxWritten < min remaining`tpSeqNo];
  if[advance and maxWritten > .wdb.lastTpSeqNo[t];
    .wdb.lastTpSeqNo[t]:maxWritten;
    if[not .wdb.saveCheckpoint[.wdb.today[]]; ok:0b]];
  if[not advance;
    -1 raze ("WDB: checkpoint for "; string t; " NOT advanced - unwritten rows have lower tpSeqNo")];
  ok
 };

/ Flush every table. Returns 1b if all succeeded.
.wdb.flushAll:{[reason] all .wdb.flushTable[;reason] each .wdb.tables};

/ -------------------------------------------------------
/ Roll: sort + move every tmp.<date> with date < today into the HDB
/ -------------------------------------------------------
/ One routine serves three triggers: WDB's own clock passing midnight (after
/ a grace period), TP's endofday message, and startup orphan recovery.

/ tmp.<date> entries with date < today, oldest first.
.wdb.pendingTmpDates:{[]
  entries:@[key; hsym `$ .wdb.tmpDir; {[err] `symbol$()}];
  dates:.wdb.parseTmpDate each string entries;
  dates:dates where not null dates;
  asc dates where dates < .wdb.today[]
 };

disksort:{[t;c;a]
  if[not`s~attr(t:hsym t)c;
    if[count t;
      ii:iasc iasc flip c!t c,:();
      if[not$[(0,-1+count ii)~(first;last)@\:ii;@[{`s#x;1b};ii;0b];0b];
        {v:get y;
          if[not$[all(fv:first v)~/:256#v;all fv~/:v;0b];
            v[x]:v;
            y set v];
        }[ii] each ` sv't,'get ` sv t,`.d
      ]
    ];
    @[t;first c;a]
  ];
  t}

.wdb.sortTmpTable:{[tmpSym;t]
  @[{[p] disksort[p; `sym; `p#]; 1b};
    ` sv tmpSym, t, `;
    {[t;err] -1 raze ("WDB: ERROR sorting "; string t; " - "; err); 0b}[t]]
 };

/ Sort and move tmp.<d> into the HDB partition for d. Returns 1b on success.
/ Failure leaves the tmp dir in place for inspection. A destination that
/ already exists (late rows written after an earlier roll) is never merged.
.wdb.rollDate:{[d]
  tmpSym:.wdb.tmpPath d;
  tmpStr:1 _ string tmpSym;
  tabs:@[key; tmpSym; {[err] `symbol$()}];
  if[0 = count tabs;
    -1 raze ("WDB: "; tmpStr; " is empty - leaving in place");
    :0b];
  dest:.Q.par[.wdb.cfg.hdbDir; d; `];
  destStr:-1 _ 1 _ string dest;
  if[not () ~ key dest;
    -1 raze ("WDB: ERROR HDB partition "; destStr; " already exists - leaving "; tmpStr;
             " for manual review (late rows?)");
    :0b];
  -1 raze ("WDB: rolling "; tmpStr; " (tables: "; ", " sv string tabs; ")");
  if[not all .wdb.sortTmpTable[tmpSym] each tabs;
    -1 raze ("WDB: ABORTING roll of "; tmpStr; " - sort failures, data preserved");
    :0b];
  moveOk:@[{[c] system c; 1b}; raze ("mv "; tmpStr; " "; destStr);
            {[err] -1 raze ("WDB: ERROR moving partition - "; err); 0b}];
  if[not moveOk; :0b];
  if[() ~ key dest;
    -1 raze ("WDB: ERROR move appeared to succeed but destination missing - "; destStr);
    :0b];
  -1 raze ("WDB: HDB partition created: "; destStr);
  .wdb.lastRollDate:d;
  1b
 };

.wdb.resetDailyStats:{[]
  .wdb.stats.flushCount:0j;
  .wdb.stats.rowsWritten:0j;
  .wdb.stats.tradesReceived:0j;
  .wdb.stats.aggTradesReceived:0j;
  .wdb.stats.quotesReceived:0j;
  .wdb.stats.replayRowsApplied:0j;
  .wdb.stats.replayDuplicatesFiltered:0j;
 };

/ Flush everything (so buffered rows for past dates land in their tmp dirs),
/ then roll every pending past date.
.wdb.roll:{[reason]
  -1 raze ("WDB: roll ("; reason; ") - today="; string .wdb.today[]);
  if[.wdb.halted; -1 "WDB: halted - roll skipped"; :()];
  flushOk:.wdb.flushAll["roll"];
  if[not flushOk; -1 "WDB: roll aborted - flush failures, nothing moved"; :()];
  pending:.wdb.pendingTmpDates[];
  if[0 = count pending; -1 "WDB: nothing to roll"; :()];
  results:.wdb.rollDate each pending;
  -1 raze ("WDB: roll complete - "; string sum results; "/"; string count pending; " partition(s) moved");
  if[any results; .wdb.resetDailyStats[]];
 };

/ Timer hook: detect the date advancing and schedule a roll after the
/ grace period; run the roll once due.
.wdb.checkRoll:{[]
  today:.wdb.today[];
  if[today > .wdb.currentDate;
    -1 raze ("WDB: date advanced "; string .wdb.currentDate; " -> "; string today;
             "; roll in "; string .wdb.cfg.rollGraceSec; "s");
    .wdb.currentDate:today;
    .wdb.rollDueAt:.z.p + `long$.wdb.cfg.rollGraceSec * 1000000000];
  if[(not null .wdb.rollDueAt) and .z.p >= .wdb.rollDueAt;
    .wdb.rollDueAt:0Np;
    .wdb.roll["clock"]];
 };

/ TP's end-of-day broadcast. Redundant with the clock check; schedule the
/ same roll (with grace, since rows with yesterday's time may still be in
/ flight behind the message).
endofday:{[]
  -1 "WDB: endofday received from TP";
  if[null .wdb.rollDueAt;
    .wdb.rollDueAt:.z.p + `long$.wdb.cfg.rollGraceSec * 1000000000];
 };

/ -------------------------------------------------------
/ Connection Management (Resilient)
/ -------------------------------------------------------

.wdb.conn.getDelay:{[]
  delay:`long$ .wdb.conn.cfg.baseDelayMs * .wdb.conn.cfg.backoffMultiplier xexp .wdb.conn.retryCount;
  delay & .wdb.conn.cfg.maxDelayMs
  };

.wdb.conn.canRetry:{[]
  if[null .wdb.conn.lastAttempt; :1b];
  elapsed:`long$(.z.p - .wdb.conn.lastAttempt) % 1000000;
  elapsed >= .wdb.conn.getDelay[]
  };

/ -------------------------------------------------------
/ Replay-on-reconnect logic
/ -------------------------------------------------------
/ Protocol (called from .wdb.connect after subscribe):
/   0. Counter-reset safeguard (see .wdb.checkCounterReset)
/   1. Capture TP's current tpSeqNo as cutoff (separates replay from live)
/   2. Ask TP to replay rows with tpSeqNo > checkpoint, per table
/   3. Apply each replayed row through the dedupe + buffer path
/   4. Drain the live buffer, filtering duplicates of replay output
/   5. Clear replayMode so future upd() calls go through the normal path

.wdb.countReceived:{[tbl]
  $[tbl = `trade_binance;     .wdb.stats.tradesReceived+:1;
    tbl = `trade_binance_fut; .wdb.stats.aggTradesReceived+:1;
    tbl = `quote_binance;     .wdb.stats.quotesReceived+:1;
    ()];
 };

/ Accept a row into the buffer (after dedupe), stamping wdbRecvTimeUtcNs.
.wdb.ingest:{[tbl;row;source]
  if[not .wdb.acceptRow[tbl;row;source]; :()];
  .wdb.countReceived tbl;
  if[source = `replay; .wdb.stats.replayRowsApplied+:1];
  tbl insert row, .wdb.tsToNs[.z.p];
  if[.wdb.cfg.maxRows < count value tbl; .wdb.flushTable[tbl; "maxRows"]];
 };

.wdb.drainLiveBuffer:{[tbl]
  buf:.wdb.replayLiveBuffer[tbl];
  if[0 = count buf; :()];
  bufSeqs:{[tbl;r] r .wdb.idx.tpSeqNo[tbl]}[tbl] each buf;
  isDup:bufSeqs <= .wdb.replayCutoff;
  dupCount:sum isDup;
  newRows:buf where not isDup;
  -1 raze ("WDB: drained "; string count buf; " buffered "; string tbl;
           " (filtered "; string dupCount; " replay dups, "; string count newRows; " new)");
  .wdb.stats.replayDuplicatesFiltered+:dupCount;
  .wdb.ingest[tbl;;`drain] each newRows;
  .wdb.replayLiveBuffer[tbl]:();
 };

/ Counter-reset safeguard. TP's tpSeqNo (cutoff) below any table's
/ checkpoint means TP's counter went backwards (a restart on a day with no
/ log, review finding 4). If the checkpoint was written on a previous day
/ (or has no date), TP's log is a fresh epoch and nothing in it is on our
/ disk: reset the checkpoints to zero, log loudly, count it, and let replay
/ start from the beginning of the new log. If the checkpoint was written
/ TODAY, that cannot be explained by a day roll: halt and wait for an
/ operator. Returns 1b if replay may proceed.
.wdb.checkCounterReset:{[cutoff]
  behind:where cutoff < .wdb.lastTpSeqNo;
  if[0 = count behind; :1b];
  today:.wdb.today[];
  if[(null .wdb.checkpointDate) or .wdb.checkpointDate < today;
    .wdb.stats.counterResets+:1;
    -1 raze ("WDB: ERROR TP tpSeqNo "; string cutoff; " is below checkpoint for ";
             ", " sv string behind; " ("; .Q.s1 .wdb.lastTpSeqNo;
             "); checkpoint dated "; string .wdb.checkpointDate;
             " < today "; string today;
             " -> treating TP log as a new epoch, resetting checkpoints to 0");
    .wdb.lastTpSeqNo:.wdb.tables ! 0 0 0j;
    .wdb.saveCheckpoint[today];
    :1b];
  .wdb.halted:1b;
  .wdb.haltReason:raze ("TP tpSeqNo "; string cutoff; " below today's checkpoint for ";
                        ", " sv string behind; " ("; .Q.s1 .wdb.lastTpSeqNo; ")");
  -1 raze ("WDB: HALTED - "; .wdb.haltReason;
           ". Not writing until an operator intervenes (fix checkpoint "; string .wdb.cfg.checkpointFile;
           " and restart).");
  0b
 };

.wdb.runReplay:{[h]
  -1 raze ("WDB: starting replay - checkpoint: trade="; string .wdb.lastTpSeqNo`trade_binance;
           " aggTrade="; string .wdb.lastTpSeqNo`trade_binance_fut;
           " quote="; string .wdb.lastTpSeqNo`quote_binance;
           " date="; string .wdb.checkpointDate);
  .wdb.replayCutoff:h ".tp.currentSeqNo[]";
  -1 raze ("WDB: replay cutoff = "; string .wdb.replayCutoff);
  if[not .wdb.checkCounterReset .wdb.replayCutoff;
    / Halted: discard the live buffer (counted) and stay out of replay mode
    {[tbl] n:count .wdb.replayLiveBuffer[tbl]; .wdb.stats.haltedRowsDropped+:n; .wdb.replayLiveBuffer[tbl]:()} each .wdb.tables;
    .wdb.replayMode:0b; .wdb.replayCutoff:0j;
    :()];
  {[h;tbl]
    fromSeq:.wdb.lastTpSeqNo[tbl] + 1;
    if[.wdb.replayCutoff < fromSeq;
      -1 raze ("WDB: nothing to replay for "; string tbl;
               " (cutoff "; string .wdb.replayCutoff; " < fromSeq "; string fromSeq; ")");
      :()];
    replayRows:h (`.tp.replayFrom; tbl; fromSeq);
    -1 raze ("WDB: replaying "; string count replayRows; " "; string tbl;
             " rows from tpSeqNo "; string fromSeq);
    {[tbl;rowDict] .wdb.ingest[tbl; value rowDict; `replay]}[tbl;] each replayRows;
  }[h;] each .wdb.tables;
  .wdb.drainLiveBuffer each .wdb.tables;
  .wdb.replayMode:0b;
  .wdb.replayCutoff:0j;
  -1 "WDB: replay complete, switching to live mode";
 };

.wdb.replayStatus:{[]
  `lastTpSeqNoTrade`lastTpSeqNoAggTrade`lastTpSeqNoQuote`checkpointDate`replayMode`replayCutoff`replayRowsApplied`replayDupsFiltered`duplicatesDropped`lateRows`unexpectedDateRows`counterResets`checkpointBehindDisk`halted`haltReason`haltedRowsDropped`lastRollDate`bufferTrades`bufferAggTrades`bufferQuotes!(
    .wdb.lastTpSeqNo`trade_binance;
    .wdb.lastTpSeqNo`trade_binance_fut;
    .wdb.lastTpSeqNo`quote_binance;
    .wdb.checkpointDate;
    .wdb.replayMode;
    .wdb.replayCutoff;
    .wdb.stats.replayRowsApplied;
    .wdb.stats.replayDuplicatesFiltered;
    .wdb.stats.duplicatesDropped;
    .wdb.stats.lateRows;
    .wdb.stats.unexpectedDateRows;
    .wdb.stats.counterResets;
    .wdb.stats.checkpointBehindDisk;
    .wdb.halted;
    .wdb.haltReason;
    .wdb.stats.haltedRowsDropped;
    .wdb.lastRollDate;
    count .wdb.replayLiveBuffer.trade_binance;
    count .wdb.replayLiveBuffer.trade_binance_fut;
    count .wdb.replayLiveBuffer.quote_binance)
 };

/ Main connection function - NEVER THROWS
.wdb.connect:{[]
  if[not null .wdb.conn.handle; :1b];
  if[not .wdb.conn.canRetry[]; :0b];
  .wdb.conn.state:`connecting;
  .wdb.conn.lastAttempt:.z.p;
  -1 "WDB: Connecting to TP on port ",string[.wdb.cfg.tpPort],
     " (attempt ",string[.wdb.conn.retryCount + 1],")...";
  h:@[hopen; `$"::",string[.wdb.cfg.tpPort]; {[err] -1 "WDB: Connection failed - ",err; 0N}];
  if[null h;
    .wdb.conn.retryCount+:1;
    .wdb.conn.state:`disconnected;
    -1 "WDB: Will retry in ",string[.wdb.conn.getDelay[]],"ms";
    :0b
  ];
  / Order matters: replayMode on, subscribe (live rows buffer), replay,
  / drain; runReplay clears replayMode.
  subResult:@[{[h]
    .wdb.replayMode:: 1b;
    res:h(`pubsub.subscribe;`trade_binance;`);
    -1 "WDB: Subscribed to ",string first first res;
    res:h(`pubsub.subscribe;`trade_binance_fut;`);
    -1 "WDB: Subscribed to ",string first first res;
    res:h(`pubsub.subscribe;`quote_binance;`);
    -1 "WDB: Subscribed to ",string first first res;
    .wdb.runReplay[h];
    1b
  }; h; {[err]
    .wdb.replayMode:: 0b;
    -1 "WDB: Subscription/replay failed - ",err;
    0b
  }];
  if[not subResult;
    @[hclose; h; {}];
    .wdb.conn.retryCount+:1;
    .wdb.conn.state:`disconnected;
    :0b
  ];
  .wdb.conn.handle:h;
  .wdb.conn.state:`connected;
  .wdb.conn.retryCount:0;
  -1 "WDB: Connected successfully (handle ",string[h],")";
  1b
  };

.z.pc:{[h]
  if[h = .wdb.conn.handle;
    -1 "WDB: TP connection lost (handle ",string[h],")";
    .wdb.conn.handle:0N;
    .wdb.conn.state:`disconnected;
    .wdb.conn.retryCount:0;
    -1 "WDB: Will attempt reconnection on next timer tick";
  ];
  };

/ -------------------------------------------------------
/ Update Handler
/ -------------------------------------------------------

.wdb.haltLogCount:0j;

upd:{[tbl;data]
  if[not tbl in .wdb.tables; :()];
  if[.wdb.halted;
    .wdb.stats.haltedRowsDropped+:1;
    .wdb.haltLogCount+:1;
    if[(.wdb.haltLogCount <= 10) or 0 = .wdb.haltLogCount mod 10000;
      -1 raze ("WDB: HALTED - dropping "; string tbl; " tpSeqNo="; string data .wdb.idx.tpSeqNo[tbl];
               " (dropped so far: "; string .wdb.stats.haltedRowsDropped; ")")];
    :()];
  / During replay-mode, stash live messages for the post-replay drain.
  if[.wdb.replayMode;
    .wdb.replayLiveBuffer[tbl],:enlist data;
    :()];
  .wdb.ingest[tbl; data; `live];
  };

/ -------------------------------------------------------
/ Health Check
/ -------------------------------------------------------

.health:{[]
  memMB:(`long$.Q.w[][`used]) % 1000000;
  st:$[.wdb.halted; `error;
       .wdb.conn.state = `connected;
         $[(.wdb.stats.lateRows > 0) or .wdb.stats.unexpectedDateRows > 0; `degraded; `ok];
       .wdb.conn.state = `connecting; `degraded;
       `disconnected];
  `process`port`uptime`status`connState`memMB`tradesRecv`aggTradesRecv`quotesRecv`flushes`rowsWritten`bufferTrades`bufferAggTrades`bufferQuotes`duplicatesDropped`lateRows`unexpectedDateRows`counterResets`halted`haltedRowsDropped`lastRollDate!(
    `wdb;
    .wdb.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    .wdb.conn.state;
    memMB;
    .wdb.stats.tradesReceived;
    .wdb.stats.aggTradesReceived;
    .wdb.stats.quotesReceived;
    .wdb.stats.flushCount;
    .wdb.stats.rowsWritten;
    count trade_binance;
    count trade_binance_fut;
    count quote_binance;
    .wdb.stats.duplicatesDropped;
    .wdb.stats.lateRows;
    .wdb.stats.unexpectedDateRows;
    .wdb.stats.counterResets;
    .wdb.halted;
    .wdb.stats.haltedRowsDropped;
    .wdb.lastRollDate
  )
  };

/ -------------------------------------------------------
/ Status Query
/ -------------------------------------------------------

.wdb.status:{[]
  `port`tpPort`connected`maxRows`today`tmpSave`hdbDir`checkpointFile`flushes`rowsWritten`bufferTrades`bufferAggTrades`bufferQuotes`memMB!(
    .wdb.cfg.port;
    .wdb.cfg.tpPort;
    .wdb.conn.state = `connected;
    .wdb.cfg.maxRows;
    .wdb.today[];
    .wdb.tmpPath .wdb.today[];
    .wdb.cfg.hdbDir;
    .wdb.cfg.checkpointFile;
    .wdb.stats.flushCount;
    .wdb.stats.rowsWritten;
    count trade_binance;
    count trade_binance_fut;
    count quote_binance;
    (`long$.Q.w[][`used]) % 1000000
  )
  };

/ -------------------------------------------------------
/ Manual flush, graceful shutdown, exit handler
/ -------------------------------------------------------

.wdb.flush:{[]
  -1 "WDB: Manual flush requested";
  ok:.wdb.flushAll["manual"];
  -1 $[ok; "WDB: Manual flush complete"; "WDB: Manual flush had failures - see above"];
  ok
 };

/ Graceful shutdown: flush everything with checkpoint, then exit. Called by
/ stop.sh over IPC; also the body of the SIGTERM handler below.
.wdb.shutdownFlushed:0b;
.wdb.shutdown:{[]
  if[.wdb.shutdownFlushed; -1 "WDB: shutdown flush already done"; :()];
  -1 "WDB: shutdown requested - flushing all buffers with checkpoint...";
  ok:$[.wdb.halted; [-1 "WDB: halted - nothing to flush"; 1b]; .wdb.flushAll["shutdown"]];
  .wdb.shutdownFlushed:1b;
  -1 $[ok;
       raze ("WDB: shutdown flush complete - checkpoint "; .Q.s1 .wdb.lastTpSeqNo; " date "; string .wdb.checkpointDate);
       "WDB: shutdown flush had FAILURES - buffers left in memory, check log"];
  ok
 };

/ IPC entry point for stop.sh: flush, then exit asynchronously so the
/ caller's message completes before the process goes away.
.wdb.shutdownAndExit:{[]
  ok:.wdb.shutdown[];
  -1 "WDB: exiting";
  system "t 0";
  .z.ts:{[] exit 0};
  system "t 100";
  ok
 };

/ SIGTERM / exit: same flush + checkpoint path.
.z.exit:{[x]
  -1 "WDB: Exit signal received (code: ",string[x],")";
  .wdb.shutdown[];
  };

/ -------------------------------------------------------
/ Timer - reconnection + roll check
/ -------------------------------------------------------

.z.ts:{[]
  if[null .wdb.conn.handle; .wdb.connect[]];
  .wdb.checkRoll[];
  };

/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system"p ",string .wdb.cfg.port;

-1"=======================================================";
-1"WDB (Write-only RDB) starting on port ",string[.wdb.cfg.port];
-1"=======================================================";
-1"Configuration:";
-1"  TP port: ",string[.wdb.cfg.tpPort];
-1"  MAXROWS: ",string[.wdb.cfg.maxRows];
-1"  Today (WDB clock): ",string[.wdb.today[]],$[null .wdb.clock.fixed; ""; "  (FIXED - test hook)"];
-1"  Tmp dir: ",.wdb.tmpDir;
-1"  HDB: ",string[.wdb.cfg.hdbDir];
-1"  Checkpoint: ",string[.wdb.cfg.checkpointFile];
-1"  Roll grace: ",string[.wdb.cfg.rollGraceSec],"s";
-1"";

/ The HDB root must exist before .Q.en can write the sym file into it.
system "mkdir -p ",1 _ string .wdb.cfg.hdbDir;

/ Load checkpoint before connecting so replay knows where to start
cp:.wdb.loadCheckpoint[];
.wdb.lastTpSeqNo:cp 0;
.wdb.checkpointDate:cp 1;
.wdb.reconcileCheckpointWithDisk[];

/ Roll any tmp.<date> left behind by a previous run (orphan recovery) before
/ accepting live data.
.wdb.roll["startup"];

connected:.wdb.connect[];

system "t ",string .wdb.cfg.timerMs;

-1"";
-1"Query Interface:";
-1"  .health[]              / Standardized health check";
-1"  .wdb.status[]          / Full status";
-1"  .wdb.replayStatus[]    / Replay + durability counters";
-1"  .wdb.flush[]           / Manual flush to disk (with checkpoint)";
-1"  .wdb.roll[\"manual\"]    / Flush + move past-date tmp dirs into HDB";
-1"  .wdb.shutdownAndExit[] / Graceful stop (used by stop.sh)";
-1"";
-1"Tables: trade_binance trade_binance_fut quote_binance";
-1"";

$[connected; -1 "WDB: Ready and processing"; -1 "WDB: Started in DEGRADED mode - waiting for TP connection"];
-1"=======================================================";
