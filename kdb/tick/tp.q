/ tp.q - Tickerplant with KDB-X pubsub module
/ -
/ Durability model (see markdown_docs/review.md, pass 3 items 3 and 5):
/   - Schemas come from kdb/schemas.q, the single source of truth. Every
/     index TP uses (fhSeqNo position, expected feed-handler row width) is
/     derived from the loaded schema PER TABLE.
/   - tpSeqNo never goes backwards. TP persists a reservation (current
/     counter + .tp.cfg.seqReserve) atomically in .tp.cfg.seqFile and only
/     hands out numbers below the persisted reservation, so a crash can skip
/     numbers but never reuse one. The file lives in the log dir and MUST
/     NOT be removed by log retention.
/   - Feed handlers announce themselves: every connection to TP starts with
/     .tp.registerSession[table; sessionId; nextFhSeqNo; rowWidth]. A new
/     sessionId is a handler restart (counted, logged, expected sequence
/     taken from the announcement); the same sessionId on a new handle is a
/     reconnect (counted; the gap since TP's last accepted fhSeqNo is
/     counted as missed). A wrong row width is rejected at registration so
/     a mismatched binary fails at its own startup.
/   - TP never drops a row without a log line and a counter. Rows with an
/     unexpected width are rejected and counted (schemaMismatch); rows from
/     a handle that never registered are accepted and counted
/     (unregisteredRows); a backward fhSeqNo inside one session is accepted
/     and counted (outOfOrder).
/   - Replay never runs inside TP. TP writes a seek index next to each log
/     (<date>.idx, one entry per .tp.cfg.indexEvery rows) and answers
/     .tp.replayInfo[] with the log dir, the committed length of today's
/     log and the current tpSeqNo; WDB reads the logs itself (see
/     kdb/tick/logreader.q). The live path is never blocked by a replay.

/ -------------------------------------------------------
/ Configuration
/ -------------------------------------------------------

/ Port and log directory can be overridden by environment variables so a
/ test harness can sandbox the process without editing this file. Defaults
/ are the production values.
.tp.cfg.port:$[count v:getenv `T2S_TP_PORT; "J"$v; 5010];
.tp.cfg.logDir:$[count v:getenv `T2S_TP_LOG_DIR; v; "logs"];
.tp.cfg.logEnabled:1b;

/ tpSeqNo reservation file. Default: next to the daily logs. Log retention
/ must never delete it (see .tp.seq.* below and the README).
.tp.cfg.seqFile:hsym `$ $[count v:getenv `T2S_TP_SEQ_FILE; v; .tp.cfg.logDir,"/tp.tpSeqNo"];
.tp.cfg.seqReserve:10000;

/ Replay index: one (tpSeqNo; byte offset; chunk) entry per this many rows,
/ written next to the log as <date>.idx, so WDB can seek instead of
/ rescanning the day. See kdb/tick/logreader.q.
.tp.cfg.indexEvery:$[count v:getenv `T2S_TP_INDEX_EVERY; "J"$v; 10000];   / env: test hook

/ Disk-space health: .health[] degrades when the log dir's filesystem has
/ less than this many MB free. T2S_TP_MIN_FREE_MB overrides; default 5 GB.
.tp.cfg.minFreeMB:$[count v:getenv `T2S_TP_MIN_FREE_MB; "J"$v; 5120];

/ Clock skew: median over recent rows of (TP receive time - exchange event
/ time) in ms. Normal values are tens to a few hundred ms (network + the
/ exchange's own clock). A WSL clock that drifted after sleep shows up here
/ as a large skew of either sign; .health[] degrades above this threshold.
/ T2S_TP_MAX_SKEW_MS overrides; 0 disables the check (sandbox tests use
/ synthetic rows on fake dates, whose exchange times are days off).
.tp.cfg.maxSkewMs:$[count v:getenv `T2S_TP_MAX_SKEW_MS; "J"$v; 5000];
.tp.cfg.skewSamples:200;

/ WDB's checkpoint, read-only, only to warn at migration time if the seeded
/ counter would be below what WDB has already persisted (WDB would halt).
/ Same resolution rule as wdb.q.
.tp.cfg.wdbCheckpointFile:hsym `$ $[count v:getenv `T2S_WDB_CHECKPOINT; v; raze ($[count v:getenv `T2S_TMP_DIR; v; "../"]; "wdb.lastTpSeqNo")];

system "g 0";

.tp.epochOffset:neg"j"$1970.01.01D0;
.proc.startTime:.z.p;

/ -------------------------------------------------------
/ Clock (test hook)
/ -------------------------------------------------------
/ T2S_TP_FAKE_DATE replaces .z.d for the log file name and the EOD check so a
/ sandbox can simulate "a new day with no log". It is NEVER for production:
/ start.sh refuses to start with it set, and tests/t_guard.q only allows it
/ inside the sandbox.
.tp.clock.fixed:$[count v:getenv `T2S_TP_FAKE_DATE; "D"$v; 0Nd];
.tp.today:{[] $[null .tp.clock.fixed; .z.d; .tp.clock.fixed]};
/ Runtime form of the same hook (tests move TP across midnight): fixes the
/ date and runs the EOD check at once.
.tp.clock.set:{[d]
  .tp.clock.fixed:d;
  -1 "TP: WARNING - clock override -> ",string[d]," (TEST HOOK)";
  .tp.checkEOD[];
 };
if[not null .tp.clock.fixed;
  -1 "=======================================================";
  -1 "TP: WARNING - T2S_TP_FAKE_DATE is set: today is FIXED to ",string[.tp.clock.fixed];
  -1 "TP: WARNING - this is a TEST HOOK; never run production with it";
  -1 "======================================================="];

/ -------------------------------------------------------
/ Table schemas - from the shared file (must exist before pubsub init)
/ -------------------------------------------------------

\l ../schemas.q

/ Refuse to start if existing partitions / tmp dirs hold a live table whose
/ layout differs from the schema: another quote depth, or columns the
/ schema has gained since (see schemas.q). TP writes neither place; it
/ checks so that a mismatch stops the pipeline at its first process
/ instead of after rows were logged. Same defaults as wdb.q.
.tp.cfg.hdbDir:$[count v:getenv `T2S_HDB_DIR; v; "../hdb"];
.tp.cfg.tmpDir:$[count v:getenv `T2S_TMP_DIR; v; "../"];
.schema.requireLayout["TP"; .tp.cfg.hdbDir; .tp.cfg.tmpDir];

trade_binance:.schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo];
trade_binance_fut:.schema.extend[.schema.aggTrade; `tpRecvTimeUtcNs`tpSeqNo];
quote_binance:.schema.extend[.schema.quote; `tpRecvTimeUtcNs`tpSeqNo];
quote_binance_fut:.schema.extend[.schema.quoteFut; `tpRecvTimeUtcNs`tpSeqNo];
health_feed_handler:.schema.health;

.tp.tables:`trade_binance`trade_binance_fut`quote_binance`quote_binance_fut;
.tp.nTables:count .tp.tables;

/ Per-table indices derived from the schema. The incoming feed-handler row
/ has the schema's columns minus the two TP appends (tpRecvTimeUtcNs,
/ tpSeqNo), so fhSeqNo's position is the same in the FH row and the logged
/ row, and the expected FH row width is the schema width minus 2.
.tp.idx.fhSeqNo:.tp.tables ! {[t] (cols value t)?`fhSeqNo} each .tp.tables;
.tp.idx.exchEvent:.tp.tables ! {[t] (cols value t)?`exchEventTimeMs} each .tp.tables;
.tp.fhWidth:.tp.tables ! {[t] -2 + count cols value t} each .tp.tables;
if[any .tp.idx.fhSeqNo >= .tp.fhWidth;
  '"tp.q: schemas.q has a table without fhSeqNo among the feed-handler columns"];
if[not all {[t] `tpRecvTimeUtcNs`tpSeqNo ~ -2 # cols value t} each .tp.tables;
  '"tp.q: tpRecvTimeUtcNs and tpSeqNo must be the last two columns of every table"];

/ -------------------------------------------------------
/ Pub/Sub - KDB-X module (must be named 'pubsub' for IPC)
/ -------------------------------------------------------

pubsub:use`di.pubsub

pubsub.init[]

/ -------------------------------------------------------
/ Logging
/ -------------------------------------------------------

.tp.logHandle:0N;
.tp.logFile:`;
.tp.logCount:0j;

.tp.logFilePath:{[] hsym`$(.tp.cfg.logDir,"/",string[.tp.today[]],".log")};

/ ---- seek index for the current log ----
.tp.idx.entries:([] tpSeqNo:`long$(); offset:`long$(); chunk:`long$());
.tp.idx.forceNext:1b;      / record an entry at the first write after (re)open

.tp.idx.filePath:{[logFile] hsym `$ (-4 _ 1 _ string logFile),".idx"};

.tp.idx.load:{[logFile]
  f:.tp.idx.filePath logFile;
  if[() ~ key f; :0#.tp.idx.entries];
  r:@[get; f; {[e] `error}];
  $[(not r ~ `error) and 98h = type r; r; 0#.tp.idx.entries]};

/ Atomic rewrite (temp file + rename); the table is a few hundred rows a day.
.tp.idx.save:{[]
  f:.tp.idx.filePath .tp.logFile;
  fStr:1 _ string f;
  tmpFile:hsym `$ fStr,".tmp";
  r:.[set; (tmpFile; .tp.idx.entries); {[e] -1 "TP: ERROR writing index - ",e; `error}];
  if[r ~ `error; :()];
  @[{[c] system c; 1b}; "mv ",fStr,".tmp ",fStr; {[e] -1 "TP: ERROR renaming index - ",e; 0b}];
  };

/ Called before each chunk is written: record (tpSeqNo of the chunk; its
/ byte offset; its chunk number) every indexEvery chunks, and always for the
/ first chunk after the log is opened so a restart point is seekable.
.tp.idx.maybeRecord:{[seq]
  if[.tp.idx.forceNext or 0 = .tp.logCount mod .tp.cfg.indexEvery;
    .tp.idx.forceNext:0b;
    .tp.idx.entries,:(seq; hcount .tp.logFile; .tp.logCount);
    .tp.idx.save[]];
  };

.tp.initLog:{[f]
  if[0=@[hcount;f;0j];f set()];
  hopen f
  };

.tp.openLog:{[]
  if[not .tp.cfg.logEnabled;:()];
  system"mkdir -p ",.tp.cfg.logDir;
  .tp.logFile:.tp.logFilePath[];
  .tp.logHandle:.tp.initLog[.tp.logFile];
  .tp.logCount:@[{-11!(-2;x)};.tp.logFile;0j];
  if[0h = type .tp.logCount; .tp.logCount:first .tp.logCount];   / (chunks; bytes) if the tail is bad
  .tp.idx.entries:.tp.idx.load .tp.logFile;
  .tp.idx.forceNext:1b;
  -1"TP: Log file: ",string[.tp.logFile]," (",string[.tp.logCount]," chunks, ",string[count .tp.idx.entries]," index entries)";
  };

.tp.closeLog:{[]
  if[not .tp.cfg.logEnabled;:()];
  if[not null .tp.logHandle;@[hclose;.tp.logHandle;{}];.tp.logHandle:0N];
  };

.tp.log:{[tbl;data]
  if[not .tp.cfg.logEnabled;:()];
  if[tbl=`health_feed_handler;:()];
  .tp.idx.maybeRecord[last data];
  .tp.logHandle enlist(`upd;tbl;data);
  .tp.logCount+:1;
  };

.tp.rotate:{[]
  -1"TP: Rotating log...";
  .tp.closeLog[];
  .tp.openLog[];
  };

/ -------------------------------------------------------
/ Durable tpSeqNo (reservation file)
/ -------------------------------------------------------
/ .tp.tpSeqNo        last number handed out
/ .tp.seq.reserved   highest number we may hand out without touching disk
/ A number n is handed out only once a reservation >= n is on disk.

.tp.tpSeqNo:0j;
.tp.seq.reserved:0j;

/ Persist a new reservation atomically (temp file + rename). Throws if the
/ file cannot be written: TP must not hand out numbers it cannot protect.
.tp.seq.persist:{[reserved]
  f:.tp.cfg.seqFile;
  fStr:1 _ string f;
  tmpFile:hsym `$ fStr,".tmp";
  system "mkdir -p ",1 _ string ` sv -1 _ ` vs f;
  payload:`reserved`updated`tpSeqNo!(reserved; .z.p; .tp.tpSeqNo);
  r:.[set; (tmpFile; payload); {[e] -1 "TP: ERROR writing tpSeqNo reservation - ",e; `error}];
  if[r ~ `error; '"tpSeqNo reservation write failed"];
  ok:@[{[c] system c; 1b}; "mv ",fStr,".tmp ",fStr; {[e] -1 "TP: ERROR renaming tpSeqNo reservation - ",e; 0b}];
  if[not ok; '"tpSeqNo reservation rename failed"];
  .tp.seq.reserved:reserved;
 };

/ Hand out the next tpSeqNo, extending the reservation on disk first when
/ the current one is used up.
.tp.nextSeqNo:{[]
  n:.tp.tpSeqNo + 1;
  if[n > .tp.seq.reserved; .tp.seq.persist[n + .tp.cfg.seqReserve]];
  .tp.tpSeqNo:n;
  n
 };

/ Newest daily log in the log dir by file name (YYYY.MM.DD.log), or ` if none.
.tp.newestLog:{[]
  d:hsym `$ .tp.cfg.logDir;
  files:@[key; d; {[e] `symbol$()}];
  if[0 = count files; :`];
  names:string files;
  isLog:{[n] (count[n] = 14) and (n like "*.log") and not null "D"$ 10#n} each names;
  logs:asc names where isLog;
  $[0 = count logs; `; hsym `$ .tp.cfg.logDir,"/",last logs]
 };

/ Read WDB's checkpoint (any of its historical shapes) and return the max
/ persisted tpSeqNo, or 0N if unreadable/absent. Read-only.
.tp.wdbCheckpointMax:{[]
  f:.tp.cfg.wdbCheckpointFile;
  if[() ~ key f; :0Nj];
  v:@[get; f; {[e] `error}];
  if[v ~ `error; :0Nj];
  if[-7h = type v; :v];
  if[99h = type v; if[`seq in key v; v:v `seq]; :max value v];
  0Nj
 };

/ Seed the counter at startup. With a reservation file: resume at the
/ reservation (numbers below it may have been handed out before a crash).
/ Without one (first start on this code): migrate from the NEWEST log in
/ the log dir, whatever its date - seeding from today's log alone would
/ seed zero after midnight and WDB would halt.
.tp.seq.load:{[]
  f:.tp.cfg.seqFile;
  if[not () ~ key f;
    v:@[get; f; {[e] -1 "TP: ERROR reading tpSeqNo reservation - ",e; `error}];
    if[(not v ~ `error) and (99h = type v) and `reserved in key v;
      .tp.tpSeqNo:v `reserved;
      .tp.seq.reserved:v `reserved;
      -1 raze ("TP: tpSeqNo resumed from reservation file: "; string .tp.tpSeqNo;
               " (last used before "; string v `tpSeqNo; ", updated "; string v `updated; ")");
      / Extend immediately so this run has its own reservation on disk.
      .tp.seq.persist[.tp.tpSeqNo + .tp.cfg.seqReserve];
      :()];
    -1 "TP: ERROR tpSeqNo reservation file unreadable or malformed - falling back to log scan"];
  newest:.tp.newestLog[];
  seed:0j;
  if[not newest ~ `;
    -1 raze ("TP: no tpSeqNo reservation file - migrating: scanning newest log "; string newest);
    seed:.tp.scanLog[newest] 0;
    -1 raze ("TP: migration seed from "; string newest; " = "; string seed)];
  if[newest ~ `; -1 "TP: no tpSeqNo reservation file and no logs - starting tpSeqNo from 0"];
  cpMax:.tp.wdbCheckpointMax[];
  if[(not null cpMax) and seed < cpMax;
    -1 raze ("TP: ERROR migration seed "; string seed; " is BELOW WDB's persisted checkpoint "; string cpMax;
             " ("; string .tp.cfg.wdbCheckpointFile; ") - WDB will halt; check the log dir for missing logs")];
  .tp.tpSeqNo:seed;
  .tp.seq.persist[seed + .tp.cfg.seqReserve];
 };

/ -------------------------------------------------------
/ Sequence Tracking: sessions, gaps, restarts
/ -------------------------------------------------------

/ Per-table state
.tp.seq.last:.tp.tables ! .tp.nTables#0Nj;        / last accepted fhSeqNo
.tp.seq.firstTp:.tp.tables ! .tp.nTables#0Nj;     / first tpSeqNo logged today per table (replay start hint)
.tp.session.id:.tp.tables ! .tp.nTables#0Nj;      / current sessionId
.tp.session.handle:.tp.tables ! .tp.nTables#0Ni;  / handle of the current session (0N when away)
.tp.handleTable:(`int$())!`symbol$();       / registered handle -> table
.tp.unregisteredSeen:`int$();               / handles we already warned about

/ Per-table counters
.tp.ctr.gaps:.tp.tables ! .tp.nTables#0j;             / forward jumps in fhSeqNo
.tp.ctr.missed:.tp.tables ! .tp.nTables#0j;           / rows missed in those jumps (incl. during TP downtime)
.tp.ctr.restarts:.tp.tables ! .tp.nTables#0j;         / registrations with a new sessionId
.tp.ctr.reconnects:.tp.tables ! .tp.nTables#0j;       / registrations with the same sessionId on a new handle
.tp.ctr.outOfOrder:.tp.tables ! .tp.nTables#0j;       / fhSeqNo <= last inside one session (accepted)
.tp.ctr.unregisteredRows:.tp.tables ! .tp.nTables#0j; / rows from handles that never registered (accepted)
.tp.ctr.schemaMismatch:.tp.tables ! .tp.nTables#0j;   / rows rejected for wrong width
.tp.ctr.rejectedRegistrations:0j;             / registrations refused (wrong width / unknown table)
.tp.ctr.unknownTableRows:0j;                  / rows for tables we don't know (passed through)

/ Feed-handler counters. Each handler reports its own counters every few
/ seconds with an async .tp.fhStats[table; names; values]: what happened on
/ the exchange hop, which TP cannot see (book sequence gaps, resyncs,
/ snapshot requests and failures for the quote handlers; trade-id gaps for
/ the trade handlers). TP keeps the latest report per table. The counters
/ are cumulative since that handler process started.
.tp.fh.stats:(`symbol$())!();
.tp.fh.time:(`symbol$())!`timestamp$();
.tp.fhStats:{[tbl; names; vals]
  if[not tbl in .tp.tables; :(::)];
  .tp.fh.stats[tbl]:(names; vals);   / kept as a pair: dicts with different keys do not share a list
  .tp.fh.time[tbl]:.z.p;
  };
/ One dictionary per reporting table: table, reportedAt, then its counters
.tp.fhDict:{[t] nv:.tp.fh.stats t; nv[0]!nv 1};
.tp.fhStatus:{[] {[t] (`table`reportedAt!(t; .tp.fh.time t)), .tp.fhDict t} each key .tp.fh.stats};

/ Registration. Called synchronously by every feed handler right after it
/ connects (and after every reconnect). Throws on a wrong width or unknown
/ table so the handler sees an error and exits at its own startup.
.tp.registerSession:{[tbl; sessionId; nextSeq; width]
  h:.z.w;
  if[not tbl in .tp.tables;
    .tp.ctr.rejectedRegistrations+:1;
    -1 raze ("TP: REJECTED registration from handle "; string h; " for unknown table "; string tbl);
    '"unknown table: ", string tbl];
  if[width <> .tp.fhWidth tbl;
    .tp.ctr.rejectedRegistrations+:1;
    -1 raze ("TP: REJECTED registration from handle "; string h; " for "; string tbl;
             ": row width "; string width; " but schema expects "; string .tp.fhWidth tbl);
    '"schema width mismatch for ", string[tbl], ": handler sends ", string[width],
      " columns, schema expects ", string .tp.fhWidth tbl];
  prevId:.tp.session.id tbl;
  lastSeq:.tp.seq.last tbl;
  kind:$[null prevId; `new; sessionId = prevId; `reconnect; `restart];
  if[kind = `new;
    / First registration this TP process has seen for the table. If TP
    / recovered a last fhSeqNo from the log, the handler may have continued
    / (TP was down: count what it missed) or restarted meanwhile.
    $[null lastSeq;
        -1 raze ("TP: session "; string sessionId; " registered for "; string tbl; " (handle "; string h; ", next fhSeqNo "; string nextSeq; ")");
      (nextSeq - 1) < lastSeq;
        [.tp.ctr.restarts[tbl]+:1;
         -1 raze ("TP: FH RESTART detected for "; string tbl; " (handler restarted while TP was down): fhSeqNo was "; string lastSeq; ", resumes at "; string nextSeq)];
      [missed:(nextSeq - 1) - lastSeq;   / parenthesised: q evaluates right to left
       if[missed > 0; .tp.ctr.gaps[tbl]+:1; .tp.ctr.missed[tbl]+:missed];
       -1 raze ("TP: session "; string sessionId; " continues for "; string tbl; " after TP restart: last logged fhSeqNo "; string lastSeq;
                ", next "; string nextSeq; $[missed > 0; raze (" -> "; string missed; " rows MISSED while TP was down"); ", no gap"])]]];
  if[kind = `restart;
    .tp.ctr.restarts[tbl]+:1;
    -1 raze ("TP: FH RESTART detected for "; string tbl; ": session "; string prevId; " -> "; string sessionId;
             " (handle "; string h; "), fhSeqNo was "; string lastSeq; ", resumes at "; string nextSeq)];
  if[kind = `reconnect;
    .tp.ctr.reconnects[tbl]+:1;
    missed:$[null lastSeq; 0; (nextSeq - 1) - lastSeq];
    if[missed > 0; .tp.ctr.gaps[tbl]+:1; .tp.ctr.missed[tbl]+:missed];
    -1 raze ("TP: FH RECONNECT for "; string tbl; " session "; string sessionId; " (handle "; string h; "): last fhSeqNo "; string lastSeq;
             ", next "; string nextSeq; $[missed > 0; raze (" -> "; string missed; " rows MISSED"); ", no gap"])];
  .tp.seq.last[tbl]:nextSeq - 1;
  .tp.session.id[tbl]:sessionId;
  .tp.session.handle[tbl]:h;
  .tp.handleTable[h]:tbl;
  `ok
 };

/ Sequence check for one accepted row. Never drops: returns after counting.
.tp.checkSeq:{[tbl; seq]
  lastSeq:.tp.seq.last tbl;
  if[null lastSeq; .tp.seq.last[tbl]:seq; :()];
  if[seq = lastSeq + 1; .tp.seq.last[tbl]:seq; :()];
  if[seq > lastSeq + 1;
    missed:(seq - lastSeq) - 1;   / parenthesised: q evaluates right to left
    .tp.ctr.gaps[tbl]+:1;
    .tp.ctr.missed[tbl]+:missed;
    -1 raze ("TP: "; string tbl; " gap - expected "; string lastSeq+1; " got "; string seq; " (missed "; string missed; ")");
    .tp.seq.last[tbl]:seq;
    :()];
  / seq <= last inside the session: cannot happen over one TCP stream unless
  / the handler misbehaves; accept, count, keep the high-water mark.
  .tp.ctr.outOfOrder[tbl]+:1;
  -1 raze ("TP: "; string tbl; " OUT OF ORDER fhSeqNo "; string seq; " (last "; string lastSeq; ") - accepted and counted");
 };

/ -------------------------------------------------------
/ Update handling
/ -------------------------------------------------------

.tp.tsToNs:{[ts] .tp.epochOffset+"j"$ts};
.tp.mismatchLogCount:0j;

/ Clock-skew ring buffer
.tp.skew.samples:.tp.cfg.skewSamples # 0Nj;
.tp.skew.pos:0j;
.tp.skew.record:{[tbl; data]
  i:.tp.idx.exchEvent tbl;
  if[i >= count data; :()];
  ev:data i;
  if[not -7h = type ev; :()];
  nowMs:("j"$.z.p - 1970.01.01D0) div 1000000;
  .tp.skew.samples[.tp.skew.pos]:nowMs - ev;
  .tp.skew.pos:(.tp.skew.pos + 1) mod .tp.cfg.skewSamples;
  };
.tp.skew.median:{[] s:.tp.skew.samples where not null .tp.skew.samples; $[count s; `long$med s; 0Nj]};
.tp.skew.high:{[] m:.tp.skew.median[]; (.tp.cfg.maxSkewMs > 0) and (not null m) and (abs m) > .tp.cfg.maxSkewMs};

upd:{[tbl;data]
  / Health messages bypass sequence checks and the durability log entirely.
  if[tbl=`health_feed_handler;
    pubsub.publish[tbl;data];
    :()];
  if[not tbl in .tp.tables;
    / Unknown tables pass through (logged + published) and are counted.
    .tp.ctr.unknownTableRows+:1;
    data:data, (.tp.tsToNs[.z.p]; .tp.nextSeqNo[]);
    .tp.log[tbl; data];
    pubsub.publish[tbl; data];
    :()];
  / Width guard: a row that does not match the schema can neither be logged
  / nor published safely. Reject, log (rate-limited), count.
  if[(count data) <> .tp.fhWidth tbl;
    .tp.ctr.schemaMismatch[tbl]+:1;
    .tp.mismatchLogCount+:1;
    if[(.tp.mismatchLogCount <= 10) or 0 = .tp.mismatchLogCount mod 1000;
      -1 raze ("TP: SCHEMA MISMATCH - rejected "; string tbl; " row with "; string count data;
               " columns (schema expects "; string .tp.fhWidth tbl; ") from handle "; string .z.w;
               " (total rejected: "; string sum .tp.ctr.schemaMismatch; ")")];
    :()];
  h:.z.w;
  registered:$[h in key .tp.handleTable; tbl = .tp.handleTable h; 0b];
  if[not registered;
    .tp.ctr.unregisteredRows[tbl]+:1;
    if[not h in .tp.unregisteredSeen;
      .tp.unregisteredSeen,:h;
      -1 raze ("TP: handle "; string h; " publishes "; string tbl; " without a session registration - accepting and counting")]];
  .tp.checkSeq[tbl; data .tp.idx.fhSeqNo tbl];
  .tp.skew.record[tbl; data];
  / Append TP-side fields: tpRecvTimeUtcNs, tpSeqNo (the last two schema columns)
  data:data, (.tp.tsToNs[.z.p]; .tp.nextSeqNo[]);
  if[null .tp.seq.firstTp tbl; .tp.seq.firstTp[tbl]:last data];
  / Log first (durability), then publish (best-effort fanout). TP is a
  / router, not a store: it does not insert into the local table copies.
  .tp.log[tbl; data];
  pubsub.publish[tbl; data];
  };

/ Alias for feed handlers that call .u.upd over IPC.
.u.upd:upd;

/ Connection close: forget the handle; keep the session so a reconnect with
/ the same sessionId is recognised. Also let pubsub drop subscriptions.
.z.pc:{[h]
  if[h in key .tp.handleTable;
    tbl:.tp.handleTable h;
    -1 raze ("TP: handle "; string h; " closed ("; string tbl; " session "; string .tp.session.id tbl; ")");
    .tp.handleTable:(enlist h) _ .tp.handleTable;
    if[h = .tp.session.handle tbl; .tp.session.handle[tbl]:0Ni]];
  .tp.unregisteredSeen:.tp.unregisteredSeen except h;
  pubsub.closesub[h];
  };

/ -------------------------------------------------------
/ Query API
/ -------------------------------------------------------

/ Highest fhSeqNo accepted for a table (0 if none).
.tp.lastAccepted:{[tbl] s:.tp.seq.last tbl; $[null s; 0; s]};

/ Current tpSeqNo cursor - the latest assigned. Subscribers capture this at
/ reconnect time as the cutoff for separating replay from live data.
.tp.currentSeqNo:{[] .tp.tpSeqNo};

/ Scan a log file: returns (max tpSeqNo; per-table fhSeqNo of the LAST row
/ logged). The last row's fhSeqNo, not the day's maximum: a handler that
/ restarted earlier in the day leaves higher numbers from its previous
/ session in the log, and comparing a reconnecting handler against that
/ maximum misreported every post-restart registration as a handler
/ restart (seen in the first live check of this code). Uses -11! with a
/ temporary upd; rows whose width does not match the schema are ignored.
.tp.scan.tpMax:0j;
.tp.scan.fhLast:.tp.tables ! .tp.nTables#0Nj;
.tp.scan.tpFirst:.tp.tables ! .tp.nTables#0Nj;
.tp.scanUpd:{[t;d]
  if[not t in .tp.tables; :()];
  if[(count d) <> count cols value t; :()];
  .tp.scan.tpMax:.tp.scan.tpMax | last d;
  .tp.scan.fhLast[t]:d .tp.idx.fhSeqNo t;
  if[null .tp.scan.tpFirst t; .tp.scan.tpFirst[t]:last d];
 };
.tp.scanLog:{[f]
  .tp.scan.tpMax:0j;
  .tp.scan.fhLast:.tp.tables ! .tp.nTables#0Nj;
  .tp.scan.tpFirst:.tp.tables ! .tp.nTables#0Nj;
  oldUpd:upd;
  upd::.tp.scanUpd;
  .[{-11!x}; enlist f; {[err] -1 "TP: log scan error: ",err}];
  upd::oldUpd;
  (.tp.scan.tpMax; .tp.scan.fhLast; .tp.scan.tpFirst)
 };

/ Replay support. TP does not read its logs for anyone: it tells WDB where
/ they are and how far today's log is committed, and WDB reads them itself
/ with the index (kdb/tick/logreader.q). committedBytes is the length of
/ today's log at this instant; every write is synchronous, so everything
/ below it is complete. firstTpSeqNo gives, per table, the first tpSeqNo
/ logged today (null if none), so a subscriber that has never seen a table
/ knows where that table's rows start instead of scanning from the top.
.tp.replayInfo:{[]
  `logDir`today`todayLog`committedBytes`cutoff`indexEvery`firstTpSeqNo!(
    first system "realpath -m ",.tp.cfg.logDir;
    .tp.today[];
    string .tp.logFilePath[];
    @[hcount; .tp.logFilePath[]; 0j];
    .tp.tpSeqNo;
    .tp.cfg.indexEvery;
    .tp.seq.firstTp)
 };

/ Recover per-table last fhSeqNo from today's log so gap detection spans a
/ TP restart. tpSeqNo itself comes from the reservation file (.tp.seq.load).
.tp.recoverFhSeq:{[]
  logFile:.tp.logFilePath[];
  if[() ~ key logFile; -1 "TP: no log for today - no fhSeqNo to recover"; :()];
  r:.tp.scanLog[logFile];
  .tp.seq.last:r 1;
  .tp.seq.firstTp:r 2;
  -1 raze ("TP: recovered fhSeqNo of the last logged row per table: "; .Q.s1 .tp.seq.last;
           " (log max tpSeqNo "; string r 0; ")");
  if[(r 0) > .tp.tpSeqNo;
    -1 raze ("TP: ERROR today's log holds tpSeqNo "; string r 0; " above the reservation "; string .tp.tpSeqNo;
             " - reservation file out of date? Raising the counter to the log");
    .tp.tpSeqNo:r 0;
    .tp.seq.persist[.tp.tpSeqNo + .tp.cfg.seqReserve]];
 };

/ -------------------------------------------------------
/ Disk-space health
/ -------------------------------------------------------
.tp.disk.freeMB:0Nj;
.tp.disk.check:{[]
  r:@[system; "df -Pk ",.tp.cfg.logDir; {[e] ()}];
  if[2 > count r; .tp.disk.freeMB:0Nj; :()];
  toks:{x where 0 < count each x} " " vs r 1;
  if[4 > count toks; .tp.disk.freeMB:0Nj; :()];
  .tp.disk.freeMB:("J"$toks 3) div 1024;
  if[.tp.disk.freeMB < .tp.cfg.minFreeMB;
    -1 raze ("TP: WARNING low disk space for logs: "; string .tp.disk.freeMB; " MB free (threshold "; string .tp.cfg.minFreeMB; " MB)")];
  };
.tp.disk.low:{[] (not null .tp.disk.freeMB) and .tp.disk.freeMB < .tp.cfg.minFreeMB};

/ -------------------------------------------------------
/ Status and Monitoring
/ -------------------------------------------------------

/ Standardized health check (consistent across all processes)
.health:{[]
  st:$[((sum .tp.ctr.schemaMismatch) + .tp.ctr.rejectedRegistrations) > 0; `degraded;
       (sum .tp.ctr.gaps) > 0; `degraded;
       .tp.disk.low[]; `degraded;
       .tp.skew.high[]; `degraded;
       `ok];
  `process`port`uptime`status`memMB`msgsIn`msgsOut`tpSeqNo`gaps`missed`restarts`reconnects`outOfOrder`unregisteredRows`schemaMismatch`rejectedRegistrations`diskFreeMB`diskLow`clockSkewMs`clockSkewHigh`fhStats!(
    `tp;
    .tp.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    (`long$.Q.w[][`used]) % 1000000;
    .tp.logCount;
    .tp.logCount;
    .tp.tpSeqNo;
    sum .tp.ctr.gaps;
    sum .tp.ctr.missed;
    sum .tp.ctr.restarts;
    sum .tp.ctr.reconnects;
    sum .tp.ctr.outOfOrder;
    sum .tp.ctr.unregisteredRows;
    sum .tp.ctr.schemaMismatch;
    .tp.ctr.rejectedRegistrations;
    .tp.disk.freeMB;
    .tp.disk.low[];
    .tp.skew.median[];
    .tp.skew.high[];
    (key .tp.fh.stats)!.tp.fhDict each key .tp.fh.stats)
  }

/ Per-table detail table
.tp.status:{[]
  ([] table:.tp.tables;
      sessionId:value .tp.session.id;
      handle:value .tp.session.handle;
      lastFhSeqNo:value .tp.seq.last;
      gaps:value .tp.ctr.gaps;
      missed:value .tp.ctr.missed;
      restarts:value .tp.ctr.restarts;
      reconnects:value .tp.ctr.reconnects;
      outOfOrder:value .tp.ctr.outOfOrder;
      unregisteredRows:value .tp.ctr.unregisteredRows;
      schemaMismatch:value .tp.ctr.schemaMismatch)
  };

/ Compact status as dictionary. Keys kept from the previous version where
/ they still mean the same thing; the per-side "dups" keys are gone because
/ nothing is dropped on a guess any more.
.tp.statusDict:{[]
  `port`uptime`logFile`logChunks`tpSeqNo`seqReserved`seqFile`tradeGaps`tradeMissed`tradeRestarts`tradeReconnects`tradeOutOfOrder`lastTradeSeq`aggTradeGaps`aggTradeMissed`aggTradeRestarts`aggTradeReconnects`aggTradeOutOfOrder`lastAggTradeSeq`quoteGaps`quoteMissed`quoteRestarts`quoteReconnects`quoteOutOfOrder`lastQuoteSeq`quoteFutGaps`quoteFutMissed`quoteFutRestarts`quoteFutReconnects`quoteFutOutOfOrder`lastQuoteFutSeq`unregisteredRows`schemaMismatch`rejectedRegistrations`unknownTableRows!
   (.tp.cfg.port;
    `second$.z.p-.proc.startTime;
    .tp.logFile;
    .tp.logCount;
    .tp.tpSeqNo;
    .tp.seq.reserved;
    .tp.cfg.seqFile;
    .tp.ctr.gaps`trade_binance; .tp.ctr.missed`trade_binance; .tp.ctr.restarts`trade_binance; .tp.ctr.reconnects`trade_binance; .tp.ctr.outOfOrder`trade_binance; .tp.seq.last`trade_binance;
    .tp.ctr.gaps`trade_binance_fut; .tp.ctr.missed`trade_binance_fut; .tp.ctr.restarts`trade_binance_fut; .tp.ctr.reconnects`trade_binance_fut; .tp.ctr.outOfOrder`trade_binance_fut; .tp.seq.last`trade_binance_fut;
    .tp.ctr.gaps`quote_binance; .tp.ctr.missed`quote_binance; .tp.ctr.restarts`quote_binance; .tp.ctr.reconnects`quote_binance; .tp.ctr.outOfOrder`quote_binance; .tp.seq.last`quote_binance;
    .tp.ctr.gaps`quote_binance_fut; .tp.ctr.missed`quote_binance_fut; .tp.ctr.restarts`quote_binance_fut; .tp.ctr.reconnects`quote_binance_fut; .tp.ctr.outOfOrder`quote_binance_fut; .tp.seq.last`quote_binance_fut;
    sum .tp.ctr.unregisteredRows;
    sum .tp.ctr.schemaMismatch;
    .tp.ctr.rejectedRegistrations;
    .tp.ctr.unknownTableRows)
  };

.tp.logStatus:{[]
  ([]file:enlist .tp.logFile;chunks:enlist .tp.logCount;sizeMB:enlist(@[hcount;.tp.logFile;0j])%1e6)
  };

/ -------------------------------------------------------
/ End-of-Day
/ -------------------------------------------------------

.tp.endOfDay:{[]
  -1 raze ("TP: EOD - chunks:"; string .tp.logCount;
           " gaps:"; .Q.s1 .tp.ctr.gaps;
           " tpSeqNo:"; string .tp.tpSeqNo);
  pubsub.callendofday[];
  .tp.rotate[];
  .tp.logCount:0j;
  / Daily operational counters reset; sessions, last fhSeqNo and tpSeqNo
  / carry across midnight (handlers do not restart at EOD, and tpSeqNo is
  / monotonic by construction).
  .tp.ctr.gaps:.tp.tables ! .tp.nTables#0j;
  .tp.ctr.missed:.tp.tables ! .tp.nTables#0j;
  .tp.ctr.restarts:.tp.tables ! .tp.nTables#0j;
  .tp.ctr.reconnects:.tp.tables ! .tp.nTables#0j;
  .tp.ctr.outOfOrder:.tp.tables ! .tp.nTables#0j;
  .tp.ctr.unregisteredRows:.tp.tables ! .tp.nTables#0j;
  .tp.ctr.schemaMismatch:.tp.tables ! .tp.nTables#0j;
  .tp.seq.firstTp:.tp.tables ! .tp.nTables#0Nj;
 };

.tp.currentDate:.tp.today[];

.tp.checkEOD:{[]
  if[.tp.today[] > .tp.currentDate;
    -1 "TP: Midnight UTC detected - triggering EOD";
    .tp.endOfDay[];
    .tp.currentDate:.tp.today[];
  ];
  };

.tp.ticks:0j;
.z.ts:{[]
  .tp.checkEOD[];
  .tp.ticks+:1;
  if[0 = .tp.ticks mod 60; .tp.disk.check[]];
  };

/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system"p ",string .tp.cfg.port;

/ Order matters: seed tpSeqNo from the reservation file (or migrate), then
/ recover per-table fhSeqNo from today's log, then open the log for writes.
.tp.seq.load[];
.tp.recoverFhSeq[];
.tp.openLog[];
.tp.disk.check[];

system "t 1000";   / EOD check every second

-1"=======================================================";
-1"TP (KDB-X module) starting on port ",string[.tp.cfg.port];
-1"=======================================================";
-1"Tables: ",(" " sv string .tp.tables)," health_feed_handler";
-1"Schema: kdb/schemas.q; FH row widths ",.Q.s1[.tp.fhWidth];
-1"Shared: ",.schema.cfg.file,"; quote depth ",string[.schema.depth],"; symbols ",(" " sv string .schema.symbols);
-1"tpSeqNo: ",string[.tp.tpSeqNo]," reserved to ",string[.tp.seq.reserved]," in ",string .tp.cfg.seqFile;
-1"Replay index: every ",string[.tp.cfg.indexEvery]," rows; disk free ",string[.tp.disk.freeMB]," MB (threshold ",string[.tp.cfg.minFreeMB]," MB)";
-1"";
-1"Monitoring:";
-1"  .health[]            / Standardized health check";
-1"  .tp.status[]         / Per-table sessions and counters";
-1"  .tp.statusDict[]     / Status as dictionary";
-1"  .tp.logStatus[]      / Log file status";
-1"";
-1"Feed handler API:";
-1"  .tp.registerSession[tbl; sessionId; nextFhSeqNo; rowWidth]  / once per connection";
-1"  .u.upd[tbl; row]                                            / per row";
-1"";
-1"Subscriber API:";
-1"  .tp.currentSeqNo[]             / Current monotonic tpSeqNo (replay cutoff)";
-1"  .tp.replayInfo[]               / Log dir, committed length, cutoff, index step (WDB reads the logs itself)";
-1"";
-1"TP ready";
-1"=======================================================";
