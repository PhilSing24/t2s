/ logmgr.q - TP durability log management: listing, completeness and
/ retention. Run on demand from anywhere:
/ -
/   q kdb/utils/logmgr.q                       interactive (banner + summary)
/   q kdb/utils/logmgr.q -retention            dry run: what would be deleted and why
/   q kdb/utils/logmgr.q -retention -apply     delete what the dry run lists
/   q kdb/utils/logmgr.q -summary              completeness of every log vs the HDB
/   q kdb/utils/logmgr.q -check-eod [DATE]     one day (default yesterday): partition vs log,
/                                              exit 0 if complete, 1 otherwise (for cron)
/ -
/ Retention policy. A day's log (and its .idx) is deleted only if ALL hold:
/   (a) the HDB partition for that date exists
/   (b) every tpSeqNo logged for every table is present on disk in the
/       partitions for that date and its two neighbours (rows are
/       partitioned by their own `time`, so a midnight straddle lands next
/       door; this check is exact, not a count comparison)
/   (c) the log is older than T2S_LOG_RETENTION_DAYS (default 7)
/ Never deleted: today's log, any date in T2S_LOG_PROTECTED (default none;
/ 2026.06.08 was protected until its partition was rebuilt on 2026-10-03),
/ and tp.tpSeqNo (the tpSeqNo reservation file; only date-named *.log files
/ are candidates).
/ -
/ Configuration (environment):
/   T2S_TP_LOG_DIR          log dir      (default <repo>/kdb/tick/logs)
/   T2S_HDB_DIR             HDB root     (default <repo>/hdb)
/   T2S_LOG_RETENTION_DAYS  days         (default 7)
/   T2S_LOG_PROTECTED       dates, comma separated (default none)

\c 25 400

/ Directory of this script, so defaults resolve relative to the repo and the
/ shared schema file can be loaded from any working directory.
.lm.dir:first system "dirname ",string .z.f;
system "l ",.lm.dir,"/../schemas.q";

.log.cfg.logDir:$[count v:getenv `T2S_TP_LOG_DIR; v; .lm.dir,"/../tick/logs"];
.log.cfg.hdbDir:$[count v:getenv `T2S_HDB_DIR; v; .lm.dir,"/../../hdb"];
.log.cfg.retentionDays:$[count v:getenv `T2S_LOG_RETENTION_DAYS; "J"$v; 7];
.log.cfg.protected:$[count v:getenv `T2S_LOG_PROTECTED; "D"$"," vs v; `date$()];
.log.protectedStr:{[] $[count .log.cfg.protected; ", " sv string .log.cfg.protected; "none"]};
.log.cfg.tables:`trade_binance`trade_binance_fut`quote_binance`quote_binance_fut`trade_gap;

/ Logged row widths (feed-handler columns + tpRecvTimeUtcNs + tpSeqNo)
.log.schema:.log.cfg.tables ! (
  .schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo];
  .schema.extend[.schema.aggTrade; `tpRecvTimeUtcNs`tpSeqNo];
  .schema.extend[.schema.quote; `tpRecvTimeUtcNs`tpSeqNo];
  .schema.extend[.schema.quoteFut; `tpRecvTimeUtcNs`tpSeqNo];
  .schema.extend[.schema.tradeGap; `tpRecvTimeUtcNs`tpSeqNo]);
.log.width:.log.cfg.tables ! {[t] count cols .log.schema t} each .log.cfg.tables;

/ -------------------------------------------------------
/ Listing
/ -------------------------------------------------------

.log.logPath:{[d] hsym `$ .log.cfg.logDir,"/",string[d],".log"};
.log.idxPath:{[d] hsym `$ .log.cfg.logDir,"/",string[d],".idx"};

.log.list:{[]
  files:@[key; hsym `$ .log.cfg.logDir; {[e] `symbol$()}];
  names:string files;
  logs:names where names like "????.??.??.log";
  dates:asc "D"$ 10#' logs;
  dates:dates where not null dates;
  ([] date:dates; file:.log.logPath each dates; sizeMB:{0.01 * `long$ 100 * (hcount x) % 1e6} each .log.logPath each dates)
 };

/ -------------------------------------------------------
/ Scanning a log (native -11!, whole file; logs are closed except today's)
/ -------------------------------------------------------

/ Returns `rows`seqs`unknownShape ! (per-table row count dict; per-table
/ tpSeqNo list dict; rows whose table or width is unknown).
.log.scan:{[f]
  .log.acc.seqs::.log.cfg.tables ! (count .log.cfg.tables)#enlist `long$();
  .log.acc.unknown::0j;
  upd::{[t;d]
    $[(t in .log.cfg.tables) and (count d) = .log.width t;
      .log.acc.seqs[t],::last d;
      .log.acc.unknown+::1]};
  r:.[{-11! x}; enlist f; {[e] (`error; e)}];
  if[(0h = type r) and (first r) ~ `error; '"log scan failed: ", last r];
  `rows`seqs`unknownShape ! (count each .log.acc.seqs; .log.acc.seqs; .log.acc.unknown)
 };

/ tpSeqNo on disk for table t in the partitions of d-1, d, d+1 (those that exist)
.log.diskSeqs:{[d; t]
  paths:{[hdb; dd; t] hsym `$ hdb,"/",string[dd],"/",string[t],"/tpSeqNo"}[.log.cfg.hdbDir;; t] each d + -1 0 1;
  raze {[p] $[() ~ key p; `long$(); @[get; p; {[e] `long$()}]]} each paths
 };

.log.partitionExists:{[d] not () ~ key hsym `$ .log.cfg.hdbDir,"/",string d};

/ -------------------------------------------------------
/ Assessment
/ -------------------------------------------------------

/ One row per log: date, size, rows, complete (every logged row on disk),
/ status (`delete or `keep), reason, and per-table detail.
.log.assess:{[d]
  f:.log.logPath d;
  sizeMB:0.01 * `long$ 100 * (hcount f) % 1e6;
  today:.z.d;
  ageDays:today - d;
  partition:.log.partitionExists d;
  / Completeness is computed for every log, whatever the retention outcome,
  / so the summary shows which days are safe in the HDB and which could only
  / be rebuilt from their log.
  sc:@[.log.scan; f; {[e] (`error; e)}];
  if[(0h = type sc) and (first sc) ~ `error;
    :`date`sizeMB`logRows`complete`status`reason`detail!(d; sizeMB; 0Nj; 0b; `keep; "log unreadable: ", last sc; "")];
  missing:{[d; sc; t] count (sc[`seqs] t) except .log.diskSeqs[d; t]}[d; sc] each .log.cfg.tables;
  missing:.log.cfg.tables ! missing;
  logRows:sum sc `rows;
  complete:partition and (0 = sum missing) and 0 = sc `unknownShape;
  detail:", " sv {[t; n; m] raze (string t; ":"; string n; " rows, "; string m; " missing")}'[.log.cfg.tables; sc[`rows] .log.cfg.tables; missing .log.cfg.tables];
  if[sc[`unknownShape] > 0; detail:detail, raze ("; "; string sc `unknownShape; " rows of unknown table/shape")];
  / Decide. The first reason that forbids deletion wins.
  reason:$[d = today;                        "today's log";
           d in .log.cfg.protected;          "protected (T2S_LOG_PROTECTED)";
           not partition;                    raze ("HDB partition "; string d; " missing");
           sc[`unknownShape] > 0;            "rows of unknown shape in log";
           0 < sum missing;                  raze (string sum missing; " logged rows not found in HDB partitions "; string d-1; ".."; string d+1);
           ageDays <= .log.cfg.retentionDays; raze ("complete in HDB but only "; string ageDays; " day(s) old (retention "; string .log.cfg.retentionDays; ")");
           raze ("complete in HDB, "; string ageDays; " days old")];
  status:$[(d <> today) and (not d in .log.cfg.protected) and complete and ageDays > .log.cfg.retentionDays; `delete; `keep];
  `date`sizeMB`logRows`complete`status`reason`detail!(d; sizeMB; logRows; complete; status; reason; detail)
 };

.log.summary:{[]
  dates:exec date from .log.list[];
  if[0 = count dates; :([] date:`date$(); sizeMB:`float$(); logRows:`long$(); complete:`boolean$(); status:`symbol$(); reason:(); detail:())];
  t:.log.assess each dates;
  t:([] date:t[;`date]; sizeMB:t[;`sizeMB]; logRows:t[;`logRows]; complete:t[;`complete]; status:t[;`status]; reason:t[;`reason]; detail:t[;`detail]);
  t
 };

/ -------------------------------------------------------
/ Retention
/ -------------------------------------------------------

/ apply=0b: dry run (default). apply=1b: delete the logs (and their .idx)
/ that the assessment marks `delete. Returns the assessment table.
.log.retention:{[apply]
  -1 "LOG: retention ",$[apply; "APPLY"; "DRY RUN"],
     " - log dir ",.log.cfg.logDir,", HDB ",.log.cfg.hdbDir,
     ", retention ",string[.log.cfg.retentionDays]," days, protected ",.log.protectedStr[];
  t:.log.summary[];
  show select date, sizeMB, logRows, complete, status, reason from t;
  del:select from t where status = `delete;
  -1 "";
  -1 "LOG: ",string[count del]," log(s) deletable, ",string[0.01 * `long$ 100 * sum del `sizeMB]," MB; ",
     string[count[t] - count del]," kept";
  if[not apply;
    -1 "LOG: dry run - nothing deleted (run with -apply to delete)";
    :t];
  {[d]
    f:.log.logPath d; i:.log.idxPath d;
    -1 "LOG: deleting ",string f;
    @[hdel; f; {[e] -1 "LOG:   ERROR deleting log - ",e}];
    if[not () ~ key i; @[hdel; i; {[e] -1 "LOG:   ERROR deleting index - ",e}]];
   } each del `date;
  -1 "LOG: done";
  t
 };

/ -------------------------------------------------------
/ Entry
/ -------------------------------------------------------

.lm.args:.z.x;
/ `in` on an empty .z.x compares per character; match each argument instead.
.lm.has:{[flag] any .lm.args ~\: flag};
if[.lm.has "-check-eod";
  i:.lm.args ? "-check-eod";
  d:$[(i + 1) < count .lm.args; "D"$.lm.args i + 1; .z.d - 1];
  if[null d; -2 "LOG: bad date"; exit 2];
  if[() ~ key .log.logPath d;
    -1 "LOG: check-eod ",string[d],": no log for that date in ",.log.cfg.logDir;
    system "sleep 0.1"; exit 1];
  a:.log.assess d;
  -1 "LOG: check-eod ",string[d],": ",$[a `complete; "COMPLETE"; "INCOMPLETE"]," - ",a `reason;
  -1 "LOG:   ",a `detail;
  -1 "LOG:   log ",string[a `sizeMB]," MB, ",string[a `logRows]," rows; partition ",$[.log.partitionExists d; "present"; "MISSING"];
  system "sleep 0.1";
  exit $[a `complete; 0; 1]];
if[.lm.has "-retention";
  .log.retention[.lm.has "-apply"];
  system "sleep 0.1";
  exit 0];
if[.lm.has "-summary";
  show select date, sizeMB, logRows, complete, status, reason, detail from .log.summary[];
  system "sleep 0.1";
  exit 0];

-1 "=======================================================";
-1 "LOG Manager (on-demand)";
-1 "=======================================================";
-1 "  Log directory: ",.log.cfg.logDir;
-1 "  HDB:           ",.log.cfg.hdbDir;
-1 "  Retention:     ",string[.log.cfg.retentionDays]," days; protected: ",.log.protectedStr[];
-1 "";
show .log.list[];
-1 "";
-1 "Commands:";
-1 "  .log.list[]            / logs on disk";
-1 "  .log.summary[]         / completeness of every log vs the HDB (scans each log)";
-1 "  .log.retention[0b]     / dry run";
-1 "  .log.retention[1b]     / delete eligible logs";
-1 "=======================================================";
