/ rebuild_day.q - rebuild one day's HDB partition from the TP logs, safely.
/ -
/   q kdb/utils/rebuild_day.q -date D                 report: rows for D in the logs vs the
/                                                     existing partition (nothing written)
/   q kdb/utils/rebuild_day.q -date D -build          build hdb/.rebuild/D from the logs and
/                                                     verify it against the logs
/   q kdb/utils/rebuild_day.q -date D -swap           move the existing partition to
/                                                     hdb/.rebuild/D.bak.<stamp> and the
/                                                     staging dir to hdb/D (needs -build first)
/   q kdb/utils/rebuild_day.q -date D -compare DIR    also compare against another copy of
/                                                     the day (e.g. a backed-up tmp dir)
/ -
/ Which rows belong to day D: every row whose own `time` column (the feed
/ handler receive timestamp) falls on D, which is how WDB partitions. Those
/ rows live mostly in D's log, with stragglers in the logs of D-1 and D+1,
/ so all three are scanned. Rows of an unknown table or width are counted
/ and skipped.
/ -
/ The rebuilt partition has WDB's exact shape: the logged columns plus
/ wdbRecvTimeUtcNs, which is NULL for rebuilt rows ("rebuilt from the log,
/ not received live"), sorted by sym with the parted attribute, enumerated
/ against the HDB's sym file, compressed like WDB writes.
/ -
/ Staging and backups live under hdb/.rebuild/, a dotted directory that a
/ partitioned-HDB load ignores, so neither is ever mistaken for a partition.
/ -
/ Environment: T2S_TP_LOG_DIR (default <repo>/kdb/tick/logs), T2S_HDB_DIR
/ (default <repo>/hdb). Exit code 0 on success, 1 on a failed verification
/ or refused step, 2 on usage error.

\c 25 400

.rb.dir:first system "dirname ",string .z.f;
system "l ",.rb.dir,"/../schemas.q";

.rb.cfg.logDir:$[count v:getenv `T2S_TP_LOG_DIR; v; .rb.dir,"/../tick/logs"];
.rb.cfg.hdbDir:$[count v:getenv `T2S_HDB_DIR; v; .rb.dir,"/../../hdb"];
.rb.cfg.hdb:hsym `$ .rb.cfg.hdbDir;
.rb.cfg.tables:`trade_binance`trade_binance_fut`quote_binance;
.z.zd:(17;5;1);   / same compression as wdb.q

/ Logged schemas (feed-handler columns + tpRecvTimeUtcNs + tpSeqNo)
.rb.schema:.rb.cfg.tables ! (
  .schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo];
  .schema.extend[.schema.aggTrade; `tpRecvTimeUtcNs`tpSeqNo];
  .schema.extend[.schema.quote; `tpRecvTimeUtcNs`tpSeqNo]);
.rb.width:.rb.cfg.tables ! {[t] count cols .rb.schema t} each .rb.cfg.tables;

.rb.logPath:{[d] hsym `$ raze (.rb.cfg.logDir; "/"; string d; ".log")};
.rb.partPath:{[d] hsym `$ raze (.rb.cfg.hdbDir; "/"; string d)};
.rb.stagePath:{[d] hsym `$ raze (.rb.cfg.hdbDir; "/.rebuild/"; string d)};
.rb.tablePath:{[dir;t] hsym `$ raze (1 _ string dir; "/"; string t)};

.rb.fail:{[msg] -2 "REBUILD: ERROR ",msg; system "sleep 0.1"; exit 1};
.rb.say:{[msg] -1 "REBUILD: ",msg};

/ -------------------------------------------------------
/ Collect: rows dated D from the logs of D-1, D, D+1
/ -------------------------------------------------------
.rb.acc:.rb.schema;   / empty typed tables, one per table
.rb.unknown:0j;
.rb.scanned:0j;
.rb.collectUpd:{[t;d]
  .rb.scanned+:1;
  if[not (t in .rb.cfg.tables) and (count d) = .rb.width t; .rb.unknown+:1; :()];
  if[not .rb.day = `date$ d 0; :()];
  .rb.acc[t]:.rb.acc[t] upsert d;
  };

.rb.collect:{[d]
  .rb.day::d;
  .rb.acc::.rb.schema;
  .rb.unknown::0j; .rb.scanned::0j;
  logs:.rb.logPath each d + -1 0 1;
  logs:logs where not () ~/: key each logs;
  if[0 = count logs; .rb.fail raze ("no log for "; string d; " or its neighbours in "; .rb.cfg.logDir)];
  .rb.say raze ("scanning "; ", " sv string logs);
  upd::.rb.collectUpd;
  {[f] t0:.z.p; n:-11! f; .rb.say raze ("  "; string f; ": "; string n; " chunks in "; string `long$(.z.p - t0) % 1000000000; " s")} each logs;
  .rb.say raze ("rows dated "; string d; ": "; ", " sv {[t] raze (string t; "="; string count .rb.acc t)} each .rb.cfg.tables;
                "  ("; string .rb.unknown; " rows of unknown table/shape skipped)");
  };

/ -------------------------------------------------------
/ Reading an existing copy of the day (partition, staging, or a tmp dir)
/ -------------------------------------------------------
/ tpSeqNo of table t under dir (any splay with a tpSeqNo column), or empty.
.rb.dirSeqs:{[dir;t]
  p:` sv (.rb.tablePath[dir;t]), `tpSeqNo;
  $[() ~ key p; `long$(); @[get; p; {[e] `long$()}]]};
.rb.dirDates:{[dir;t]
  p:` sv (.rb.tablePath[dir;t]), `time;
  $[() ~ key p; `date$(); distinct `date$ @[get; p; {[e] `timestamp$()}]]};

.rb.compareWith:{[label; dir]
  if[() ~ key dir; .rb.say raze (label; ": "; string dir; " does not exist"); :()];
  .rb.say raze (label; ": "; string dir);
  {[dir;t]
    have:.rb.dirSeqs[dir;t]; want:.rb.acc[t] `tpSeqNo;
    onlyLog:count want except have; onlyDir:count have except want;
    .rb.say raze ("  "; string t; ": rows "; string count have; "  distinct "; string count distinct have;
                  "  logs-only "; string onlyLog; "  dir-only "; string onlyDir;
                  $[0 = count have; ""; raze ("  dates "; .Q.s1 .rb.dirDates[dir;t])]);
   }[dir] each .rb.cfg.tables;
  };

/ -------------------------------------------------------
/ Build + verify the staging partition
/ -------------------------------------------------------
.rb.build:{[d]
  stage:.rb.stagePath d;
  if[not () ~ key stage;
    .rb.say raze ("removing previous staging dir "; string stage);
    system raze ("rm -rf "; 1 _ string stage)];
  system raze ("mkdir -p "; 1 _ string stage);
  {[d;stage;t]
    tbl:.rb.acc t;
    tbl:update wdbRecvTimeUtcNs:0Nj from tbl;
    tbl:update `p#sym from `sym xasc tbl;
    path:` sv (.rb.tablePath[stage;t]), `;
    path set .Q.en[.rb.cfg.hdb] tbl;
    .rb.say raze ("  wrote "; string t; ": "; string count tbl; " rows");
   }[d;stage] each .rb.cfg.tables;
  .rb.verify[d; stage]
  };

/ Verify a dir holds exactly the collected rows for d. Returns 1b/0b.
.rb.verify:{[d; dir]
  ok:1b;
  {[d;dir;t]
    have:.rb.dirSeqs[dir;t]; want:.rb.acc[t] `tpSeqNo; dates:.rb.dirDates[dir;t];
    good:(count[have] = count want) and (count[have] = count distinct have) and (0 = count want except have) and (0 = count have except want) and (dates ~ $[count have; enlist d; `date$()]);
    .rb.say raze ("  verify "; string t; ": "; string count have; " rows, distinct "; string count distinct have;
                  ", missing "; string count want except have; ", extra "; string count have except want;
                  ", dates "; .Q.s1 dates; $[good; "  OK"; "  MISMATCH"]);
    if[not good; ok::0b];
   }[d;dir] each .rb.cfg.tables;
  ok
  };

/ -------------------------------------------------------
/ Swap
/ -------------------------------------------------------
.rb.swap:{[d]
  stage:.rb.stagePath d; part:.rb.partPath d;
  if[() ~ key stage; .rb.fail raze ("no staging dir "; string stage; " - run -build first")];
  .rb.say "re-verifying staging before the swap";
  if[not .rb.verify[d; stage]; .rb.fail "staging does not match the logs - not swapping"];
  if[not () ~ key part;
    bak:hsym `$ raze (.rb.cfg.hdbDir; "/.rebuild/"; string d; ".bak."; string `long$ .z.p);
    system raze ("mv "; 1 _ string part; " "; 1 _ string bak);
    .rb.say raze ("old partition moved to "; string bak)];
  system raze ("mv "; 1 _ string stage; " "; 1 _ string part);
  .rb.say raze ("staging moved to "; string part);
  if[not .rb.verify[d; part]; .rb.fail "partition does not match the logs after the swap (old copy kept in hdb/.rebuild/)"];
  .rb.say raze ("swap complete for "; string d);
  };

/ -------------------------------------------------------
/ Entry
/ -------------------------------------------------------
.rb.args:.z.x;
.rb.argAfter:{[flag] i:.rb.args ? flag; $[(i + 1) < count .rb.args; .rb.args i + 1; ""]};
if[not "-date" in .rb.args; -2 "usage: q kdb/utils/rebuild_day.q -date YYYY.MM.DD [-build] [-swap] [-compare DIR]"; exit 2];
d:"D"$.rb.argAfter "-date";
if[null d; -2 "REBUILD: bad -date"; exit 2];
if[d >= .z.d; .rb.fail "refusing to rebuild today or a future date (the log is still being written)"];

.rb.say raze ("day "; string d; "  logs "; .rb.cfg.logDir; "  HDB "; .rb.cfg.hdbDir);
.rb.collect d;
.rb.compareWith["existing partition"; .rb.partPath d];
cmp:.rb.argAfter "-compare";
if[count cmp; .rb.compareWith["compare dir"; hsym `$ cmp]];

if["-build" in .rb.args;
  .rb.say raze ("building "; string .rb.stagePath d);
  if[not .rb.build d; .rb.fail "staging verification failed"];
  .rb.say raze ("staging verified against the logs: "; string .rb.stagePath d)];
if["-swap" in .rb.args; .rb.swap d];
if[not any ("-build";"-swap") in .rb.args; .rb.say "report only (add -build, then -swap)"];
system "sleep 0.1";
exit 0
