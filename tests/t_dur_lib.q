/ t_dur_lib.q - shared q helpers for the sandboxed durability tests
/ (tests/wdb_dur_body.q and tests/tp_dur_body.q).
/ -
/ Realistic synthetic rows for all three tables, a per-side fhSeqNo counter
/ that survives across step invocations, readers for splayed columns and for
/ the sandbox TP logs, and the shape check shared by the assert steps.
/ -
/ Environment (set by the shell scripts):
/   TEST_TP_PORT TEST_WDB_PORT SANDBOX_TMP_PATH SANDBOX_HDB_PATH
/   SANDBOX_TPLOG_PATH SANDBOX_CHECKPOINT

\c 25 400
\l kdb/schemas.q

.d.opt:.Q.opt .z.x;
.d.arg:{[k] $[k in key .d.opt; first .d.opt k; ""]};
.d.step:.d.arg `step;
.d.tpPort:"J"$getenv `TEST_TP_PORT;
.d.wdbPort:"J"$getenv `TEST_WDB_PORT;
.d.sbTmp:getenv `SANDBOX_TMP_PATH;
.d.sbHdb:getenv `SANDBOX_HDB_PATH;
.d.tpLogDir:getenv `SANDBOX_TPLOG_PATH;
.d.checkpoint:hsym `$ getenv `SANDBOX_CHECKPOINT;

.d.fail:{[msg] -2 raze ("  FAIL: "; msg); system "sleep 0.1"; exit 1};
.d.pass:{[msg] -1 raze ("  PASS: "; msg)};
.d.done:{[] system "sleep 0.1"; exit 0};

.d.open:{[port]
  h:@[hopen; (`$":localhost:",string port; 5000); {[e] 0N}];
  if[null h; .d.fail raze ("cannot connect to port "; string port)];
  h};

/ Tables, their fhSeqNo counter side, and the feed-handler row width.
.d.tables:`trade_binance`quote_binance`trade_binance_fut;
.d.side:.d.tables ! `trade`quote`aggTrade;
.d.width:.d.tables ! 12 28 14;

/ ---------------------------------------------------------------------------
/ Synthetic rows. fhSeqNo must stay contiguous per side for the whole life of
/ a feed-handler session, so the counter lives in a file in the sandbox and
/ continues across invocations. The shell resets it when it simulates a
/ handler restart (or a new TP).
/ ---------------------------------------------------------------------------
.d.seqFile:hsym `$ .d.sbTmp, "../fhseq";
.d.loadSeq:{[] $[() ~ key .d.seqFile; `trade`quote`aggTrade!0 0 0j; get .d.seqFile]};
.d.nextSeq:{[side]
  d:.d.loadSeq[];
  d[side]+:1;
  .d.seqFile set d;
  d side};
.d.peekSeq:{[side] (.d.loadSeq[]) side};
.d.resetSeq:{[side] d:.d.loadSeq[]; d[side]:0j; .d.seqFile set d};

/ Base timestamp for rows dated D: noon, so they stay well inside the day.
.d.baseTs:{[d] ("p"$d) + 0D12:00:00};

.d.mkTrade:{[ts;i;seq]
  (ts; `BTCUSDT; 100000+i; 78000.0+i*0.5; 0.001+i*0.0001; 0b;
   `long$1700000000000+i; `long$1700000000000+i; "j"$ts; 10j; 15j; seq)};

/ Quote prices are deliberately SMALL (~100): the per-table tpSeqNo index
/ regression (a trade-schema index applied to quotes read askPrice2) only
/ shows when that misread value is below the checkpoint.
.d.mkQuote:{[ts;i;seq]
  (ts; `BTCUSDT;
   100.0+i*0.01; 100.5+i*0.01; 101.0+i*0.01; 101.5+i*0.01; 102.0+i*0.01;
   1.0; 0.9; 0.8; 0.7; 0.6;
   102.5+i*0.01; 103.0+i*0.01; 103.5+i*0.01; 104.0+i*0.01; 104.5+i*0.01;
   1.0; 0.9; 0.8; 0.7; 0.6;
   1b; `long$1700000000000+i; "j"$ts; 10j; 15j; seq)};

/ Futures aggTrade row: 14 feed-handler columns (aggTradeId, firstTradeId,
/ lastTradeId between sym and price). fhSeqNo sits at index 13.
.d.mkAggTrade:{[ts;i;seq]
  (ts; `BTCUSDT; 500000+i; 900000+2*i; 900001+2*i; 78000.0+i*0.5; 0.002+i*0.0001; 1b;
   `long$1700000000000+i; `long$1700000000000+i; "j"$ts; 10j; 15j; seq)};

.d.mkRow:{[t;ts;i;seq]
  $[t = `trade_binance; .d.mkTrade[ts;i;seq];
    t = `quote_binance; .d.mkQuote[ts;i;seq];
    t = `trade_binance_fut; .d.mkAggTrade[ts;i;seq];
    .d.fail raze ("unknown table "; string t)]};

/ Publish n rows of table t dated d over handle h (synchronous upd calls),
/ advancing the side's fhSeqNo counter. Returns n.
.d.publishRows:{[h;t;n;d]
  base:.d.baseTs d;
  {[h;t;base;i] h (`upd; t; .d.mkRow[t; base + i*0D00:00:00.001; i; .d.nextSeq .d.side t])}[h;t;base] each til n;
  n};

/ ---------------------------------------------------------------------------
/ Disk readers
/ ---------------------------------------------------------------------------
.d.tmpTable:{[d;t] hsym `$ .d.sbTmp, "tmp.", string[d], "/", string t};
.d.hdbTable:{[d;t] hsym `$ .d.sbHdb, "/", string[d], "/", string t};
.d.col:{[dir;c] @[get; ` sv dir, c; {[e] ()}]};

/ All tpSeqNo for table t across every tmp.* dir and every HDB partition.
.d.allDiskSeqs:{[t]
  tmpRoot:hsym `$ .d.sbTmp;
  tmpEntries:@[key; tmpRoot; {[e] `symbol$()}];
  tmpDirs:tmpEntries where (string tmpEntries) like "tmp.*";
  tmpSeqs:raze {[root;t;e] .d.col[` sv root, e, t; `tpSeqNo]}[tmpRoot; t] each tmpDirs;
  hdbRoot:hsym `$ .d.sbHdb;
  parts:@[key; hdbRoot; {[e] `symbol$()}];
  parts:parts where not null "D"$ string parts;
  hdbSeqs:raze {[root;t;p] .d.col[` sv root, p, t; `tpSeqNo]}[hdbRoot; t] each parts;
  (tmpSeqs; hdbSeqs)};

/ Every daily log in the sandbox TP log dir, oldest first.
.d.tpLogFiles:{[]
  d:hsym `$ .d.tpLogDir;
  files:@[key; d; {[e] `symbol$()}];
  names:asc string files where (string files) like "????.??.??.log";
  hsym each `$ (.d.tpLogDir, "/"),/: names};

/ (tpSeqNo; fhSeqNo) of every row for table t across ALL sandbox TP logs,
/ in log order. TP may have rotated to a (fake) new day mid-scenario.
.d.tpLogRows:{[t]
  .d.accTp::`long$(); .d.accFh::`long$();
  .d.tgt::t;
  .d.fhIdx::(cols .schema.extend[$[t = `trade_binance; .schema.trade; t = `quote_binance; .schema.quote; .schema.aggTrade]; `tpRecvTimeUtcNs`tpSeqNo])?`fhSeqNo;
  upd::{[tb;d] if[tb = .d.tgt; .d.accTp,:last d; .d.accFh,:d .d.fhIdx]};
  {[f] -11! f} each .d.tpLogFiles[];
  (.d.accTp; .d.accFh)};
.d.tpLogSeqs:{[t] first .d.tpLogRows t};

/ tpSeqNo of EVERY row (all tables) across all sandbox TP logs, in log order.
.d.tpLogAllSeqs:{[]
  .d.accAll::`long$();
  upd::{[tb;d] if[tb in .d.tables; .d.accAll,:last d]};
  {[f] -11! f} each .d.tpLogFiles[];
  .d.accAll};

/ Shape check shared by assert_tmp and assert_partition.
.d.checkRows:{[what;dir;d;n]
  seqs:.d.col[dir; `tpSeqNo];
  times:.d.col[dir; `time];
  if[n <> count seqs; .d.fail raze (what; " "; string dir; ": expected "; string n; " rows, got "; string count seqs)];
  if[n <> count distinct seqs; .d.fail raze (what; " "; string dir; ": duplicate tpSeqNo ("; string count seqs; " rows, "; string count distinct seqs; " distinct)")];
  bad:distinct (`date$times) except d;
  if[count bad; .d.fail raze (what; " "; string dir; ": rows dated "; .Q.s1 bad; " in a "; string d; " location")];
  .d.pass raze (what; " "; string dir; ": "; string n; " rows, distinct tpSeqNo, all dated "; string d)};

/ Format a status value for comparison with a -value string.
.d.fmt:{[v] $[10h = type v; v; -11h = type v; string v; string v]};
