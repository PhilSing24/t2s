/ wdb_dur_body.q - q steps for tests/test_wdb_durability.sh
/ -
/ The shell script owns process lifecycles (spawn, kill -9, restart); this
/ file provides the q-side steps it calls between them. One step per
/ invocation:
/ -
/   q tests/wdb_dur_body.q -step <name> [-table t] [-date D] [-rows N]
/                          [-key k] [-value v] [-by N] -q
/ -
/ Steps:
/   publish            publish -rows N rows of -table (trade_binance |
/                      quote_binance) to the TP, with `time` on -date
/   flush              .wdb.flush[] on the WDB (flush + checkpoint)
/   shutdown           .wdb.shutdownAndExit[] on the WDB (async)
/   set_clock          .wdb.clock.set[-date] on the WDB
/   rewind_checkpoint  lower the sandbox checkpoint for -table by -by
/   inject_dup         send an `upd for -table straight to the WDB with
/                      tpSeqNo = -seq (as a TP resend would), to exercise
/                      receipt-time dedupe
/   assert_tmp         tmp.<date>/<table> has -rows rows, all distinct
/                      tpSeqNo, all `time` dates = -date
/   assert_no_tmp      tmp.<date> does not exist
/   assert_partition   hdb/<date>/<table> has -rows rows, all distinct
/                      tpSeqNo, all `time` dates = -date
/   assert_checkpoint  checkpoint seq for -table = max tpSeqNo of that
/                      table in the TP log, and checkpoint date = -date
/   assert_vs_tplog    union of tpSeqNo for -table over every tmp.* and
/                      HDB partition equals the set in the TP log exactly
/   assert_status      .wdb.replayStatus[][-key] (or .health[][-key])
/                      formatted with string equals -value
/ -
/ Environment (set by the shell script):
/   TEST_TP_PORT TEST_WDB_PORT SANDBOX_TMP_PATH SANDBOX_HDB_PATH
/   SANDBOX_TPLOG_PATH SANDBOX_CHECKPOINT
/ -
/ Exit code 0 on success, 1 on any failed assertion.

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

/ ---------------------------------------------------------------------------
/ Synthetic rows. fhSeqNo must stay contiguous per side for the whole life of
/ a TP (its gap/dup detection), so the counter lives in a file in the sandbox
/ and continues across invocations. It is reset by the shell when the TP is.
/ ---------------------------------------------------------------------------
.d.seqFile:hsym `$ .d.sbTmp, "../fhseq";
.d.nextSeq:{[side]
  d:$[() ~ key .d.seqFile; `trade`quote!0 0j; get .d.seqFile];
  d[side]+:1;
  .d.seqFile set d;
  d side};

/ Base timestamp for rows dated D: noon, so they stay well inside the day.
.d.baseTs:{[d] ("p"$d) + 0D12:00:00};

.d.mkTrade:{[ts;i;seq]
  (ts; `BTCUSDT; 100000+i; 78000.0+i*0.5; 0.001+i*0.0001; 0b;
   `long$1700000000000+i; `long$1700000000000+i; "j"$ts; 10j; 15j; seq)};

.d.mkQuote:{[ts;i;seq]
  (ts; `BTCUSDT;
   78000.0+i*0.5; 78000.5+i*0.5; 78001.0+i*0.5; 78001.5+i*0.5; 78002.0+i*0.5;
   1.0; 0.9; 0.8; 0.7; 0.6;
   78002.5+i*0.5; 78003.0+i*0.5; 78003.5+i*0.5; 78004.0+i*0.5; 78004.5+i*0.5;
   1.0; 0.9; 0.8; 0.7; 0.6;
   1b; `long$1700000000000+i; "j"$ts; 10j; 15j; seq)};

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

/ tpSeqNo of every row for table t in the sandbox TP log (today's file).
.d.tpLogSeqs:{[t]
  f:hsym `$ .d.tpLogDir, "/", string[.z.d], ".log";
  if[() ~ key f; :`long$()];
  .d.acc::`long$();
  .d.tgt::t;
  upd::{[tb;d] if[tb = .d.tgt; .d.acc,:last d]};
  -11! f;
  .d.acc};

/ Shape check shared by assert_tmp and assert_partition.
.d.checkRows:{[what;dir;d;n]
  seqs:.d.col[dir; `tpSeqNo];
  times:.d.col[dir; `time];
  if[n <> count seqs; .d.fail raze (what; " "; string dir; ": expected "; string n; " rows, got "; string count seqs)];
  if[n <> count distinct seqs; .d.fail raze (what; " "; string dir; ": duplicate tpSeqNo ("; string count seqs; " rows, "; string count distinct seqs; " distinct)")];
  bad:distinct (`date$times) except d;
  if[count bad; .d.fail raze (what; " "; string dir; ": rows dated "; .Q.s1 bad; " in a "; string d; " location")];
  .d.pass raze (what; " "; string dir; ": "; string n; " rows, distinct tpSeqNo, all dated "; string d)};

/ ---------------------------------------------------------------------------
/ Steps
/ ---------------------------------------------------------------------------
if[.d.step ~ "publish";
  t:`$.d.arg `table; n:"J"$.d.arg `rows; d:"D"$.d.arg `date;
  h:.d.open .d.tpPort;
  base:.d.baseTs d;
  $[t = `trade_binance;
    {[h;base;i] h (`upd; `trade_binance; .d.mkTrade[base + i*0D00:00:00.001; i; .d.nextSeq `trade])}[h;base] each til n;
    t = `quote_binance;
    {[h;base;i] h (`upd; `quote_binance; .d.mkQuote[base + i*0D00:00:00.001; i; .d.nextSeq `quote])}[h;base] each til n;
    .d.fail "publish: unknown table"];
  hclose h;
  -1 raze ("  published "; string n; " "; string t; " rows dated "; string d);
  .d.done[]];

if[.d.step ~ "flush";
  h:.d.open .d.wdbPort; ok:h ".wdb.flush[]"; hclose h;
  if[not ok; .d.fail "flush reported failures"];
  .d.pass "flush ok"; .d.done[]];

if[.d.step ~ "shutdown";
  h:.d.open .d.wdbPort; neg[h] ".wdb.shutdownAndExit[]"; neg[h] (::); hclose h;
  -1 "  shutdown requested"; .d.done[]];

if[.d.step ~ "set_clock";
  d:"D"$.d.arg `date;
  h:.d.open .d.wdbPort; h (".wdb.clock.set"; d); hclose h;
  -1 raze ("  clock set to "; string d); .d.done[]];

if[.d.step ~ "rewind_checkpoint";
  t:`$.d.arg `table; by:"J"$.d.arg `by;
  v:get .d.checkpoint;
  seq:v `seq; old:seq t; seq[t]:0 | old - by; v[`seq]:seq;
  .d.checkpoint set v;
  -1 raze ("  checkpoint "; string t; " rewound "; string old; " -> "; string seq t); .d.done[]];

if[.d.step ~ "inject_dup";
  t:`$.d.arg `table; seq:"J"$.d.arg `seq; d:"D"$.d.arg `date;
  row:$[t = `trade_binance; .d.mkTrade[.d.baseTs d; 0; 0]; .d.mkQuote[.d.baseTs d; 0; 0]];
  / shape as TP would deliver it: FH row + tpRecvTimeUtcNs + tpSeqNo
  row:row, ("j"$.z.p; seq);
  h:.d.open .d.wdbPort; neg[h] (`upd; t; row); h ""; hclose h;
  -1 raze ("  injected "; string t; " row with tpSeqNo "; string seq; " directly into WDB"); .d.done[]];

if[.d.step ~ "assert_tmp";
  t:`$.d.arg `table; d:"D"$.d.arg `date; n:"J"$.d.arg `rows;
  .d.checkRows["tmp"; .d.tmpTable[d;t]; d; n]; .d.done[]];

if[.d.step ~ "assert_no_tmp";
  d:"D"$.d.arg `date;
  p:hsym `$ .d.sbTmp, "tmp.", string d;
  if[not () ~ key p; .d.fail raze ("tmp dir still present: "; string p)];
  .d.pass raze ("no tmp dir for "; string d); .d.done[]];

if[.d.step ~ "assert_partition";
  t:`$.d.arg `table; d:"D"$.d.arg `date; n:"J"$.d.arg `rows;
  .d.checkRows["partition"; .d.hdbTable[d;t]; d; n]; .d.done[]];

if[.d.step ~ "assert_checkpoint";
  t:`$.d.arg `table; d:"D"$.d.arg `date;
  if[() ~ key .d.checkpoint; .d.fail "checkpoint file missing"];
  v:get .d.checkpoint;
  if[not (99h = type v) and `seq in key v; .d.fail raze ("checkpoint has old shape: "; .Q.s1 v)];
  expected:max .d.tpLogSeqs t;
  if[expected <> v[`seq] t; .d.fail raze ("checkpoint "; string t; " = "; string v[`seq] t; ", expected max tpSeqNo in TP log = "; string expected)];
  if[d <> v `date; .d.fail raze ("checkpoint date = "; string v `date; ", expected "; string d)];
  .d.pass raze ("checkpoint "; string t; " = "; string expected; " (TP log max), date "; string d); .d.done[]];

if[.d.step ~ "assert_vs_tplog";
  t:`$.d.arg `table;
  logSeqs:.d.tpLogSeqs t;
  disk:.d.allDiskSeqs t;
  diskSeqs:raze disk;
  dups:count[diskSeqs] - count distinct diskSeqs;
  missing:logSeqs except diskSeqs;
  extra:diskSeqs except logSeqs;
  if[dups > 0; .d.fail raze (string t; ": "; string dups; " duplicate tpSeqNo on disk (tmp "; string count disk 0; " + hdb "; string count disk 1; ")")];
  if[count missing; .d.fail raze (string t; ": "; string count missing; " rows in TP log missing on disk, e.g. "; .Q.s1 5 sublist missing)];
  if[count extra; .d.fail raze (string t; ": "; string count extra; " rows on disk not in TP log")];
  .d.pass raze (string t; ": disk (tmp "; string count disk 0; " + hdb "; string count disk 1; ") == TP log ("; string count logSeqs; " rows), no duplicates, none missing");
  .d.done[]];

if[.d.step ~ "assert_status";
  k:`$.d.arg `key; want:.d.arg `value;
  h:.d.open .d.wdbPort;
  rs:h ".wdb.replayStatus[]"; hd:h ".health[]"; hclose h;
  / .health[] first: it holds the in-memory buffer counts (bufferTrades etc.);
  / .wdb.replayStatus[] reuses those names for the reconnect buffer.
  v:$[k in key hd; hd k; k in key rs; rs k; .d.fail raze ("unknown status key "; string k)];
  got:$[10h = type v; v; string v];
  if[not got ~ want; .d.fail raze (string k; " = "; got; ", expected "; want)];
  .d.pass raze (string k; " = "; got); .d.done[]];

.d.fail raze ("unknown step: "; .d.step);
