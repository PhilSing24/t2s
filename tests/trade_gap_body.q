/ trade_gap_body.q - q assertions for tests/test_trade_gap.sh
/   q tests/trade_gap_body.q -step <name> [args] -q
/ Steps:
/   show            print the trade_gap rows in the sandbox TP logs
/   assert_events   -count N: the logs hold N trade_gap rows; -statuses a,b,c:
/                   their status sequence (log order)
/   assert_gap      a logged row with -status S has -first F, -last L and
/                   (optional) -recovered R, -reason X, -src TABLE
/   assert_disk     WDB's tmp/HDB copy of trade_gap matches the logs (no row
/                   missing, none twice) and holds -count N rows
/   assert_ids      -table T: ids -first..-last appear exactly once on disk;
/                   with -backfilled 1 they all carry a null exchEventTimeMs
/                   and no other row does
/ Exit code 0 on success.

\l tests/t_dur_lib.q

gapCols:(cols .schema.tradeGap), `tpRecvTimeUtcNs`tpSeqNo;
gapRows:{[]
  acc::();
  upd::{[t;d] if[t = `trade_gap; acc,:enlist d]};
  {[f] -11! f} each .d.tpLogFiles[];
  $[count acc; flip gapCols ! flip acc; 0#flip gapCols ! (count gapCols)#enlist ()]};

/ every stored row of table t across the sandbox tmp dirs and HDB partitions
diskTable:{[t]
  roots:raze value .schema.dataRoots[.d.sbHdb; .d.sbTmp];
  sym::@[get; hsym `$ .d.sbHdb, "/sym"; {[e] `symbol$()}];
  raze {[t;r] d:` sv r, t; $[() ~ key d; (); enlist update sym:value sym from flip (.schema.splayCols d) ! {[d;c] get ` sv d, c}[d] each .schema.splayCols d]}[t] each roots};

if[.d.step ~ "show";
  show gapRows[]; .d.done[]];

if[.d.step ~ "assert_events";
  g:gapRows[]; n:"J"$.d.arg `count;
  if[n <> count g; show g; .d.fail raze ("trade_gap rows in the logs: "; string count g; ", expected "; string n)];
  if[count st:.d.arg `statuses;
    want:`$"," vs st;
    if[not want ~ g `status; .d.fail raze ("status sequence "; .Q.s1 g `status; ", expected "; .Q.s1 want)]];
  .d.pass raze (string n; " trade_gap event(s) logged"; $[count st; raze (": "; st); ""]); .d.done[]];

if[.d.step ~ "assert_gap";
  g:gapRows[]; st:`$.d.arg `status;
  r:select from g where status = st, firstMissingId = "J"$.d.arg `first, lastMissingId = "J"$.d.arg `last;
  if[0 = count r; show g; .d.fail raze ("no "; string st; " row for ids "; .d.arg `first; ".."; .d.arg `last)];
  r:last r;
  if[not r[`missing] = 1 + r[`lastMissingId] - r `firstMissingId; .d.fail "missing column is not last - first + 1"];
  if[count x:.d.arg `recovered; if[not r[`recovered] = "J"$x; .d.fail raze ("recovered = "; string r `recovered; ", expected "; x)]];
  if[count x:.d.arg `reason; if[not r[`reason] = `$x; .d.fail raze ("reason = "; string r `reason; ", expected "; x)]];
  if[count x:.d.arg `src; if[not r[`srcTable] = `$x; .d.fail raze ("srcTable = "; string r `srcTable; ", expected "; x)]];
  if[count x:.d.arg `sym; if[not r[`sym] = `$x; .d.fail raze ("sym = "; string r `sym; ", expected "; x)]];
  .d.pass raze (string st; " row: "; string r `sym; " "; string r `srcTable; " ids "; string r `firstMissingId; ".."; string r `lastMissingId;
                " missing "; string r `missing; " recovered "; string r `recovered; $[null r `reason; ""; raze (" reason "; string r `reason)]);
  .d.done[]];

if[.d.step ~ "assert_disk";
  g:gapRows[]; d:diskTable `trade_gap; n:"J"$.d.arg `count;
  ds:$[count d; (raze d) `tpSeqNo; `long$()];
  if[n <> count ds; .d.fail raze ("trade_gap on disk: "; string count ds; " rows, expected "; string n)];
  if[not (asc ds) ~ asc g `tpSeqNo; .d.fail "trade_gap on disk differs from the TP logs"];
  .d.pass raze ("trade_gap on disk: "; string n; " row(s), identical to the TP logs"); .d.done[]];

if[.d.step ~ "assert_ids";
  t:`$.d.arg `table; f:"J"$.d.arg `first; l:"J"$.d.arg `last;
  d:raze diskTable t;
  idc:$[t = `trade_binance; `tradeId; `aggTradeId];
  ids:d idc;
  want:f + til 1 + l - f;
  if[count miss:want except ids; .d.fail raze (string count miss; " id(s) of "; string f; ".."; string l; " are not on disk, e.g. "; string first miss)];
  if[(count where ids in want) <> count want; .d.fail raze ("ids "; string f; ".."; string l; " appear "; string count where ids in want; " times on disk, expected "; string count want)];
  if[(count ids) <> count distinct ids; .d.fail raze (string (count ids) - count distinct ids; " duplicated id(s) on disk")];
  if["1" ~ .d.arg `backfilled;
    bf:exec idc from (update idc:ids from d) where null exchEventTimeMs;
    if[not (asc bf) ~ want; .d.fail raze ("rows marked as backfilled (null exchEventTimeMs): "; string count bf; ", expected exactly ids "; string f; ".."; string l)]];
  .d.pass raze (string t; ": ids "; string f; ".."; string l; " are on disk exactly once"; $["1" ~ .d.arg `backfilled; ", and they are exactly the rows marked as backfilled"; ""]; " ("; string count ids; " rows, no duplicate id)");
  .d.done[]];

.d.fail raze ("unknown step: "; .d.step);
