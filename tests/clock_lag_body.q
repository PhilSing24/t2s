/ clock_lag_body.q - q assertions for tests/test_clock_lag.sh
/   q tests/clock_lag_body.q -step assert -total T -corrected C -lagMs M
/                           [-backfilled B] [-crossDay 1] -q
/ Reads every stored trade_binance row of the sandbox (tmp dirs and HDB
/ partitions) and checks, with the helper users are given
/ (.hdb.clockCorrectedRows in kdb/utils/hdbUtils.q):
/   - T rows stored, C of them clock-corrected (time <> fhRecvTimeUtcNs)
/   - a corrected row with an event time has exactly that time; the B
/     corrected rows without one (backfilled) have a time close to now
/   - fhRecvTimeUtcNs of a corrected row is the stale reading, M ms behind
/   - an uncorrected row's time is its receive time, close to its event
/     time (at most the default 2 s threshold before it)
/   - every row is stored in the partition of its `time`; with -crossDay 1
/     the stale receive time of the corrected rows is on the previous day
/     and no directory exists for that day
/   q tests/clock_lag_body.q -step assert_hdb -corrected C -q
/ Saves the stored rows as a date-partitioned HDB inside the sandbox, loads
/ it with .hdb.use and checks that .hdb.clockCorrected[table; date] returns
/ the C corrected rows with fhRecvTime and clockLagMs.
/ Exit code 0 on success.

\l tests/t_dur_lib.q
\l kdb/utils/hdbUtils.q

/ every stored row of table t, with the date of the directory it is in
diskTable:{[t]
  roots:raze value .schema.dataRoots[.d.sbHdb; .d.sbTmp];
  sym::@[get; hsym `$ .d.sbHdb, "/sym"; {[e] `symbol$()}];
  raze {[t;r] d:` sv r, t;
    $[() ~ key d; ();
      update sym:value sym, pdate:"D"$-10#string r from flip (.schema.splayCols d) ! {[d;c] get ` sv d, c}[d] each .schema.splayCols d]}[t] each roots};

if[.d.step ~ "assert";
  total:"J"$.d.arg `total; nCorr:"J"$.d.arg `corrected; lagMs:"J"$.d.arg `lagMs;
  nBf:$[count x:.d.arg `backfilled; "J"$x; 0j]; crossDay:"1" ~ .d.arg `crossDay;
  d:diskTable `trade_binance;
  if[total <> count d; .d.fail raze ("rows stored: "; string count d; ", expected "; string total)];
  c:.hdb.clockCorrectedRows d;
  if[nCorr <> count c; .d.fail raze ("clock-corrected rows: "; string count c; ", expected "; string nCorr)];
  .d.pass raze (string total; " rows stored, "; string nCorr; " clock-corrected (.hdb.clockCorrectedRows)");

  / uncorrected rows: time is the receive time, a moment after the event,
  / or before it by less than the threshold (a lag too small to correct)
  u:select from d where fhRecvTimeUtcNs = .hdb.epochOffsetNs + `long$time, not null exchEventTimeMs;
  if[count u;
    late:((`long$u `time) + .hdb.epochOffsetNs) - 1000000 * u `exchEventTimeMs;
    if[any (late < -2000000000) or late > 5000000000; .d.fail "an uncorrected row's time is not close to its event time"]];
  if[0 = nCorr; .d.pass "no row was corrected: time is the receive time everywhere"; .d.done[]];

  own:select from c where not null exchEventTimeMs;
  if[not all own[`time] = `timestamp$(1000000 * own `exchEventTimeMs) - .hdb.epochOffsetNs;
    .d.fail "a corrected row's time is not its exchange event time"];
  .d.pass raze (string count own; " corrected rows carry their own exchange event time as time");

  bf:select from c where null exchEventTimeMs;
  if[nBf <> count bf; .d.fail raze ("corrected rows without an event time: "; string count bf; ", expected "; string nBf)];
  if[nBf > 0;
    if[any 60000 < abs (`long$.z.p - bf `time) div 1000000; .d.fail "a corrected backfilled row's time is not a recent event time"];
    .d.pass raze (string nBf; " backfilled rows (no event time) took the most recent event time")];

  if[any 2000 < abs c[`clockLagMs] - lagMs;
    .d.fail raze ("clockLagMs "; .Q.s1 (min; max) @\: c `clockLagMs; ", expected about "; string lagMs)];
  .d.pass raze ("fhRecvTimeUtcNs keeps the stale reading: clockLagMs "; string min c `clockLagMs; ".."; string max c `clockLagMs);

  if[not all d[`pdate] = `date$d `time; .d.fail "a row is not in the partition of its time"];
  .d.pass "every row is stored under the date of its time";
  if[crossDay;
    if[not all (`date$c `fhRecvTime) < `date$c `time; .d.fail "expected the stale receive times on the previous day"];
    stale:distinct `date$c `fhRecvTime;
    if[any stale in d `pdate; .d.fail "rows were stored under the stale clock's date"];
    .d.pass raze ("stale receive times are on "; " " sv string stale; ", the rows are in "; " " sv string distinct c `pdate)];
  .d.done[]];

if[.d.step ~ "assert_hdb";
  nCorr:"J"$.d.arg `corrected;
  d:delete pdate from diskTable `trade_binance;
  root:hsym `$ .d.sbTmp, "../hdbcheck";
  dts:asc distinct `date$d `time;
  {[root;d;dt] (` sv root, (`$string dt), `trade_binance, `) set .Q.en[root] select from d where dt = `date$time}[root; d] each dts;
  if[not .hdb.use root; .d.fail "cannot load the sandbox HDB"];
  c:raze {[dt] .hdb.clockCorrected[`trade_binance; dt]} each dts;
  if[nCorr <> count c; .d.fail raze (".hdb.clockCorrected returned "; string count c; " rows, expected "; string nCorr)];
  if[not all `fhRecvTime`clockLagMs in cols c; .d.fail "fhRecvTime / clockLagMs columns missing"];
  if[not all c[`time] > c `fhRecvTime; .d.fail "a corrected row's time is not after its receive time"];
  if[count .hdb.clockCorrected[`trade_binance; -1 + first dts]; .d.fail "rows returned for a date with no partition"];
  .d.pass raze (".hdb.clockCorrected[`trade_binance; "; string first dts; "] on a partitioned HDB: "; string count c; " rows, clockLagMs "; string min c `clockLagMs; ".."; string max c `clockLagMs);
  .d.done[]];

.d.fail raze ("unknown step: "; .d.step);
