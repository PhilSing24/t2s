/ hdb_migrate_body.q - q steps for tests/test_hdb_migrate.sh
/   q tests/hdb_migrate_body.q -step <build|bad|verify|query> -q
/ Environment: SANDBOX_HDB_PATH, SANDBOX_TMP_PATH. Exit 1 on a failed check.

\l kdb/schemas.q

hdb:getenv `SANDBOX_HDB_PATH; tmpd:getenv `SANDBOX_TMP_PATH;
step:first (.Q.opt .z.x) `step;
bad:0;
chk:{[msg;ok] -1 raze ($[ok; "  PASS: "; "  FAIL: "]; msg); if[not ok; bad+::1]};
finish:{[] system "sleep 0.05"; exit bad > 0};
dir:{[root;t] hsym `$ raze (root; "/"; string t)};
c:{[d;col] get ` sv d, col};

/ Columns the schema gained since the "old" layout this test writes
drop:`trade_binance`trade_binance_fut`quote_binance`quote_binance_fut ! (`symbol$(); enlist `qtyExRpi; `exchFirstUpdateId`exchUpdateId; `exchFirstUpdateId`exchUpdateId`exchPrevUpdateId);

/ n rows of table t with recognisable values, in the OLD layout
gen:{[n;v] $[11h = type v; n#`BTCUSDT`ETHUSDT; 12h = type v; 2026.01.05D10:00:00 + 1000000 * til n; 1h = type v; n#10b; 9h = type v; 100 + 0.5 * til n; 1000 + til n]};
mk:{[t;n] s:.schema.stored t; drop[t] _ flip (cols s) ! gen[n] each value flip s};
wr:{[root;t;n] (hsym `$ raze (root; "/"; string t; "/")) set .Q.en[hsym `$hdb] update `p#sym from `sym xasc mk[t;n]};

if[step ~ "build";
  / 2026.01.05: three tables, no futures quotes at all
  wr[raze (hdb; "/2026.01.05")] ./: ((`trade_binance; 7); (`trade_binance_fut; 4); (`quote_binance; 6));
  / 2026.01.06: all four tables
  wr[raze (hdb; "/2026.01.06")] ./: ((`trade_binance; 3); (`trade_binance_fut; 5); (`quote_binance; 8); (`quote_binance_fut; 9));
  / an intraday dir left behind in the old layout
  wr[raze (tmpd; "tmp.2026.01.07")] ./: ((`quote_binance; 2); (`quote_binance_fut; 3));
  finish[]];

if[step ~ "bad";
  / a quote table at depth 2, and a trade table with a column the schema does not know
  (hsym `$ raze (hdb; "/2026.01.08/quote_binance/")) set .Q.en[hsym `$hdb] ([] time:2#2026.01.08D10:00:00; sym:`BTCUSDT`BTCUSDT; bidPrice1:1 2f; bidPrice2:1 2f; tpSeqNo:1 2);
  (hsym `$ raze (hdb; "/2026.01.09/trade_binance/")) set .Q.en[hsym `$hdb] ([] time:1#2026.01.09D10:00:00; sym:1#`BTCUSDT; mystery:1#1f);
  finish[]];

if[step ~ "verify";
  d1:dir[raze (hdb; "/2026.01.05"); `quote_binance];
  chk["quote_binance .d is the schema's column list, in order"; (.schema.splayCols d1) ~ .schema.storedCols `quote_binance];
  chk["new long columns: 6 nulls each"; (6 = count c[d1; `exchUpdateId]) and (all null c[d1; `exchUpdateId]) and (all null c[d1; `exchFirstUpdateId]) and 7h = type c[d1; `exchUpdateId]];
  chk["old columns still hold their values"; (asc c[d1; `tpSeqNo]) ~ 1000 + til 6];
  d2:dir[raze (hdb; "/2026.01.06"); `trade_binance_fut];
  chk["qtyExRpi: 5 null floats"; (5 = count c[d2; `qtyExRpi]) and (all null c[d2; `qtyExRpi]) and 9h = type c[d2; `qtyExRpi]];
  chk["qtyExRpi sits after qty in .d"; 1 = ((.schema.splayCols d2)?`qtyExRpi) - (.schema.splayCols d2)?`qty];
  d3:dir[raze (hdb; "/2026.01.05"); `quote_binance_fut];
  chk["missing futures quote table created empty with the full layout"; ((.schema.splayCols d3) ~ .schema.storedCols `quote_binance_fut) and 0 = count c[d3; `tpSeqNo]];
  d4:dir[raze (tmpd; "tmp.2026.01.07"); `quote_binance_fut];
  chk["tmp dir: exchPrevUpdateId added, 3 nulls"; (3 = count c[d4; `exchPrevUpdateId]) and all null c[d4; `exchPrevUpdateId]];
  chk["tmp dir: no table created where none existed"; () ~ key dir[raze (tmpd; "tmp.2026.01.07"); `trade_binance]];
  finish[]];

if[step ~ "query";
  system "l ", hdb;
  r:select n:count i, nullIds:sum null exchUpdateId by date from quote_binance;
  chk["quote_binance across both dates: 6 and 8 rows"; (exec n from r) ~ 6 8];
  chk["quote_binance.exchUpdateId is null in the old data"; (exec nullIds from r) ~ 6 8i];
  chk["quote_binance_fut answers for every date (2026.01.05 empty, 2026.01.06 has 9)"; (0 = count select from quote_binance_fut where date = 2026.01.05) and 9 = count select from quote_binance_fut where date within 2026.01.05 2026.01.06];
  chk["trade_binance_fut.qtyExRpi selectable across dates"; 9 = count select qty, qtyExRpi from trade_binance_fut where null qtyExRpi];
  chk["a where clause on a new column works on old partitions"; 0 = count select from quote_binance_fut where exchPrevUpdateId > 0];
  finish[]];

-2 "unknown -step"; exit 2
