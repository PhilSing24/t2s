/ quote_seq_body.q - builds synthetic quote partitions for tests/test_quote_seq.sh
/   q tests/quote_seq_body.q -step <clean|breaks|noids> -q
/ Environment: SANDBOX_HDB_PATH.

\l kdb/schemas.q

hdb:getenv `SANDBOX_HDB_PATH;
step:first (.Q.opt .z.x) `step;

/ r: table with sym, isValid, U, u, pu, fhSeqNo in publication order; fills
/ the rest of the stored layout with defaults and writes the splay.
wr:{[date; t; r]
  n:count r;
  s:.schema.stored t;
  def:{[n;v] $[11h = type v; n#`; 12h = type v; n#0Np; 1h = type v; n#1b; 9h = type v; n#100f; n#0j]}[n] each value flip s;
  x:flip (cols s) ! def;
  x:update time:2026.01.05D10:00:00 + 100000000 * til n, sym:r `sym, isValid:r `isValid,
           exchFirstUpdateId:r `U, exchUpdateId:r `u, fhSeqNo:r `fhSeqNo, tpSeqNo:1000 + til n from x;
  if[t = `quote_binance_fut; x:update exchPrevUpdateId:r `pu from x];
  (hsym `$ raze (hdb; "/"; string date; "/"; string t; "/")) set .Q.en[hsym `$hdb] update `p#sym from `sym xasc x};

row:{[s;v;U;u;pu;fh] `sym`isValid`U`u`pu`fhSeqNo!(s; v; U; u; pu; fh)};

if[step ~ "clean";
  / spot: contiguous ranges, a heartbeat, an overlapping event
  wr[2026.01.05; `quote_binance] (
    row[`BTCUSDT;1b;100;104;0N;1]; row[`ETHUSDT;1b;500;500;0N;2]; row[`BTCUSDT;1b;105;111;0N;3];
    row[`BTCUSDT;1b;105;111;0N;4];                                / heartbeat: ids repeat
    row[`ETHUSDT;1b;501;507;0N;5]; row[`BTCUSDT;1b;110;120;0N;6]; / overlap: U <= previous u
    row[`BTCUSDT;1b;121;121;0N;7]);
  / futures: non-consecutive ids chained by pu, a heartbeat
  wr[2026.01.05; `quote_binance_fut] (
    row[`BTCUSDT;1b;990;1010;985;1]; row[`BTCUSDT;1b;1500;1520;1010;2]; row[`BTCUSDT;1b;1500;1520;1010;3];
    row[`ETHUSDT;1b;40;44;31;4]; row[`BTCUSDT;1b;2900;2950;1520;5]; row[`ETHUSDT;1b;300;301;44;6]);
  exit 0];

if[step ~ "breaks";
  wr[2026.01.06; `quote_binance] (
    row[`BTCUSDT;1b;100;104;0N;1]; row[`BTCUSDT;1b;105;111;0N;2];
    / ETH: a gap the handler marked with an invalid row
    row[`ETHUSDT;1b;500;505;0N;3]; row[`ETHUSDT;0b;0N;0N;0N;4]; row[`ETHUSDT;1b;900;910;0N;5];
    / SOL: ids jump from 205 to 300 with nothing in between
    row[`SOLUSDT;1b;200;205;0N;6]; row[`SOLUSDT;1b;300;301;0N;7]; row[`SOLUSDT;1b;302;302;0N;8]);
  wr[2026.01.06; `quote_binance_fut] (
    row[`BTCUSDT;1b;990;1010;985;10]; row[`BTCUSDT;1b;1500;1520;1010;11];
    / ETH: the handler was restarted (fhSeqNo starts again), no invalid row
    row[`ETHUSDT;1b;40;44;31;12]; row[`ETHUSDT;1b;700;720;690;1]; row[`ETHUSDT;1b;800;801;720;2];
    / SOL: pu does not match the previous u, nothing marks it
    row[`SOLUSDT;1b;60;66;50;13]; row[`SOLUSDT;1b;90;95;80;14]);
  exit 0];

if[step ~ "noids";
  / rows stored before the id columns existed: migrated, ids null
  wr[2026.01.07; `quote_binance] (row[`BTCUSDT;1b;0N;0N;0N;1]; row[`BTCUSDT;1b;0N;0N;0N;2]);
  exit 0];

-2 "unknown -step"; exit 2
