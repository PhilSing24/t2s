/ test_hdb_utils.q - kdb/utils/hdbUtils.q on a small partitioned HDB, loaded
/ by a RELATIVE path and by an absolute one. After .hdb.use the HDB is the
/ current directory; helpers that built "<hdb path>/<date>" from a relative
/ path looked inside the HDB for itself and found nothing.
/ Run from project root: q tests/test_hdb_utils.q

\l tests/t_lib.q
\l kdb/utils/hdbUtils.q

.t.start "hdbUtils";

root:first system "pwd";
rel:"tests/sandbox_hdbutils";               / its own directory, removed at the end
system "rm -rf ", rel;
off:.hdb.epochOffsetNs;
mk:{[d;n] t0:(`timestamp$d) + 0D12;
  ([] time:t0 + 1000000000 * til n; sym:n#`BTCUSDT`ETHUSDT; price:100f + til n;
      fhRecvTimeUtcNs:off + `long$t0 + 1000000000 * til n)};
d1:2026.01.02; d2:2026.01.03;
t1:mk[d1; 10]; t2:mk[d2; 20];
/ three rows of d2 are clock-corrected: time 5 s after the receive time
t2:update time:time + 0D00:00:05 from t2 where i in 3 4 5;
{[d;t] (hsym `$raze (rel; "/"; string d; "/trade_binance/")) set .Q.en[hsym `$rel] t}'[(d1; d2); (t1; t2)];

check:{[how; path]
  system "cd ", root;
  .t.assert[how, ": .hdb.use loads the HDB"; .hdb.use path];
  .t.assertEq[how, ": .hdb.tables lists the table"; enlist `trade_binance; .hdb.tables[]];
  rc:.hdb.rowCounts[`trade_binance; d1; d2];
  .t.assertEq[how, ": .hdb.rowCounts"; 10 20; rc `rows];
  cp:.hdb.compression[`trade_binance; d1; d2];
  .t.assertEq[how, ": .hdb.compression returns one row per date"; (d1; d2); cp `date];
  .t.assert[how, ": .hdb.compression sizes are positive"; all 0 < cp `compressedMB];
  .t.assert[how, ": .hdb.compression logical size >= compressed size"; all cp[`logicalMB] >= cp `compressedMB];
  .t.assert[how, ": the larger partition is larger"; (cp[`logicalMB] 1) > cp[`logicalMB] 0];
  cc:.hdb.clockCorrected[`trade_binance; d2];
  .t.assertEq[how, ": .hdb.clockCorrected finds the corrected rows"; 3; count cc];
  .t.assertEq[how, ": clockLagMs"; 3#5000; cc `clockLagMs];
  .t.assertEq[how, ": no corrected rows on the other date"; 0; count .hdb.clockCorrected[`trade_binance; d1]];
  .t.assertEq[how, ": .hdb.load"; 30; count .hdb.load[`trade_binance; d1; d2]];
  };
check["relative path"; hsym `$rel];
check["absolute path"; hsym `$raze (root; "/"; rel)];

system "cd ", root;
system "rm -rf ", rel;
.t.finish[];
