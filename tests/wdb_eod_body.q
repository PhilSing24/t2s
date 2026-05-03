/ test_wdb_eod_body.q - Run the WDB EOD assertions.
/ Spawned by test_wdb_eod.sh, which must have started TP and WDB
/ on TEST_TP_PORT and TEST_WDB_PORT respectively.

\l tests/t_lib.q
\l kdb/schemas.q

.t.start "wdb_eod";

/ -------------------------------------------------------
/ Read configuration from environment (set by the .sh wrapper)
/ -------------------------------------------------------

tpPort:"J"$getenv `TEST_TP_PORT;
wdbPort:"J"$getenv `TEST_WDB_PORT;
hdbPath:getenv `SANDBOX_HDB_PATH;

if[null tpPort; -1 "ERROR: TEST_TP_PORT not set"; exit 1];
if[null wdbPort; -1 "ERROR: TEST_WDB_PORT not set"; exit 1];
if[0 = count hdbPath; -1 "ERROR: SANDBOX_HDB_PATH not set"; exit 1];

-1 .t.msg ("Test TP port: "; string tpPort);
-1 .t.msg ("Test WDB port: "; string wdbPort);
-1 .t.msg ("Sandbox HDB: "; hdbPath);

/ -------------------------------------------------------
/ Connect to test TP
/ -------------------------------------------------------

hTP:@[hopen; (`$":localhost:",string tpPort; 5000); {[err] -1 "ERROR opening TP: ",err; -1}];
.t.assert["TP connection opened"; hTP > 0];
if[hTP <= 0; .t.finish[]];

/ -------------------------------------------------------
/ Publish synthetic data via TP's upd handler
/ TP receives 12-element rows (no tpRecvTimeUtcNs - TP appends that itself)
/ -------------------------------------------------------

now:.z.p;
nTrades:100;
nQuotes:50;

/ Build N synthetic trade rows. Field order MUST match .schema.trade.
mkTrade:{[i]
  (now + i*0D00:00:01;     / time
   `BTCUSDT;                / sym
   100000+i;                / tradeId
   78000.0+i*0.5;           / price
   0.001+i*0.0001;          / qty
   0b;                      / buyerIsMaker
   `long$1700000000000+i;   / exchEventTimeMs
   `long$1700000000000+i;   / exchTradeTimeMs
   `long$now+i*1000;        / fhRecvTimeUtcNs
   `long$10+i;              / fhParseUs
   `long$15+i;              / fhSendUs
   `long$1+i)               / fhSeqNo
  };

mkQuote:{[i]
  (now + i*0D00:00:02;
   `BTCUSDT;
   78000.0+i*0.5; 78000.5+i*0.5; 78001.0+i*0.5; 78001.5+i*0.5; 78002.0+i*0.5;
   1.0; 0.9; 0.8; 0.7; 0.6;
   78002.5+i*0.5; 78003.0+i*0.5; 78003.5+i*0.5; 78004.0+i*0.5; 78004.5+i*0.5;
   1.0; 0.9; 0.8; 0.7; 0.6;
   1b;
   `long$1700000000000+i;
   `long$now+i*1000;
   `long$10+i;
   `long$15+i;
   `long$1+i)
  };

/ Synchronous send so we know it lands before we proceed
{[h;row] h(`upd;`trade_binance;row)}[hTP;] each mkTrade each til nTrades;
{[h;row] h(`upd;`quote_binance;row)}[hTP;] each mkQuote each til nQuotes;

-1 .t.msg ("Published "; string nTrades; " trades and "; string nQuotes; " quotes");

/ Sleep briefly to let WDB's pubsub subscription deliver
system "sleep 1";

/ -------------------------------------------------------
/ Pre-EOD: verify WDB received the data
/ -------------------------------------------------------

hWDB:@[hopen; (`$":localhost:",string wdbPort; 5000); {[err] -1 "ERROR opening WDB: ",err; -1}];
.t.assert["WDB connection opened"; hWDB > 0];
if[hWDB <= 0; hclose hTP; .t.finish[]];

wdbTrades:hWDB ".wdb.stats.tradesReceived";
wdbQuotes:hWDB ".wdb.stats.quotesReceived";
.t.assertEq["WDB received all trades"; nTrades; wdbTrades];
.t.assertEq["WDB received all quotes"; nQuotes; wdbQuotes];

/ -------------------------------------------------------
/ Force EOD via TP. Setting .tp.currentDate to yesterday
/ makes the next .tp.checkEOD timer tick fire endOfDay.
/ -------------------------------------------------------

-1 "Forcing EOD by setting TP currentDate to yesterday...";
hTP ".tp.currentDate:.z.d-1";

/ Wait for TP timer (1s interval) + EOD propagation. Generous.
system "sleep 4";

/ -------------------------------------------------------
/ Post-EOD assertions
/ -------------------------------------------------------

/ Today's date (the day endOfDay was triggered as)
/ WDB's endofday computes d:-1+.z.d which is the *previous* day.
/ When we forced .tp.currentDate:.z.d-1, the next tick of TP saw
/ .z.d > currentDate and fired EOD. WDB then computed d as today-1
/ = yesterday. So the partition should be at <yesterday>.
yesterday:.z.d-1;
yesterdayStr:string yesterday;

partitionPath:hsym `$ hdbPath,"/",yesterdayStr;
tradeSplay:hsym `$ hdbPath,"/",yesterdayStr,"/trade_binance";
quoteSplay:hsym `$ hdbPath,"/",yesterdayStr,"/quote_binance";

.t.assert["HDB partition exists for yesterday"; not () ~ key partitionPath];
.t.assert["trade_binance splay exists in partition"; not () ~ key tradeSplay];
.t.assert["quote_binance splay exists in partition"; not () ~ key quoteSplay];

/ Verify content. Splay row count = length of any column file.
/ Use protected eval so a failure here is informative, not crashy.
tradeSymCol:hsym `$ hdbPath,"/",yesterdayStr,"/trade_binance/sym";
quoteSymCol:hsym `$ hdbPath,"/",yesterdayStr,"/quote_binance/sym";

tradeRows:@[{count get x}; tradeSymCol; {[err] -1 "ERR reading trade sym col: ",err; -1}];
quoteRows:@[{count get x}; quoteSymCol; {[err] -1 "ERR reading quote sym col: ",err; -1}];

.t.assertEq["trade splay has expected row count"; nTrades; tradeRows];
.t.assertEq["quote splay has expected row count"; nQuotes; quoteRows];

/ Temp directory should be gone after successful move.
/ Compare counts (not values) since `key` returns a typed empty symbol list,
/ which doesn't match-equal the generic `()` even when both are empty.
sandboxParent:hsym `$ hdbPath,"/..";
tmpDirs:key sandboxParent;
tmpRemaining:tmpDirs where tmpDirs like "tmp.*";
.t.assertEq["no leftover tmp directories"; 0; count tmpRemaining];

/ WDB stats should have reset
wdbStatsAfter:hWDB ".wdb.stats.tradesReceived";
.t.assertEq["WDB stats reset after EOD"; 0; wdbStatsAfter];

/ Cleanup connections
hclose hTP;
hclose hWDB;

.t.finish[];
