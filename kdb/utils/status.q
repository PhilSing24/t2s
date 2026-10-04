/ status.q - the q half of ./status.sh: TP and WDB health in a few lines,
/ rows today per table, the counters that matter, and an "attention" list.
/ Exit code 1 when anything needs attention, 0 otherwise.
/ -
/ Environment: T2S_TP_PORT (5010), T2S_WDB_PORT (5011), T2S_TMP_DIR (for
/ today's tmp dir row counts; default ../ relative to kdb/tick, i.e. the
/ same rule wdb.q uses when nothing is exported).

\c 25 400

.st.tpPort:$[count v:getenv `T2S_TP_PORT; "J"$v; 5010];
.st.wdbPort:$[count v:getenv `T2S_WDB_PORT; "J"$v; 5011];
.st.tmpDir:$[count v:getenv `T2S_TMP_DIR; v; first system "dirname ",string .z.f; ""];
.st.attention:();
.st.note:{[msg] .st.attention,:enlist msg};
.st.fmtInt:{[n] $[null n; "-"; string n]};

.st.open:{[port] @[hopen; (`$":localhost:",string port; 2000); {[e] 0N}]};

/ ---------------- TP ----------------
h:.st.open .st.tpPort;
if[null h;
  -1 "TP   : DOWN (port ",string[.st.tpPort],")";
  .st.note "TP is not reachable"];
if[not null h;
  hd:h ".health[]"; st:h ".tp.status[]"; fs:h ".tp.fhStatus[]"; inc:0!h ".tp.incidents[]"; gp:h ".tp.gapStatus[]"; hclose h;
  win:hd `alertWindowSec;
  winTxt:{[w] $[w < 120; raze (string w; " s"); raze (string w div 60; " min")]};
  -1 "TP   : ",string[hd `status]," up ",string[hd `uptime],"  tpSeqNo ",string[hd `tpSeqNo],
     "  msgs ",string[hd `msgsIn],"  disk free ",.st.fmtInt[hd `diskFreeMB]," MB",
     "  clock skew ",.st.fmtInt[hd `clockSkewMs]," ms";
  -1 "       sessions: ",", " sv {[r] string[r `table],$[null r `sessionId; " NONE"; " fh#",string r `lastFhSeqNo]} each st;
  -1 "       gaps ",string[hd `gaps],"  missed ",string[hd `missed],"  restarts ",string[hd `restarts],
     "  reconnects ",string[hd `reconnects],"  outOfOrder ",string[hd `outOfOrder],
     "  schemaMismatch ",string[hd `schemaMismatch],"  rejectedReg ",string[hd `rejectedRegistrations],
     "  unregistered ",string[hd `unregisteredRows];
  / Feed-handler counters per table: totals since each handler started.
  / They are shown, not flagged; what is flagged is what happened recently.
  {[d]
    k:(key d) except `table`reportedAt`msgsReceived`rowsPublished;
    k:k where (0 < d k) or k in `wsReconnects`bookGaps`resyncs`exchGaps`exchMissed`gapsOpen;
    age:`long$(.z.p - d `reportedAt) % 1000000000;
    -1 "FH   : ",string[d `table],"  msgs ",string[d `msgsReceived],"  rows ",string[d `rowsPublished],"  ",
       ("  " sv {[d;k] string[k]," ",string d k}[d] each k),"  (",string[age]," s ago)";
    if[age > 60; .st.note string[d `table]," handler has not reported counters for ",string[age]," s"];
   } each fs;
  / Trade-id gaps (trade_gap): today's, plus any still open
  nOpen:exec count i from gp where status in `detected`partial;
  -1 "GAPS : open ",string[nOpen],$[nOpen > 0; raze (" (oldest "; string hd `oldestOpenGapSec; " s)"); ""],
     "  recovered ",string[exec count i from gp where status = `recovered],
     " (",string[exec sum recovered from gp where status = `recovered]," trades backfilled)",
     "  unrecoverable ",string exec count i from gp where status = `unrecoverable;
  if[(nOpen > 0) and 600 < 0 ^ hd `oldestOpenGapSec; .st.note string[nOpen]," trade gap(s) open for more than 10 minutes (oldest ",string[hd `oldestOpenGapSec]," s)"];
  / Problems within the alert window, TP's own and the handlers'
  fmtAge:{[t] raze (string `long$(.z.p - t) % 60000000000; " min ago")};
  -1 "RECENT (",winTxt[win],"): ",$[count inc; "; " sv {[fmtAge;r] raze (string r `src; " "; string r `kind; " +"; string r `n; " ("; fmtAge r `lastTime; ")")}[fmtAge] each inc; "nothing"];
  {[win;winTxt;r] .st.note raze (string r `src; ": "; string r `kind; " +"; string r `n; " in the last "; winTxt win)}[win; winTxt] each inc where not inc[`kind] in `outOfOrder;
  if[not hd[`status] ~ `ok; .st.note "TP status is ",string hd `status];
  if[hd `diskLow; .st.note "disk free below threshold: ",string[hd `diskFreeMB]," MB"];
  if[hd `clockSkewHigh; .st.note "clock skew vs exchange ",string[hd `clockSkewMs]," ms - check the WSL clock (sudo hwclock -s)"];
  / Only the tables of the markets start.sh launched are expected to have a
  / handler (T2S_STATUS_MARKETS = run/markets.active; empty = all four).
  mk:getenv `T2S_STATUS_MARKETS;
  expected:$[0 = count mk; st `table;
    raze ($[mk like "*spot*"; `trade_binance`quote_binance; `symbol$()]; $[mk like "*futures*"; `trade_binance_fut`quote_binance_fut; `symbol$()])];
  noSession:exec table from st where null sessionId, table in expected;
  if[count noSession; .st.note "no handler session for: ",", " sv string noSession]];

/ ---------------- WDB ----------------
w:.st.open .st.wdbPort;
if[null w;
  -1 "WDB  : DOWN (port ",string[.st.wdbPort],")";
  .st.note "WDB is not reachable"];
if[not null w;
  wh:w ".health[]"; rs:w ".wdb.replayStatus[]"; ws:w ".wdb.status[]"; hclose w;
  -1 "WDB  : ",string[wh `status]," up ",string[wh `uptime],"  ",string[wh `connState]," to TP",
     "  flushes today ",string[wh `flushes],"  rows written today ",string[wh `rowsWritten],
     "  checkpoint ",string[rs `lastTpSeqNoTrade],"/",string[rs `lastTpSeqNoAggTrade],"/",string[rs `lastTpSeqNoQuote],"/",string[rs `lastTpSeqNoQuoteFut];
  / rows today per table: on disk in tmp.<today> plus buffered in memory
  today:ws `today;
  tmpPath:ws `tmpSave;
  diskRows:{[p;t] c:` sv p,t,`tpSeqNo; $[() ~ key c; 0j; count get c]}[tmpPath] each `trade_binance`trade_binance_fut`quote_binance`quote_binance_fut;
  buf:wh `bufferTrades`bufferAggTrades`bufferQuotes`bufferQuotesFut;
  tot:diskRows + buf;
  -1 "       rows ",string[today],": trades ",string[tot 0]," (",string[buf 0]," buffered)",
     "  fut trades ",string[tot 1]," (",string[buf 1]," buffered)",
     "  quotes ",string[tot 2]," (",string[buf 2]," buffered)",
     "  fut quotes ",string[tot 3]," (",string[buf 3]," buffered)";
  -1 "       duplicates ",string[wh `duplicatesDropped],"  late ",string[wh `lateRows],"  unexpectedDate ",string[wh `unexpectedDateRows],
     "  replayFailures ",string[wh `replayFailures],"  halted ",string[wh `halted],
     "  last replay ",string[rs `lastReplayRows]," rows/",string[rs `lastReplayMs]," ms",
     "  last roll ",$[null rs `lastRollDate; "-"; string rs `lastRollDate];
  / Totals above; flagged below is the state now (status, halted) and what
  / happened within the alert window.
  wr:wh `recent; wwin:wh `alertWindowSec;
  wTxt:$[wwin < 120; raze (string wwin; " s"); raze (string wwin div 60; " min")];
  -1 "       recent (",wTxt,"): ",$[count wr; ", " sv {[k;v] raze (string k; " +"; string v)}'[key wr; value wr]; "nothing"];
  if[not wh[`status] ~ `ok; .st.note "WDB status is ",string wh `status];
  if[wh `halted; .st.note "WDB is HALTED: ",rs `haltReason];
  {[wTxt;k;v]
    kind:last ` vs k;
    if[kind = `replayFailure; .st.note raze (string v; " replay failure(s) in the last "; wTxt)];
    if[kind = `lateRows; .st.note raze (string v; " late rows in the last "; wTxt; " ("; string first ` vs k; "), kept in a tmp dir for manual review")];
    if[kind = `unexpectedDateRows; .st.note raze (string v; " rows with an unexpected date in the last "; wTxt; " ("; string first ` vs k; ")")];
   }[wTxt]'[key wr; value wr];
  if[not wh[`connState] ~ `connected; .st.note "WDB is ",string[wh `connState]," from TP"]];

/ ---------------- verdict ----------------
if[0 = count .st.attention; -1 "ATTN : nothing"; system "sleep 0.05"; exit 0];
-1 "ATTN : ",string[count .st.attention]," item(s)";
{-1 "       - ",x} each .st.attention;
system "sleep 0.05";
exit 1
