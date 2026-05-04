/ tp.q - Tickerplant with KDB-X pubsub module

/ -------------------------------------------------------
/ Configuration
/ -------------------------------------------------------

.tp.cfg.port:5010;
.tp.cfg.logDir:"logs";
.tp.cfg.logEnabled:1b;

system "g 0";

.tp.epochOffset:neg"j"$1970.01.01D0;
.proc.startTime:.z.p;

/ -------------------------------------------------------
/ Table schemas (loaded from shared definition)
/ Must exist before pubsub init.
/ TP appends its own receive timestamp to trade and quote.
/ -------------------------------------------------------

\l ../schemas.q

trade_binance:.schema.extend[.schema.trade; enlist `tpRecvTimeUtcNs];
quote_binance:.schema.extend[.schema.quote; enlist `tpRecvTimeUtcNs];
health_feed_handler:.schema.health;

/ -------------------------------------------------------
/ Pub/Sub - KDB-X module (must be named 'pubsub' for IPC)
/ -------------------------------------------------------

pubsub:use`di.pubsub

pubsub.init[]

/ -------------------------------------------------------
/ Logging
/ -------------------------------------------------------

.tp.logHandle:0N;
.tp.logFile:`;
.tp.logCount:0j;

.tp.logFilePath:{[] hsym`$(.tp.cfg.logDir,"/",string[.z.d],".log")};

.tp.initLog:{[f]
  if[0=@[hcount;f;0j];f set()];
  hopen f
  };

.tp.openLog:{[]
  if[not .tp.cfg.logEnabled;:()];
  system"mkdir -p ",.tp.cfg.logDir;
  .tp.logFile:.tp.logFilePath[];
  .tp.logHandle:.tp.initLog[.tp.logFile];
  .tp.logCount:@[{-11!(-2;x)};.tp.logFile;0j];
  -1"TP: Log file: ",string[.tp.logFile]," (",string[.tp.logCount]," chunks)";
  };

.tp.closeLog:{[]
  if[not .tp.cfg.logEnabled;:()];
  if[not null .tp.logHandle;@[hclose;.tp.logHandle;{}];.tp.logHandle:0N];
  };

.tp.log:{[tbl;data]
  if[not .tp.cfg.logEnabled;:()];
  if[tbl=`health_feed_handler;:()];
  .tp.logHandle enlist(`upd;tbl;data);
  .tp.logCount+:1;
  };

.tp.rotate:{[]
  -1"TP: Rotating log...";
  .tp.closeLog[];
  .tp.openLog[];
  };

/ -------------------------------------------------------
/ Gap Detection
/ -------------------------------------------------------

/ Field index for fhSeqNo - derived from schema (single source of truth).
/ Note: trade_binance includes tpRecvTimeUtcNs as the last column,
/ but the FH sends rows without it (TP appends in upd), so the index
/ here matches the position in the incoming data list.
.tp.idx.tradeSeq:(cols trade_binance)?`fhSeqNo;

/ State: last seen sequence
.tp.seq.trade:0N;      / null = not yet initialized

/ Counters: total gaps and total missed messages
.tp.gaps.trade:0j;     / number of gap events
.tp.missed.trade:0j;   / total messages missed

/ FH restart counter (sequence went backwards)
.tp.restarts.trade:0j;

/ Check trade sequence and update gap counters
/ Returns: 1b if OK, 0b if gap detected (still processes message)
.tp.checkSeq:{[seq]
  lastSeq:.tp.seq.trade;
  / First message - initialize
  if[null lastSeq; .tp.seq.trade:seq; :1b];
  / Normal case - sequential
  if[seq = lastSeq + 1; .tp.seq.trade:seq; :1b];
  / Gap detected - missed messages
  if[seq > lastSeq + 1;
    missed:seq - lastSeq - 1;
    .tp.gaps.trade+:1;
    .tp.missed.trade+:missed;
    -1 "TP: Trade gap - expected ",string[lastSeq+1]," got ",string[seq]," (missed ",string[missed],")";
    .tp.seq.trade:seq;
    :0b
  ];
  / Sequence went backwards or duplicate - FH restart
  .tp.restarts.trade+:1;
  -1 "TP: Trade FH restart detected - seq reset from ",string[lastSeq]," to ",string[seq];
  .tp.seq.trade:seq;
  1b
  };

/ -------------------------------------------------------
/ Update handling
/ -------------------------------------------------------

.tp.tsToNs:{[ts] .tp.epochOffset+"j"$ts};

upd:{[tbl;data]
  / Health messages - no sequence check, no logging
  if[tbl=`health_feed_handler;
    pubsub.publish[tbl;data];
    :();
  ];
  
  / Check sequence for gap detection (trades only)
  if[tbl=`trade_binance; .tp.checkSeq[data .tp.idx.tradeSeq]];
  
  / Add TP receive timestamp
  data:data,.tp.tsToNs[.z.p];
  
  / Log, insert, publish
  .tp.log[tbl;data];
  pubsub.publish[tbl;data];
  };

/ Alias for feed handlers that call .u.upd
.u.upd:upd;

/ -------------------------------------------------------
/ Status and Monitoring
/ -------------------------------------------------------

/ Standardized health check (consistent across all processes)
.health:{[]
  st:$[.tp.gaps.trade > 0; `degraded; `ok];
  `process`port`uptime`status`memMB`msgsIn`msgsOut!(
    `tp;
    .tp.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    (`long$.Q.w[][`used]) % 1000000;
    .tp.logCount;      / Total logged messages
    .tp.logCount)      / Same (no separate tracking)
  }

.tp.status:{[]
  flip `metric`value!(
    `port`uptime`logFile`logChunks`logSizeMB`tradeGaps`tradeMissed`tradeRestarts`lastTradeSeq;
    (.tp.cfg.port;`second$.z.p-.proc.startTime;.tp.logFile;.tp.logCount;0.01*`long$100*(@[hcount;.tp.logFile;0j])%1e6;.tp.gaps.trade;.tp.missed.trade;.tp.restarts.trade;.tp.seq.trade))
  };

/ Compact status as dictionary (for programmatic use)
.tp.statusDict:{[]
  `port`uptime`logChunks`tradeGaps`tradeMissed!(.tp.cfg.port;`second$.z.p-.proc.startTime;.tp.logCount;.tp.gaps.trade;.tp.missed.trade)
  };

/ Log status (unchanged for compatibility)
.tp.logStatus:{[]
  ([]file:enlist .tp.logFile;chunks:enlist .tp.logCount;sizeMB:enlist(@[hcount;.tp.logFile;0j])%1e6)
  };

/ -------------------------------------------------------
/ End-of-Day
/ -------------------------------------------------------

.tp.endOfDay:{[]
  -1"TP: EOD - chunks:",string[.tp.logCount]," tradeGaps:",string[.tp.gaps.trade];
  pubsub.callendofday[];
  .tp.rotate[];
  delete from`trade_binance;
  delete from`quote_binance;
  delete from`health_feed_handler;
  .tp.logCount:0j;
  / Reset gap counters for new day
  .tp.gaps.trade:0j;
  .tp.missed.trade:0j;
  .tp.restarts.trade:0j;
  / Keep last seq (FH doesn't reset on EOD)
  };

/ -------------------------------------------------------
/ EOD - Midnight UTC Detection
/ -------------------------------------------------------

.tp.currentDate:.z.d;

.tp.checkEOD:{[]
  if[.z.d > .tp.currentDate;
    -1 "TP: Midnight UTC detected - triggering EOD";
    .tp.endOfDay[];
    .tp.currentDate:.z.d;
  ];
  };

/ Timer - check every 60 seconds for date rollover
.z.ts:{[] .tp.checkEOD[] };



/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system"p ",string .tp.cfg.port;

.tp.openLog[];

system "t 1000";   / Check every 1 second

-1"=======================================================";
-1"TP (KDB-X module) starting on port ",string[.tp.cfg.port];
-1"=======================================================";
-1"Tables: trade_binance quote_binance health_feed_handler";
-1"";
-1"Monitoring:";
-1"  .health[]        / Standardized health check";
-1"  .tp.status[]     / Full status table";
-1"  .tp.statusDict[] / Status as dictionary";
-1"  .tp.logStatus[]  / Log file status";
-1"";
-1"Operations:";
-1"  .tp.endOfDay[]    / Trigger end-of-day";
-1"";
-1"TP ready";
-1"=======================================================";
