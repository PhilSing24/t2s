/ tp.q - Tickerplant with STANDARD tick.q pub/sub (u.q)
/ Production-grade with single log file for temporal consistency

/ -------------------------------------------------------
/ Configuration
/ -------------------------------------------------------

.tp.cfg.port:5010;
.tp.cfg.logDir:"logs";
.tp.cfg.logEnabled:1b;

/ Nanoseconds between kdb+ epoch (2000.01.01) and Unix epoch (1970.01.01)
.tp.epochOffset: neg "j"$1970.01.01D0;

/ to store start time of the process
.proc.startTime:.z.p;

/ -------------------------------------------------------
/ Table schema
/ -------------------------------------------------------

/ Trade table - 13 fields (TP adds tpRecvTimeUtcNs, RDB adds rdbApplyTimeUtcNs)
trade_binance:([]
  time:`timestamp$();
  sym:`symbol$();
  tradeId:`long$();
  price:`float$();
  qty:`float$();
  buyerIsMaker:`boolean$();
  exchEventTimeMs:`long$();
  exchTradeTimeMs:`long$();
  fhRecvTimeUtcNs:`long$();
  fhParseUs:`long$();
  fhSendUs:`long$();
  fhSeqNo:`long$();
  tpRecvTimeUtcNs:`long$()
  );

/ Quote table - L5 depth (29 fields: 22 price/qty + 7 meta + tpRecvTimeUtcNs)
/ FH sends 28 fields, TP adds tpRecvTimeUtcNs
quote_binance:([]
  time:`timestamp$();
  sym:`symbol$();
  / L5 bid prices (best to worst)
  bidPrice1:`float$();
  bidPrice2:`float$();
  bidPrice3:`float$();
  bidPrice4:`float$();
  bidPrice5:`float$();
  / L5 bid quantities
  bidQty1:`float$();
  bidQty2:`float$();
  bidQty3:`float$();
  bidQty4:`float$();
  bidQty5:`float$();
  / L5 ask prices (best to worst)
  askPrice1:`float$();
  askPrice2:`float$();
  askPrice3:`float$();
  askPrice4:`float$();
  askPrice5:`float$();
  / L5 ask quantities
  askQty1:`float$();
  askQty2:`float$();
  askQty3:`float$();
  askQty4:`float$();
  askQty5:`float$();
  / Metadata
  isValid:`boolean$();
  exchEventTimeMs:`long$();
  fhRecvTimeUtcNs:`long$();
  fhParseUs:`long$();
  fhSendUs:`long$();
  fhSeqNo:`long$();
  tpRecvTimeUtcNs:`long$()
  );

/ Health metrics from feed handlers (no tpRecvTimeUtcNs added)
health_feed_handler:([]
  time:`timestamp$();
  handler:`symbol$();
  startTimeUtc:`timestamp$();
  uptimeSec:`long$();
  msgsReceived:`long$();
  msgsPublished:`long$();
  lastMsgTimeUtc:`timestamp$();
  lastPubTimeUtc:`timestamp$();
  connState:`symbol$();
  symbolCount:`int$()
  );

/ -------------------------------------------------------
/ Logging - Single file for all tables (standard tick.q)
/ Format compatible with -11! streaming replay
/ -------------------------------------------------------

/ Log handle (set at startup)
.tp.logHandle:0N;

/ Log file path (stored for reference)
.tp.logFile:`;

/ Message counter for recovery tracking
.tp.logCount:0j;

/ Build log file path for today
.tp.logFilePath:{[]
  hsym `$(.tp.cfg.logDir,"/",string[.z.D],".log")
  };

/ Initialize a log file for -11! compatibility
/ If file doesn't exist, create it with empty list
/ If file exists, keep it (append mode)
/ @param f - log file path (hsym)
/ @return file handle
.tp.initLog:{[f]
  / Check if file exists
  exists:0 < @[hcount; f; 0j];
  if[not exists; f set ()];
  hopen f
  };

/ Open log file (with proper initialization)
.tp.openLog:{[]
  if[not .tp.cfg.logEnabled; :()];
  
  / Create log directory
  system "mkdir -p ",.tp.cfg.logDir;
  
  / Store file path
  .tp.logFile:.tp.logFilePath[];
  
  / Initialize and open log file
  .tp.logHandle:.tp.initLog[.tp.logFile];
  
  / Get current message count (for recovery tracking)
  .tp.logCount:@[{-11!(-2;x)}; .tp.logFile; 0j];
  
  -1 "TP: Log file: ",string[.tp.logFile]," (",string[.tp.logCount]," existing chunks)";
  };

/ Close log file
.tp.closeLog:{[]
  if[not .tp.cfg.logEnabled; :()];
  if[not null .tp.logHandle; @[hclose; .tp.logHandle; {}]; .tp.logHandle:0N];
  -1 "TP: Log file closed";
  };

/ Write to log file
/ Format: enlist (`.u.upd; tableName; data)
/ This format is compatible with -11! replay which calls .z.ps -> value
.tp.log:{[tbl;data]
  if[not .tp.cfg.logEnabled; :()];
  / Skip health updates from logging (optional - remove if you want health in log)
  if[tbl = `health_feed_handler; :()];
  .tp.logHandle enlist (`.u.upd; tbl; data);
  .tp.logCount+:1;
  };

/ Rotate logs (call at end of day)
.tp.rotate:{[]
  -1 "TP: Rotating log file...";
  .tp.closeLog[];
  .tp.openLog[];
  };

/ Get current log status
.tp.logStatus:{[]
  ([] 
    file:enlist .tp.logFile;
    handle:enlist .tp.logHandle;
    chunks:enlist .tp.logCount;
    sizeMB:enlist @[hcount; .tp.logFile; 0j] % 1e6
  )
  };

/ -------------------------------------------------------
/ STANDARD tick.q Pub/Sub (u.q)
/ -------------------------------------------------------

/ Load the production-grade u.q pub/sub system
\l u.q

/ Initialize u.q - sets up .u.w dictionary for all tables
.u.init[];

/ -------------------------------------------------------
/ Update handling
/ -------------------------------------------------------

/ Convert kdb timestamp to nanoseconds since Unix epoch
.tp.tsToNs:{[ts] .tp.epochOffset + "j"$ts };

/ Core update function
/ Called by feed handler via .z.ps -> .u.upd
.u.upd:{[tbl;data]
  / Health updates don't get tpRecvTimeUtcNs added
  if[tbl = `health_feed_handler;
    tbl insert data;
    .u.pub[tbl;data];
    :();
  ];
  
  / Market data updates get TP timestamp
  tpRecvTs:.z.p;
  tpRecvTimeUtcNs:.tp.tsToNs[tpRecvTs];
  
  / Append tpRecvTimeUtcNs to the row
  data:data,tpRecvTimeUtcNs;
  
  / Log to disk (before insert for durability)
  .tp.log[tbl;data];
  
  / Insert locally
  tbl insert data;
  
  / Publish to subscribers
  .u.pub[tbl;data];
  };

/ -------------------------------------------------------
/ End-of-Day Handler
/ -------------------------------------------------------

.tp.endOfDay:{[]
  -1 "TP: End-of-day processing started...";
  
  / Log final count
  -1 "TP: Log chunks: ",string[.tp.logCount];
  
  / Broadcast EOD to all subscribers
  .u.end[.z.D];
  
  / Rotate log file (closes current, opens new for next day)
  .tp.rotate[];
  
  / Clear in-memory tables
  delete from `trade_binance;
  delete from `quote_binance;
  delete from `health_feed_handler;
  
  / Reset counter
  .tp.logCount:0j;
  
  -1 "TP: End-of-day processing complete";
  };

/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system "p ",string .tp.cfg.port;

-1 "=======================================================";
-1 "TP (Production Grade) starting on port ",string[.tp.cfg.port];
-1 "=======================================================";
-1 "Configuration:";
-1 "  Logging: ",$[.tp.cfg.logEnabled; "enabled"; "disabled"];
-1 "  Log directory: ",.tp.cfg.logDir;
-1 "  Log format: single file (standard tick.q)";

/ Open log file
.tp.openLog[];

-1 "";
-1 "Tables:";
-1 "  trade_binance: ",string[count cols trade_binance]," fields";
-1 "  quote_binance: ",string[count cols quote_binance]," fields (L5)";
-1 "  health_feed_handler: ",string[count cols health_feed_handler]," fields";
-1 "";
-1 "Query interface:";
-1 "  .tp.logStatus[]         / View log file status";
-1 "  .tp.endOfDay[]          / Trigger end-of-day processing";
-1 "";
-1 "TP ready - feed handlers can connect";
-1 "=======================================================";

/ -------------------------------------------------------
/ End
/ -------------------------------------------------------
