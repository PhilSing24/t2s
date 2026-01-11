/ rdb.q - Real-Time Database (Production Grade with u.q)
/ With log replay capability for crash recovery

/ -----------------------------------------------------------------------------
/ Configuration
/ -----------------------------------------------------------------------------

.rdb.cfg.port:5011;
.rdb.cfg.tpPort:5010;
.rdb.cfg.logDir:"logs";

/ Epoch offset: nanoseconds between 2000.01.01 and 1970.01.01
.rdb.epochOffset:946684800000000000j;

/ to store start time of the process
.proc.startTime:.z.p;

/ -----------------------------------------------------------------------------
/ Table Schema
/ -----------------------------------------------------------------------------

/ Trade table - 14 fields (includes rdbApplyTimeUtcNs)
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
  tpRecvTimeUtcNs:`long$();
  rdbApplyTimeUtcNs:`long$()
  );

/ Quote table - L5 depth (30 fields: includes rdbApplyTimeUtcNs)
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
  tpRecvTimeUtcNs:`long$();
  rdbApplyTimeUtcNs:`long$()
  );

/ -----------------------------------------------------------------------------
/ Utility Functions
/ -----------------------------------------------------------------------------

/ Convert kdb timestamp to nanoseconds since Unix epoch
.rdb.tsToNs:{[ts]
  .rdb.epochOffset + "j"$ts - 2000.01.01D0
  };

/ -----------------------------------------------------------------------------
/ Update Handler (called by TP via pub/sub and during replay)
/ -----------------------------------------------------------------------------

.u.upd:{[tbl;data]
  / Always add rdbApplyTimeUtcNs - both live and replay need this
  / (TP log contains 13 fields, RDB table has 14 fields)
  rdbApplyTs:.z.p;
  rdbApplyTimeUtcNs:.rdb.tsToNs[rdbApplyTs];
  data:data,rdbApplyTimeUtcNs;
  / Insert into table
  tbl insert data;
  };

/ Create root-level upd function for u.q compatibility
upd:.u.upd;

/ -----------------------------------------------------------------------------
/ Log Replay
/ -----------------------------------------------------------------------------

/ Build log file path for a given date
/ @param d - date
.rdb.logFile:{[d]
  hsym `$(.rdb.cfg.logDir,"/",string[d],".log")
  };

/ Check if log file exists and has content
/ @param f - log file path (hsym)
/ @return 1b if exists and non-empty, 0b otherwise
.rdb.logExists:{[f]
  sz:@[hcount; f; -1j];
  sz > 0
  };

/ Get chunk count from log file (also validates file)
/ @param f - log file path (hsym)
/ @return (chunks; filesize) or (chunks; validBytes) if corrupt
.rdb.logInfo:{[f]
  if[not .rdb.logExists[f]; :(0j; 0j)];
  / -11! expects file handle symbol (with :)
  info:@[-11!; (-2;f); {-1 "RDB: Error reading log: ",x; (0j;0j)}];
  / If single long returned, file is valid
  / If two values returned, file is corrupt (chunks; validBytes)
  $[1 = count info; (info; hcount f); info]
  };

/ Replay a log file
/ @param f - log file path (hsym)
/ @return number of chunks replayed
.rdb.replayFile:{[f]
  if[not .rdb.logExists[f];
    -1 "RDB: Log file not found: ",string[f];
    :0j
  ];
  
  -1 "RDB: Replaying from ",string[f];
  
  / -11! expects hsym format (`:path/to/file)
  / Use protected evaluation with error handling
  replayed:.[{-11!x}; enlist f; {[e] -1 "RDB: Replay error - ",e; 0j}];
  
  -1 "RDB: Replayed ",string[replayed]," chunks";
  replayed
  };

/ Replay log for a given date
/ @param d - date (default today)
/ @return total chunks replayed
.rdb.replay:{[d]
  if[d ~ (::); d:.z.D];
  
  -1 "RDB: Starting replay for ",string[d];
  
  / Replay single log file (contains both trades and quotes in chronological order)
  logFile:.rdb.logFile[d];
  chunks:.rdb.replayFile[logFile];
  
  -1 "RDB: Replay complete - ",string[chunks]," chunks";
  -1 "RDB: Tables now have ",string[count trade_binance]," trades, ",string[count quote_binance]," quotes";
  
  chunks
  };

/ -----------------------------------------------------------------------------
/ End-of-Day Handler
/ -----------------------------------------------------------------------------

/ Called by TP at end of day
/ In production, this would flush data to HDB and reset tables
.u.end:{[date]
  -1 "RDB: EOD received for ",string[date];
  
  / Log final counts
  -1 "RDB: Trades received today: ",string[count trade_binance];
  -1 "RDB: Quotes received today: ",string[count quote_binance];
  
  / In production, you would:
  / 1. Save trade_binance and quote_binance to HDB partitions
  / 2. Clear in-memory tables
  
  / Clear tables for new day
  delete from `trade_binance;
  delete from `quote_binance;
  
  -1 "RDB: EOD processing complete - tables cleared";
  };

/ -----------------------------------------------------------------------------
/ Subscription to Tickerplant
/ -----------------------------------------------------------------------------

.rdb.connect:{[]
  -1 "RDB: Connecting to tickerplant on port ",string[.rdb.cfg.tpPort],"...";
  h:@[hopen; `$"::",string .rdb.cfg.tpPort; {-1 "RDB: Failed to connect to TP: ",x; 0N}];
  if[null h; '"Cannot connect to tickerplant"];
  
  / Subscribe to trade_binance
  res:h (`.u.sub; `trade_binance; `);
  -1 "RDB: Subscribed to ",string[first res];
  
  / Subscribe to quote_binance
  res:h (`.u.sub; `quote_binance; `);
  -1 "RDB: Subscribed to ",string[first res];
  
  / Store handle
  .rdb.tpHandle:h;
  };

/ -----------------------------------------------------------------------------
/ Startup
/ -----------------------------------------------------------------------------

system "p ",string .rdb.cfg.port;

-1 "=======================================================";
-1 "RDB (Production Grade) starting on port ",string[.rdb.cfg.port];
-1 "=======================================================";

/ Replay today's log before subscribing to TP
/ This ensures we recover any data from earlier in the day
-1 "";
-1 "Checking for log to replay...";
logExists:.rdb.logExists[.rdb.logFile[.z.D]];

if[logExists; -1 "RDB: Found existing log for today - replaying..."; .rdb.replay[.z.D]];
if[not logExists; -1 "RDB: No existing log for today"];

/ Connect and subscribe to TP for live updates
-1 "";
.rdb.connect[];

-1 "";
-1 "Tables:";
-1 "  trade_binance: ",string[count trade_binance]," rows (",string[count cols trade_binance]," fields)";
-1 "  quote_binance: ",string[count quote_binance]," rows (",string[count cols quote_binance]," fields)";
-1 "";
-1 "Query interface:";
-1 "  .rdb.replay[.z.D]              / Replay today's log";
-1 "  .rdb.replay[2026.01.06]        / Replay specific date";
-1 "  .rdb.logInfo[`:/path/to/log]   / Check log file info";
-1 "";
-1 "RDB ready";
-1 "=======================================================";

/ -----------------------------------------------------------------------------
/ End
/ -----------------------------------------------------------------------------
