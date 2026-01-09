/ rte.q - Real-Time Engine (Production Grade with u.q)
/ VWAP (bucketed aggregation) + Imbalance (latest snapshot)
/ With log replay capability for crash recovery

/ =============================================================================
/ Configuration
/ =============================================================================

.rte.cfg.port:5012;
.rte.cfg.tpPort:5010;
.rte.cfg.logDir:"logs";
.rte.cfg.defaultWindowMin:5;           / Default VWAP window in minutes
.rte.cfg.bucketSec:1;                   / Bucket size in seconds
.rte.cfg.retentionMin:10;               / Keep 10 minutes of buckets
.rte.cfg.cleanupIntervalMs:5000;        / Cleanup every 5 seconds

/ Derived configuration
.rte.cfg.bucketNs:.rte.cfg.bucketSec * 1000000000j;
.rte.cfg.retentionNs:.rte.cfg.retentionMin * 60 * 1000000000j;

/ Store start time of the process
.proc.startTime:.z.p;

/ =============================================================================
/ VWAP State - Bucketed Aggregation (Production Grade)
/ =============================================================================

/ Time-bucketed VWAP data (1-second buckets)
/ Each bucket contains aggregated trade data for that second
vwapBuckets:([] 
  sym:`symbol$();           / Symbol
  bucket:`timestamp$();     / Bucket start time (1-second granularity)
  sumPxQty:`float$();       / Sum of (price * quantity)
  sumQty:`float$();         / Sum of quantity
  cnt:`long$()              / Number of trades in bucket
  );

/ Key by sym and bucket for O(1) upsert
`sym`bucket xkey `vwapBuckets;

/ Add trade to current bucket
.rte.vwap.add:{[s;time;price;qty]
  / Calculate bucket (floor to nearest second)
  bucketTime:`timestamp$.rte.cfg.bucketNs * `long$time div .rte.cfg.bucketNs;
  
  / Upsert into bucket (O(1) operation)
  `vwapBuckets upsert (s; bucketTime; price * qty; qty; 1j);
  };

/ Calculate VWAP for a symbol over a time window
.rte.vwap.calc:{[s;windowMin]
  / Calculate cutoff time
  windowNs:windowMin * 60 * 1000000000j;
  cutoff:.z.p - windowNs;
  
  / Query buckets within window - unkey the result with 0!
  buckets:0!select from vwapBuckets where sym in enlist s, bucket >= cutoff;
  
  / Return empty result if no data
  if[0 = count buckets;
    :([] sym:enlist s; vwap:enlist 0n; totalQty:enlist 0f; tradeCount:enlist 0j; isValid:enlist 0b)];
  
  / Aggregate across buckets
  totalPxQty:sum buckets`sumPxQty;
  totalQty:sum buckets`sumQty;
  tradeCount:sum buckets`cnt;
  
  / Calculate VWAP
  vwap:$[totalQty > 0f; totalPxQty % totalQty; 0n];
  
  / Return result as single-row table
  ([] sym:enlist s; vwap:enlist vwap; totalQty:enlist totalQty; tradeCount:enlist tradeCount; isValid:enlist tradeCount > 10)
  };

/ Cleanup old buckets (called by timer)
.rte.vwap.cleanup:{[]
  cutoff:.z.p - .rte.cfg.retentionNs;
  delete from `vwapBuckets where bucket < cutoff;
  };

/ =============================================================================
/ Imbalance State - L5 (Latest snapshot per symbol)
/ =============================================================================

/ Latest order book imbalance per symbol
.rte.imb.latest:()!();

/ Update imbalance for a symbol
.rte.imb.update:{[s;bidDepth;askDepth;time]
  total:bidDepth + askDepth;
  imb:$[total > 0f; (bidDepth - askDepth) % total; 0n];
  .rte.imb.latest[s]:`bidDepth`askDepth`imbalance`time!(bidDepth; askDepth; imb; time);
  };

/ =============================================================================
/ Query Interface
/ =============================================================================

/ Get VWAP for a symbol over N minutes
/ Usage: .rte.getVwap[`BTCUSDT; 5]
.rte.getVwap:{[s;m]
  .rte.vwap.calc[s; m]
  };

/ Get latest order book imbalance for a symbol
/ Usage: .rte.getImbalance[`BTCUSDT]
.rte.getImbalance:{[s]
  / Return result as single-row table
  if[not s in key .rte.imb.latest;
    :([] sym:enlist s; bidDepth:enlist 0n; askDepth:enlist 0n; imbalance:enlist 0n; time:enlist 0Np)];
  
  data:.rte.imb.latest[s];
  ([] sym:enlist s; bidDepth:enlist data`bidDepth; askDepth:enlist data`askDepth; 
      imbalance:enlist data`imbalance; time:enlist data`time)
  };

/ =============================================================================
/ Update Handler (Called by Tickerplant and during replay)
/ =============================================================================

.u.upd:{[tbl;data]
  / Handle trade updates
  if[tbl = `trade_binance;
    / data format: (time; sym; tradeId; price; qty; ...)
    time:data 0;
    s:data 1;
    price:data 3;
    qty:data 4;
    .rte.vwap.add[s; time; price; qty];
  ];

  / Handle quote updates
  if[tbl = `quote_binance;
    / data format: (time; sym; ...; bid1Qty through bid5Qty; ...; ask1Qty through ask5Qty)
    time:data 0;
    s:data 1;
    
    / Sum bid depth (L1-L5) - indices 7-11
    bidDepth:(data 7) + (data 8) + (data 9) + (data 10) + (data 11);
    
    / Sum ask depth (L1-L5) - indices 17-21
    askDepth:(data 17) + (data 18) + (data 19) + (data 20) + (data 21);
    
    .rte.imb.update[s; bidDepth; askDepth; time];
  ];
  };

/ Create root-level upd function for u.q compatibility
upd:.u.upd;

/ =============================================================================
/ Log Replay
/ =============================================================================

/ Build log file path for a given date and type
/ @param d - date
/ @param typ - `trade or `quote
.rte.logFile:{[d;typ]
  hsym `$(.rte.cfg.logDir,"/",string[d],".",string[typ],".log")
  };

/ Check if log file exists and has content
/ @param f - log file path (hsym)
/ @return 1b if exists and non-empty, 0b otherwise
.rte.logExists:{[f]
  sz:@[hcount; f; -1j];
  sz > 0
  };

/ Get chunk count from log file (also validates file)
/ @param f - log file path (hsym)
/ @return (chunks; filesize) or (chunks; validBytes) if corrupt
.rte.logInfo:{[f]
  if[not .rte.logExists[f]; :(0j; 0j)];
  info:-11!(-2;f);
  $[1 = count info; (info; hcount f); info]
  };

/ Replay a single log file
/ @param f - log file path (hsym)
/ @return number of chunks replayed
.rte.replayFile:{[f]
  if[not .rte.logExists[f];
    -1 "RTE: Log file not found: ",string[f];
    :0j
  ];
  
  -1 "RTE: Replaying from ",string[f];
  
  / Replay using streaming execute with error handling
  replayed:.[{-11!x}; enlist f; {[e] -1 "RTE: Replay error - ",e; 0j}];
  
  -1 "RTE: Replayed ",string[replayed]," chunks";
  replayed
  };

/ Replay all logs for a given date
/ @param d - date (default today)
/ @return total chunks replayed
.rte.replay:{[d]
  if[d ~ (::); d:.z.D];
  
  -1 "RTE: Starting replay for ",string[d];
  
  / Replay trade log (for VWAP)
  tradeLog:.rte.logFile[d;`trade];
  tradeChunks:.rte.replayFile[tradeLog];
  
  / Replay quote log (for imbalance)
  quoteLog:.rte.logFile[d;`quote];
  quoteChunks:.rte.replayFile[quoteLog];
  
  total:tradeChunks + quoteChunks;
  -1 "RTE: Replay complete - ",string[tradeChunks]," trades, ",string[quoteChunks]," quotes";
  -1 "RTE: VWAP buckets: ",string[count vwapBuckets],", Imbalance symbols: ",string[count .rte.imb.latest];
  
  total
  };

/ =============================================================================
/ End-of-Day Handler
/ =============================================================================

/ Called by TP at end of day
.u.end:{[date]
  -1 "RTE: EOD received for ",string[date];
  
  / Clear VWAP buckets (start fresh for new day)
  delete from `vwapBuckets;
  
  / Clear imbalance state
  .rte.imb.latest:()!();
  
  -1 "RTE: EOD processing complete - state cleared";
  };

/ =============================================================================
/ Subscription to Tickerplant
/ =============================================================================

.rte.connect:{[]
  -1 "RTE: Connecting to tickerplant on port ",string[.rte.cfg.tpPort],"...";
  h:@[hopen; `$"::",string .rte.cfg.tpPort; {-1 "RTE: Failed to connect to TP: ",x; 0N}];
  if[null h; '"Cannot connect to TP"];
  
  / Subscribe to trades (for VWAP)
  res:h (`.u.sub; `trade_binance; `);
  -1 "RTE: Subscribed to ",string[first res];
  
  / Subscribe to quotes (for imbalance)
  res:h (`.u.sub; `quote_binance; `);
  -1 "RTE: Subscribed to ",string[first res];
  
  .rte.tpHandle:h;
  };

/ =============================================================================
/ Periodic Cleanup Timer
/ =============================================================================

/ Timer function to clean up old buckets
.z.ts:{[]
  .rte.vwap.cleanup[];
  };

/ =============================================================================
/ Startup
/ =============================================================================

system "p ",string .rte.cfg.port;

-1 "=======================================================";
-1 "RTE (Production Grade) starting on port ",string[.rte.cfg.port];
-1 "=======================================================";
-1 "Configuration:";
-1 "  Bucket size: ",string[.rte.cfg.bucketSec],"s";
-1 "  Retention: ",string[.rte.cfg.retentionMin]," minutes";
-1 "  Cleanup interval: ",string[.rte.cfg.cleanupIntervalMs],"ms";

/ Replay today's logs before subscribing to TP
/ Note: RTE only keeps recent data, so old data will be cleaned up
-1 "";
-1 "Checking for logs to replay...";
tradeLogExists:.rte.logExists[.rte.logFile[.z.D;`trade]];
quoteLogExists:.rte.logExists[.rte.logFile[.z.D;`quote]];

if[tradeLogExists | quoteLogExists; -1 "RTE: Found existing logs for today - replaying..."; .rte.replay[.z.D]; .rte.vwap.cleanup[]; -1 "RTE: Cleanup applied - keeping only last ",string[.rte.cfg.retentionMin]," minutes"];
if[not tradeLogExists | quoteLogExists; -1 "RTE: No existing logs for today"];

/ Connect and subscribe to TP for live updates
-1 "";
.rte.connect[];

/ Start cleanup timer
system "t ",string .rte.cfg.cleanupIntervalMs;

-1 "";
-1 "State:";
-1 "  VWAP buckets: ",string[count vwapBuckets];
-1 "  Imbalance symbols: ",string[count .rte.imb.latest];
-1 "";
-1 "Query interface:";
-1 "  .rte.getVwap[`BTCUSDT; 5]      / 5-minute VWAP";
-1 "  .rte.getVwap[`BTCUSDT; 1]      / 1-minute VWAP";
-1 "  .rte.getImbalance[`BTCUSDT]    / Latest imbalance";
-1 "  .rte.replay[.z.D]              / Replay today's logs";
-1 "";
-1 "RTE ready";
-1 "=======================================================";
