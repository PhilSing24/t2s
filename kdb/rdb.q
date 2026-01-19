/ rdb.q - Query-only Real-time Database
/ Subscribes to Chained TP (batched data)
/ For user queries - with memory management

/ -------------------------------------------------------
/ Configuration
/ -------------------------------------------------------

.rdb.cfg.port:5017;
.rdb.cfg.chainedTP:5014;

/ Memory management
.rdb.cfg.retentionMin:60;              / Keep 1 hour of data
.rdb.cfg.cleanupIntervalMs:30000;      / Cleanup every 30 seconds
.rdb.cfg.memWarnMB:1000;               / Warn if memory exceeds 1GB
.rdb.cfg.memCriticalMB:2000;           / Critical if memory exceeds 2GB

/ Connection resilience
.rdb.conn.handle:0N;                   / CTP connection handle
.rdb.conn.state:`disconnected;         / `disconnected`connecting`connected
.rdb.conn.lastAttempt:0Np;             / Last connection attempt time
.rdb.conn.retryCount:0;                / Consecutive failed attempts
.rdb.conn.cfg.baseDelayMs:1000;        / Initial retry delay (1 sec)
.rdb.conn.cfg.maxDelayMs:30000;        / Max retry delay (30 sec)
.rdb.conn.cfg.backoffMultiplier:1.5;   / Exponential backoff factor

system "g 0";

.rdb.epochOffset:neg"j"$1970.01.01D0;
.proc.startTime:.z.p;

/ Statistics
.rdb.stats.cleanupCount:0j;
.rdb.stats.rowsDeleted:0j;
.rdb.stats.lastCleanup:0Np;

/ -------------------------------------------------------
/ Table schema
/ -------------------------------------------------------

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
  rdbRecvTimeUtcNs:`long$()
  );

quote_binance:([]
  time:`timestamp$();
  sym:`symbol$();
  bidPrice1:`float$();
  bidPrice2:`float$();
  bidPrice3:`float$();
  bidPrice4:`float$();
  bidPrice5:`float$();
  bidQty1:`float$();
  bidQty2:`float$();
  bidQty3:`float$();
  bidQty4:`float$();
  bidQty5:`float$();
  askPrice1:`float$();
  askPrice2:`float$();
  askPrice3:`float$();
  askPrice4:`float$();
  askPrice5:`float$();
  askQty1:`float$();
  askQty2:`float$();
  askQty3:`float$();
  askQty4:`float$();
  askQty5:`float$();
  isValid:`boolean$();
  exchEventTimeMs:`long$();
  fhRecvTimeUtcNs:`long$();
  fhParseUs:`long$();
  fhSendUs:`long$();
  fhSeqNo:`long$();
  tpRecvTimeUtcNs:`long$();
  rdbRecvTimeUtcNs:`long$()
  );

/ -------------------------------------------------------
/ Connection Management (Resilient)
/ -------------------------------------------------------

.rdb.tsToNs:{[ts] .rdb.epochOffset+"j"$ts};

/ Calculate next retry delay with exponential backoff
.rdb.conn.getDelay:{[]
  delay:.rdb.conn.cfg.baseDelayMs * `long$.rdb.conn.cfg.backoffMultiplier xexp .rdb.conn.retryCount;
  delay & .rdb.conn.cfg.maxDelayMs  / Cap at max
  };

/ Check if enough time has passed since last attempt
.rdb.conn.canRetry:{[]
  if[null .rdb.conn.lastAttempt; :1b];
  elapsed:`long$(.z.p - .rdb.conn.lastAttempt) % 1000000;  / ms
  elapsed >= .rdb.conn.getDelay[]
  };

/ Main connection function - NEVER THROWS
.rdb.connect:{[]
  / Guard: already connected
  if[not null .rdb.conn.handle; :1b];
  
  / Guard: backoff not elapsed
  if[not .rdb.conn.canRetry[]; :0b];
  
  .rdb.conn.state:`connecting;
  .rdb.conn.lastAttempt:.z.p;
  
  -1 "RDB: Connecting to Chained TP on port ",string[.rdb.cfg.chainedTP],
     " (attempt ",string[.rdb.conn.retryCount + 1],")...";
  
  / Protected connection attempt
  h:@[hopen; `$"::",string[.rdb.cfg.chainedTP]; {[err] -1 "RDB: Connection failed - ",err; 0N}];
  
  if[null h;
    .rdb.conn.retryCount+:1;
    .rdb.conn.state:`disconnected;
    nextDelay:.rdb.conn.getDelay[];
    -1 "RDB: Will retry in ",string[nextDelay],"ms";
    :0b
  ];
  
  / Connection successful - now subscribe
  subResult:@[{[h]
    -1 "RDB: Subscribing to trade_binance...";
    res:h(`pubsub.subscribe;`trade_binance;`);
    -1 "RDB: Subscribed to trade_binance";
    -1 "RDB: Subscribing to quote_binance...";
    res:h(`pubsub.subscribe;`quote_binance;`);
    -1 "RDB: Subscribed to quote_binance";
    1b
  }; h; {[err] -1 "RDB: Subscription failed - ",err; 0b}];
  
  if[not subResult;
    @[hclose; h; {}];  / Clean up failed connection
    .rdb.conn.retryCount+:1;
    .rdb.conn.state:`disconnected;
    :0b
  ];
  
  / Success - update state
  .rdb.conn.handle:h;
  .rdb.conn.state:`connected;
  .rdb.conn.retryCount:0;
  -1 "RDB: Connected successfully (handle ",string[h],")";
  1b
  };

/ Disconnect handler
.z.pc:{[h]
  if[h = .rdb.conn.handle;
    -1 "RDB: Chained TP connection lost (handle ",string[h],")";
    .rdb.conn.handle:0N;
    .rdb.conn.state:`disconnected;
    .rdb.conn.retryCount:0;  / Reset backoff on disconnect
    -1 "RDB: Will attempt reconnection on next timer tick";
  ];
  };

/ -------------------------------------------------------
/ Update handling (receive from Chained TP)
/ -------------------------------------------------------

upd:{[tbl;data]
  rdbRecvTimeUtcNs:.rdb.tsToNs[.z.p];
  $[98h=type data;
    / Table (batch): add column and insert
    tbl insert update rdbRecvTimeUtcNs:rdbRecvTimeUtcNs from data;
    / Single row (list): append timestamp and insert
    tbl insert data,rdbRecvTimeUtcNs
  ];
  };

/ -------------------------------------------------------
/ Memory Management
/ -------------------------------------------------------

/ Cleanup old data based on retention period
.rdb.cleanup:{[]
  cutoff:.z.p - `long$.rdb.cfg.retentionMin * 60 * 1000000000;
  
  / Count rows before deletion
  tradesBefore:count trade_binance;
  quotesBefore:count quote_binance;
  
  / Delete old rows
  delete from `trade_binance where time < cutoff;
  delete from `quote_binance where time < cutoff;
  
  / Track statistics
  deleted:(tradesBefore - count trade_binance) + (quotesBefore - count quote_binance);
  if[deleted > 0;
    .rdb.stats.rowsDeleted+:deleted;
  ];
  
  .rdb.stats.cleanupCount+:1;
  .rdb.stats.lastCleanup:.z.p;
  };

/ Check memory usage and warn if high
.rdb.checkMem:{[]
  memMB:(`long$.Q.w[][`used]) % 1000000;
  
  if[memMB > .rdb.cfg.memCriticalMB;
    -1 "RDB: CRITICAL - Memory usage: ",string[memMB],"MB - forcing aggressive cleanup";
    / Emergency cleanup - keep only last 10 minutes
    emergencyCutoff:.z.p - 0D00:10:00;
    delete from `trade_binance where time < emergencyCutoff;
    delete from `quote_binance where time < emergencyCutoff;
    .Q.gc[];  / Force garbage collection
    :();
  ];
  
  if[memMB > .rdb.cfg.memWarnMB;
    -1 "RDB: WARNING - Memory usage high: ",string[memMB],"MB (threshold: ",string[.rdb.cfg.memWarnMB],"MB)";
  ];
  };

/ Get memory statistics
.rdb.memStats:{[]
  mem:.Q.w[];
  `usedMB`peakMB`mappedMB`heapMB!(
    (`long$mem`used) % 1000000;
    (`long$mem`peak) % 1000000;
    (`long$mem`mmap) % 1000000;
    (`long$mem`heap) % 1000000
  )
  };

/ -------------------------------------------------------
/ End-of-Day
/ -------------------------------------------------------

endofday:{[]
  -1 "RDB: EOD - trades:",string[count trade_binance]," quotes:",string[count quote_binance];
  delete from`trade_binance;
  delete from`quote_binance;
  .rdb.stats.rowsDeleted:0j;
  .rdb.stats.cleanupCount:0j;
  -1 "RDB: Tables cleared";
  };

/ -------------------------------------------------------
/ Health Check
/ -------------------------------------------------------

/ Standardized health check (consistent across all processes)
.health:{[]
  memMB:(`long$.Q.w[][`used]) % 1000000;
  
  / Determine status
  st:$[.rdb.conn.state <> `connected; `disconnected;
       memMB > .rdb.cfg.memCriticalMB; `critical;
       memMB > .rdb.cfg.memWarnMB; `degraded;
       `ok];
  
  `process`port`uptime`status`connState`memMB`trades`quotes`rowsDeleted`cleanups!(
    `rdb;
    .rdb.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    .rdb.conn.state;
    memMB;
    count trade_binance;
    count quote_binance;
    .rdb.stats.rowsDeleted;
    .rdb.stats.cleanupCount
  )
  };

/ -------------------------------------------------------
/ Query interface
/ -------------------------------------------------------

.rdb.status:{[]
  `port`chainedTP`connected`trades`quotes`symbols`lastTrade`lastQuote`retentionMin`memMB!
  (.rdb.cfg.port;.rdb.cfg.chainedTP;.rdb.conn.state = `connected;
   count trade_binance;count quote_binance;
   count distinct(exec distinct sym from trade_binance),(exec distinct sym from quote_binance);
   exec last time from trade_binance;exec last time from quote_binance;
   .rdb.cfg.retentionMin;
   (`long$.Q.w[][`used]) % 1000000)
  };

.rdb.tradeSummary:{[]
  select trades:count i,volume:sum qty,dollarVol:sum price*qty,
         minPx:min price,maxPx:max price,lastPx:last price,
         firstTime:first time,lastTime:last time
  by sym from trade_binance
  };

.rdb.quoteSummary:{[]
  select quotes:count i,valid:sum isValid,invalid:sum not isValid,
         avgSpread:avg askPrice1-bidPrice1,lastBid:last bidPrice1,lastAsk:last askPrice1,
         firstTime:first time,lastTime:last time
  by sym from quote_binance
  };

.rdb.lastTrades:{[n] select from(update rn:i by sym from trade_binance)where rn>=count[i]-n};

.rdb.lastQuotes:{[n] select from(update rn:i by sym from quote_binance)where rn>=count[i]-n};

/ Get data within a time range
.rdb.tradeRange:{[s;startTime;endTime]
  $[s ~ `;
    select from trade_binance where time within (startTime;endTime);
    select from trade_binance where sym = s, time within (startTime;endTime)
  ]
  };

.rdb.quoteRange:{[s;startTime;endTime]
  $[s ~ `;
    select from quote_binance where time within (startTime;endTime);
    select from quote_binance where sym = s, time within (startTime;endTime)
  ]
  };

/ -------------------------------------------------------
/ Timer - reconnect + cleanup + memory check
/ -------------------------------------------------------

.z.ts:{[]
  / Attempt reconnection if disconnected
  if[null .rdb.conn.handle; .rdb.connect[]];
  
  / Run cleanup
  .rdb.cleanup[];
  
  / Check memory usage
  .rdb.checkMem[];
  };

/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system "p ",string .rdb.cfg.port;

-1 "=======================================================";
-1 "RDB (Query-only) on port ",string[.rdb.cfg.port];
-1 "=======================================================";
-1 "Configuration:";
-1 "  Chained TP: ",string[.rdb.cfg.chainedTP];
-1 "  Retention: ",string[.rdb.cfg.retentionMin]," minutes";
-1 "  Cleanup interval: ",string[.rdb.cfg.cleanupIntervalMs],"ms";
-1 "  Memory warn: ",string[.rdb.cfg.memWarnMB],"MB";
-1 "  Memory critical: ",string[.rdb.cfg.memCriticalMB],"MB";
-1 "";
-1 "Connection Settings:";
-1 "  Base retry delay: ",string[.rdb.conn.cfg.baseDelayMs],"ms";
-1 "  Max retry delay: ",string[.rdb.conn.cfg.maxDelayMs],"ms";
-1 "";
-1 "Tables: trade_binance quote_binance";
-1 "";

/ Attempt initial connection (non-blocking)
connected:.rdb.connect[];

/ Start timer for reconnect + cleanup + memory check
system "t ",string .rdb.cfg.cleanupIntervalMs;

-1 "Query Interface:";
-1 "  .health[]              / Standardized health check";
-1 "  .rdb.status[]          / Full status";
-1 "  .rdb.tradeSummary[]    / Trade summary by symbol";
-1 "  .rdb.quoteSummary[]    / Quote summary by symbol";
-1 "  .rdb.lastTrades[10]    / Last 10 trades per symbol";
-1 "  .rdb.lastQuotes[10]    / Last 10 quotes per symbol";
-1 "  .rdb.tradeRange[`BTCUSDT; startTime; endTime]  / Trades in range";
-1 "  .rdb.quoteRange[`; startTime; endTime]         / All quotes in range";
-1 "";
-1 "Memory Interface:";
-1 "  .rdb.memStats[]        / Memory statistics";
-1 "  .rdb.cleanup[]         / Manual cleanup";
-1 "  .rdb.stats             / Cleanup statistics";
-1 "";

$[connected; -1 "RDB: Ready and processing"; -1 "RDB: Started in DEGRADED mode - waiting for CTP connection"];
-1 "=======================================================";
