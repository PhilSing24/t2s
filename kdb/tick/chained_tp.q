/ chained_tp.q - Chained Tickerplant with batched publishing
/ Subscribes to primary TP, batches updates, publishes to downstream
/ Also receives positions from SIG and forwards immediately
/ No logging (primary TP handles durability)

/ -------------------------------------------------------
/ Configuration
/ -------------------------------------------------------

.ctp.cfg.port:5014;
.ctp.cfg.primaryTP:5010;
.ctp.cfg.batchMs:1000;          / 1 second batching

/ Connection resilience
.ctp.conn.handle:0N;                   / TP connection handle
.ctp.conn.state:`disconnected;         / `disconnected`connecting`connected
.ctp.conn.lastAttempt:0Np;             / Last connection attempt time
.ctp.conn.retryCount:0;                / Consecutive failed attempts
.ctp.conn.cfg.baseDelayMs:1000;        / Initial retry delay (1 sec)
.ctp.conn.cfg.maxDelayMs:30000;        / Max retry delay (30 sec)
.ctp.conn.cfg.backoffMultiplier:1.5;   / Exponential backoff factor

system "g 0";

.proc.startTime:.z.p;

/ -------------------------------------------------------
/ Table schemas (loaded from shared definition)
/ Must exist before pubsub init.
/ CTP forwards TP's data, so trade and quote include tpRecvTimeUtcNs.
/ -------------------------------------------------------

\l ../schemas.q

trade_binance:.schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo];
trade_binance_fut:.schema.extend[.schema.aggTrade; `tpRecvTimeUtcNs`tpSeqNo];
quote_binance:.schema.extend[.schema.quote; `tpRecvTimeUtcNs`tpSeqNo];

/ Health metrics from feed handlers (forwarded immediately, not batched)
health_feed_handler:.schema.health;

/ Positions from SIG (forwarded immediately, not batched - CTP-specific table)
positions:([]
  time:`timestamp$();
  sym:`symbol$();
  side:`int$();
  qty:`float$();
  tradedPrice:`float$()
  );

/ -------------------------------------------------------
/ Pub/Sub - KDB-X module (must be named 'pubsub' for IPC)
/ -------------------------------------------------------

pubsub:use`di.pubsub

pubsub.init[]

/ -------------------------------------------------------
/ Batch buffers
/ -------------------------------------------------------

.ctp.buf.trade:trade_binance;
.ctp.buf.aggTrade:trade_binance_fut;
.ctp.buf.quote:quote_binance;

.ctp.stats.flushCount:0j;
.ctp.stats.tradeCount:0j;
.ctp.stats.aggTradeCount:0j;
.ctp.stats.quoteCount:0j;
.ctp.stats.healthCount:0j;
.ctp.stats.positionsCount:0j;
.ctp.stats.lastFlush:.z.p;

/ -------------------------------------------------------
/ Connection Management (Resilient)
/ -------------------------------------------------------

/ Calculate next retry delay with exponential backoff
.ctp.conn.getDelay:{[]
  delay:.ctp.conn.cfg.baseDelayMs * `long$.ctp.conn.cfg.backoffMultiplier xexp .ctp.conn.retryCount;
  delay & .ctp.conn.cfg.maxDelayMs  / Cap at max
  };

/ Check if enough time has passed since last attempt
.ctp.conn.canRetry:{[]
  if[null .ctp.conn.lastAttempt; :1b];
  elapsed:`long$(.z.p - .ctp.conn.lastAttempt) % 1000000;  / ms
  elapsed >= .ctp.conn.getDelay[]
  };

/ Main connection function - NEVER THROWS
.ctp.connect:{[]
  / Guard: already connected
  if[not null .ctp.conn.handle; :1b];
  
  / Guard: backoff not elapsed
  if[not .ctp.conn.canRetry[]; :0b];
  
  .ctp.conn.state:`connecting;
  .ctp.conn.lastAttempt:.z.p;
  
  -1 "CTP: Connecting to primary TP on port ",string[.ctp.cfg.primaryTP],
     " (attempt ",string[.ctp.conn.retryCount + 1],")...";
  
  / Protected connection attempt
  h:@[hopen; `$"::",string[.ctp.cfg.primaryTP]; {[err] -1 "CTP: Connection failed - ",err; 0N}];
  
  if[null h;
    .ctp.conn.retryCount+:1;
    .ctp.conn.state:`disconnected;
    nextDelay:.ctp.conn.getDelay[];
    -1 "CTP: Will retry in ",string[nextDelay],"ms";
    :0b
  ];
  
  / Connection successful - now subscribe
  subResult:@[{[h]
    -1 "CTP: Subscribing to trade_binance...";
    res:h(`pubsub.subscribe;`trade_binance;`);
    -1 "CTP: Subscribed to trade_binance";

    -1 "CTP: Subscribing to trade_binance_fut...";
    res:h(`pubsub.subscribe;`trade_binance_fut;`);
    -1 "CTP: Subscribed to trade_binance_fut";

    -1 "CTP: Subscribing to quote_binance...";
    res:h(`pubsub.subscribe;`quote_binance;`);
    -1 "CTP: Subscribed to quote_binance";
    
    -1 "CTP: Subscribing to health_feed_handler...";
    res:h(`pubsub.subscribe;`health_feed_handler;`);
    -1 "CTP: Subscribed to health_feed_handler";
    1b
  }; h; {[err] -1 "CTP: Subscription failed - ",err; 0b}];
  
  if[not subResult;
    @[hclose; h; {}];  / Clean up failed connection
    .ctp.conn.retryCount+:1;
    .ctp.conn.state:`disconnected;
    :0b
  ];
  
  / Success - update state
  .ctp.conn.handle:h;
  .ctp.conn.state:`connected;
  .ctp.conn.retryCount:0;
  -1 "CTP: Connected successfully (handle ",string[h],")";
  1b
  };

/ Handle disconnect from primary TP and downstream subscribers
.z.pc:{[h]
  / Check if primary TP disconnected
  if[h = .ctp.conn.handle;
    -1 "CTP: Primary TP disconnected (handle ",string[h],")";
    .ctp.conn.handle:0N;
    .ctp.conn.state:`disconnected;
    .ctp.conn.retryCount:0;  / Reset backoff on disconnect
    -1 "CTP: Will attempt reconnection on next timer tick";
  ];
  / Clean up downstream subscribers
  pubsub.closesub[h];
  };

/ -------------------------------------------------------
/ Update handling (receive from primary TP and SIG)
/ -------------------------------------------------------

upd:{[tbl;data]
  / Counter logic: if data is a table (type 98h, e.g. replayed batch),
  / count rows. Otherwise it's a single row (a generic list, type 0h),
  / count as 1. The previous check used 0h which incorrectly matched
  / single rows and inflated counters by the number of fields per row.
  $[tbl=`trade_binance;
    [`.ctp.buf.trade insert data;.ctp.stats.tradeCount+:$[98h=type data;count data;1]];
    tbl=`trade_binance_fut;
    [`.ctp.buf.aggTrade insert data;.ctp.stats.aggTradeCount+:$[98h=type data;count data;1]];
    tbl=`quote_binance;
    [`.ctp.buf.quote insert data;.ctp.stats.quoteCount+:$[98h=type data;count data;1]];
    tbl=`health_feed_handler;
    [pubsub.publish[`health_feed_handler;data];.ctp.stats.healthCount+:1];
    tbl=`positions;
    [pubsub.publish[`positions;data];.ctp.stats.positionsCount+:1;
     -1 "CTP: Position received and forwarded - ",string[data 1]];
    ()
  ];
  };

/ -------------------------------------------------------
/ Batch flush (publish to downstream)
/ -------------------------------------------------------

.ctp.flush:{[]
  if[count .ctp.buf.trade;
    pubsub.publish[`trade_binance;.ctp.buf.trade];
    .ctp.buf.trade:0#.ctp.buf.trade;
  ];
  if[count .ctp.buf.aggTrade;
    pubsub.publish[`trade_binance_fut;.ctp.buf.aggTrade];
    .ctp.buf.aggTrade:0#.ctp.buf.aggTrade;
  ];
  if[count .ctp.buf.quote;
    pubsub.publish[`quote_binance;.ctp.buf.quote];
    .ctp.buf.quote:0#.ctp.buf.quote;
  ];
  .ctp.stats.flushCount+:1;
  .ctp.stats.lastFlush:.z.p;
  };

/ -------------------------------------------------------
/ Health Check
/ -------------------------------------------------------

.health:{[]
  / Determine status based on connection state
  st:$[.ctp.conn.state = `connected; `ok;
       .ctp.conn.state = `connecting; `degraded;
       `disconnected];
  
  `process`port`uptime`status`connState`retryCount`memMB`trades`aggTrades`quotes`health`positions`flushes`bufTrades`bufAggTrades`bufQuotes!(
    `ctp;
    .ctp.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    .ctp.conn.state;
    .ctp.conn.retryCount;
    (`long$.Q.w[][`used]) % 1000000;
    .ctp.stats.tradeCount;
    .ctp.stats.aggTradeCount;
    .ctp.stats.quoteCount;
    .ctp.stats.healthCount;
    .ctp.stats.positionsCount;
    .ctp.stats.flushCount;
    count .ctp.buf.trade;
    count .ctp.buf.aggTrade;
    count .ctp.buf.quote
  )
  };

/ -------------------------------------------------------
/ Timer - batch flush + reconnect
/ -------------------------------------------------------

.z.ts:{[]
  / Attempt reconnection if disconnected
  if[null .ctp.conn.handle; .ctp.connect[]];
  / Flush batched data
  .ctp.flush[];
  };

/ -------------------------------------------------------
/ End-of-Day
/ -------------------------------------------------------

endofday:{[]
  -1 "CTP: EOD - flushing...";
  .ctp.flush[];
  pubsub.callendofday[];
  delete from`trade_binance;
  delete from`trade_binance_fut;
  delete from`quote_binance;
  delete from`positions;
  .ctp.stats.tradeCount:0j;
  .ctp.stats.aggTradeCount:0j;
  .ctp.stats.quoteCount:0j;
  .ctp.stats.healthCount:0j;
  .ctp.stats.positionsCount:0j;
  .ctp.stats.flushCount:0j;
  -1 "CTP: EOD complete";
  };

/ -------------------------------------------------------
/ Query interface
/ -------------------------------------------------------

.ctp.status:{[]
  `port`primaryTP`batchMs`connected`connState`retryCount`flushes`trades`aggTrades`quotes`health`positions`lastFlush`bufTrades`bufAggTrades`bufQuotes!
  (.ctp.cfg.port;.ctp.cfg.primaryTP;.ctp.cfg.batchMs;.ctp.conn.state = `connected;
   .ctp.conn.state;.ctp.conn.retryCount;
   .ctp.stats.flushCount;.ctp.stats.tradeCount;.ctp.stats.aggTradeCount;.ctp.stats.quoteCount;.ctp.stats.healthCount;.ctp.stats.positionsCount;
   .ctp.stats.lastFlush;count .ctp.buf.trade;count .ctp.buf.aggTrade;count .ctp.buf.quote)
  };

/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system "p ",string .ctp.cfg.port;

-1 "=======================================================";
-1 "Chained TP (KDB-X module) on port ",string[.ctp.cfg.port];
-1 "=======================================================";
-1 "Configuration:";
-1 "  Primary TP: ",string[.ctp.cfg.primaryTP];
-1 "  Batch interval: ",string[.ctp.cfg.batchMs]," ms";
-1 "";
-1 "Connection Settings:";
-1 "  Base retry delay: ",string[.ctp.conn.cfg.baseDelayMs],"ms";
-1 "  Max retry delay: ",string[.ctp.conn.cfg.maxDelayMs],"ms";
-1 "";
-1 "Tables: trade_binance trade_binance_fut quote_binance health_feed_handler positions";
-1 "";
-1 "Data Sources:";
-1 "  Primary TP: trades (spot + futures), quotes, health";
-1 "  SIG: positions (direct publish)";
-1 "";

/ Attempt initial connection (non-blocking)
connected:.ctp.connect[];

/ Start timer for batching and reconnection
system "t ",string .ctp.cfg.batchMs;

-1 "Query Interface:";
-1 "  .health[]       / Standardized health check";
-1 "  .ctp.status[]   / Full status";
-1 "";

$[connected; -1 "CTP: Ready and processing"; -1 "CTP: Started in DEGRADED mode - waiting for TP connection"];
-1 "=======================================================";
