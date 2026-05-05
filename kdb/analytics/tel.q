/ tel.q - Telemetry Process (Simplified)
/ Feed Handler Latency Only
/ Subscribes to Chained TP (batched)

/ =============================================================================
/ Configuration
/ =============================================================================

.tel.cfg.port:5016;
.tel.cfg.ctpPort:5014;            / Chained TP
.tel.cfg.bucketSec:5;
.tel.cfg.retentionMin:15;

/ Connection resilience
.tel.conn.handle:0N;                   / CTP connection handle
.tel.conn.state:`disconnected;         / `disconnected`connecting`connected
.tel.conn.lastAttempt:0Np;             / Last connection attempt time
.tel.conn.retryCount:0;                / Consecutive failed attempts
.tel.conn.cfg.baseDelayMs:1000;        / Initial retry delay (1 sec)
.tel.conn.cfg.maxDelayMs:30000;        / Max retry delay (30 sec)
.tel.conn.cfg.backoffMultiplier:1.5;   / Exponential backoff factor

system "g 0";

.proc.startTime:.z.p;

/ Derived configuration
.tel.cfg.bucketNs:.tel.cfg.bucketSec * 1000000000j;
.tel.cfg.bucketSpan:`timespan$.tel.cfg.bucketNs;
.tel.cfg.retentionNs:.tel.cfg.retentionMin * 60 * 1000000000j;

/ =============================================================================
/ Schema and derived field indices
/ =============================================================================
/ TEL declares local trade/quote schemas matching what CTP forwards (with TP's
/ receive timestamp). All field indices are derived from these schemas, so
/ adding a column to schemas.q automatically updates indices on next restart.

\l ../schemas.q

trade_binance:.schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo];
quote_binance:.schema.extend[.schema.quote; `tpRecvTimeUtcNs`tpSeqNo];

.tel.idx.trade.sym       :(cols trade_binance)?`sym;
.tel.idx.trade.fhParseUs :(cols trade_binance)?`fhParseUs;
.tel.idx.trade.fhSendUs  :(cols trade_binance)?`fhSendUs;

.tel.idx.quote.sym       :(cols quote_binance)?`sym;
.tel.idx.quote.fhParseUs :(cols quote_binance)?`fhParseUs;
.tel.idx.quote.fhSendUs  :(cols quote_binance)?`fhSendUs;

/ Expected field counts (also derived) - used by schema validators below
.tel.idx.trade.expectedFields:count cols trade_binance;
.tel.idx.quote.expectedFields:count cols quote_binance;

/ =============================================================================
/ Schema Validation
/ =============================================================================

/ Validate that received data matches expected schema
/ Returns: 1b if valid, 0b if mismatch (logs warning)
.tel.validateTradeSchema:{[data]
  if[0 = count data; :1b];  / Empty is ok
  fieldCount:count first data;
  if[fieldCount <> .tel.idx.trade.expectedFields;
    -1 "TEL: WARNING - Trade schema mismatch! Expected ",
       string[.tel.idx.trade.expectedFields]," fields, got ",string[fieldCount];
    -1 "TEL: Field indices may be incorrect - check .tel.idx.trade.*";
    :0b
  ];
  1b
  };

.tel.validateQuoteSchema:{[data]
  if[0 = count data; :1b];  / Empty is ok
  fieldCount:count first data;
  if[fieldCount <> .tel.idx.quote.expectedFields;
    -1 "TEL: WARNING - Quote schema mismatch! Expected ",
       string[.tel.idx.quote.expectedFields]," fields, got ",string[fieldCount];
    -1 "TEL: Field indices may be incorrect - check .tel.idx.quote.*";
    :0b
  ];
  1b
  };

/ Track validation state
.tel.schema.tradeValidated:0b;
.tel.schema.quoteValidated:0b;
.tel.schema.tradeValid:1b;
.tel.schema.quoteValid:1b;

/ =============================================================================
/ Telemetry Tables
/ =============================================================================

/ FH latency stats (parse and send times)
telemetry_latency_fh:([]
  bucket:`timestamp$();
  sym:`symbol$();
  handler:`symbol$();         / `trade_fh or `quote_fh
  parseUs_p50:`float$();
  parseUs_p95:`float$();
  parseUs_max:`long$();
  sendUs_p50:`float$();
  sendUs_p95:`float$();
  sendUs_max:`long$();
  cnt:`long$()
  );

/ Health metrics from feed handlers
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

/ =============================================================================
/ Buffers for batched data
/ =============================================================================

.tel.buf.trades:();
.tel.buf.quotes:();

/ Statistics
.tel.stats.tradesProcessed:0j;
.tel.stats.quotesProcessed:0j;
.tel.stats.bucketsComputed:0j;

/ =============================================================================
/ Connection Management (Resilient)
/ =============================================================================

/ Calculate next retry delay with exponential backoff
.tel.conn.getDelay:{[]
  delay:.tel.conn.cfg.baseDelayMs * `long$.tel.conn.cfg.backoffMultiplier xexp .tel.conn.retryCount;
  delay & .tel.conn.cfg.maxDelayMs  / Cap at max
  };

/ Check if enough time has passed since last attempt
.tel.conn.canRetry:{[]
  if[null .tel.conn.lastAttempt; :1b];
  elapsed:`long$(.z.p - .tel.conn.lastAttempt) % 1000000;  / ms
  elapsed >= .tel.conn.getDelay[]
  };

/ Main connection function - NEVER THROWS
.tel.connect:{[]
  / Guard: already connected
  if[not null .tel.conn.handle; :1b];
  
  / Guard: backoff not elapsed
  if[not .tel.conn.canRetry[]; :0b];
  
  .tel.conn.state:`connecting;
  .tel.conn.lastAttempt:.z.p;
  
  -1 "TEL: Connecting to Chained TP on port ",string[.tel.cfg.ctpPort],
     " (attempt ",string[.tel.conn.retryCount + 1],")...";
  
  / Protected connection attempt
  h:@[hopen; `$"::",string[.tel.cfg.ctpPort]; {[err] -1 "TEL: Connection failed - ",err; 0N}];
  
  if[null h;
    .tel.conn.retryCount+:1;
    .tel.conn.state:`disconnected;
    nextDelay:.tel.conn.getDelay[];
    -1 "TEL: Will retry in ",string[nextDelay],"ms";
    :0b
  ];
  
  / Connection successful - now subscribe
  subResult:@[{[h]
    -1 "TEL: Subscribing to trade_binance...";
    res:h(`pubsub.subscribe; `trade_binance; `);
    -1 "TEL: Subscribed to trade_binance";
    -1 "TEL: Subscribing to quote_binance...";
    res:h(`pubsub.subscribe; `quote_binance; `);
    -1 "TEL: Subscribed to quote_binance";
    -1 "TEL: Subscribing to health_feed_handler...";
    res:h(`pubsub.subscribe; `health_feed_handler; `);
    -1 "TEL: Subscribed to health_feed_handler";
    1b
  }; h; {[err] -1 "TEL: Subscription failed - ",err; 0b}];
  
  if[not subResult;
    @[hclose; h; {}];  / Clean up failed connection
    .tel.conn.retryCount+:1;
    .tel.conn.state:`disconnected;
    :0b
  ];
  
  / Success - update state
  .tel.conn.handle:h;
  .tel.conn.state:`connected;
  .tel.conn.retryCount:0;
  -1 "TEL: Connected successfully (handle ",string[h],")";
  1b
  };

/ Disconnect handler
.z.pc:{[h]
  if[h = .tel.conn.handle;
    -1 "TEL: Chained TP connection lost (handle ",string[h],")";
    .tel.conn.handle:0N;
    .tel.conn.state:`disconnected;
    .tel.conn.retryCount:0;  / Reset backoff on disconnect
    -1 "TEL: Will attempt reconnection on next timer tick";
  ];
  };

/ =============================================================================
/ Update Handler
/ =============================================================================

upd:{[tbl;data]
  / Handle batch (table) from chained TP - convert each row to list
  if[98h = type data;
    {[tbl;row] upd[tbl;value row]} [tbl] each data;
    :();
  ];
  
  if[tbl = `trade_binance;
    / Validate schema on first trade (once)
    if[not .tel.schema.tradeValidated;
      .tel.schema.tradeValid:.tel.validateTradeSchema[enlist data];
      .tel.schema.tradeValidated:1b;
    ];
    .tel.buf.trades,:enlist data;
  ];
  
  if[tbl = `quote_binance;
    / Validate schema on first quote (once)
    if[not .tel.schema.quoteValidated;
      .tel.schema.quoteValid:.tel.validateQuoteSchema[enlist data];
      .tel.schema.quoteValidated:1b;
    ];
    .tel.buf.quotes,:enlist data;
  ];
  
  if[tbl = `health_feed_handler;
    `health_feed_handler insert data;
  ];
  };

.u.upd:upd;

/ =============================================================================
/ Percentile Function
/ =============================================================================

.tel.percentile:{[p;x]
  if[0 = n:count x; :0n];
  idx:0 | ("j"$p * n - 1) & n - 1;
  `float$(asc x) idx
  };

/ =============================================================================
/ Aggregation Logic
/ =============================================================================

.tel.lastBucket:0Np;

.tel.compute:{[]
  now:.z.p;
  currentBucket:`timestamp$.tel.cfg.bucketNs * `long$now div .tel.cfg.bucketNs;
  bucket:currentBucket - .tel.cfg.bucketSpan;
  if[bucket <= .tel.lastBucket; :()];
  
  / Process trade FH latency from buffer (using named indices)
  if[0 < count .tel.buf.trades;
    / Only process if schema is valid
    if[.tel.schema.tradeValid;
      trades:([] 
        sym:.tel.buf.trades[;.tel.idx.trade.sym]; 
        fhParseUs:.tel.buf.trades[;.tel.idx.trade.fhParseUs]; 
        fhSendUs:.tel.buf.trades[;.tel.idx.trade.fhSendUs]
      );
      fhStats:select 
        parseUs_p50:.tel.percentile[0.5; fhParseUs], 
        parseUs_p95:.tel.percentile[0.95; fhParseUs], 
        parseUs_max:max fhParseUs, 
        sendUs_p50:.tel.percentile[0.5; fhSendUs], 
        sendUs_p95:.tel.percentile[0.95; fhSendUs], 
        sendUs_max:max fhSendUs, 
        cnt:count i 
        by sym from trades;
      fhStats:update bucket:bucket, handler:`trade_fh from fhStats;
      `telemetry_latency_fh insert `bucket xcols 0!fhStats;
      .tel.stats.tradesProcessed+:count .tel.buf.trades;
    ];
    .tel.buf.trades:();
  ];
  
  / Process quote FH latency from buffer (using named indices)
  if[0 < count .tel.buf.quotes;
    / Only process if schema is valid
    if[.tel.schema.quoteValid;
      quotes:([] 
        sym:.tel.buf.quotes[;.tel.idx.quote.sym]; 
        fhParseUs:.tel.buf.quotes[;.tel.idx.quote.fhParseUs]; 
        fhSendUs:.tel.buf.quotes[;.tel.idx.quote.fhSendUs]
      );
      fhStats:select 
        parseUs_p50:.tel.percentile[0.5; fhParseUs], 
        parseUs_p95:.tel.percentile[0.95; fhParseUs], 
        parseUs_max:max fhParseUs, 
        sendUs_p50:.tel.percentile[0.5; fhSendUs], 
        sendUs_p95:.tel.percentile[0.95; fhSendUs], 
        sendUs_max:max fhSendUs, 
        cnt:count i 
        by sym from quotes;
      fhStats:update bucket:bucket, handler:`quote_fh from fhStats;
      `telemetry_latency_fh insert `bucket xcols 0!fhStats;
      .tel.stats.quotesProcessed+:count .tel.buf.quotes;
    ];
    .tel.buf.quotes:();
  ];
  
  .tel.lastBucket:bucket;
  .tel.stats.bucketsComputed+:1;
  
  / Cleanup old data
  cutoff:now - .tel.cfg.retentionNs;
  delete from `telemetry_latency_fh where bucket < cutoff;
  delete from `health_feed_handler where time < now - 00:01:00;
  };

/ =============================================================================
/ End-of-Day Handler
/ =============================================================================

endofday:{[]
  -1 "TEL: EOD received";
  delete from `telemetry_latency_fh;
  delete from `health_feed_handler;
  .tel.buf.trades:();
  .tel.buf.quotes:();
  .tel.lastBucket:0Np;
  .tel.stats.tradesProcessed:0j;
  .tel.stats.quotesProcessed:0j;
  .tel.stats.bucketsComputed:0j;
  / Reset schema validation for new day
  .tel.schema.tradeValidated:0b;
  .tel.schema.quoteValidated:0b;
  -1 "TEL: State cleared";
  };

/ =============================================================================
/ Health Check
/ =============================================================================

.health:{[]
  / Determine status
  st:$[.tel.conn.state <> `connected; `disconnected;
       (not .tel.schema.tradeValid) or (not .tel.schema.quoteValid); `degraded;
       `ok];
  
  `process`port`uptime`status`connState`schemaOk`tradesProcessed`quotesProcessed`buckets`memMB!(
    `tel;
    .tel.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    .tel.conn.state;
    .tel.schema.tradeValid and .tel.schema.quoteValid;
    .tel.stats.tradesProcessed;
    .tel.stats.quotesProcessed;
    .tel.stats.bucketsComputed;
    (`long$.Q.w[][`used]) % 1000000
  )
  };

/ =============================================================================
/ Query Interface
/ =============================================================================

/ FH latency summary (latest bucket per handler/sym)
.tel.getFhLatency:{[]
  select last parseUs_p50, last parseUs_p95, last parseUs_max,
         last sendUs_p50, last sendUs_p95, last sendUs_max, last cnt
    by sym, handler from telemetry_latency_fh
  };

/ FH health status (raw)
.tel.fhStatus:{[]
  select last time, last uptimeSec, last msgsReceived, last msgsPublished, 
         last connState, last symbolCount
    by handler from health_feed_handler
  };

/ FH health status with computed fields
.tel.fhStatusTable:{[]
  data:0!.tel.fhStatus[];
  if[0 = count data; :data];
  now:.z.p;
  update avgRate:`long$msgsPublished % uptimeSec,
         lastActivitySec:`long$(now - time) % 1000000000
    from data
  };

/ Schema status
.tel.schemaStatus:{[]
  ([] 
    table:`trade_binance`quote_binance;
    validated:.tel.schema.tradeValidated,.tel.schema.quoteValidated;
    valid:.tel.schema.tradeValid,.tel.schema.quoteValid;
    expectedFields:.tel.idx.trade.expectedFields,.tel.idx.quote.expectedFields
  )
  };

/ Format helpers
.tel.fmtDuration:{[x]
  if[null x; :"--"];
  h:x div 3600; m:(x mod 3600) div 60; s:x mod 60;
  $[h > 0; string[h],"h ",string[m],"m"; m > 0; string[m],"m ",string[s],"s"; string[s],"s"]
  };

.tel.fmtCount:{[x]
  if[null x; :"--"];
  $[x >= 1000000; string[0.01 * `long$x % 10000],"M"; x >= 1000; string[0.1 * `long$x % 100],"K"; string[x]]
  };

.tel.fmtTimeAgo:{[x]
  if[null x; :"--"];
  $[x < 60; string[x],"s ago"; x < 3600; string[x div 60],"m ago"; string[x div 3600],"h ago"]
  };

/ Dashboard view: FH status
.tel.vsFhStatus:{[]
  data:.tel.fhStatusTable[];
  if[0 = count data; 
    :([] Handler:`symbol$(); State:`symbol$(); Uptime:(); MsgsSec:`long$(); Published:(); LastMsg:())
  ];
  select Handler:handler, State:connState, Uptime:.tel.fmtDuration each uptimeSec, 
         MsgsSec:avgRate, Published:.tel.fmtCount each msgsPublished, 
         LastMsg:.tel.fmtTimeAgo each lastActivitySec 
    from data
  };

/ Dashboard view: FH latency
.tel.vsFhLatency:{[]
  data:0!.tel.getFhLatency[];
  if[0 = count data; :([] Sym:`$(); Handler:`$(); ParseP50:`float$(); ParseP95:`float$(); SendP50:`float$(); SendP95:`float$())];
  select Sym:sym, Handler:handler, ParseP50:parseUs_p50, ParseP95:parseUs_p95, 
         SendP50:sendUs_p50, SendP95:sendUs_p95 
    from data
  };

/ =============================================================================
/ Timer
/ =============================================================================

.z.ts:{[]
  if[null .tel.conn.handle; .tel.connect[]];
  .tel.compute[];
  };

/ =============================================================================
/ Startup
/ =============================================================================

system "p ",string .tel.cfg.port;

-1 "=======================================================";
-1 "TEL (Telemetry) on port ",string[.tel.cfg.port];
-1 "=======================================================";
-1 "Configuration:";
-1 "  Chained TP: ",string[.tel.cfg.ctpPort];
-1 "  Bucket size: ",string[.tel.cfg.bucketSec],"s";
-1 "  Retention: ",string[.tel.cfg.retentionMin]," minutes";
-1 "";
-1 "Schema Indices (trade_binance):";
-1 "  sym:       ",string[.tel.idx.trade.sym];
-1 "  fhParseUs: ",string[.tel.idx.trade.fhParseUs];
-1 "  fhSendUs:  ",string[.tel.idx.trade.fhSendUs];
-1 "  expected fields: ",string[.tel.idx.trade.expectedFields];
-1 "";
-1 "Schema Indices (quote_binance):";
-1 "  sym:       ",string[.tel.idx.quote.sym];
-1 "  fhParseUs: ",string[.tel.idx.quote.fhParseUs];
-1 "  fhSendUs:  ",string[.tel.idx.quote.fhSendUs];
-1 "  expected fields: ",string[.tel.idx.quote.expectedFields];
-1 "";
-1 "Connection Settings:";
-1 "  Base retry delay: ",string[.tel.conn.cfg.baseDelayMs],"ms";
-1 "  Max retry delay: ",string[.tel.conn.cfg.maxDelayMs],"ms";
-1 "";

/ Attempt initial connection (non-blocking)
connected:.tel.connect[];

/ Start timer
system "t ",string .tel.cfg.bucketSec * 1000;

-1 "";
-1 "Query Interface:";
-1 "  .health[]              / Standardized health check";
-1 "  .tel.getFhLatency[]    / FH latency stats";
-1 "  .tel.fhStatus[]        / FH health status (raw)";
-1 "  .tel.fhStatusTable[]   / FH health status (with computed fields)";
-1 "  .tel.vsFhStatus[]      / FH status (dashboard)";
-1 "  .tel.vsFhLatency[]     / FH latency (dashboard)";
-1 "  .tel.schemaStatus[]    / Schema validation status";
-1 "  telemetry_latency_fh   / Raw latency table";
-1 "  health_feed_handler    / Raw health table";
-1 "";

$[connected; -1 "TEL: Ready and processing"; -1 "TEL: Started in DEGRADED mode - waiting for CTP connection"];
-1 "=======================================================";
