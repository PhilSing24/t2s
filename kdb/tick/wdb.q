/ wdb.q - Write-only RDB with intraday writedown
/ Based on w.q pattern - writes to disk when MAXROWS exceeded
/ At EOD: sorts on disk, moves to HDB partition

/ -------------------------------------------------------
/ Configuration
/ -------------------------------------------------------

.wdb.cfg.port:5011;
.wdb.cfg.tpPort:5010;
.wdb.cfg.hdbDir:`$":../hdb";
.wdb.cfg.maxRows:50000;

/ Enable compression for HDB writes
/ zstd, 2^17 block, level 1
.z.zd:(17;5;1);

/ Connection resilience
.wdb.conn.handle:0N;                   / TP connection handle
.wdb.conn.state:`disconnected;         / `disconnected`connecting`connected
.wdb.conn.lastAttempt:0Np;             / Last connection attempt time
.wdb.conn.retryCount:0;                / Consecutive failed attempts
.wdb.conn.cfg.baseDelayMs:1000;        / Initial retry delay (1 sec)
.wdb.conn.cfg.maxDelayMs:30000;        / Max retry delay (30 sec)
.wdb.conn.cfg.backoffMultiplier:1.5;   / Exponential backoff factor

/ Timer interval for reconnection checks
.wdb.cfg.timerMs:5000;

system "g 0";

.wdb.epochOffset:neg "j"$1970.01.01D0;
.proc.startTime:.z.p;

/ Statistics
.wdb.stats.flushCount:0j;
.wdb.stats.rowsWritten:0j;
.wdb.stats.tradesReceived:0j;
.wdb.stats.quotesReceived:0j;

/ -------------------------------------------------------
/ TMPSAVE - temporary directory for intraday writes
/ -------------------------------------------------------

.wdb.getTmpSave:{`$":../tmp.",string[.z.i],".",string x}
TMPSAVE:.wdb.getTmpSave .z.d

/ -------------------------------------------------------
/ Table Schema (must match TP)
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
  wdbRecvTimeUtcNs:`long$()
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
  wdbRecvTimeUtcNs:`long$()
  );

/ -------------------------------------------------------
/ Utility Functions
/ -------------------------------------------------------

.wdb.tsToNs:{[ts] .wdb.epochOffset+"j"$ts};

/ -------------------------------------------------------
/ Connection Management (Resilient)
/ -------------------------------------------------------

/ Calculate next retry delay with exponential backoff
.wdb.conn.getDelay:{[]
  delay:.wdb.conn.cfg.baseDelayMs * `long$.wdb.conn.cfg.backoffMultiplier xexp .wdb.conn.retryCount;
  delay & .wdb.conn.cfg.maxDelayMs  / Cap at max
  };

/ Check if enough time has passed since last attempt
.wdb.conn.canRetry:{[]
  if[null .wdb.conn.lastAttempt; :1b];
  elapsed:`long$(.z.p - .wdb.conn.lastAttempt) % 1000000;  / ms
  elapsed >= .wdb.conn.getDelay[]
  };

/ Main connection function - NEVER THROWS
.wdb.connect:{[]
  / Guard: already connected
  if[not null .wdb.conn.handle; :1b];
  
  / Guard: backoff not elapsed
  if[not .wdb.conn.canRetry[]; :0b];
  
  .wdb.conn.state:`connecting;
  .wdb.conn.lastAttempt:.z.p;
  
  -1 "WDB: Connecting to TP on port ",string[.wdb.cfg.tpPort],
     " (attempt ",string[.wdb.conn.retryCount + 1],")...";
  
  / Protected connection attempt
  h:@[hopen; `$"::",string[.wdb.cfg.tpPort]; {[err] -1 "WDB: Connection failed - ",err; 0N}];
  
  if[null h;
    .wdb.conn.retryCount+:1;
    .wdb.conn.state:`disconnected;
    nextDelay:.wdb.conn.getDelay[];
    -1 "WDB: Will retry in ",string[nextDelay],"ms";
    :0b
  ];
  
  / Connection successful - now subscribe
  subResult:@[{[h]
    res:h(`pubsub.subscribe;`trade_binance;`);
    -1 "WDB: Subscribed to ",string first first res;
    res:h(`pubsub.subscribe;`quote_binance;`);
    -1 "WDB: Subscribed to ",string first first res;
    1b
  }; h; {[err] -1 "WDB: Subscription failed - ",err; 0b}];
  
  if[not subResult;
    @[hclose; h; {}];  / Clean up failed connection
    .wdb.conn.retryCount+:1;
    .wdb.conn.state:`disconnected;
    :0b
  ];
  
  / Success - update state
  .wdb.conn.handle:h;
  .wdb.conn.state:`connected;
  .wdb.conn.retryCount:0;
  -1 "WDB: Connected successfully (handle ",string[h],")";
  1b
  };

/ Disconnect handler
.z.pc:{[h]
  if[h = .wdb.conn.handle;
    -1 "WDB: TP connection lost (handle ",string[h],")";
    .wdb.conn.handle:0N;
    .wdb.conn.state:`disconnected;
    .wdb.conn.retryCount:0;  / Reset backoff on disconnect
    -1 "WDB: Will attempt reconnection on next timer tick";
  ];
  };

/ -------------------------------------------------------
/ Disk Sort - efficient on-disk sorting by sym
/ -------------------------------------------------------

disksort:{[t;c;a]
  if[not`s~attr(t:hsym t)c;
    if[count t;
      ii:iasc iasc flip c!t c,:();
      if[not$[(0,-1+count ii)~(first;last)@\:ii;@[{`s#x;1b};ii;0b];0b];
        {v:get y;
          if[not$[all(fv:first v)~/:256#v;all fv~/:v;0b];
            v[x]:v;
            y set v];
        }[ii] each ` sv't,'get ` sv t,`.d
      ]
    ];
    @[t;first c;a]
  ];
  t}

/ -------------------------------------------------------
/ Append - insert and write to disk if MAXROWS exceeded
/ -------------------------------------------------------

append:{[t;data]
  t insert data;
  if[.wdb.cfg.maxRows<count value t;
    -1"WDB: Flushing ",string[t]," to disk (",string[count value t]," rows)";
    / Append enumerated buffer to disk
    .[` sv TMPSAVE,t,`;();,;.Q.en[.wdb.cfg.hdbDir]`. t];
    / Track statistics
    .wdb.stats.flushCount+:1;
    .wdb.stats.rowsWritten+:count value t;
    / Clear buffer
    @[`.;t;0#];
  ]};

/ -------------------------------------------------------
/ Update Handler
/ -------------------------------------------------------

upd:{[tbl;data]
  / Add WDB receive timestamp
  data:data,.wdb.tsToNs[.z.p];
  
  / Track statistics
  $[tbl = `trade_binance; .wdb.stats.tradesReceived+:1;
    tbl = `quote_binance; .wdb.stats.quotesReceived+:1;
    ()];
  
  / Append (will flush to disk if needed)
  append[tbl;data];
  };

/ -------------------------------------------------------
/ Health Check
/ -------------------------------------------------------

.health:{[]
  memMB:(`long$.Q.w[][`used]) % 1000000;
  
  / Determine status based on connection state
  st:$[.wdb.conn.state = `connected; `ok;
       .wdb.conn.state = `connecting; `degraded;
       `disconnected];
  
  `process`port`uptime`status`connState`memMB`tradesRecv`quotesRecv`flushes`rowsWritten`bufferTrades`bufferQuotes!(
    `wdb;
    .wdb.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    .wdb.conn.state;
    memMB;
    .wdb.stats.tradesReceived;
    .wdb.stats.quotesReceived;
    .wdb.stats.flushCount;
    .wdb.stats.rowsWritten;
    count trade_binance;
    count quote_binance
  )
  };

/ -------------------------------------------------------
/ Status Query
/ -------------------------------------------------------

.wdb.status:{[]
  `port`tpPort`connected`maxRows`tmpSave`hdbDir`flushes`rowsWritten`bufferTrades`bufferQuotes`memMB!(
    .wdb.cfg.port;
    .wdb.cfg.tpPort;
    .wdb.conn.state = `connected;
    .wdb.cfg.maxRows;
    TMPSAVE;
    .wdb.cfg.hdbDir;
    .wdb.stats.flushCount;
    .wdb.stats.rowsWritten;
    count trade_binance;
    count quote_binance;
    (`long$.Q.w[][`used]) % 1000000
  )
  };

/ -------------------------------------------------------
/ End-of-Day Handler
/ -------------------------------------------------------

endofday:{[]
  / Closing date = yesterday
  d:-1 + .z.d; 
  -1"WDB: EOD processing for ",string[d];
  
  / Get tables with sym column
  t:tables`.;
  t@:where 11h=type each t@\:`sym;
  
  -1"WDB: Tables to process: ",", "sv string t;
  
  / Flush remaining data to disk
  {
    if[0 < count value x;
      -1"WDB: Flushing final ",string[x]," (",string[count value x]," rows)";
      .[` sv TMPSAVE,x,`;();,;.Q.en[.wdb.cfg.hdbDir]`. x];
      .wdb.stats.rowsWritten+:count value x;
    ];
    @[`.;x;0#];
  } each t;
  
  / Sort on disk by sym and set `p# attribute
  -1"WDB: Sorting on disk...";
  {disksort[` sv TMPSAVE,x,`;`sym;`p#]} each t;
  
  / Move from tmp to HDB partition
  -1"WDB: Moving to HDB partition...";
  dest:.Q.par[.wdb.cfg.hdbDir;d;`];
  system"r ",(1_string TMPSAVE)," ",-1_1_string dest;
  
  / Reset TMPSAVE for new day which is current day
  TMPSAVE::.wdb.getTmpSave .z.d;
  
  / Reset statistics for new day
  .wdb.stats.flushCount:0j;
  .wdb.stats.rowsWritten:0j;
  .wdb.stats.tradesReceived:0j;
  .wdb.stats.quotesReceived:0j;

  / RESUBSCRIBE - pubsub subscription was cleared by callendofday
  if[not null .wdb.conn.handle;
    @[{[h]
      h(`pubsub.subscribe;`trade_binance;`);
      -1 "WDB: Resubscribed to trade_binance";
      h(`pubsub.subscribe;`quote_binance;`);
      -1 "WDB: Resubscribed to quote_binance";
    }; .wdb.conn.handle; {[err] -1 "WDB: Resubscription failed - ",err}];
  ];
  
  -1"WDB: EOD complete";
  };

/ -------------------------------------------------------
/ Exit Handler - SAVE data instead of destroying it
/ -------------------------------------------------------

.z.exit:{[x]
  -1 "WDB: Exit signal received (code: ",string[x],") - emergency flush...";
  
  t:tables `.;
  t@:where 11h=type each t@\:`sym;
  
  totalFlushed:0j;
  
  {[t]
    cnt:count value t;
    if[cnt > 0;
      -1 "WDB: Emergency flushing ",string[t]," (",string[cnt]," rows)";
      / Protected write - try to save even if something fails
      @[{[t;cnt]
        .[` sv TMPSAVE,t,`;();,;.Q.en[.wdb.cfg.hdbDir]`. t];
        -1 "WDB: Successfully flushed ",string[t];
      }[t;cnt]; ::; {[t;err] -1 "WDB: ERROR flushing ",string[t]," - ",err}[t]];
    ];
  } each t;
  
  -1 "WDB: Emergency flush complete - data preserved in ",string[TMPSAVE];
  -1 "WDB: To recover, check ",string[TMPSAVE]," directory";
  };

/ -------------------------------------------------------
/ Manual Flush (for testing/maintenance)
/ -------------------------------------------------------

.wdb.flush:{[]
  -1 "WDB: Manual flush requested";
  t:tables `.;
  t@:where 11h=type each t@\:`sym;
  
  {
    if[0 < count value x;
      -1 "WDB: Flushing ",string[x]," (",string[count value x]," rows)";
      .[` sv TMPSAVE,x,`;();,;.Q.en[.wdb.cfg.hdbDir]`. x];
      .wdb.stats.flushCount+:1;
      .wdb.stats.rowsWritten+:count value x;
      @[`.;x;0#];
    ];
  } each t;
  
  -1 "WDB: Manual flush complete";
  };

/ -------------------------------------------------------
/ Timer - reconnection
/ -------------------------------------------------------

.z.ts:{[]
  / Attempt reconnection if disconnected
  if[null .wdb.conn.handle; .wdb.connect[]];
  };

/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system"p ",string .wdb.cfg.port;

-1"=======================================================";
-1"WDB (Write-only RDB) starting on port ",string[.wdb.cfg.port];
-1"=======================================================";
-1"Configuration:";
-1"  TP port: ",string[.wdb.cfg.tpPort];
-1"  MAXROWS: ",string[.wdb.cfg.maxRows];
-1"  TMPSAVE: ",string[TMPSAVE];
-1"  HDB: ",string[.wdb.cfg.hdbDir];
-1"";
-1"Connection Settings:";
-1"  Base retry delay: ",string[.wdb.conn.cfg.baseDelayMs],"ms";
-1"  Max retry delay: ",string[.wdb.conn.cfg.maxDelayMs],"ms";
-1"";

/ Attempt initial connection (non-blocking)
connected:.wdb.connect[];

/ Start timer for reconnection
system "t ",string .wdb.cfg.timerMs;

-1"";
-1"Query Interface:";
-1"  .health[]        / Standardized health check";
-1"  .wdb.status[]    / Full status";
-1"  .wdb.flush[]     / Manual flush to disk";
-1"";
-1"Tables: trade_binance quote_binance";
-1"";

$[connected; -1 "WDB: Ready and processing"; -1 "WDB: Started in DEGRADED mode - waiting for TP connection"];
-1"=======================================================";
