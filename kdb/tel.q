/ tel.q - Telemetry Aggregation Process

/ Configuration
.tel.cfg.port:5013;
.tel.cfg.tpPort:5010;
.tel.cfg.rdbPort:5011;
.tel.cfg.rtePort:5012;
.tel.cfg.bucketSec:5;
.tel.cfg.retentionMin:15;

/ Derived configuration
.tel.cfg.bucketNs:.tel.cfg.bucketSec * 1000000000j;
.tel.cfg.bucketSpan:`timespan$.tel.cfg.bucketNs;
.tel.cfg.timerMs:.tel.cfg.bucketSec * 1000;
.tel.cfg.retentionNs:.tel.cfg.retentionMin * 60 * 1000000000j;

/ to store start time of the process
.proc.startTime:.z.p;

/ -----------------------------------------------------------------------------
/ Persistent Handle Management
/ -----------------------------------------------------------------------------

/ Handle dictionary: port -> handle (0N = not connected)
/ Initialize as empty dictionary with int keys and int values
.tel.h:(`int$())!`int$();

/ Check if handle exists and is valid (not null)
/ @param p - port number
/ @return 1b if handle exists and is valid, 0b otherwise
.tel.hasValidH:{[p]
  if[not p in key .tel.h; :0b];
  not null .tel.h[p]
  };

/ Get or create persistent handle to a port
/ Uses lazy connection - only connects when needed
/ @param p - port number
/ @return handle (int) or 0N if connection failed
.tel.getH:{[p]
  / Return existing valid handle
  if[.tel.hasValidH[p]; :.tel.h[p]];
  
  / Build connection string
  connStr:`$"::",string p;
  
  / Attempt connection with error handling
  h:@[hopen; connStr; {[port;err] 
    -1 "TEL: Failed to connect to port ",string[port]," - ",err; 
    0N
    }[p]];
  
  / Store result (valid handle or 0N)
  .tel.h[p]:h;
  
  / Log successful connection
  if[not null h; -1 "TEL: Connected to port ",string[p]," (handle ",string[h],")"];
  
  h
  };

/ Close handle and remove from dictionary
/ Safe to call even if handle doesn't exist
/ @param p - port number
.tel.closeH:{[p]
  / Nothing to do if port not tracked
  if[not p in key .tel.h; :()];
  
  / Get handle before removing
  h:.tel.h[p];
  
  / Remove from dictionary first (defensive - prevents stale handle use)
  .tel.h _: p;
  
  / Close if valid handle
  if[not null h;
    @[hclose; h; {[port;err] 
      -1 "TEL: Warning - error closing handle for port ",string[port],": ",err
      }[p]];
    -1 "TEL: Closed handle for port ",string[p];
  ];
  };

/ Reconnect a specific handle (close and reopen)
/ @param p - port number
/ @return new handle or 0N if connection failed
.tel.reconnectH:{[p]
  -1 "TEL: Reconnecting to port ",string[p],"...";
  .tel.closeH[p];
  .tel.getH[p]
  };

/ Close all handles (for graceful shutdown)
.tel.closeAll:{[]
  ports:key .tel.h;
  if[0 = count ports; -1 "TEL: No handles to close"; :()];
  -1 "TEL: Closing ",string[count ports]," handle(s)...";
  .tel.closeH each ports;
  -1 "TEL: All persistent handles closed";
  };

/ Get handle status for diagnostics
/ @return table with port, handle, and status
.tel.handleStatus:{[]
  if[0 = count .tel.h; :([] port:`int$(); handle:`int$(); status:`$())];
  ports:key .tel.h;
  handles:.tel.h ports;
  status:?[null each handles; `disconnected; `connected];
  ([] port:ports; handle:handles; status:status)
  };

/ -----------------------------------------------------------------------------
/ Telemetry Tables
/ -----------------------------------------------------------------------------

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

/ E2E latency table - includes handler column for trade/quote distinction
telemetry_latency_e2e:([]
  bucket:`timestamp$();
  sym:`symbol$();
  handler:`symbol$();         / `trade_fh or `quote_fh
  fhToTpMs_p50:`float$();
  fhToTpMs_p95:`float$();
  fhToTpMs_max:`float$();
  tpToRdbMs_p50:`float$();
  tpToRdbMs_p95:`float$();
  tpToRdbMs_max:`float$();
  e2eMs_p50:`float$();
  e2eMs_p95:`float$();
  e2eMs_max:`float$();
  cnt:`long$()
  );

telemetry_system:([]
  bucket:`timestamp$();
  component:`symbol$();
  usedMB:`float$();
  heapMB:`float$();
  peakMB:`float$()
  );

/ Health metrics from feed handlers (received via TP subscription)
health_feed_handler:([]
  time:`timestamp$();
  handler:`symbol$();            / `trade_fh or `quote_fh
  startTimeUtc:`timestamp$();    / When FH started
  uptimeSec:`long$();            / Seconds since start
  msgsReceived:`long$();         / Total messages from Binance
  msgsPublished:`long$();        / Total messages sent to TP
  lastMsgTimeUtc:`timestamp$();  / Time of last received message
  lastPubTimeUtc:`timestamp$();  / Time of last publish to TP
  connState:`symbol$();          / `connected, `reconnecting, `disconnected
  symbolCount:`int$()            / Number of symbols subscribed
  );

/ -----------------------------------------------------------------------------
/ Utility Functions
/ -----------------------------------------------------------------------------

/ Percentile function
.tel.percentile:{[p;x]
  if[0 = n:count x; :0n];
  idx:0 | ("j"$p * n - 1) & n - 1;
  `float$(asc x) idx
  };

/ Safe query using persistent handles
/ Automatically reconnects on connection failure
/ @param port - target port number
/ @param query - query string or function to execute
/ @return query result, or empty list () on failure
.tel.safeQuery:{[port;query]
  / Get or establish connection
  h:.tel.getH[port];
  if[null h; :()];
  
  / Execute query with error trapping
  res:@[h; query; {[p;q;err] 
    / Log the error with context
    -1 "TEL: Query failed on port ",string[p]," - ",err;
    / Mark handle as invalid for reconnection on next call
    .tel.h[p]:0N;
    `..tel.queryError
    }[port;query]];
  
  / Return empty on error (sentinel value check)
  if[res ~ `..tel.queryError; :()];
  
  res
  };

/ -----------------------------------------------------------------------------
/ E2E Latency Calculation Helper
/ -----------------------------------------------------------------------------

/ Calculate and insert E2E latency stats for a given dataset
/ @param data - table with fhRecvTimeUtcNs, tpRecvTimeUtcNs, rdbApplyTimeUtcNs
/ @param bucket - timestamp bucket
/ @param handler - `trade_fh or `quote_fh
.tel.calcE2E:{[data;bucket;handler]
  if[0 = count data; :()];
  
  / Calculate latencies in milliseconds
  dataWithLatency:update 
    fhToTpMs:(tpRecvTimeUtcNs - fhRecvTimeUtcNs) % 1e6, 
    tpToRdbMs:(rdbApplyTimeUtcNs - tpRecvTimeUtcNs) % 1e6, 
    e2eMs:(rdbApplyTimeUtcNs - fhRecvTimeUtcNs) % 1e6 
    from data;
  
  / Aggregate by symbol
  e2eStats:select 
    fhToTpMs_p50:.tel.percentile[0.5; fhToTpMs], 
    fhToTpMs_p95:.tel.percentile[0.95; fhToTpMs], 
    fhToTpMs_max:max fhToTpMs, 
    tpToRdbMs_p50:.tel.percentile[0.5; tpToRdbMs], 
    tpToRdbMs_p95:.tel.percentile[0.95; tpToRdbMs], 
    tpToRdbMs_max:max tpToRdbMs, 
    e2eMs_p50:.tel.percentile[0.5; e2eMs], 
    e2eMs_p95:.tel.percentile[0.95; e2eMs], 
    e2eMs_max:max e2eMs, 
    cnt:count i 
    by sym from dataWithLatency;
  
  / Add bucket and handler columns
  e2eStats:update bucket:bucket, handler:handler from e2eStats;
  
  / Insert into telemetry table
  `telemetry_latency_e2e insert `bucket xcols 0!e2eStats;
  };

/ -----------------------------------------------------------------------------
/ TP Subscription Handler (for health_feed_handler)
/ -----------------------------------------------------------------------------

.u.upd:{[tbl;data]
  / Only handle health_feed_handler updates
  if[tbl = `health_feed_handler;
    tbl insert data;
  ];
  };

/ Create root-level upd function for u.q compatibility
/ u.q publishes to `upd, not `.u.upd
upd:.u.upd;

/ Subscribe to TP for health updates
.tel.subscribe:{[]
  -1 "Connecting to tickerplant on port ",string[.tel.cfg.tpPort],"...";
  h:@[hopen; `$"::",string .tel.cfg.tpPort; {-1 "Failed to connect to TP: ",x; 0N}];
  if[null h; -1 "WARNING: Cannot connect to TP - health updates disabled"; :()];
  
  res:@[h; (`.u.sub; `health_feed_handler; `); {-1 "Subscribe error: ",x; `error}];
  if[res ~ `error; -1 "WARNING: Could not subscribe to health_feed_handler"; :()];
  
  -1 "Subscribed to: health_feed_handler";
  .tel.tpHandle:h;
  };

/ -----------------------------------------------------------------------------
/ Dashboard Query Functions
/ -----------------------------------------------------------------------------

/ FH health status (base function)
/ Returns latest health record per handler
.tel.fhStatus:{[]
  select last time, last startTimeUtc, last uptimeSec, last msgsReceived, 
         last msgsPublished, last lastMsgTimeUtc, last connState, last symbolCount
    by handler from health_feed_handler
  };

/ FH status table with computed fields
.tel.fhStatusTable:{[]
  current:.tel.fhStatus[];
  if[0 = count current; :current];
  
  / Calculate average rate and last activity
  select handler, connState, uptimeSec, avgRate:`long$msgsReceived % uptimeSec, msgsReceived, msgsPublished, lastActivitySec:`long$(.z.p - lastMsgTimeUtc) % 1000000000 from 0!current
  };

/ -----------------------------------------------------------------------------
/ Dashboard View States
/ -----------------------------------------------------------------------------

/ Format seconds as human-readable duration string
/ @param x - seconds (long)
/ @return string like "4h 23m" or "45m" or "30s"
.tel.fmtDuration:{[x]
  if[null x; :"--"];
  h:x div 3600;
  m:(x mod 3600) div 60;
  s:x mod 60;
  $[h > 0; string[h],"h ",string[m],"m";
    m > 0; string[m],"m ",string[s],"s";
    string[s],"s"]
  };

/ Format seconds ago as relative time string
/ @param x - seconds ago (long)
/ @return string like "< 1s ago", "5s ago", "2m ago"
.tel.fmtTimeAgo:{[x]
  if[null x; :"--"];
  $[x < 1;    "< 1s ago";
    x < 60;   string[x],"s ago";
    x < 3600; string[x div 60],"m ago";
    string[x div 3600],"h ago"]
  };

/ Format large numbers with K/M suffix
/ @param x - number (long)
/ @return string like "4.2M", "523K", "1234"
.tel.fmtCount:{[x]
  if[null x; :"--"];
  $[x >= 1000000; string[0.01 * `long$x % 10000],"M";
    x >= 1000;    string[0.1 * `long$x % 100],"K";
    string[x]]
  };

/ View State: Feed Handler Status Grid
/ Returns data formatted for dashboard data grid component
.tel.vsFhStatus:{[]
  data:.tel.fhStatusTable[];
  if[0 = count data; 
    :([] Handler:`symbol$(); State:`symbol$(); Uptime:(); MsgsSec:`long$(); Published:(); LastMsg:())
  ];
  select Handler:handler, State:connState, Uptime:.tel.fmtDuration each uptimeSec, MsgsSec:avgRate, Published:.tel.fmtCount each msgsPublished, LastMsg:.tel.fmtTimeAgo each lastActivitySec from data
  };

/ View State: System Resources
/ Returns memory stats per component
.tel.vsSystemResources:{[]
  data:select last usedMB, last heapMB, last peakMB by component from telemetry_system;
  if[0 = count data; 
    :([] Component:`symbol$(); UsedMB:`int$(); HeapMB:`int$(); PeakMB:`int$())
  ];
  select Component:component, UsedMB:`int$usedMB, HeapMB:`int$heapMB, PeakMB:`int$peakMB from 0!data
  };

/ View State: Data Volume per symbol
/ Returns trade and quote counts from RDB
.tel.vsDataVolume:{[]
  trades:.tel.safeQuery[.tel.cfg.rdbPort; "select Trades:count i by Sym:sym from trade_binance"];
  quotes:.tel.safeQuery[.tel.cfg.rdbPort; "select Quotes:count i by Sym:sym from quote_binance"];
  if[(0 = count trades) & 0 = count quotes; :([] Sym:`symbol$(); Trades:`long$(); Quotes:`long$())];
  if[0 = count trades; :select Sym, Trades:0j, Quotes from 0!quotes];
  if[0 = count quotes; :select Sym, Trades, Quotes:0j from 0!trades];
  0!trades lj quotes
  };

/ -----------------------------------------------------------------------------
/ Aggregation Logic
/ -----------------------------------------------------------------------------

/ Last processed bucket
.tel.lastBucket:0Np;

/ Main computation function
.tel.compute:{[]
  now:.z.p;
  currentBucket:`timestamp$.tel.cfg.bucketNs * `long$now div .tel.cfg.bucketNs;
  bucket:currentBucket - .tel.cfg.bucketSpan;
  if[bucket <= .tel.lastBucket; :()];
  bucketStart:bucket;
  bucketEnd:bucket + .tel.cfg.bucketSpan;
  
  / Query trades from RDB
  query:"select from trade_binance where time >= ",string[bucketStart],", time < ",string[bucketEnd];
  trades:.tel.safeQuery[.tel.cfg.rdbPort; query];
  
  / Query quotes from RDB
  queryQuotes:"select from quote_binance where time >= ",string[bucketStart],", time < ",string[bucketEnd];
  quotes:.tel.safeQuery[.tel.cfg.rdbPort; queryQuotes];
  
  / Process trade FH latency stats
  if[0 < count trades;
    fhStatsTrade:select 
      parseUs_p50:.tel.percentile[0.5; fhParseUs], 
      parseUs_p95:.tel.percentile[0.95; fhParseUs], 
      parseUs_max:max fhParseUs, 
      sendUs_p50:.tel.percentile[0.5; fhSendUs], 
      sendUs_p95:.tel.percentile[0.95; fhSendUs], 
      sendUs_max:max fhSendUs, 
      cnt:count i 
      by sym from trades;
    fhStatsTrade:update bucket:bucket, handler:`trade_fh from fhStatsTrade;
    `telemetry_latency_fh insert `bucket xcols 0!fhStatsTrade;
  ];
  
  / Process quote FH latency stats
  if[0 < count quotes;
    fhStatsQuote:select 
      parseUs_p50:.tel.percentile[0.5; fhParseUs], 
      parseUs_p95:.tel.percentile[0.95; fhParseUs], 
      parseUs_max:max fhParseUs, 
      sendUs_p50:.tel.percentile[0.5; fhSendUs], 
      sendUs_p95:.tel.percentile[0.95; fhSendUs], 
      sendUs_max:max fhSendUs, 
      cnt:count i 
      by sym from quotes;
    fhStatsQuote:update bucket:bucket, handler:`quote_fh from fhStatsQuote;
    `telemetry_latency_fh insert `bucket xcols 0!fhStatsQuote;
  ];
  
  / E2E latency stats - for both trades AND quotes
  .tel.calcE2E[trades; bucket; `trade_fh];
  .tel.calcE2E[quotes; bucket; `quote_fh];
  
  .tel.lastBucket:bucket;
  
  / System stats - RDB
  rdbMem:.tel.safeQuery[.tel.cfg.rdbPort; ".Q.w[]"];
  if[0 < count rdbMem;
    `telemetry_system insert (bucket; `RDB; rdbMem[`used] % 1e6; rdbMem[`heap] % 1e6; rdbMem[`peak] % 1e6);
  ];
  
  / RTE stats
  rteMem:.tel.safeQuery[.tel.cfg.rtePort; ".Q.w[]"];
  if[0 < count rteMem;
    `telemetry_system insert (bucket; `RTE; rteMem[`used] % 1e6; rteMem[`heap] % 1e6; rteMem[`peak] % 1e6);
  ];
  
  / TEL stats
  telMem:.Q.w[];
  `telemetry_system insert (bucket; `TEL; telMem[`used] % 1e6; telMem[`heap] % 1e6; telMem[`peak] % 1e6);
  
  / Cleanup old data
  cutoff:now - .tel.cfg.retentionNs;
  delete from `telemetry_latency_fh where bucket < cutoff;
  delete from `telemetry_latency_e2e where bucket < cutoff;
  delete from `telemetry_system where bucket < cutoff;
  / Keep only recent health records (last 1 minute per handler)
  delete from `health_feed_handler where time < now - 00:01:00;
  };

/ -----------------------------------------------------------------------------
/ Startup
/ -----------------------------------------------------------------------------

system "p ",string .tel.cfg.port;
-1 "=======================================================";
-1 "TEL (Telemetry) starting on port ",string[.tel.cfg.port];
-1 "=======================================================";
-1 "Configuration:";
-1 "  Bucket size: ",string[.tel.cfg.bucketSec],"s";
-1 "  Retention: ",string[.tel.cfg.retentionMin]," minutes";
-1 "  Timer interval: ",string[.tel.cfg.timerMs],"ms";
-1 "  RDB port: ",string[.tel.cfg.rdbPort];
-1 "  RTE port: ",string[.tel.cfg.rtePort];
-1 "  TP port: ",string[.tel.cfg.tpPort];

/ Subscribe to TP for health updates
.tel.subscribe[];

/ Pre-warm persistent handles to RDB and RTE
-1 "";
-1 "Establishing persistent connections...";
rdbH:.tel.getH[.tel.cfg.rdbPort];
rteH:.tel.getH[.tel.cfg.rtePort];

/ Report connection status
-1 "";
-1 "Connection status:";
-1 "  RDB (port ",string[.tel.cfg.rdbPort],"): ",$[null rdbH; "FAILED - will retry on first query"; "OK (handle ",string[rdbH],")"];
-1 "  RTE (port ",string[.tel.cfg.rtePort],"): ",$[null rteH; "FAILED - will retry on first query"; "OK (handle ",string[rteH],")"];

/ Start timer for periodic computation
.z.ts:{.tel.compute[]};
system "t ",string .tel.cfg.timerMs;

-1 "";
-1 "Timer started (",string[.tel.cfg.timerMs],"ms interval)";
-1 "";
-1 "Query interface:";
-1 "  .tel.handleStatus[]           / View connection status";
-1 "  .tel.fhStatusTable[]          / Feed handler health (raw)";
-1 "  .tel.vsFhStatus[]             / Feed handler status (dashboard)";
-1 "  .tel.vsSystemResources[]      / System memory (dashboard)";
-1 "  .tel.vsDataVolume[]           / Data volume per symbol (dashboard)";
-1 "  telemetry_latency_fh          / FH latency metrics";
-1 "  telemetry_latency_e2e         / End-to-end latency (trades + quotes)";
-1 "  telemetry_system              / System memory stats";
-1 "";
-1 "TEL ready";
-1 "=======================================================";

/ -----------------------------------------------------------------------------
/ Graceful Shutdown
/ -----------------------------------------------------------------------------

/ Handle process exit - clean up all resources
.z.exit:{[code]
  -1 "";
  -1 "TEL shutting down (exit code: ",string[code],")...";
  .tel.closeAll[];
  -1 "TEL shutdown complete";
  };
