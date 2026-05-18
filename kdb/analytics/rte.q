/ rte.q - Real-Time Engine (Simplified)
/ VWAP, Daily Vol, Order Book, Smooth OBI
/ Subscribes to Chained TP (batched)
/ With 60-minute history for OBI and Volatility

/ =============================================================================
/ Configuration
/ =============================================================================

.rte.cfg.port:5015;
.rte.cfg.ctpPort:5014;                / Chained TP
.rte.cfg.logDir:"logs";
.rte.cfg.volSampleIntervalSec:30;     / How often we sample a price + compute one return (sec)
.rte.cfg.volMinReturns:10;            / Min returns before flagging vol as valid (was 30 with tick-rate sampling; lowered for 30s sampling)
.rte.cfg.volRollingWindowMin:60;      / Returns older than this are pruned from the vol stddev sample
.rte.cfg.obiAlpha:0.05;               / EMA smoothing (lower = smoother)
.rte.cfg.iVol:`BTCUSDT`ETHUSDT`SOLUSDT!45.00 65.00 67.00;  / 30-day implied vol (%)
.rte.cfg.historyRetentionMin:60;      / Keep 60 minutes of history
.rte.cfg.historyInsertIntervalNs:5000000000;  / History insert throttle: 5 seconds (in nanoseconds)

/ Connection resilience
.rte.conn.handle:0N;                   / CTP connection handle
.rte.conn.state:`disconnected;         / `disconnected`connecting`connected
.rte.conn.lastAttempt:0Np;             / Last connection attempt time
.rte.conn.retryCount:0;                / Consecutive failed attempts
.rte.conn.cfg.baseDelayMs:1000;        / Initial retry delay (1 sec)
.rte.conn.cfg.maxDelayMs:30000;        / Max retry delay (30 sec)
.rte.conn.cfg.backoffMultiplier:1.5;   / Exponential backoff factor

/ Timer interval
.rte.cfg.timerMs:5000;

system "g 0";

.proc.startTime:.z.p;

/ Derived config
.rte.cfg.volSampleIntervalNs:.rte.cfg.volSampleIntervalSec * 1000000000j;
.rte.cfg.volRollingWindowNs:.rte.cfg.volRollingWindowMin * 60 * 1000000000j;
/ Expected sample count when the rolling window is fully loaded. isValid
/ flips to 1b only when nReturns reaches this. With defaults: 60*60/30 = 120.
.rte.cfg.volTargetCount:`long$(.rte.cfg.volRollingWindowMin * 60) % .rte.cfg.volSampleIntervalSec;
.rte.cfg.historyRetentionNs:.rte.cfg.historyRetentionMin * 60 * 1000000000j;

/ Statistics
.rte.stats.tradesProcessed:0j;
.rte.stats.quotesProcessed:0j;

/ =============================================================================
/ Schema and derived field indices
/ =============================================================================
/ RTE declares local trade/quote schemas matching what CTP forwards (with TP's
/ receive timestamp). Field positions are derived from the schemas as
/ name->index dictionaries, so column reordering is automatically picked up.

\l ../schemas.q

trade_binance:.schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo];
quote_binance:.schema.extend[.schema.quote; `tpRecvTimeUtcNs`tpSeqNo];

.rte.idx.trade:(cols trade_binance)!til count cols trade_binance;
.rte.idx.quote:(cols quote_binance)!til count cols quote_binance;

/ Pre-compute composite index lists used in the hot path
.rte.idx.bidQtyAll:.rte.idx.quote each `bidQty1`bidQty2`bidQty3`bidQty4`bidQty5;
.rte.idx.askQtyAll:.rte.idx.quote each `askQty1`askQty2`askQty3`askQty4`askQty5;
.rte.idx.bookAll:.rte.idx.quote each
  `bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5,
  `bidQty1`bidQty2`bidQty3`bidQty4`bidQty5,
  `askPrice1`askPrice2`askPrice3`askPrice4`askPrice5,
  `askQty1`askQty2`askQty3`askQty4`askQty5;

/ =============================================================================
/ State Initialization
/ =============================================================================

/ VWAP - sym -> (sumPxQty; sumQty; lastPrice; tradeCount)
.rte.st.vwap:()!();

/ Volatility - non-overlapping sampled returns
/ Sample one return every volSampleIntervalSec; keep returns within
/ volRollingWindowMin; stddev across those is the realized vol estimator.
.rte.st.volLastSample:()!();   / sym -> (time; price) of last successful sample
.rte.st.volReturns:()!();      / sym -> list of (time; return) pairs, pruned to volRollingWindowMin
.rte.st.volLatest:()!();       / sym -> (annualizedVol; returnCount; isValid)
.rte.st.volLastInsert:()!();   / sym -> last history insert time (throttle)

/ Order Book - L5 snapshot
.rte.st.book:()!();        / sym -> L2 data

/ OBI - EMA smoothed
.rte.st.obi:()!();         / sym -> (rawOBI; smoothOBI; time)
.rte.st.obiLastInsert:()!();  / sym -> last history insert time (throttle)

/ =============================================================================
/ History Tables (60 min retention)
/ =============================================================================

/ OBI History
obi_history:([]
  time:`timestamp$();
  sym:`symbol$();
  rawOBI:`float$();
  smoothOBI:`float$()
  );

/ Volatility History
vol_history:([]
  time:`timestamp$();
  sym:`symbol$();
  annualizedVol:`float$()
  );

/ =============================================================================
/ Connection Management (Resilient)
/ =============================================================================

/ Calculate next retry delay with exponential backoff
.rte.conn.getDelay:{[]
  / Cast to long AFTER the multiplication (was: cast multiplier first, which
  / truncated 1.5^N to int, producing degenerate sequence 1s, 1s, 2s, 3s, 5s
  / instead of smooth 1s, 1.5s, 2.25s, 3.375s, ...)
  delay:`long$ .rte.conn.cfg.baseDelayMs * .rte.conn.cfg.backoffMultiplier xexp .rte.conn.retryCount;
  delay & .rte.conn.cfg.maxDelayMs  / Cap at max
  };

/ Check if enough time has passed since last attempt
.rte.conn.canRetry:{[]
  if[null .rte.conn.lastAttempt; :1b];
  elapsed:`long$(.z.p - .rte.conn.lastAttempt) % 1000000;  / ms
  elapsed >= .rte.conn.getDelay[]
  };

/ Main connection function - NEVER THROWS
.rte.connect:{[]
  / Guard: already connected
  if[not null .rte.conn.handle; :1b];
  
  / Guard: backoff not elapsed
  if[not .rte.conn.canRetry[]; :0b];
  
  .rte.conn.state:`connecting;
  .rte.conn.lastAttempt:.z.p;
  
  -1 "RTE: Connecting to Chained TP on port ",string[.rte.cfg.ctpPort],
     " (attempt ",string[.rte.conn.retryCount + 1],")...";
  
  / Protected connection attempt
  h:@[hopen; `$"::",string[.rte.cfg.ctpPort]; {[err] -1 "RTE: Connection failed - ",err; 0N}];
  
  if[null h;
    .rte.conn.retryCount+:1;
    .rte.conn.state:`disconnected;
    nextDelay:.rte.conn.getDelay[];
    -1 "RTE: Will retry in ",string[nextDelay],"ms";
    :0b
  ];
  
  / Connection successful - now subscribe
  subResult:@[{[h]
    -1 "RTE: Subscribing to trade_binance...";
    res:h(`pubsub.subscribe; `trade_binance; `);
    -1 "RTE: Subscribed to trade_binance";
    -1 "RTE: Subscribing to quote_binance...";
    res:h(`pubsub.subscribe; `quote_binance; `);
    -1 "RTE: Subscribed to quote_binance";
    1b
  }; h; {[err] -1 "RTE: Subscription failed - ",err; 0b}];
  
  if[not subResult;
    @[hclose; h; {}];  / Clean up failed connection
    .rte.conn.retryCount+:1;
    .rte.conn.state:`disconnected;
    :0b
  ];
  
  / Success - update state
  .rte.conn.handle:h;
  .rte.conn.state:`connected;
  .rte.conn.retryCount:0;
  -1 "RTE: Connected successfully (handle ",string[h],")";
  1b
  };

/ Disconnect handler
.z.pc:{[h]
  if[h = .rte.conn.handle;
    -1 "RTE: Chained TP disconnected (handle ",string[h],")";
    .rte.conn.handle:0N;
    .rte.conn.state:`disconnected;
    .rte.conn.retryCount:0;  / Reset backoff on disconnect
    -1 "RTE: Will attempt reconnection on next timer tick";
  ];
  };

/ =============================================================================
/ VWAP - Cumulative Daily
/ =============================================================================

.rte.updVwap:{[s;price;qty]
  pxqty:price * qty;
  $[s in key .rte.st.vwap;
    .rte.st.vwap[s]+:(pxqty; qty; 0f; 1j);
    .rte.st.vwap[s]:(pxqty; qty; price; 1j)
  ];
  .rte.st.vwap[s;2]:price;  / Update last price
  };

.rte.getVwap:{[]
  if[0 = count .rte.st.vwap; :flip `sym`vwap`lastPrice`volume`trades!(`$();`float$();`float$();`float$();`long$())];
  syms:key .rte.st.vwap;
  data:value .rte.st.vwap;
  vwaps:{$[x[1]>0f; x[0]%x[1]; 0n]} each data;
  ([] sym:syms; vwap:vwaps; lastPrice:data[;2]; volume:data[;1]; trades:data[;3])
  };

/ =============================================================================
/ Daily Volatility - Rolling 30-sec Returns
/ =============================================================================

.rte.updVol:{[s;time;price]
  / Initialize on first trade we see for this symbol. We need a baseline
  / before we can compute any return, so first trade just stores the
  / baseline and exits.
  if[not s in key .rte.st.volLastSample;
    .rte.st.volLastSample[s]: (time; price);
    :()
  ];

  / Has enough time elapsed since the last sample?
  / If not, this trade is inside the current sample window - skip it.
  / This is the core of non-overlapping sampling: only the FIRST trade
  / at or after `lastTime + sampleIntervalNs` becomes the next sample.
  lastSample: .rte.st.volLastSample[s];
  lastTime: lastSample[0];
  if[(time - lastTime) < .rte.cfg.volSampleIntervalNs; :()];

  / This trade crosses the sample boundary. Compute one log return
  / against the previous sample.
  lastPrice: lastSample[1];
  if[(null lastPrice) or lastPrice <= 0; :()];

  ret: log price % lastPrice;
  if[(null ret) or (ret = 0w) or (ret = -0w); :()];

  / Advance the sample marker. Subsequent trades within the next
  / sampleIntervalNs window will be ignored until we cross again.
  .rte.st.volLastSample[s]: (time; price);

  / Store the return with its timestamp so we can prune by clock time.
  $[s in key .rte.st.volReturns;
    .rte.st.volReturns[s],:enlist (time; ret);
    .rte.st.volReturns[s]:enlist (time; ret)
  ];

  / Prune returns older than the rolling window. Strict `>` (not `>=`) so
  / the count is stable at exactly volTargetCount once steady state is
  / reached - inclusive prune would oscillate between target and target+1
  / at the boundary.
  retCutoff: time - .rte.cfg.volRollingWindowNs;
  .rte.st.volReturns[s]: .rte.st.volReturns[s] where .rte.st.volReturns[s][;0] > retCutoff;

  / At steady state, count stabilizes at volTargetCount (= 120 with
  / defaults). isValid=1b only once the window is fully loaded.
  nReturns: count .rte.st.volReturns[s];
  if[nReturns >= .rte.cfg.volMinReturns;
    retsOnly: .rte.st.volReturns[s][;1];
    / Annualize: periods per year = (365 days * 24h * 60m * 60s) / sampleIntervalSec
    periodsPerYear: 31536000 % .rte.cfg.volSampleIntervalSec;
    vol: sqrt[periodsPerYear] * dev retsOnly;
    isValid: nReturns >= .rte.cfg.volTargetCount;
    if[(not null vol) and vol > 0;
      .rte.st.volLatest[s]: (vol; nReturns; isValid);
      / Throttled history insert: only every 5 seconds
      lastIns: $[s in key .rte.st.volLastInsert; .rte.st.volLastInsert[s]; 0Np];
      if[(null lastIns) or (time - lastIns) >= .rte.cfg.historyInsertIntervalNs;
        `vol_history insert (time; s; 100*vol);
        .rte.st.volLastInsert[s]: time;
      ];
    ];
  ];
  };

.rte.getVol:{[]
  if[0 = count .rte.st.volLatest; :flip `sym`annualizedVol`returnCount`isValid!(`$();`float$();`long$();`boolean$())];
  syms:key .rte.st.volLatest;
  data:value .rte.st.volLatest;
  ([] sym:syms; annualizedVol:100*data[;0]; returnCount:data[;1]; isValid:data[;2])
  };

.rte.getVolComparison:{[]
  if[0 = count .rte.st.volLatest; :flip `sym`realizedVol`impliedVol`volDiff!(`$();`float$();`float$();`float$())];
  syms:key .rte.st.volLatest;
  data:value .rte.st.volLatest;
  rVols:100*data[;0];
  iVols:.rte.cfg.iVol[syms];
  ([] sym:syms; realizedVol:rVols; impliedVol:iVols; volDiff:rVols - iVols)
  };

/ =============================================================================
/ Summary View
/ =============================================================================

.rte.getSummary:{[]
  if[0 = count .rte.st.vwap; :flip `sym`lastPrice`vwap`volume`trades`annualizedVol!(`$();`float$();`float$();`float$();`long$();`float$())];
  
  vwapTab:.rte.getVwap[];
  volTab:.rte.getVol[];
  
  / Left join vol onto vwap
  result:vwapTab lj `sym xkey select sym, annualizedVol from volTab;
  
  / Fill nulls
  update annualizedVol:0n from result where null annualizedVol
  };

/ =============================================================================
/ Order Book - L5 Snapshot
/ =============================================================================

.rte.updBook:{[s;data;time]
  / Capture L5 book using derived field positions (see .rte.idx.bookAll above).
  / Internal book layout: bid1-5, bidQty1-5, ask1-5, askQty1-5 (20 elements).
  / This layout is then read back by .rte.getOrderBook and .rte.getSpread.
  .rte.st.book[s]:data .rte.idx.bookAll;
  };

.rte.getOrderBook:{[s]
  if[not s in key .rte.st.book; :flip `bidQty`bid`ask`askQty!(`float$();`float$();`float$();`float$())];
  d:.rte.st.book[s];
  ([] bidQty:d 5 6 7 8 9; bid:d 0 1 2 3 4; ask:d 10 11 12 13 14; askQty:d 15 16 17 18 19)
  };

.rte.getSpread:{[s]
  if[not s in key .rte.st.book; :`sym`bid`ask`spread`mid!(s;0n;0n;0n;0n)];
  d:.rte.st.book[s];
  bid:d 0; ask:d 10;
  / NOTE on mid formula: q has NO operator precedence and evaluates strictly
  / right-to-left, so `0.5*bid+ask` parses as `0.5 * (bid+ask)` - the correct
  / midpoint. A reader applying Python/C precedence would see this as
  / `(0.5*bid)+ask` (incorrect), but in q that would require explicit parens.
  / Verify: q) 0.5*100+200 -> 150 (not 250).
  `sym`bid`ask`spread`mid!(s; bid; ask; ask-bid; 0.5*bid+ask)
  };

/ =============================================================================
/ OBI - Order Book Imbalance with EMA Smoothing
/ =============================================================================

.rte.updOBI:{[s;bidDepth;askDepth;time]
  total:bidDepth + askDepth;
  rawOBI:$[total > 0f; (bidDepth - askDepth) % total; 0n];
  
  / EMA: smooth = alpha * raw + (1-alpha) * prevSmooth
  prevSmooth:$[s in key .rte.st.obi; .rte.st.obi[s;1]; rawOBI];
  smoothOBI:(.rte.cfg.obiAlpha * rawOBI) + ((1 - .rte.cfg.obiAlpha) * prevSmooth);
  
  .rte.st.obi[s]:(rawOBI; smoothOBI; time);
  
  / Throttled history insert: only every 5 seconds
  lastIns:$[s in key .rte.st.obiLastInsert; .rte.st.obiLastInsert[s]; 0Np];
  if[(null lastIns) or (time - lastIns) >= .rte.cfg.historyInsertIntervalNs;
    `obi_history insert (time; s; rawOBI; smoothOBI);
    .rte.st.obiLastInsert[s]:time;
  ];
  };

.rte.getOBI:{[mode]
  / Default to smoothed OBI when called with no arg (.rte.getOBI[]).
  / Without this, `:: = `smooth` throws 'type since q can't compare a
  / generic null to a symbol.
  if[mode ~ (::); mode: `smooth];
  if[0 = count .rte.st.obi; :flip `sym`OBI!(`$();`float$())];
  syms:key .rte.st.obi;
  data:value .rte.st.obi;
  obi:$[mode=`smooth; data[;1]; data[;0]];
  ([] sym:syms; OBI:obi)
  };

.rte.getOBIAll:{[]
  if[0 = count .rte.st.obi; :flip `sym`rawOBI`smoothOBI!(`$();`float$();`float$())];
  syms:key .rte.st.obi;
  data:value .rte.st.obi;
  ([] sym:syms; rawOBI:data[;0]; smoothOBI:data[;1])
  };

/ =============================================================================
/ History Query Functions
/ =============================================================================

/ Get OBI history for charting
/ @param s - symbol (` for all symbols; default `)
/ @param minutes - minutes of history to retrieve (default 60)
.rte.getOBIHistory:{[s;minutes]
  if[s ~ (::); s: `];
  if[minutes ~ (::); minutes: 60];
  cutoff:.z.p - `long$minutes * 60000000000;
  $[s ~ `;
    select time, sym, rawOBI, smoothOBI from obi_history where time > cutoff;
    select time, sym, rawOBI, smoothOBI from obi_history where sym = s, time > cutoff
  ]
  };

/ Get Volatility history for charting
/ @param s - symbol (` for all symbols; default `)
/ @param minutes - minutes of history to retrieve (default 60)
.rte.getVolHistory:{[s;minutes]
  if[s ~ (::); s: `];
  if[minutes ~ (::); minutes: 60];
  cutoff:.z.p - `long$minutes * 60000000000;
  $[s ~ `;
    select time, sym, annualizedVol from vol_history where time > cutoff;
    select time, sym, annualizedVol from vol_history where sym = s, time > cutoff
  ]
  };

/ =============================================================================
/ History Cleanup
/ =============================================================================

.rte.cleanupHistory:{[]
  cutoff:.z.p - .rte.cfg.historyRetentionNs;
  delete from `obi_history where time < cutoff;
  delete from `vol_history where time < cutoff;
  };

/ =============================================================================
/ Health Check
/ =============================================================================

/ Standardized health check (consistent across all processes)
.health:{[]
  / Determine status based on connection state
  st:$[.rte.conn.state = `connected; `ok;
       .rte.conn.state = `connecting; `degraded;
       `disconnected];
  
  `process`port`uptime`status`connState`retryCount`memMB`tradesProcessed`quotesProcessed`symbols!(
    `rte;
    .rte.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    .rte.conn.state;
    .rte.conn.retryCount;
    (`long$.Q.w[][`used]) % 1000000;
    .rte.stats.tradesProcessed;
    .rte.stats.quotesProcessed;
    count key .rte.st.vwap
  )
  };

/ =============================================================================
/ Update Handler
/ =============================================================================

upd:{[tbl;data]
  / Handle batch (table) from chained TP
  if[98h = type data;
    {[tbl;row] upd[tbl;value row]} [tbl] each data;
    :();
  ];
  
  if[tbl = `trade_binance;
    time :data .rte.idx.trade`time;
    s    :data .rte.idx.trade`sym;
    price:data .rte.idx.trade`price;
    qty  :data .rte.idx.trade`qty;
    .rte.updVwap[s; price; qty];
    .rte.updVol[s; time; price];
    .rte.stats.tradesProcessed+:1;
  ];
  
  if[tbl = `quote_binance;
    time:data .rte.idx.quote`time;
    s   :data .rte.idx.quote`sym;
    .rte.updBook[s; data; time];
    bidDepth:sum data .rte.idx.bidQtyAll;
    askDepth:sum data .rte.idx.askQtyAll;
    .rte.updOBI[s; bidDepth; askDepth; time];
    .rte.stats.quotesProcessed+:1;
  ];
  };

.u.upd:upd;

/ =============================================================================
/ Log Replay
/ =============================================================================

.rte.logFile:{[d] hsym `$(.rte.cfg.logDir,"/",string[d],".log")};

.rte.logExists:{[f] 0 < @[hcount; f; 0j]};

.rte.replay:{[d]
  if[d ~ (::); d:.z.D];
  f:.rte.logFile[d];
  if[not .rte.logExists[f]; -1 "RTE: No log for ",string[d]; :0j];
  -1 "RTE: Replaying ",string[f];
  n:-11!f;
  -1 "RTE: Replayed ",string[n]," chunks";
  n
  };

/ =============================================================================
/ End-of-Day Handler
/ =============================================================================

endofday:{[]
  -1 "RTE: EOD received";
  .rte.st.vwap:()!();
  .rte.st.volLastSample:()!();
  .rte.st.volReturns:()!();
  .rte.st.volLatest:()!();
  .rte.st.volLastInsert:()!();
  .rte.st.book:()!();
  .rte.st.obi:()!();
  .rte.st.obiLastInsert:()!();
  .rte.stats.tradesProcessed:0j;
  .rte.stats.quotesProcessed:0j;
  / Clear history tables
  delete from `obi_history;
  delete from `vol_history;
  -1 "RTE: State cleared";
  };

/ =============================================================================
/ Timer - Reconnect + Cleanup
/ =============================================================================

.z.ts:{[]
  / Attempt reconnection if disconnected
  if[null .rte.conn.handle; .rte.connect[]];
  / Cleanup old history
  .rte.cleanupHistory[];
  };

/ =============================================================================
/ Startup
/ =============================================================================

system "p ",string .rte.cfg.port;

-1 "=======================================================";
-1 "RTE (Real-Time Engine) on port ",string[.rte.cfg.port];
-1 "=======================================================";
-1 "Configuration:";
-1 "  Chained TP: ",string[.rte.cfg.ctpPort];
-1 "  Vol sample interval: ",string[.rte.cfg.volSampleIntervalSec],"s";
-1 "  Vol rolling window: ",string[.rte.cfg.volRollingWindowMin]," min";
-1 "  Vol min returns: ",string[.rte.cfg.volMinReturns]," (before showing any number)";
-1 "  Vol target count: ",string[.rte.cfg.volTargetCount]," (before isValid=1)";
-1 "  OBI alpha: ",string[.rte.cfg.obiAlpha];
-1 "  History retention: ",string[.rte.cfg.historyRetentionMin]," minutes";
-1 "  History insert interval: ",string[`long$.rte.cfg.historyInsertIntervalNs % 1000000000],"s";
-1 "";
-1 "Connection Settings:";
-1 "  Base retry delay: ",string[.rte.conn.cfg.baseDelayMs],"ms";
-1 "  Max retry delay: ",string[.rte.conn.cfg.maxDelayMs],"ms";
-1 "";

/ Replay log if exists
if[.rte.logExists[.rte.logFile[.z.D]]; -1 "RTE: Found log for today - replaying..."; .rte.replay[.z.D]];

/ Attempt initial connection (non-blocking)
connected:.rte.connect[];

/ Start timer for reconnection + cleanup
system "t ",string .rte.cfg.timerMs;

-1 "";
-1 "Query Interface:";
-1 "  .health[]               / Standardized health check";
-1 "  .rte.getVwap[]          / VWAP all symbols";
-1 "  .rte.getSummary[]       / Summary (lastPrice, vwap, volume, trades)";
-1 "  .rte.getVol[]           / Daily vol (annualized %)";
-1 "  .rte.getVolComparison[] / Vol vs implied vol";
-1 "  .rte.getOrderBook[`SYM] / L2 order book";
-1 "  .rte.getSpread[`SYM]    / Bid/ask/spread/mid";
-1 "  .rte.getOBI[`smooth]    / Smooth OBI";
-1 "  .rte.getOBI[`raw]       / Raw OBI";
-1 "  .rte.getOBIAll[]        / Both raw and smooth OBI";
-1 "  .rte.replay[.z.D]       / Replay today's log";
-1 "";
-1 "History Interface:";
-1 "  .rte.getOBIHistory[`BTCUSDT;30]  / OBI history (30 min)";
-1 "  .rte.getOBIHistory[`;60]         / All symbols (60 min)";
-1 "  .rte.getVolHistory[`BTCUSDT;30]  / Vol history (30 min)";
-1 "  .rte.getVolHistory[`;60]         / All symbols (60 min)";
-1 "  obi_history              / Raw OBI history table";
-1 "  vol_history              / Raw vol history table";
-1 "";

$[connected;-1 "RTE: Ready and processing"; -1 "RTE: Started in DEGRADED mode - waiting for CTP connection"];
-1 "=======================================================";
