/ mle.q - Machine Learning Engine (Optimized)
/ With resilient connection handling

/ =============================================================================
/ 1. Configuration
/ =============================================================================

.mle.cfg.port:5032;
.mle.cfg.tpPort:5010;

/ Target bars per day (used to bootstrap initial threshold)
/ Crypto trades 24/7, so ~50 bars/day = 1 bar every ~30 min
.mle.cfg.targetBarsPerDay:50;

/ EWMA decay factors (lower = more stable, higher = more reactive)
.mle.cfg.alpha.threshold:0.02;   / Threshold adaptation (slow)
.mle.cfg.alpha.imbalance:0.05;   / Imbalance ratio adaptation (medium)

/ Initial bootstrap values (will adapt quickly)
/ Per-symbol thresholds based on typical dollar volume
/ Rule of thumb: threshold ≈ 0.1% of daily dollar volume for ~100 bars/day
.mle.cfg.init.threshold:`BTCUSDT`ETHUSDT`SOLUSDT!100000 75000 40000f;
.mle.cfg.init.thresholdDefault:50000.0;   / Fallback for unknown symbols
.mle.cfg.init.imbalRatio:0.2;             / Expected |2P(buy)-1| ~ 20% imbalance

/ Bar retention (minutes)
.mle.cfg.barRetentionMin:1440;     / Keep 24 hours of bars

/ Garbage collection: deferred (0) for lower latency
system "g 0";

.proc.startTime:.z.p;

/ Derived config
.mle.cfg.barRetentionNs:.mle.cfg.barRetentionMin * 60 * 1000000000j;

/ Cleanup interval
.mle.cfg.cleanupIntervalMs:30000;  / Every 30 seconds

/ =============================================================================
/ 2. Connection Configuration (Resilient)
/ =============================================================================

.mle.conn.handle:0N;                    / TP connection handle
.mle.conn.state:`disconnected;          / `disconnected`connecting`connected
.mle.conn.lastAttempt:0Np;              / Last connection attempt time
.mle.conn.retryCount:0;                 / Consecutive failed attempts
.mle.conn.cfg.baseDelayMs:1000;         / Initial retry delay (1 sec)
.mle.conn.cfg.maxDelayMs:30000;         / Max retry delay (30 sec)
.mle.conn.cfg.backoffMultiplier:1.5;    / Exponential backoff factor

/ =============================================================================
/ 3. Position Signal Configuration
/ =============================================================================

.mle.cfg.signal.buyThreshold:65.0;    / Enter long if buyDollarPct > 65%
.mle.cfg.signal.sellThreshold:35.0;   / Enter short if buyDollarPct < 35%

/ =============================================================================
/ 4. Schema Definitions
/ =============================================================================

/ Dollar Imbalance Bars - samples when cumulative signed dollar flow exceeds threshold
/ Captures net buy/sell pressure weighted by economic significance
dib_bars:([]
    time:`timestamp$();           / Bar close time
    sym:`symbol$();               / Symbol
    open:`float$();               / Open price
    high:`float$();               / High price
    low:`float$();                / Low price
    close:`float$();              / Close price
    volume:`float$();             / Total volume (base currency)
    dollarVolume:`float$();       / Total dollar volume
    ticks:`long$();               / Number of trades in bar
    theta:`float$();              / Final imbalance (signed dollars)
    threshold:`float$();          / Threshold that triggered bar
    buyDollarPct:`float$();       / % of dollar volume from buys
    duration:`long$()             / Bar duration in milliseconds
    );

/ Dollar Runs Bars - samples when consecutive same-direction dollar flow exceeds threshold  
/ Captures persistent one-sided activity (accumulation/distribution)
drb_bars:([]
    time:`timestamp$();           / Bar close time
    sym:`symbol$();               / Symbol
    open:`float$();               / Open price
    high:`float$();               / High price
    low:`float$();                / Low price
    close:`float$();              / Close price
    volume:`float$();             / Total volume
    dollarVolume:`float$();       / Total dollar volume
    ticks:`long$();               / Number of trades in bar
    maxRun:`float$();             / Max run value (dollars) that triggered
    runDirection:`short$();       / 1 = buy run, -1 = sell run
    threshold:`float$();          / Threshold that triggered bar
    duration:`long$()             / Bar duration in milliseconds
    );

/ Position change history
positions:([]
    time:`timestamp$();           / Position change time
    sym:`symbol$();               / Symbol
    position:`short$();           / New position: -1, 0, or 1
    price:`float$();              / Price at position change
    trigger:`float$();            / buyDollarPct that triggered change
    dollarVolume:`float$();       / Bar dollar volume
    ticks:`long$()                / Bar tick count
    );

/ =============================================================================
/ 5. State Management - VECTOR-BASED (O(1) access)
/ =============================================================================

/ Symbol registry - single hash lookup per trade
.mle.syms:`$();                   / List of symbols (index = position)
.mle.symIdx:()!`long$();          / Symbol -> index mapping

/ State vectors (all indexed by symbol position)
/ Price tracking (for tick rule)
.mle.lastPrice:`float$();         / Last trade price
.mle.lastBt:`long$();             / Last tick direction (+1/-1)

/ Intra-bar accumulators
.mle.tickCount:`long$();          / Trades in current bar
.mle.theta:`float$();             / Cumulative signed dollar imbalance
.mle.buyRun:`float$();            / Current buy run (dollars)
.mle.sellRun:`float$();           / Current sell run (dollars)
.mle.cumVol:`float$();            / Cumulative volume
.mle.cumDollarVol:`float$();      / Cumulative dollar volume
.mle.buyDollarVol:`float$();      / Buy-side dollar volume
.mle.openPrice:`float$();         / Bar open price
.mle.highPrice:`float$();         / Bar high price
.mle.lowPrice:`float$();          / Bar low price
.mle.barStartTime:`timestamp$();  / Bar start timestamp

/ Adaptive threshold state (EWMA)
.mle.ewmaThreshold:`float$();     / Expected threshold (adapts to actual bar sizes)
.mle.ewmaImbalRatio:`float$();    / Expected imbalance ratio |theta/dollarVol|

/ Position state (per symbol)
.mle.position:`short$();          / Current position: -1, 0, or 1
.mle.positionTime:`timestamp$();  / Timestamp of last position change
.mle.positionPrice:`float$();     / Price at last position change

/ =============================================================================
/ 6. Core Logic
/ =============================================================================

/ -----------------------------------------------------------------------------
/ 6.1 Symbol Registration - Get index for symbol (creates if new)
/ -----------------------------------------------------------------------------
.mle.getIdx:{[s]
    / Fast path: symbol already registered
    if[s in key .mle.symIdx; :.mle.symIdx[s]];
    
    / Slow path: new symbol - register and initialize
    idx:count .mle.syms;
    .mle.syms,:s;
    .mle.symIdx[s]:idx;
    
    / Extend all vectors with initial values
    .mle.lastPrice,:0n;
    .mle.lastBt,:0N;
    .mle.tickCount,:0j;
    .mle.theta,:0f;
    .mle.buyRun,:0f;
    .mle.sellRun,:0f;
    .mle.cumVol,:0f;
    .mle.cumDollarVol,:0f;
    .mle.buyDollarVol,:0f;
    .mle.openPrice,:0n;
    .mle.highPrice,:0n;
    .mle.lowPrice,:0n;
    .mle.barStartTime,:0Np;
    
    / Per-symbol threshold lookup with fallback
    initThresh:$[s in key .mle.cfg.init.threshold;
                 .mle.cfg.init.threshold[s];
                 .mle.cfg.init.thresholdDefault];
    .mle.ewmaThreshold,:initThresh;
    .mle.ewmaImbalRatio,:.mle.cfg.init.imbalRatio;
    
    / Position state
    .mle.position,:0h;
    .mle.positionTime,:0Np;
    .mle.positionPrice,:0n;
    
    idx
    };

/ -----------------------------------------------------------------------------
/ 6.2 Initialize Bar (for new symbol or after bar emission)
/ -----------------------------------------------------------------------------
.mle.initBar:{[idx;p;t]
    .mle.tickCount[idx]:0j;
    .mle.theta[idx]:0f;
    .mle.buyRun[idx]:0f;
    .mle.sellRun[idx]:0f;
    .mle.cumVol[idx]:0f;
    .mle.cumDollarVol[idx]:0f;
    .mle.buyDollarVol[idx]:0f;
    .mle.openPrice[idx]:p;
    .mle.highPrice[idx]:p;
    .mle.lowPrice[idx]:p;
    .mle.barStartTime[idx]:t;
    };

/ -----------------------------------------------------------------------------
/ 6.3 Position Logic - Check and update position based on bar
/ -----------------------------------------------------------------------------
.mle.checkPosition:{[idx;s;t;price;buyPct;dollarVol;ticks]
    / Determine new position based on thresholds
    newPos:$[buyPct > .mle.cfg.signal.buyThreshold; 1h;
             buyPct < .mle.cfg.signal.sellThreshold; -1h;
             .mle.position[idx]];  / No change if between thresholds
    
    / Only act if position changed
    if[newPos <> .mle.position[idx];
        / Update state
        .mle.position[idx]:newPos;
        .mle.positionTime[idx]:t;
        .mle.positionPrice[idx]:price;
        
        / Record position change
        `positions insert (t; s; newPos; price; buyPct; dollarVol; ticks);
    ];
    };

/ -----------------------------------------------------------------------------
/ 6.4 Emit Dollar Imbalance Bar
/ -----------------------------------------------------------------------------
.mle.emitDIB:{[idx;s;p;t;threshold]
    duration:`long$(t - .mle.barStartTime[idx]) % 1000000j;  / ms
    buyPct:$[.mle.cumDollarVol[idx] > 0; 
             100 * .mle.buyDollarVol[idx] % .mle.cumDollarVol[idx]; 
             50f];
    
    `dib_bars insert (
        t;                          / time
        s;                          / sym
        .mle.openPrice[idx];        / open
        .mle.highPrice[idx];        / high
        .mle.lowPrice[idx];         / low
        p;                          / close
        .mle.cumVol[idx];           / volume
        .mle.cumDollarVol[idx];     / dollarVolume
        .mle.tickCount[idx];        / ticks
        .mle.theta[idx];            / theta
        threshold;                  / threshold
        buyPct;                     / buyDollarPct
        duration                    / duration
        );
    
    / Check position after bar emission
    .mle.checkPosition[idx; s; t; p; buyPct; .mle.cumDollarVol[idx]; .mle.tickCount[idx]];
    
    / Update adaptive threshold using actual bar dollar volume
    actualBarDollar:.mle.cumDollarVol[idx];
    actualImbalRatio:$[actualBarDollar > 0; abs[.mle.theta[idx]] % actualBarDollar; .mle.ewmaImbalRatio[idx]];
    
    / EWMA update for threshold (based on dollar volume that triggered)
    .mle.ewmaThreshold[idx]:(.mle.cfg.alpha.threshold * actualBarDollar * actualImbalRatio) + 
                          (1 - .mle.cfg.alpha.threshold) * .mle.ewmaThreshold[idx];
    
    / EWMA update for imbalance ratio
    .mle.ewmaImbalRatio[idx]:(.mle.cfg.alpha.imbalance * actualImbalRatio) + 
                           (1 - .mle.cfg.alpha.imbalance) * .mle.ewmaImbalRatio[idx];
    };

/ -----------------------------------------------------------------------------
/ 6.5 Emit Dollar Runs Bar
/ -----------------------------------------------------------------------------
.mle.emitDRB:{[idx;s;p;t;maxRun;direction;threshold]
    duration:`long$(t - .mle.barStartTime[idx]) % 1000000j;
    
    `drb_bars insert (
        t;                          / time
        s;                          / sym
        .mle.openPrice[idx];        / open
        .mle.highPrice[idx];        / high
        .mle.lowPrice[idx];         / low
        p;                          / close
        .mle.cumVol[idx];           / volume
        .mle.cumDollarVol[idx];     / dollarVolume
        .mle.tickCount[idx];        / ticks
        maxRun;                     / maxRun
        direction;                  / runDirection
        threshold;                  / threshold
        duration                    / duration
        );
    };

/ -----------------------------------------------------------------------------
/ 6.6 Main Trade Handler - HOT PATH (Optimized)
/ -----------------------------------------------------------------------------
.mle.onTrade:{[s;p;q;t]
    / Get index - SINGLE hash lookup
    idx:.mle.getIdx[s];
    
    / Tick rule - O(1) array access
    lastP:.mle.lastPrice[idx];
    bt:$[null lastP; 1j;                           / First trade assumed buy
         p > lastP; 1j;                            / Price up = buy
         p < lastP; -1j;                           / Price down = sell
         .mle.lastBt[idx]];                        / Unchanged = previous
    
    / Update price/direction - O(1)
    .mle.lastPrice[idx]:p;
    .mle.lastBt[idx]:bt;
    
    / Initialize bar if needed (first trade for this symbol today)
    if[null .mle.barStartTime[idx]; .mle.initBar[idx;p;t]];
    
    / Compute dollar value of this trade
    dollarValue:p * q;
    signedDollar:bt * dollarValue;
    
    / Update intra-bar accumulators - all O(1)
    .mle.tickCount[idx]+:1j;
    .mle.cumVol[idx]+:q;
    .mle.cumDollarVol[idx]+:dollarValue;
    .mle.theta[idx]+:signedDollar;
    
    / Update OHLC - O(1)
    if[p > .mle.highPrice[idx]; .mle.highPrice[idx]:p];
    if[p < .mle.lowPrice[idx]; .mle.lowPrice[idx]:p];
    
    / Update buy/sell dollar tracking - O(1)
    if[bt = 1j; .mle.buyDollarVol[idx]+:dollarValue];
    
    / Update runs (consecutive same-direction dollar flow) - O(1)
    $[bt = 1j;
        [.mle.buyRun[idx]+:dollarValue; .mle.sellRun[idx]:0f];
        [.mle.sellRun[idx]+:dollarValue; .mle.buyRun[idx]:0f]
    ];
    
    / Calculate dynamic threshold - O(1)
    threshold:.mle.ewmaThreshold[idx] | 1000f;  / Floor at $1000
    
    / DIB Check: Emit bar if dollar imbalance exceeds threshold
    if[abs[.mle.theta[idx]] >= threshold;
        .mle.emitDIB[idx;s;p;t;threshold];
        .mle.initBar[idx;p;t];
        :();  / Exit early after DIB emission
    ];
    
    / DRB Check: Emit bar if dollar run exceeds threshold
    maxRun:.mle.buyRun[idx] | .mle.sellRun[idx];
    if[maxRun >= threshold;
        direction:$[.mle.buyRun[idx] >= .mle.sellRun[idx]; 1h; -1h];
        .mle.emitDRB[idx;s;p;t;maxRun;direction;threshold];
        .mle.initBar[idx;p;t];
    ];
    };

/ =============================================================================
/ 7. Query Interface
/ =============================================================================

/ Standardized health check (consistent across all processes)
.health:{[]
  / Determine status based on connection state
  st:$[.mle.conn.state = `connected; `ok;
       .mle.conn.state = `connecting; `degraded;
       `disconnected];
  
  `process`port`uptime`status`connState`retryCount`memMB`msgsIn`msgsOut!(
    `mle;
    .mle.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    .mle.conn.state;
    .mle.conn.retryCount;
    (`long$.Q.w[][`used]) % 1000000;
    sum .mle.tickCount;
    (count dib_bars) + count drb_bars)
  };

/ Get current status for all symbols
.mle.status:{[]
    if[0 = count .mle.syms; :"No data yet - waiting for trades..."];
    
    thresh:.mle.ewmaThreshold | 1000f;
    thetaAbs:abs .mle.theta;
    maxRuns:.mle.buyRun | .mle.sellRun;
    
    ([] 
        sym:.mle.syms;
        ticks:.mle.tickCount;
        dollarVol:.mle.cumDollarVol;
        theta:.mle.theta;
        thetaAbs:thetaAbs;
        threshold:thresh;
        pctToBar:100 * thetaAbs % thresh;
        maxRun:maxRuns;
        runPct:100 * maxRuns % thresh;
        position:.mle.position
    )
    };

/ Get recent DIB bars
.mle.getDIB:{[s;n]
    $[s ~ `;
        neg[n] # `time xdesc dib_bars;
        neg[n] # `time xdesc select from dib_bars where sym = s
    ]
    };

/ Get recent DRB bars  
.mle.getDRB:{[s;n]
    $[s ~ `;
        neg[n] # `time xdesc drb_bars;
        neg[n] # `time xdesc select from drb_bars where sym = s
    ]
    };

/ Get bar statistics
.mle.barStats:{[]
    dibStats:select 
        bars:count i, 
        avgTicks:avg ticks, 
        avgDollarVol:avg dollarVolume,
        avgDurationSec:avg duration % 1000,
        avgBuyPct:avg buyDollarPct
        by sym from dib_bars;
    
    drbStats:select 
        bars:count i,
        avgTicks:avg ticks,
        avgDollarVol:avg dollarVolume,
        buyRuns:sum runDirection = 1h,
        sellRuns:sum runDirection = -1h
        by sym from drb_bars;
    
    `dib`drb!(0!dibStats; 0!drbStats)
    };

/ Get adaptive threshold history (current state)
.mle.thresholds:{[]
    if[0 = count .mle.syms; :()];
    ([] sym:.mle.syms; threshold:.mle.ewmaThreshold; imbalRatio:.mle.ewmaImbalRatio)
    };

/ Reset a symbol's threshold to configured initial value
.mle.resetThreshold:{[s]
    if[not s in key .mle.symIdx; -1 "Symbol not found: ",string s; :()];
    idx:.mle.symIdx[s];
    initThresh:$[s in key .mle.cfg.init.threshold;
                 .mle.cfg.init.threshold[s];
                 .mle.cfg.init.thresholdDefault];
    .mle.ewmaThreshold[idx]:initThresh;
    -1 "Reset ",string[s]," threshold to $",string[initThresh];
    };

/ Reset all thresholds to configured initial values
.mle.resetAllThresholds:{[]
    {.mle.resetThreshold[x]} each .mle.syms;
    };

/ =============================================================================
/ 8. Position Query Interface
/ =============================================================================

/ Get current position for a symbol
/ Returns: -1 (short), 0 (neutral), 1 (long)
.mle.getPosition:{[s]
    if[not s in key .mle.symIdx; :0h];
    .mle.position[.mle.symIdx[s]]
    };

/ Get all current positions
.mle.getPositions:{[]
    if[0 = count .mle.syms; :([] sym:`$(); position:`short$(); price:`float$(); time:`timestamp$())];
    ([] 
        sym:.mle.syms; 
        position:.mle.position; 
        price:.mle.positionPrice;
        time:.mle.positionTime
    )
    };

/ Get position history for a symbol (or all if `)
.mle.getPositionHistory:{[s;n]
    $[s ~ `;
        neg[n] # `time xdesc positions;
        neg[n] # `time xdesc select from positions where sym = s
    ]
    };

/ Get position statistics
.mle.positionStats:{[]
    if[0 = count positions; :"No position changes yet"];
    select 
        changes:count i,
        longs:sum position = 1h,
        shorts:sum position = -1h,
        lastPosition:last position,
        lastPrice:last price,
        lastTime:last time
        by sym from positions
    };

/ =============================================================================
/ 9. Cleanup
/ =============================================================================

.mle.cleanup:{[]
    cutoff:.z.p - .mle.cfg.barRetentionNs;
    delete from `dib_bars where time < cutoff;
    delete from `drb_bars where time < cutoff;
    delete from `positions where time < cutoff;
    };

/ =============================================================================
/ 10. Connection Management (Resilient)
/ =============================================================================

/ Calculate next retry delay with exponential backoff
.mle.conn.getDelay:{[]
  delay:.mle.conn.cfg.baseDelayMs * `long$.mle.conn.cfg.backoffMultiplier xexp .mle.conn.retryCount;
  delay & .mle.conn.cfg.maxDelayMs  / Cap at max
  };

/ Check if enough time has passed since last attempt
.mle.conn.canRetry:{[]
  if[null .mle.conn.lastAttempt; :1b];
  elapsed:`long$(.z.p - .mle.conn.lastAttempt) % 1000000;  / ms
  elapsed >= .mle.conn.getDelay[]
  };

/ Main connection function - NEVER THROWS
.mle.connect:{[]
  / Guard: already connected
  if[not null .mle.conn.handle; :1b];
  
  / Guard: backoff not elapsed
  if[not .mle.conn.canRetry[];
    :0b
  ];
  
  .mle.conn.state:`connecting;
  .mle.conn.lastAttempt:.z.p;
  
  -1 "MLE: Connecting to TP on port ",string[.mle.cfg.tpPort],
     " (attempt ",string[.mle.conn.retryCount + 1],")...";
  
  / Protected connection attempt
  h:@[hopen; `$"::",string .mle.cfg.tpPort; {[err] -1 "MLE: Connection failed - ",err; 0N}];
  
  if[null h;
    .mle.conn.retryCount+:1;
    .mle.conn.state:`disconnected;
    nextDelay:.mle.conn.getDelay[];
    -1 "MLE: Will retry in ",string[nextDelay],"ms";
    :0b
  ];
  
  / Connection successful - now subscribe
  subResult:@[{[h]
    res:h(`pubsub.subscribe;`trade_binance;`);
    -1 "MLE: Subscribed to ",string first first res;
    1b
  }; h; {[err] -1 "MLE: Subscription failed - ",err; 0b}];
  
  if[not subResult;
    @[hclose; h; {}];  / Clean up failed connection
    .mle.conn.retryCount+:1;
    .mle.conn.state:`disconnected;
    :0b
  ];
  
  / Success - update state
  .mle.conn.handle:h;
  .mle.conn.state:`connected;
  .mle.conn.retryCount:0;
  -1 "MLE: Connected successfully (handle ",string[h],")";
  1b
  };

/ Disconnect handler - called when TP connection drops
.z.pc:{[h]
  if[h = .mle.conn.handle;
    -1 "MLE: TP connection lost (handle ",string[h],")";
    .mle.conn.handle:0N;
    .mle.conn.state:`disconnected;
    .mle.conn.retryCount:0;  / Reset backoff on disconnect (was connected, so TP exists)
    -1 "MLE: Will attempt reconnection on next timer tick";
  ];
  };

/ =============================================================================
/ 11. Pub/Sub Update Handlers
/ =============================================================================

/ Standard KDB update handler (called by TP via .u.pub)
/ Trade data layout: (time; sym; tradeId; price; qty; ...)
upd:{[t;d]
    if[t = `trade_binance;
        .mle.onTrade'[d 1; d 3; d 4; d 0]
    ]
    };

/ Also support .u.upd format
.u.upd:upd;

/ End of day handler
endofday:{[date]
    -1 "MLE: EOD received for ",string[date];
    / Clear bars and positions
    delete from `dib_bars;
    delete from `drb_bars;
    delete from `positions;
    
    / Reset all vectors to empty (symbols will re-register on first trade)
    .mle.syms:`$();
    .mle.symIdx:()!`long$();
    .mle.lastPrice:`float$();
    .mle.lastBt:`long$();
    .mle.tickCount:`long$();
    .mle.theta:`float$();
    .mle.buyRun:`float$();
    .mle.sellRun:`float$();
    .mle.cumVol:`float$();
    .mle.cumDollarVol:`float$();
    .mle.buyDollarVol:`float$();
    .mle.openPrice:`float$();
    .mle.highPrice:`float$();
    .mle.lowPrice:`float$();
    .mle.barStartTime:`timestamp$();
    .mle.ewmaThreshold:`float$();
    .mle.ewmaImbalRatio:`float$();
    .mle.position:`short$();
    .mle.positionTime:`timestamp$();
    .mle.positionPrice:`float$();
    
    -1 "MLE: EOD complete - all state cleared";
    };

/ =============================================================================
/ 12. Timer
/ =============================================================================

/ Timer - reconnection + cleanup
.z.ts:{[]
  / Attempt reconnection if disconnected
  if[null .mle.conn.handle; .mle.connect[]];
  
  / Run cleanup
  .mle.cleanup[];
  };

/ =============================================================================
/ 13. Startup
/ =============================================================================

system "p ",string .mle.cfg.port;

-1 "=======================================================";
-1 "MLE (Machine Learning Engine) starting on port ",string[.mle.cfg.port];
-1 "=======================================================";
-1 "";
-1 "Dollar-Based Information Bars (Lopez de Prado AFML)";
-1 "OPTIMIZED: Vector-based state for O(1) hot path access";
-1 "";
-1 "Configuration:";
-1 "  GC mode: deferred (g=0)";
-1 "  Initial thresholds:";
{-1 "    ",string[x]," -> $",string[.mle.cfg.init.threshold x]} each key .mle.cfg.init.threshold;
-1 "    (default) -> $",string[.mle.cfg.init.thresholdDefault];
-1 "  Threshold EWMA alpha: ",string[.mle.cfg.alpha.threshold];
-1 "  Imbalance EWMA alpha: ",string[.mle.cfg.alpha.imbalance];
-1 "  Bar retention: ",string[.mle.cfg.barRetentionMin]," minutes";
-1 "  Cleanup interval: ",string[.mle.cfg.cleanupIntervalMs],"ms";
-1 "";
-1 "Connection Settings:";
-1 "  Base retry delay: ",string[.mle.conn.cfg.baseDelayMs],"ms";
-1 "  Max retry delay: ",string[.mle.conn.cfg.maxDelayMs],"ms";
-1 "  Backoff multiplier: ",string[.mle.conn.cfg.backoffMultiplier];
-1 "";
-1 "Position Signals:";
-1 "  Buy threshold:  buyDollarPct > ",string[.mle.cfg.signal.buyThreshold],"%";
-1 "  Sell threshold: buyDollarPct < ",string[.mle.cfg.signal.sellThreshold],"%";
-1 "";
-1 "Bar Types:";
-1 "  DIB (Dollar Imbalance Bars) - samples on net buy/sell pressure";
-1 "  DRB (Dollar Runs Bars) - samples on persistent one-sided flow";
-1 "";

/ Attempt initial connection (non-blocking - will not crash on failure)
connected:.mle.connect[];

/ Start timer regardless of connection status
system "t ",string .mle.cfg.cleanupIntervalMs;

-1 "";
-1 "Query Interface:";
-1 "  .health[]                 / Standardized health check";
-1 "  .mle.status[]             / Current state + positions";
-1 "  .mle.getDIB[`BTCUSDT;10]  / Last 10 DIB bars for symbol";
-1 "  .mle.getDIB[`;20]         / Last 20 DIB bars (all symbols)";
-1 "  .mle.getDRB[`BTCUSDT;10]  / Last 10 DRB bars for symbol";
-1 "  .mle.barStats[]           / Bar statistics by symbol";
-1 "  .mle.thresholds[]         / Current adaptive thresholds";
-1 "";
-1 "Position Interface:";
-1 "  .mle.getPosition[`BTCUSDT] / Current position (-1/0/1)";
-1 "  .mle.getPositions[]        / All current positions";
-1 "  .mle.getPositionHistory[`BTCUSDT;10] / Last 10 changes";
-1 "  .mle.getPositionHistory[`;20]        / All symbols";
-1 "  .mle.positionStats[]       / Position change statistics";
-1 "";
-1 "Connection Interface:";
-1 "  .mle.conn.state            / Current state: `connected`disconnected`connecting";
-1 "  .mle.conn.handle           / TP handle (0N if disconnected)";
-1 "  .mle.conn.retryCount       / Failed connection attempts";
-1 "  .mle.connect[]             / Manual reconnection attempt";
-1 "";
-1 "Tables:";
-1 "  dib_bars    / Dollar Imbalance Bars";
-1 "  drb_bars    / Dollar Runs Bars";
-1 "  positions   / Position change history";
-1 "";

$[connected;-1 "MLE: Ready and processing"; -1 "MLE: Started in DEGRADED mode - waiting for CTP connection"];
-1 "=======================================================";
