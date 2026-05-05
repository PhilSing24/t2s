/ pnl.q - P&L and Position Monitoring
/ Subscribes to Chained TP for trades and positions

/ -------------------------------------------------------
/ Configuration
/ -------------------------------------------------------

.pnl.cfg.port:5018;
.pnl.cfg.ctpPort:5014;
.pnl.cfg.calcIntervalMs:1000;       / P&L calculation every 1 second
.pnl.cfg.retentionMin:60;           / Keep 60 minutes of trade data
.pnl.cfg.cleanupIntervalMs:30000;   / Cleanup every 30 seconds

/ Connection resilience
.pnl.conn.handle:0N;
.pnl.conn.state:`disconnected;
.pnl.conn.lastAttempt:0Np;
.pnl.conn.retryCount:0;
.pnl.conn.cfg.baseDelayMs:1000;
.pnl.conn.cfg.maxDelayMs:30000;
.pnl.conn.cfg.backoffMultiplier:1.5;

system "g 0";

.proc.startTime:.z.p;

/ -------------------------------------------------------
/ Tables
/ -------------------------------------------------------

\l ../schemas.q

/ Trades - kept for historical queries (KX Dashboard)
/ Comes from CTP with TP's receive timestamp.
trade_binance:.schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo];

/ Positions from SIG (side: 1=long, -1=short) - PNL-specific
positions:([]
    time:`timestamp$();
    sym:`symbol$();
    side:`int$();
    qty:`float$();
    tradedPrice:`float$()
    );

/ P&L history - PNL-specific
pnl_history:([]
    time:`timestamp$();
    totalPnL:`float$()
    );

/ -------------------------------------------------------
/ State
/ -------------------------------------------------------

.pnl.stats.tradesReceived:0j;
.pnl.stats.positionsReceived:0j;
.pnl.stats.pnlCalcs:0j;
.pnl.stats.cleanups:0j;
.pnl.stats.rowsDeleted:0j;
.pnl.lastPnL:0n;
.pnl.lastCleanup:0Np;

/ Latest prices for fast P&L lookup
.pnl.latestPrices:()!();   / sym -> price

/ -------------------------------------------------------
/ Connection Management
/ -------------------------------------------------------

.pnl.conn.getDelay:{[]
    (.pnl.conn.cfg.baseDelayMs * `long$.pnl.conn.cfg.backoffMultiplier xexp .pnl.conn.retryCount) & .pnl.conn.cfg.maxDelayMs
    };

.pnl.conn.canRetry:{[]
    (null .pnl.conn.lastAttempt) or (`long$(.z.p - .pnl.conn.lastAttempt) % 1000000) >= .pnl.conn.getDelay[]
    };

.pnl.connect:{[]
    if[not null .pnl.conn.handle; :1b];
    if[not .pnl.conn.canRetry[]; :0b];
    
    .pnl.conn.state:`connecting;
    .pnl.conn.lastAttempt:.z.p;
    
    -1 "PNL: Connecting to Chained TP on port ",string[.pnl.cfg.ctpPort],
       " (attempt ",string[.pnl.conn.retryCount + 1],")...";
    
    h:@[hopen; `$"::",string[.pnl.cfg.ctpPort]; {[err] -1 "PNL: Connection failed - ",err; 0N}];
    
    if[null h;
        .pnl.conn.retryCount+:1;
        .pnl.conn.state:`disconnected;
        -1 "PNL: Will retry in ",string[.pnl.conn.getDelay[]],"ms";
        :0b
    ];
    
    / Subscribe to trades and positions
    subResult:@[{[h]
        -1 "PNL: Subscribing to trade_binance...";
        h(`pubsub.subscribe;`trade_binance;`);
        -1 "PNL: Subscribed to trade_binance";
        
        -1 "PNL: Subscribing to positions...";
        h(`pubsub.subscribe;`positions;`);
        -1 "PNL: Subscribed to positions";
        1b
    }; h; {[err] -1 "PNL: Subscription failed - ",err; 0b}];
    
    if[not subResult;
        @[hclose; h; {}];
        .pnl.conn.retryCount+:1;
        .pnl.conn.state:`disconnected;
        :0b
    ];
    
    .pnl.conn.handle:h;
    .pnl.conn.state:`connected;
    .pnl.conn.retryCount:0;
    -1 "PNL: Connected successfully (handle ",string[h],")";
    1b
    };

.z.pc:{[h]
    if[h = .pnl.conn.handle;
        -1 "PNL: Chained TP disconnected (handle ",string[h],")";
        .pnl.conn.handle:0N;
        .pnl.conn.state:`disconnected;
        .pnl.conn.retryCount:0;
        -1 "PNL: Will attempt reconnection on next timer tick";
    ];
    };

/ -------------------------------------------------------
/ Update Handler
/ -------------------------------------------------------

upd:{[tbl;data]
    / Handle batch (table) from Chained TP
    if[98h = type data;
        $[tbl = `trade_binance;
            [
                / Append to table for historical queries
                `trade_binance insert data;
                / Update latest prices for fast P&L calc
                .pnl.latestPrices,:exec last price by sym from data;
                .pnl.stats.tradesReceived +: count data
            ];
          tbl = `positions;
            [`positions insert data; .pnl.stats.positionsReceived +: count data];
          ()
        ];
        :();
    ];
    
    / Handle single row
    if[tbl = `trade_binance;
        `trade_binance insert data;
        .pnl.latestPrices[data 1]:data 3;  / sym -> price
        .pnl.stats.tradesReceived +:1;
    ];
    
    if[tbl = `positions;
        `positions insert data;
        .pnl.stats.positionsReceived +:1;
        -1 "PNL: Position received - ",string[data 1]," side:",string[data 2]," qty:",string[data 3];
    ];
    };

/ -------------------------------------------------------
/ Memory Management
/ -------------------------------------------------------

.pnl.cleanup:{[]
    cutoff:.z.p - `long$.pnl.cfg.retentionMin * 60 * 1000000000;
    
    tradesBefore:count trade_binance;
    pnlBefore:count pnl_history;
    
    delete from `trade_binance where time < cutoff;
    delete from `pnl_history where time < cutoff;
    
    deleted:(tradesBefore - count trade_binance) + (pnlBefore - count pnl_history);
    if[deleted > 0; .pnl.stats.rowsDeleted +: deleted];
    
    .pnl.stats.cleanups +:1;
    .pnl.lastCleanup:.z.p;
    };

/ -------------------------------------------------------
/ P&L Calculation
/ -------------------------------------------------------

/ Calculate total P&L across all positions
.pnl.calcPnL:{[]
    if[0 = count positions; :()];
    if[0 = count .pnl.latestPrices; :()];
    
    / Calculate P&L: side * (current - traded) * qty
    totalPnL:sum {[row]
        curPrice:.pnl.latestPrices row`sym;
        if[null curPrice; :0f];
        row[`side] * (curPrice - row`tradedPrice) * row`qty
    } each positions;
    
    / Store in history
    `pnl_history insert (.z.p; totalPnL);
    
    .pnl.lastPnL:totalPnL;
    .pnl.stats.pnlCalcs +:1;
    };

/ Calculate P&L for specific symbols only
/ Usage: .pnl.calcPnLBySyms[`BTCUSDT`ETHUSDT]
.pnl.calcPnLBySyms:{[syms]
    if[0 = count positions; :0f];
    if[0 = count .pnl.latestPrices; :0f];
    
    / Filter positions by symbols
    pos:select from positions where sym in syms;
    if[0 = count pos; :0f];
    
    / Calculate P&L: side * (current - traded) * qty
    sum {[row]
        curPrice:.pnl.latestPrices row`sym;
        if[null curPrice; :0f];
        row[`side] * (curPrice - row`tradedPrice) * row`qty
    } each pos
    };

/ P&L statistics for last N minutes
.pnl.getStats:{[lastMin]
    cutoff:.z.p - `long$lastMin * 60000000000;
    data:exec totalPnL from pnl_history where time > cutoff;
    if[0 = count data; :`avg`min`max`range!(0n;0n;0n;0n)];
    mn:min data; mx:max data;
    `avg`min`max`range!(avg data;mn;mx;mx - mn)
    };

/ -------------------------------------------------------
/ Query Interface (for KX Dashboard)
/ -------------------------------------------------------

/ Current P&L summary by position
.pnl.summary:{[]
    if[0 = count positions;
        :([] sym:`$(); side:`int$(); qty:`float$(); tradedPrice:`float$(); currentPrice:`float$(); pnl:`float$())
    ];
    
    update currentPrice:.pnl.latestPrices sym,
           pnl:side * ((.pnl.latestPrices sym) - tradedPrice) * qty
    from positions
    };

/ P&L summary for specific symbols
/ Usage: .pnl.summaryBySyms[`BTCUSDT`ETHUSDT]
.pnl.summaryBySyms:{[syms]
    if[0 = count positions;
        :([] sym:`$(); side:`int$(); qty:`float$(); tradedPrice:`float$(); currentPrice:`float$(); pnl:`float$())
    ];
    
    pos:select from positions where sym in syms;
    if[0 = count pos; :pos];
    
    update currentPrice:.pnl.latestPrices sym,
           pnl:side * ((.pnl.latestPrices sym) - tradedPrice) * qty
    from pos
    };

/ Total P&L
.pnl.total:{[]
    if[0 = count positions; :0f];
    sum exec pnl from .pnl.summary[]
    };

/ P&L history for charting
.pnl.getHistory:{[minutes]
    cutoff:.z.p - `long$minutes * 60000000000;
    select time, totalPnL from pnl_history where time > cutoff
    };

/ Trades for charting (last N minutes)
.pnl.getTrades:{[minutes]
    cutoff:.z.p - `long$minutes * 60000000000;
    select from trade_binance where time > cutoff
    };

/ Trades by symbol for charting
.pnl.getTradesBySym:{[s;minutes]
    cutoff:.z.p - `long$minutes * 60000000000;
    select from trade_binance where sym = s, time > cutoff
    };

/ Price series for charting (sampled by interval)
/ interval: `second, `minute, etc.
.pnl.getPriceSeries:{[s;minutes;interval]
    cutoff:.z.p - `long$minutes * 60000000000;
    data:select time, price from trade_binance where sym = s, time > cutoff;
    if[0 = count data; :data];
    select last price by interval xbar time from data
    };

/ Price series raw (all ticks) - use with caution
.pnl.getPriceSeriesRaw:{[s;minutes]
    cutoff:.z.p - `long$minutes * 60000000000;
    select time, price from trade_binance where sym = s, time > cutoff
    };

/ Latest prices
.pnl.getPrices:{[]
    flip `sym`price!flip (key .pnl.latestPrices;value .pnl.latestPrices)
    };

/ -------------------------------------------------------
/ Health Check
/ -------------------------------------------------------

.health:{[]
    / Determine status
    st:$[.pnl.conn.state <> `connected; `disconnected;
         0 = count .pnl.latestPrices; `degraded;
         `ok];
    
    `process`port`uptime`status`connState`retryCount`positions`tradesRecv`positionsRecv`pnlCalcs`memMB!(
        `pnl;
        .pnl.cfg.port;
        `second$.z.p - .proc.startTime;
        st;
        .pnl.conn.state;
        .pnl.conn.retryCount;
        count positions;
        .pnl.stats.tradesReceived;
        .pnl.stats.positionsReceived;
        .pnl.stats.pnlCalcs;
        (`long$.Q.w[][`used]) % 1000000
    )
    };

.pnl.status:{[]
    `port`ctpPort`connected`positions`trades`symbols`lastPnL`pnlCalcs`retention!(
        .pnl.cfg.port;
        .pnl.cfg.ctpPort;
        .pnl.conn.state = `connected;
        count positions;
        count trade_binance;
        count .pnl.latestPrices;
        .pnl.lastPnL;
        .pnl.stats.pnlCalcs;
        .pnl.cfg.retentionMin
    )
    };

/ -------------------------------------------------------
/ End-of-Day
/ -------------------------------------------------------

endofday:{[]
    -1 "PNL: EOD received";
    delete from `trade_binance;
    delete from `positions;
    delete from `pnl_history;
    .pnl.latestPrices:()!();
    .pnl.stats.tradesReceived:0j;
    .pnl.stats.positionsReceived:0j;
    .pnl.stats.pnlCalcs:0j;
    .pnl.stats.cleanups:0j;
    .pnl.stats.rowsDeleted:0j;
    .pnl.lastPnL:0n;
    -1 "PNL: State cleared";
    };

/ -------------------------------------------------------
/ Timer
/ -------------------------------------------------------

.pnl.timerTick:0j;

.z.ts:{[]
    / Reconnect if needed
    if[null .pnl.conn.handle; .pnl.connect[]];
    
    / Calculate P&L every tick
    .pnl.calcPnL[];
    
    / Cleanup every 30 ticks (30 seconds at 1s interval)
    .pnl.timerTick +:1;
    if[0 = .pnl.timerTick mod 30; .pnl.cleanup[]];
    };

/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system "p ",string .pnl.cfg.port;

-1 "=======================================================";
-1 "PNL (P&L Monitor) on port ",string[.pnl.cfg.port];
-1 "=======================================================";
-1 "Configuration:";
-1 "  Chained TP: ",string[.pnl.cfg.ctpPort];
-1 "  Calc interval: ",string[.pnl.cfg.calcIntervalMs],"ms";
-1 "  Retention: ",string[.pnl.cfg.retentionMin]," minutes";
-1 "";
-1 "Connection Settings:";
-1 "  Base retry delay: ",string[.pnl.conn.cfg.baseDelayMs],"ms";
-1 "  Max retry delay: ",string[.pnl.conn.cfg.maxDelayMs],"ms";
-1 "";
-1 "Tables: trade_binance positions pnl_history";
-1 "";

/ Attempt initial connection
connected:.pnl.connect[];

/ Start timer
system "t ",string .pnl.cfg.calcIntervalMs;

-1 "Query Interface:";
-1 "  .health[]                         / Health check";
-1 "  .pnl.status[]                     / Status";
-1 "  .pnl.summary[]                    / Position P&L breakdown";
-1 "  .pnl.summaryBySyms[`BTCUSDT`ETHUSDT]  / P&L by symbols";
-1 "  .pnl.total[]                      / Total P&L";
-1 "  .pnl.getStats[60]                 / P&L stats (last N minutes)";
-1 "  .pnl.calcPnLBySyms[`BTCUSDT`ETHUSDT]  / Calculate P&L for symbols";
-1 "  .pnl.getHistory[30]               / P&L history (minutes)";
-1 "  .pnl.getTrades[30]                / Trades (minutes)";
-1 "  .pnl.getTradesBySym[`BTCUSDT;30]  / Trades by symbol";
-1 "  .pnl.getPriceSeries[`BTCUSDT;30;`second]  / Price series (sampled)";
-1 "  .pnl.getPriceSeriesRaw[`BTCUSDT;30]  / Price series (all ticks)";
-1 "  .pnl.getPrices[]                  / Latest prices";
-1 "";

$[connected; -1 "PNL: Ready"; -1 "PNL: Started in DEGRADED mode - waiting for CTP connection"];
-1 "=======================================================";
