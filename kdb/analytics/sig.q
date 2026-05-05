/ sig.q - Simple RSI Signal Generator
/ Wait for n periods, sample prices, calculate RSI once
/ Publishes positions to Chained TP for PNL monitoring

/ -------------------------------------------------------
/ Config
/ -------------------------------------------------------

.sig.cfg.port:5012;
.sig.cfg.tpPort:5010;
.sig.cfg.ctpPort:5014;              / Chained TP for position publishing
.sig.cfg.rsiPeriod:14;
.sig.cfg.notionalUSDT:100000.0;
.sig.cfg.barInterval:0D00:00:20;    / 20 seconds per period
.sig.cfg.pnlInterval:0D00:00:05;    / P&L calculation frequency

/ Connection to Primary TP (subscribe to trades)
.sig.conn.handle:0N;
.sig.conn.state:`disconnected;
.sig.conn.lastAttempt:0Np;
.sig.conn.retryCount:0;
.sig.conn.cfg.baseDelayMs:1000;
.sig.conn.cfg.maxDelayMs:30000;
.sig.conn.cfg.backoffMultiplier:1.5;

/ Connection to Chained TP (publish positions)
.sig.ctp.handle:0N;
.sig.ctp.state:`disconnected;
.sig.ctp.lastAttempt:0Np;
.sig.ctp.retryCount:0;

system "g 0";
.proc.startTime:.z.p;

/ -------------------------------------------------------
/ Tables
/ -------------------------------------------------------

\l ../schemas.q

/ Trade data from primary TP (with TP's receive timestamp)
trade_binance:.schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo];

/ SIG-specific tables (not part of shared schema)
signals:([] sym:`symbol$(); rsi:`float$(); exposure:`int$());
positions:([] time:`timestamp$(); sym:`symbol$(); side:`int$(); qty:`float$(); tradedPrice:`float$());
pnl:([] time:`timestamp$(); pnl:`float$());

/ -------------------------------------------------------
/ State
/ -------------------------------------------------------

.sig.signalGenerated:0b;
.sig.stats.trades:0j;
.sig.stats.positionsPublished:0j;
.sig.rsiData:();
.sig.targetTime:0Np;
.sig.lastPnlTime:0Np;

/ -------------------------------------------------------
/ Connection to Primary TP (subscribe)
/ -------------------------------------------------------

.sig.conn.getDelay:{[] (.sig.conn.cfg.baseDelayMs * `long$.sig.conn.cfg.backoffMultiplier xexp .sig.conn.retryCount) & .sig.conn.cfg.maxDelayMs};
.sig.conn.canRetry:{[] (null .sig.conn.lastAttempt) or (`long$(.z.p - .sig.conn.lastAttempt) % 1000000) >= .sig.conn.getDelay[]};

.sig.connect:{[]
    if[not null .sig.conn.handle; :1b];
    if[not .sig.conn.canRetry[]; :0b];
    .sig.conn.state:`connecting;
    .sig.conn.lastAttempt:.z.p;
    -1 "SIG: Connecting to TP...";
    h:@[hopen; `$"::",string .sig.cfg.tpPort; {-1 "SIG: Connect failed - ",x; 0N}];
    if[null h; .sig.conn.retryCount+:1; .sig.conn.state:`disconnected; :0b];
    r:@[{x(`pubsub.subscribe;`trade_binance;`); 1b}; h; {-1 "SIG: Subscribe failed - ",x; 0b}];
    if[not r; @[hclose;h;{}]; .sig.conn.retryCount+:1; .sig.conn.state:`disconnected; :0b];
    .sig.conn.handle:h; .sig.conn.state:`connected; .sig.conn.retryCount:0;
    -1 "SIG: Connected to TP (handle ",string[h],")";
    1b};

/ -------------------------------------------------------
/ Connection to Chained TP (publish)
/ -------------------------------------------------------

.sig.ctp.getDelay:{[] (.sig.conn.cfg.baseDelayMs * `long$.sig.conn.cfg.backoffMultiplier xexp .sig.ctp.retryCount) & .sig.conn.cfg.maxDelayMs};
.sig.ctp.canRetry:{[] (null .sig.ctp.lastAttempt) or (`long$(.z.p - .sig.ctp.lastAttempt) % 1000000) >= .sig.ctp.getDelay[]};

.sig.ctpConnect:{[]
    if[not null .sig.ctp.handle; :1b];
    if[not .sig.ctp.canRetry[]; :0b];
    .sig.ctp.state:`connecting;
    .sig.ctp.lastAttempt:.z.p;
    -1 "SIG: Connecting to Chained TP for position publishing...";
    h:@[hopen; `$"::",string .sig.cfg.ctpPort; {-1 "SIG: CTP connect failed - ",x; 0N}];
    if[null h; .sig.ctp.retryCount+:1; .sig.ctp.state:`disconnected; :0b];
    .sig.ctp.handle:h; .sig.ctp.state:`connected; .sig.ctp.retryCount:0;
    -1 "SIG: Connected to Chained TP (handle ",string[h],")";
    1b};

/ Handle disconnections
.z.pc:{[h]
    if[h = .sig.conn.handle;
        -1 "SIG: TP disconnected";
        .sig.conn.handle:0N;
        .sig.conn.state:`disconnected;
        .sig.conn.retryCount:0;
    ];
    if[h = .sig.ctp.handle;
        -1 "SIG: Chained TP disconnected";
        .sig.ctp.handle:0N;
        .sig.ctp.state:`disconnected;
        .sig.ctp.retryCount:0;
    ];
    };

/ -------------------------------------------------------
/ Publish Position to Chained TP (async)
/ -------------------------------------------------------

.sig.publishPosition:{[pos]
    if[null .sig.ctp.handle;
        -1 "SIG: Cannot publish position - not connected to CTP";
        :0b
    ];
    / Async publish: neg[handle] for fire-and-forget
    data:(pos`time; pos`sym; pos`side; pos`qty; pos`tradedPrice);
    @[neg[.sig.ctp.handle]; (`upd;`positions;data); {-1 "SIG: Position publish failed - ",x; 0b}];
    .sig.stats.positionsPublished+:1;
    1b
    };

/ -------------------------------------------------------
/ Update Handler
/ -------------------------------------------------------

upd:{[tbl;data]
    if[tbl=`trade_binance;
        `trade_binance insert data;
        .sig.stats.trades+:1;
    ]};

/ -------------------------------------------------------
/ RSI (Wilder's smoothing)
/ -------------------------------------------------------

.sig.relativeStrength:{[n;y]
    begin:n#0Nf;
    start:avg (n+1)#y;
    begin,start,{(y+x*(z-1))%z}\[start;(n+1)_y;n]
    };

.sig.rsi:{[close;n]
    diff:close - prev close;
    gain:diff * diff > 0;
    loss:abs diff * diff < 0;
    rs:.sig.relativeStrength[n;gain] % .sig.relativeStrength[n;loss];
    100 * rs % 1 + rs
    };

/ -------------------------------------------------------
/ Signal Generation (runs once at target time)
/ -------------------------------------------------------

.sig.generateSignal:{[]
    if[.sig.signalGenerated; :()];
    if[0 = count trade_binance; :()];
    
    / Get all symbols
    syms:distinct exec sym from trade_binance;
    if[2 > count syms; -1 "SIG: Need at least 2 symbols"; :()];
    
    / Parameters
    n:.sig.cfg.rsiPeriod + 1;  / Need n+1 closes for RSI(n)
    interval:.sig.cfg.barInterval;
    
    / Calculate RSI for each symbol
    .sig.rsiData:();
    {[s;n;interval]
        / Get trades for this symbol, sorted by time
        symTrades:`time xasc select time, price from trade_binance where sym=s;
        if[0 = count symTrades; :()];
        
        / Create time boundaries for each period
        endTime:max symTrades`time;
        boundaries:endTime - interval * reverse til n;
        
        / Get last price at each boundary (use bin for efficiency)
        closes:{[t;times;prices]
            idx:times bin t;
            if[idx < 0; :0n];
            prices idx
        }[;symTrades`time;symTrades`price] each boundaries;
        
        / Skip if missing data
        if[any null closes; :()];
        
        / Calculate RSI
        rsiVec:.sig.rsi[closes;.sig.cfg.rsiPeriod];
        r:last rsiVec;
        if[not null r; .sig.rsiData::.sig.rsiData,enlist`sym`rsi`price!(s;r;last closes)]
    }[;n;interval] each syms;
    
    / Check we have all symbols
    if[(count .sig.rsiData) < count syms; -1 "SIG: Missing RSI for some symbols"; :()];
    
    / Sort by RSI descending
    t:`rsi xdesc .sig.rsiData;
    
    / Assign exposure: highest=1, lowest=-1, others=0
    cnt:count t;
    exposures:@[cnt#0i; 0; :; 1i];
    exposures:@[exposures; cnt-1; :; -1i];
    
    / Build signals table
    `signals insert flip `sym`rsi`exposure!(t`sym;t`rsi;exposures);
    
    .sig.signalGenerated:1b;
    
    / Create and publish positions (side: 1=long, -1=short)
    lo:first t; sh:last t;
    notional:.sig.cfg.notionalUSDT; tm:.z.p;
    
    / Long position
    longPos:`time`sym`side`qty`tradedPrice!(tm;lo`sym;1i;notional%lo`price;lo`price);
    `positions insert longPos;
    .sig.publishPosition[longPos];
    
    / Short position
    shortPos:`time`sym`side`qty`tradedPrice!(tm;sh`sym;-1i;notional%sh`price;sh`price);
    `positions insert shortPos;
    .sig.publishPosition[shortPos];
    
    / First P&L row = 0
    `pnl insert (tm;0f);
    
    -1 ""; 
    -1 "=== SIGNAL GENERATED ===";
    -1 "sym      | rsi   | exposure";
    -1 "---------|-------|----------";
    {-1 (8$string x`sym),"| ",(5$string `int$x`rsi)," | ",string x`exposure} each signals;
    -1 "=========================";
    };

/ -------------------------------------------------------
/ P&L Calculation (local, for stop-loss logic)
/ -------------------------------------------------------

.sig.calcPnL:{[]
    if[not .sig.signalGenerated; :()];
    if[0 = count positions; :()];
    
    / Get current prices (last trade for each symbol)
    curPrices:exec last price by sym from trade_binance;
    
    / Calculate P&L: side * (current - traded) * qty
    totalPnL:sum {[row;curPrices]
        curPrice:curPrices row`sym;
        if[null curPrice; :0f];
        row[`side] * (curPrice - row`tradedPrice) * row`qty
    }[;curPrices] each positions;
    
    / Append to pnl table
    `pnl insert (.z.p;totalPnL);
    };

/ -------------------------------------------------------
/ Query
/ -------------------------------------------------------

.health:{[]
    / Determine connection state (both must be connected)
    connSt:$[.sig.conn.state = `connected;
             $[.sig.ctp.state = `connected; `connected; `degraded];
             `disconnected];
    
    / Determine status
    st:$[connSt <> `connected; connSt; `ok];
    
    `process`port`uptime`status`connState`memMB`trades`signal`positionsPub!(
        `sig;
        .sig.cfg.port;
        `second$.z.p-.proc.startTime;
        st;
        connSt;
        (`long$.Q.w[][`used]) % 1000000;
        .sig.stats.trades;
        .sig.signalGenerated;
        .sig.stats.positionsPublished
    )};

.sig.status:{[]
    `uptime`tpState`ctpState`trades`syms`signal`positionsPub!(
        `second$.z.p-.proc.startTime;
        .sig.conn.state;
        .sig.ctp.state;
        .sig.stats.trades;
        distinct exec sym from trade_binance;
        .sig.signalGenerated;
        .sig.stats.positionsPublished
    )};

/ -------------------------------------------------------
/ EOD
/ -------------------------------------------------------

endofday:{[d]
    delete from `trade_binance;
    delete from `signals;
    delete from `positions;
    delete from `pnl;
    .sig.signalGenerated:0b;
    .sig.stats.trades:0j;
    .sig.stats.positionsPublished:0j;
    .sig.rsiData:();
    .sig.targetTime:0Np;
    .sig.lastPnlTime:0Np;
    };

/ -------------------------------------------------------
/ Timer
/ -------------------------------------------------------

.z.ts:{[]
    / Reconnect to TP if needed
    if[null .sig.conn.handle; .sig.connect[]];
    
    / Reconnect to Chained TP if needed
    if[null .sig.ctp.handle; .sig.ctpConnect[]];
    
    / After signal: calculate P&L at configured interval
    if[.sig.signalGenerated;
        if[(null .sig.lastPnlTime) or (.z.p >= .sig.lastPnlTime + .sig.cfg.pnlInterval);
            .sig.calcPnL[];
            .sig.lastPnlTime:.z.p;
        ];
        :();
    ];
    
    / Set target time once we have trades
    if[null .sig.targetTime;
        if[0 < count trade_binance;
            .sig.targetTime:.z.p + (.sig.cfg.rsiPeriod + 1) * .sig.cfg.barInterval;
            -1 "SIG: Will generate signal at ",string[`time$.sig.targetTime];
        ];
        :();
    ];
    
    / Check if time to generate
    if[.z.p >= .sig.targetTime;
        .sig.generateSignal[];
        :();
    ];
    };

/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system "p ",string .sig.cfg.port;
waitTime:(`int$.sig.cfg.rsiPeriod + 1) * `int$.sig.cfg.barInterval % 1000000000;
-1 "=== SIG on port ",string[.sig.cfg.port]," ===";
-1 "RSI(",string[.sig.cfg.rsiPeriod],") @ ",string[`int$.sig.cfg.barInterval % 1000000000],"s intervals";
-1 "Signal in ~",string[waitTime],"s";
-1 "Notional: $",string .sig.cfg.notionalUSDT;
-1 "";
-1 "Connections:";
-1 "  Primary TP: ",string[.sig.cfg.tpPort]," (subscribe trades)";
-1 "  Chained TP: ",string[.sig.cfg.ctpPort]," (publish positions)";
-1 "";

/ Connect to both TP and Chained TP
tpConnected:.sig.connect[];
ctpConnected:.sig.ctpConnect[];

system "t 1000";
-1 "Tables: trade_binance signals positions pnl";
$[tpConnected and ctpConnected;-1 "Ready";-1 "Waiting for connections"];
-1 "================================";
