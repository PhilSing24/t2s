/ rte.q - Real-Time Engine (Production Grade with u.q)
/ Trade Buckets (for VWAP, Var-Covar) + Order Book (latest L5 snapshot) + Imbalance
/ With log replay capability for crash recovery

/ =============================================================================
/ Configuration
/ =============================================================================

.rte.cfg.port:5012;
.rte.cfg.tpPort:5010;
.rte.cfg.logDir:"logs";
.rte.cfg.defaultWindowMin:5;                    / Default window in minutes
.rte.cfg.bucketSec:1;                           / Bucket size in seconds (for VWAP precision)
.rte.cfg.bucketRetentionMin:65;                 / Keep 65 minutes of trade buckets (for 1-hour vcov)
.rte.cfg.vcovRetentionMin:15;                   / Keep 15 minutes of var-covar history
.rte.cfg.vcovWindowMin:60;                      / 1-hour window for var-covar calculation
.rte.cfg.vcovBucketSec:30;                      / Resample to 30-sec for var-covar
.rte.cfg.vcovMinBuckets:100;                    / Minimum buckets for valid matrix (~100 of 120)
.rte.cfg.cleanupIntervalMs:5000;                / Cleanup every 5 seconds
.rte.cfg.iVol:`BTCUSDT`ETHUSDT`SOLUSDT!45.00 65.00 67.00; / 30 days implied volatility to compare with intra-day vol
.rte.cfg.obiAlpha:0.05;                         / OBI EMA smoothing factor (lower = smoother)
.rte.cfg.obiThreshold:0.3;                      / OBI threshold for buyer/seller pressure
.rte.cfg.obiRetentionMin:7;                    / Keep 15 minutes of OBI history

/ Derived configuration (precomputed for hot path efficiency)
.rte.cfg.bucketNs:.rte.cfg.bucketSec * 1000000000j;                          / Bucket size in nanoseconds
.rte.cfg.bucketRetentionNs:.rte.cfg.bucketRetentionMin * 60 * 1000000000j;   / Bucket retention in ns
.rte.cfg.vcovRetentionNs:.rte.cfg.vcovRetentionMin * 60 * 1000000000j;       / Var-covar retention in ns
.rte.cfg.obiRetentionNs:.rte.cfg.obiRetentionMin * 60 * 1000000000j;         / OBI retention in ns

/ Store start time of the process
.proc.startTime:.z.p;

/ =============================================================================
/ Trade Buckets - Shared State for VWAP and Var-Covar
/ =============================================================================

/ Time-bucketed trade data (1-second buckets)
/ Each bucket contains aggregated trade data for that second
tradeBuckets:([] sym:`symbol$(); bucket:`timestamp$(); sumPxQty:`float$(); sumQty:`float$(); cnt:`long$());

/ Key by sym and bucket for O(1) upsert
`sym`bucket xkey `tradeBuckets;

/ Add trade to current bucket (hot path)
.rte.bucket.add:{[s;time;price;qty]
  bucketTime:`timestamp$.rte.cfg.bucketNs * `long$time div .rte.cfg.bucketNs;
  k:(s;bucketTime);
  pxqty:price*qty;
  $[k in key tradeBuckets;
    tradeBuckets[k;`sumPxQty`sumQty`cnt]+:(pxqty; qty; 1j);
    `tradeBuckets upsert (s; bucketTime; pxqty; qty; 1j)
  ];
  };

/ Cleanup old buckets (called by timer)
.rte.bucket.cleanup:{[]
  cutoff:.z.p - .rte.cfg.bucketRetentionNs;
  delete from `tradeBuckets where bucket < cutoff;
  };

/ =============================================================================
/ VWAP - Calculated from Trade Buckets
/ =============================================================================

/ Calculate VWAP for a symbol over a time window
.rte.vwap.calc:{[s;windowMin]
  windowNs:windowMin * 60 * 1000000000j;
  cutoff:.z.p - windowNs;
  
  buckets:0!select from tradeBuckets where sym in enlist s, bucket >= cutoff;
  
  if[0 = count buckets;
    :([] sym:enlist s; vwap:enlist 0n; totalQty:enlist 0f; tradeCount:enlist 0j; isValid:enlist 0b)];
  
  totalPxQty:sum buckets`sumPxQty;
  totalQty:sum buckets`sumQty;
  tradeCount:sum buckets`cnt;
  vwap:$[totalQty > 0f; totalPxQty % totalQty; 0n];
  
  ([] sym:enlist s; vwap:enlist vwap; totalQty:enlist totalQty; tradeCount:enlist tradeCount; isValid:enlist tradeCount > 10)
  };

/ =============================================================================
/ Variance-Covariance Matrix - Calculated from Trade Buckets
/ =============================================================================

/ Latest var-covar matrix
.rte.vcov.latest:()!();

/ Var-covar history (flattened for charting)
.rte.vcov.history:([] time:`timestamp$(); sym1:`symbol$(); sym2:`symbol$(); covar:`float$());

/ Calculate var-covar matrix for all symbols (cold path)
/ Resamples 1-sec trade buckets to 30-sec for meaningful returns
.rte.getVarCovar:{[windowMin]
  cutoff:.z.p - windowMin * 60 * 1000000000j;
  
  / Get 1-sec bucket prices
  prices1s:select bucket, sym, price:sumPxQty % sumQty from tradeBuckets where bucket >= cutoff;
  
  / Resample to 30-sec buckets (last price in each 30-sec window)
  vcovBucketNs:.rte.cfg.vcovBucketSec * 1000000000j;
  prices:select last price by sym, bucket:`timestamp$vcovBucketNs * `long$bucket div vcovBucketNs from prices1s;
  prices:0!prices;
  
  / Get symbols dynamically
  syms:asc distinct prices`sym;
  if[2 > count syms; :`syms`matrix`window`buckets`isValid!(syms; ()!(); windowMin; 0j; 0b)];
  
  / Get all buckets (sorted)
  buckets:asc distinct prices`bucket;
  
  / Build price vectors aligned by bucket (null for missing)
  getPrices:{[prices;buckets;s]
    d:(exec bucket!price from prices where sym=s);
    d buckets
    };
  / Forward-fill nulls (use last known price when no trade)
  priceMatrix:fills each getPrices[prices;buckets] each syms;
  
  / Compute log returns
  returns:{r:log x % prev x; @[r;0;:;0n]} each priceMatrix;
  
  / Find valid indices (all symbols have values) - use all each for proper precedence
  validIdx:where all each not null each flip returns;
  if[10 > count validIdx; :`syms`matrix`window`buckets`isValid!(syms; ()!(); windowMin; count validIdx; 0b)];
  
  / Subset to valid rows
  R:returns @\: validIdx;
  
  / Build var-covar matrix
  matrix:syms!{[syms;R;i] syms!{[R;i;j] cov[R i;R j]}[R;i] each til count syms}[syms;R] each til count syms;
  
  / isValid: have enough buckets for meaningful calculation
  isValid:count[validIdx] >= .rte.cfg.vcovMinBuckets;
  
  `syms`matrix`window`buckets`isValid!(syms; matrix; windowMin; count validIdx; isValid)
  };

/ Update var-covar and store history (called by timer)
.rte.vcov.update:{[]
  res:.rte.getVarCovar[.rte.cfg.vcovWindowMin];
  
  if[0 = count res`matrix; :()];
  
  .rte.vcov.latest:res;
  
  / Flatten matrix to history rows
  t:.z.p;
  syms:res`syms;
  matrix:res`matrix;
  n:count syms;
  
  / Build columns directly (n*n rows for n x n matrix)
  times:(n*n)#t;
  s1s:raze n#/:syms;
  s2s:(n*n)#syms;
  covs:raze {[matrix;syms;s1] matrix[s1;syms]}[matrix;syms] each syms;
  
  `.rte.vcov.history insert (times; s1s; s2s; covs);
  };

/ Cleanup old var-covar history (called by timer)
.rte.vcov.cleanup:{[]
  cutoff:.z.p - .rte.cfg.vcovRetentionNs;
  delete from `.rte.vcov.history where time < cutoff;
  };

/ =============================================================================
/ Order Book State - L5 Latest Snapshot (Optimized for Update Path)
/ =============================================================================

/ Latest L5 order book per symbol
/ Stored as raw list for minimal update overhead:
/   Indices 0-4:   bidPrice1-5
/   Indices 5-9:   bidQty1-5
/   Indices 10-14: askPrice1-5
/   Indices 15-19: askQty1-5
/   Index 20:      time
.rte.book.latest:()!();

/ Update book state - hot path, minimal allocation
.rte.book.update:{[s;data;time]
  .rte.book.latest[s]:data[2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21],time;
  };

/ =============================================================================
/ Imbalance State - Derived from L5 Depth
/ =============================================================================

/ Latest order book imbalance per symbol
.rte.imb.latest:()!();

/ EMA of order book imbalance per symbol
.rte.imb.ema:()!();

/ OBI history (for charting)
.rte.imb.history:([] time:`timestamp$(); sym:`symbol$(); OBI:`float$(); smOBI:`float$());

/ Update imbalance for a symbol with EMA smoothing
.rte.imb.update:{[s;bidDepth;askDepth;time]
  total:bidDepth + askDepth;
  imb:$[total > 0f; (bidDepth - askDepth) % total; 0n];
  
  / EMA: new = alpha * current + (1-alpha) * previous
  prevSmOBI:$[s in key .rte.imb.ema; .rte.imb.ema[s]; imb];
  smOBI:(.rte.cfg.obiAlpha * imb) + (1 - .rte.cfg.obiAlpha) * prevSmOBI;
  .rte.imb.ema[s]:smOBI;
  
  .rte.imb.latest[s]:`bidDepth`askDepth`imbalance`smOBI`time!(bidDepth; askDepth; imb; smOBI; time);
  
  / Store history
  `.rte.imb.history insert (time; s; imb; smOBI);
  };

/ Cleanup old OBI history (called by timer)
.rte.imb.cleanup:{[]
  cutoff:.z.p - .rte.cfg.obiRetentionNs;
  delete from `.rte.imb.history where time < cutoff;
  };

/ =============================================================================
/ Query Interface
/ =============================================================================

/ Get VWAP for a symbol over N minutes
/ Usage: .rte.getVwap[`BTCUSDT; 5]
.rte.getVwap:{[s;m] .rte.vwap.calc[s; m] };

/ Get summary table for all symbols: last price, VWAPs, and trend
/ Usage: .rte.getSummary[1;5]  / 1-min vs 5-min VWAP
/ trend: `up if short VWAP > long VWAP, `down otherwise
.rte.getSummary:{[shortMin;longMin]
  syms:asc distinct (0!tradeBuckets)`sym;
  if[0 = count syms; :([] sym:(); lastPrice:(); vwapShort:(); vwapLong:(); trend:())];
  
  lp:exec last sumPxQty % sumQty by sym from tradeBuckets;
  vShort:{[m;s] first exec vwap from .rte.getVwap[s;m]}[shortMin] each syms;
  vLong:{[m;s] first exec vwap from .rte.getVwap[s;m]}[longMin] each syms;
  
  trend:?[vShort > vLong; `up; `down];
  
  ([] sym:syms; lastPrice:lp syms; vwapShort:vShort; vwapLong:vLong; trend:trend)
  };

/ Get latest order book imbalance for a symbol
/ Usage: .rte.getImbalance[`BTCUSDT]
.rte.getImbalance:{[s]
  if[not s in key .rte.imb.latest;
    :([] sym:enlist s; bidDepth:enlist 0n; askDepth:enlist 0n; imbalance:enlist 0n; smOBI:enlist 0n; time:enlist 0Np)];
  data:.rte.imb.latest[s];
  ([] sym:enlist s; bidDepth:enlist data`bidDepth; askDepth:enlist data`askDepth; 
      imbalance:enlist data`imbalance; smOBI:enlist data`smOBI; time:enlist data`time)
  };
  
/ Get OBI with smoothed EMA and pressure for all symbols
/ Usage: .rte.getImbalanceAll[]
.rte.getImbalanceAll:{[]
  syms:key .rte.imb.latest;
  if[0 = count syms; :([] sym:(); OBI:(); smOBI:(); pressure:())];
  obi:{.rte.imb.latest[x]`imbalance} each syms;
  smOBI:{.rte.imb.latest[x]`smOBI} each syms;
  th:.rte.cfg.obiThreshold;
  pressure:?[smOBI > th; `buyer; ?[smOBI < neg th; `seller; `neutral]];
  ([] sym:syms; OBI:obi; smOBI:smOBI; pressure:pressure)
  };

/ Get OBI history for a symbol
/ Usage: .rte.getOBIHistory[`BTCUSDT; 5]
.rte.getOBIHistory:{[s;m]
  cutoff:.z.p - m * 60 * 1000000000j;
  select time, OBI, smOBI from .rte.imb.history where sym in enlist s, time>=cutoff
  };

/ Get L5 order book for display (cold path - formatting done here)
/ Usage: .rte.getOrderBook[`BTCUSDT]
.rte.getOrderBook:{[s]
  if[not s in key .rte.book.latest;
    :([] bidQty:5#0n; bidPrice:5#0n; askPrice:5#0n; askQty:5#0n)];
  d:.rte.book.latest[s];
  ([] bidQty:d 5 6 7 8 9; bidPrice:d 0 1 2 3 4; askPrice:d 10 11 12 13 14; askQty:d 15 16 17 18 19)
  };

/ Get spread and mid-price
/ Usage: .rte.getSpread[`BTCUSDT]
.rte.getSpread:{[s]
  if[not s in key .rte.book.latest;
    :`spread`mid`bestBid`bestAsk`time!(0n;0n;0n;0n;0Np)];
  d:.rte.book.latest[s];
  bestBid:d 0; bestAsk:d 10;
  `spread`mid`bestBid`bestAsk`time!(bestAsk - bestBid; 0.5 * bestBid + bestAsk; bestBid; bestAsk; d 20)
  };

/ Get latest var-covar matrix
/ Usage: .rte.getVcov[]
.rte.getVcov:{[] .rte.vcov.latest };

/ Get var-covar history for a symbol pair
/ Usage: .rte.getVcovHistory[`BTCUSDT; `ETHUSDT; 15]
.rte.getVcovHistory:{[s1;s2;mins]
  cutoff:.z.p - mins * 60 * 1000000000j;
  select time, covar from .rte.vcov.history where sym1=s1, sym2=s2, time>=cutoff
  };

/ Get correlation matrix from var-covar (derived)
/ Usage: .rte.getCorrelation[]
.rte.getCorrelation:{[]
  if[0 = count .rte.vcov.latest; :()!()];
  syms:.rte.vcov.latest`syms;
  matrix:.rte.vcov.latest`matrix;
  syms!{[syms;matrix;i]
    vari:matrix[syms i;syms i];
    syms!{[matrix;syms;i;vari;j]
      varj:matrix[syms j;syms j];
      covij:matrix[syms i;syms j];
      $[(vari > 0) & varj > 0; covij % sqrt vari * varj; 0n]
    }[matrix;syms;i;vari] each til count syms
  }[syms;matrix] each til count syms
  };

/ Get correlation matrix as table for dashboard
/ Usage: .rte.getCorrelationTable[]
.rte.getCorrelationTable:{[]
  corr:.rte.getCorrelation[];
  if[0 = count corr; :()];
  ([] sym:key corr) ,' flip corr
  };

/ Get annualized volatility for all symbols
/ Usage: .rte.getAnnualizedVol[]
.rte.getAnnualizedVol:{[]
  if[0 = count .rte.vcov.latest; :()!()];
  / Annualization factor: seconds per year / bucket size
  / 365 * 24 * 60 * 60 = 31,536,000 seconds per year (crypto 24/7)
  factor:31536000 % .rte.cfg.vcovBucketSec;
  syms:.rte.vcov.latest`syms;
  matrix:.rte.vcov.latest`matrix;
  syms!{[matrix;factor;s] sqrt factor * matrix[s;s]}[matrix;factor] each syms
  };

/ Get vol comparison table
/ Usage: .rte.getVolComparison[]
.rte.getVolComparison:{[]
  vol:.rte.getAnnualizedVol[];
  if[0 = count vol; :([] sym:(); annualizedVol:(); impliedVol:(); vsIVol:())];
  syms:key vol;
  impliedVol:.rte.cfg.iVol syms;
  annualizedVol:100 * value vol;
  vsIVol:?[annualizedVol > impliedVol; `above; `below];
  ([] sym:syms; annualizedVol:annualizedVol; impliedVol:impliedVol; vsIVol:vsIVol)
  };

/ =============================================================================
/ Update Handler (Called by Tickerplant and during replay)
/ =============================================================================

/ Trade data layout:
/   Index 0:  time
/   Index 1:  sym
/   Index 2:  tradeId
/   Index 3:  price
/   Index 4:  qty
/   Index 5:  buyerIsMaker
/   Index 6:  exchEventTimeMs
/   Index 7:  exchTradeTimeMs
/   Index 8:  fhRecvTimeUtcNs
/   Index 9:  fhParseUs
/   Index 10: fhSendUs
/   Index 11: fhSeqNo
/   Index 12: tpRecvTimeUtcNs

/ Quote data layout:
/   Index 0:     time
/   Index 1:     sym
/   Index 2-6:   bidPrice1-5
/   Index 7-11:  bidQty1-5
/   Index 12-16: askPrice1-5
/   Index 17-21: askQty1-5
/   Index 22+:   metadata (fhRecvTimeUtcNs, fhParseUs, etc.)

.u.upd:{[tbl;data]
  if[tbl = `trade_binance;
    time:data 0;
    s:data 1;
    price:data 3;
    qty:data 4;
    
    .rte.bucket.add[s; time; price; qty];
  ];

  if[tbl = `quote_binance;
    time:data 0;
    s:data 1;
    .rte.book.update[s;data;time];
    bidDepth:(data 7) + (data 8) + (data 9) + (data 10) + (data 11);
    askDepth:(data 17) + (data 18) + (data 19) + (data 20) + (data 21);
    .rte.imb.update[s; bidDepth; askDepth; time];
  ];
  };

upd:.u.upd;

/ =============================================================================
/ Log Replay
/ =============================================================================

.rte.logFile:{[d] hsym `$(.rte.cfg.logDir,"/",string[d],".log") };

.rte.logExists:{[f] sz:@[hcount; f; -1j]; sz > 0 };

.rte.logInfo:{[f]
  if[not .rte.logExists[f]; :(0j; 0j)];
  info:-11!(-2;f);
  $[1 = count info; (info; hcount f); info]
  };

.rte.replayFile:{[f]
  if[not .rte.logExists[f]; -1 "RTE: Log file not found: ",string[f]; :0j];
  -1 "RTE: Replaying from ",string[f];
  replayed:.[{-11!x}; enlist f; {[e] -1 "RTE: Replay error - ",e; 0j}];
  -1 "RTE: Replayed ",string[replayed]," chunks";
  replayed
  };

.rte.replay:{[d]
  if[d ~ (::); d:.z.D];
  -1 "RTE: Starting replay for ",string[d];
  logFile:.rte.logFile[d];
  chunks:.rte.replayFile[logFile];
  -1 "RTE: Replay complete - ",string[chunks]," chunks";
  -1 "RTE: Trade buckets: ",string[count tradeBuckets],", Book symbols: ",string[count .rte.book.latest],", Imbalance symbols: ",string[count .rte.imb.latest];
  chunks
  };

/ =============================================================================
/ End-of-Day Handler
/ =============================================================================

.u.end:{[date]
  -1 "RTE: EOD received for ",string[date];
  delete from `tradeBuckets;
  .rte.book.latest:()!();
  .rte.imb.latest:()!();
  .rte.imb.ema:()!();
  delete from `.rte.imb.history;
  .rte.vcov.latest:()!();
  delete from `.rte.vcov.history;
  -1 "RTE: EOD processing complete - state cleared";
  };

/ =============================================================================
/ Subscription to Tickerplant
/ =============================================================================

.rte.connect:{[]
  -1 "RTE: Connecting to tickerplant on port ",string[.rte.cfg.tpPort],"...";
  h:@[hopen; `$"::",string .rte.cfg.tpPort; {-1 "RTE: Failed to connect to TP: ",x; 0N}];
  if[null h; '"Cannot connect to TP"];
  res:h (`.u.sub; `trade_binance; `);
  -1 "RTE: Subscribed to ",string[first res];
  res:h (`.u.sub; `quote_binance; `);
  -1 "RTE: Subscribed to ",string[first res];
  .rte.tpHandle:h;
  };

/ =============================================================================
/ Periodic Cleanup Timer
/ =============================================================================

.z.ts:{[]
  .rte.bucket.cleanup[];
  .rte.vcov.update[];
  .rte.vcov.cleanup[];
  .rte.imb.cleanup[];
  };

/ =============================================================================
/ Startup
/ =============================================================================

system "p ",string .rte.cfg.port;

-1 "=======================================================";
-1 "RTE (Production Grade) starting on port ",string[.rte.cfg.port];
-1 "=======================================================";
-1 "Configuration:";
-1 "  Bucket size: ",string[.rte.cfg.bucketSec],"s (for VWAP)";
-1 "  Bucket retention: ",string[.rte.cfg.bucketRetentionMin]," minutes";
-1 "  Var-Covar window: ",string[.rte.cfg.vcovWindowMin]," minutes";
-1 "  Var-Covar bucket: ",string[.rte.cfg.vcovBucketSec],"s (resampled)";
-1 "  Var-Covar min buckets: ",string[.rte.cfg.vcovMinBuckets]," (for isValid)";
-1 "  Var-Covar retention: ",string[.rte.cfg.vcovRetentionMin]," minutes";
-1 "  OBI alpha: ",string[.rte.cfg.obiAlpha]," (EMA smoothing)";
-1 "  OBI threshold: ",string[.rte.cfg.obiThreshold]," (pressure)";
-1 "  OBI retention: ",string[.rte.cfg.obiRetentionMin]," minutes";
-1 "  Cleanup interval: ",string[.rte.cfg.cleanupIntervalMs],"ms";

-1 "";
-1 "Checking for log to replay...";
logExists:.rte.logExists[.rte.logFile[.z.D]];

if[logExists; -1 "RTE: Found existing log for today - replaying..."; .rte.replay[.z.D]; .rte.bucket.cleanup[]; -1 "RTE: Cleanup applied"];
if[not logExists; -1 "RTE: No existing log for today"];

-1 "";
.rte.connect[];

system "t ",string .rte.cfg.cleanupIntervalMs;

-1 "";
-1 "State:";
-1 "  Trade buckets: ",string[count tradeBuckets];
-1 "  Book symbols: ",string[count .rte.book.latest];
-1 "  Imbalance symbols: ",string[count .rte.imb.latest];
-1 "";
-1 "Query interface:";
-1 "  .rte.getVwap[`BTCUSDT; 5]                 / 5-minute VWAP";
-1 "  .rte.getVwap[`BTCUSDT; 1]                 / 1-minute VWAP";
-1 "  .rte.getSummary[1;5]                      / Summary with VWAP trend";
-1 "  .rte.getImbalance[`BTCUSDT]               / Latest imbalance";
-1 "  .rte.getImbalanceAll[]                    / OBI with EMA and pressure";
-1 "  .rte.getOBIHistory[`BTCUSDT; 5]           / OBI history (5 min)";
-1 "  .rte.getOrderBook[`BTCUSDT]               / L5 order book for display";
-1 "  .rte.getSpread[`BTCUSDT]                  / Spread and mid-price";
-1 "  .rte.getVarCovar[60]                      / Var-covar matrix (60-min)";
-1 "  .rte.getVcov[]                            / Latest var-covar matrix";
-1 "  .rte.getVcovHistory[`BTCUSDT;`ETHUSDT;15] / Covariance history";
-1 "  .rte.getCorrelation[]                     / Correlation matrix";
-1 "  .rte.getCorrelationTable[]                / Correlation as table";
-1 "  .rte.getAnnualizedVol[]                   / Annualized volatility";
-1 "  .rte.getVolComparison[]                   / Vol vs implied vol";
-1 "  .rte.replay[.z.D]                         / Replay today's log";
-1 "";
-1 "RTE ready";
-1 "=======================================================";
