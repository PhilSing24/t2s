/ features.q - Feature Engineering Library
/ Selective feature building

/ =============================================================================
/ Feature Registry
/ =============================================================================

.feat.reg:()!();

/ =============================================================================
/ Price Features (.feat.price)
/ =============================================================================

.feat.price.logReturn:{[dib;p] fills 0n,1_log ratios dib`close};
.feat.price.momentum:{[dib;p] fills dib[`close] % p[`n] xprev dib`close};
.feat.price.meanReversion:{[dib;p] ma:mavg[p[`n];dib`close]; (dib[`close]-ma)%ma};

.feat.reg[`logReturn]:.feat.price.logReturn;
.feat.reg[`momentum]:.feat.price.momentum;
.feat.reg[`meanReversion]:.feat.price.meanReversion;

/ =============================================================================
/ Volatility Features (.feat.vol)
/ =============================================================================

.feat.vol.realizedVol:{[dib;p] rets:.feat.price.logReturn[dib;p]; mavg[p[`n];rets*rets] xexp 0.5};
.feat.vol.hlRange:{[dib;p] (dib[`high]-dib[`low])%dib`low};
.feat.vol.parkinson:{[dib;p] hl2:(log[dib[`high]%dib`low])xexp 2; (mavg[p[`n];hl2]%(4*log 2))xexp 0.5};

.feat.reg[`realizedVol]:.feat.vol.realizedVol;
.feat.reg[`hlRange]:.feat.vol.hlRange;
.feat.reg[`parkinson]:.feat.vol.parkinson;

/ =============================================================================
/ Technical Features (.feat.tech)
/ =============================================================================

.feat.tech.EMA:{[prices;n] alpha:2f%1+n; {[a;prev;curr](a*curr)+(1-a)*prev}[alpha]\[first prices;prices]};

.feat.tech.relativeStrength:{[n;y]
    begin:n#0Nf;
    start:avg (n+1)#y;
    begin,start,{(y+x*(z-1))%z}\[start;(n+1)_y;n]
    };

.feat.tech.rsi:{[dib;p]
    n:14;
    diff:dib[`close] - prev dib`close;
    gains:?[diff>0;diff;0f];
    losses:?[diff<0;neg diff;0f];
    rs:.feat.tech.relativeStrength[n;gains] % .feat.tech.relativeStrength[n;losses];
    100*rs%(1+rs)
    };

.feat.tech.macd:{[dib;p]
    c:dib`close;
    emaFast:.feat.tech.EMA[c;12];
    emaSlow:.feat.tech.EMA[c;26];
    emaFast - emaSlow
    };

.feat.tech.macdSignal:{[dib;p]
    macdLine:.feat.tech.macd[dib;p];
    .feat.tech.EMA[macdLine;9]
    };

.feat.tech.macdHist:{[dib;p]
    .feat.tech.macd[dib;p] - .feat.tech.macdSignal[dib;p]
    };

.feat.reg[`rsi]:.feat.tech.rsi;
.feat.reg[`macd]:.feat.tech.macd;
.feat.reg[`macdSignal]:.feat.tech.macdSignal;
.feat.reg[`macdHist]:.feat.tech.macdHist;

/ =============================================================================
/ Trade Flow Features (.feat.flow)
/ =============================================================================

.feat.flow.tradeImbalance:{[dib;p] (dib[`bqty]-dib[`sqty])%(dib[`bqty]+dib[`sqty])};
.feat.flow.vpin:{[dib;p] imb:abs dib[`bqty]-dib[`sqty]; tot:dib[`bqty]+dib[`sqty]; mavg[p[`n];imb]%mavg[p[`n];tot]};
.feat.flow.tradeIntensity:{[dib;p] dib[`ticks]%1|.feat.bar.duration[dib;p]};

.feat.reg[`tradeImbalance]:.feat.flow.tradeImbalance;
.feat.reg[`vpin]:.feat.flow.vpin;
.feat.reg[`tradeIntensity]:.feat.flow.tradeIntensity;

/ =============================================================================
/ Bar-Specific Features (.feat.bar)
/ =============================================================================

.feat.bar.duration:{[dib;p] `long$(dib[`barend]-dib`barstart)%1000000000};
.feat.bar.closePosition:{[dib;p] range:dib[`high]-dib`low; ?[range=0;0.5;(dib[`close]-dib`low)%range]};
.feat.bar.thetaNorm:{[dib;p] dib[`theta]%dib`dqty};
.feat.bar.buyPct:{[dib;p] dib[`bqty]%(dib[`bqty]+dib`sqty)};

.feat.reg[`duration]:.feat.bar.duration;
.feat.reg[`closePosition]:.feat.bar.closePosition;
.feat.reg[`thetaNorm]:.feat.bar.thetaNorm;
.feat.reg[`buyPct]:.feat.bar.buyPct;

/ =============================================================================
/ Feature Builder
/ =============================================================================

/ List available features
.feat.list:{[] asc key .feat.reg};

/ Helper function
.feat.calc:{[dib;params;f] .feat.reg[f][dib;params]};

/ Build selected features
/ n = lookback period for moving averages
.feat.build:{[dib;features;n] 
    p:enlist[`n]!enlist n;
    ([] bn:dib`bn; sym:dib`sym; barstart:dib`barstart; close:dib`close),'flip features!.feat.calc[dib;p] each features
    };

/ Drop rows with null in specified columns
.feat.dropNull:{[t;cols] t where not any null each t cols};