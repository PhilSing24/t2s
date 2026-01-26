/ mlresearch.q - Research Engine (v2)
/ DIB Bar Generation from HDB (Fixed + Adaptive with EWMA)

/ NOTE: This implementation is "DIB-inspired" but NOT strict AFML-compliant.
/ AFML uses: threshold = E[T] × E[|Iₜ|]
/ We use: threshold ∝ √E[T] with target anchoring and price adjustment
/ Reason: Dollar imbalance behaves as a random walk; AFML formula caused threshold explosion.

/ v2 ENHANCEMENTS:
/ - Target Anchoring: Threshold mean-reverts toward warm-up calibration value (prevents drift)
/ - Price Adjustment: Threshold scales with price changes (daily adjustment)

/ =============================================================================
/ Configuration
/ =============================================================================

.re.cfg.hdbPath:`$":../binancedata";
.re.cfg.syms: enlist `BTCUSDT;
.re.cfg.targetBarsPerDay:200;
.re.cfg.span:100;
.re.cfg.anchorStrength:0.02;  / kappa: 2% mean-reversion toward anchor per bar

/ =============================================================================
/ HDB Connection
/ =============================================================================

.re.loadHDB:{[]
    @[system; "l ", 1_ string .re.cfg.hdbPath; {-1 "RE: Failed to load HDB - ",x}]
    };

/ =============================================================================
/ Tick Retrieval
/ =============================================================================

.re.getTrades:{[s;d1;d2]
    select from trade where date within (d1;d2), sym=s
    };

.re.getTradesAllSyms:{[d]
    select from trade where date=d, sym in .re.cfg.syms
    };

/ =============================================================================
/ Trade Direction: derive bt from buyerIsMaker
/ buyerIsMaker=0 → aggressive buy → bt=+1
/ buyerIsMaker=1 → aggressive sell → bt=-1
/ =============================================================================

.re.getBt:{[buyerIsMaker]
    1 - 2*buyerIsMaker
    };

/ =============================================================================
/ State & Output Tables
/ =============================================================================

/ Initialize empty state table (added avgPrice for price adjustment)
.re.initState:{[]
    .re.dibState:([sym:`symbol$(); date:`date$()] 
        barNum:`long$();
        theta:`float$();
        E_T:`float$();
        E_absTheta:`float$();
        threshold:`float$();
        ticksInBar:`long$();
        sumAbsTheta:`float$();
        avgPrice:`float$()          / NEW: average price for price adjustment
    );
    };

/ Initialize anchor table (warm-up reference values per symbol)
.re.initAnchor:{[]
    .re.anchor:([sym:`symbol$()] 
        threshold:`float$();        / Warm-up calibrated threshold
        avgPrice:`float$()          / Warm-up average price
    );
    };

/ Initialize empty bars table
.re.initBars:{[]
    .re.dibBars:([]
        sym:`symbol$();
        barNum:`long$();
        barStart:`timestamp$();
        barEnd:`timestamp$();
        open:`float$();
        high:`float$();
        low:`float$();
        close:`float$();
        volume:`float$();
        dollarVol:`float$();
        tickCount:`long$();
        buyVol:`float$();
        sellVol:`float$();
        vwap:`float$();
        thetaAtTrigger:`float$();
        threshold:`float$();
        isWarmUp:`boolean$()
    );
    };

/ =============================================================================
/ Fixed DIB Bar Generation (no adaptation - for reproducible experiments)
/ =============================================================================

.re.buildDIB:{[trades;threshold]
    / Add trade direction and dollar values
    trades:update bt:.re.getBt[buyerIsMaker] from trades;
    trades:update dollarVol:price*qty, signedDollar:bt*price*qty from trades;
    
    / Bar assignment scan - output barNum for THIS tick, not next
    / State: (theta; barNumState; barNumOutput)
    barAssign:{[th;state;sd]
        theta:state[0]+sd;
        $[abs[theta]>=th; 
            (0f; state[1]+1; state[1]);    / trigger: output current barNum, then advance state
            (theta; state[1]; state[1])]   / no trigger: output current barNum
        };
    
    / Run scan and extract barNumOutput (index 2)
    trades:update barNum:{x[;2]}(0f;0j;0j) barAssign[threshold]\signedDollar by sym from trades;
    
    / Aggregate into bars
    0!select
        barStart:first exchTradeTs,
        barEnd:last exchTradeTs,
        open:first price,
        high:max price,
        low:min price,
        close:last price,
        volume:sum qty,
        dollarVol:sum dollarVol,
        tickCount:count i,
        buyVol:sum qty where bt=1,
        sellVol:sum qty where bt=-1,
        vwap:qty wavg price,
        thetaAtTrigger:sum signedDollar,
        threshold:threshold,
        isWarmUp:0b
        by sym, barNum from trades
    };

/ =============================================================================
/ Warm-Up Calibration (Day 1)
/ Establishes anchor threshold and anchor price per symbol
/ =============================================================================

.re.calibrateWarmUp:{[dt;targetBarsPerDay]
    -1 "RE: Starting warm-up calibration for date ",string dt;
    
    / Load all trades for warm-up day
    trades:.re.getTradesAllSyms[dt];
    -1 "RE: Loaded ",string[count trades]," ticks";
    
    / Add trade direction and dollar values
    trades:update bt:.re.getBt[buyerIsMaker] from trades;
    trades:update dollarVol:price*qty, signedDollar:bt*price*qty, absSignedDollar:abs price*qty from trades;
    
    / Two-pass calibration per symbol
    results:{[trades;targetBarsPerDay;dt;s]
        symTrades:select from trades where sym=s;
        tickCount:count symTrades;
        stdSD:dev symTrades`signedDollar;
        
        / Calculate average price for this symbol (anchor price)
        anchorPrice:avg symTrades`price;
        
        / Pass 1: initial estimate using random walk formula
        / threshold = σ × √(N/target)
        initThreshold:stdSD * sqrt tickCount % targetBarsPerDay;
        
        / Bar assignment with correct boundary handling
        barAssign:{[th;state;sd] 
            theta:state[0]+sd; 
            $[abs[theta]>=th; 
                (0f; state[1]+1; state[1]);
                (theta; state[1]; state[1])]
        };
        
        pass1States:(0f;0j;0j) barAssign[initThreshold]\ symTrades`signedDollar;
        pass1Bars:last[pass1States][1];
        
        / Pass 2: adjust threshold based on actual count
        adjFactor:sqrt pass1Bars % targetBarsPerDay;
        finalThreshold:initThreshold * adjFactor;
        
        -1 "RE: ",string[s]," - Pass1: ",string[pass1Bars]," bars, adjusting by ",string[adjFactor];
        
        / Run final bar assignment
        allStates:(0f;0j;0j) barAssign[finalThreshold]\ symTrades`signedDollar;
        symTrades:update barNum:allStates[;2] from symTrades;
        
        lastState:last allStates;
        finalBarNum:lastState[1];
        finalTheta:lastState[0];
        
        / Completed bars
        completedTrades:select from symTrades where barNum < finalBarNum;
        
        / Partial bar state
        ticksInPartial:tickCount - count completedTrades;
        sumAbsThetaPartial:0f;
        
        / Compute E_T from actual data
        E_T:count[completedTrades] % max(1;finalBarNum);
        E_absTheta:finalThreshold % sqrt max(1f;E_T);
        
        bars:$[0=count completedTrades;
            ([] sym:`symbol$(); barNum:`long$(); barStart:`timestamp$(); barEnd:`timestamp$();
               open:`float$(); high:`float$(); low:`float$(); close:`float$();
               volume:`float$(); dollarVol:`float$(); tickCount:`long$();
               buyVol:`float$(); sellVol:`float$(); vwap:`float$();
               thetaAtTrigger:`float$(); threshold:`float$(); isWarmUp:`boolean$());
            0!select
                barStart:first exchTradeTs,
                barEnd:last exchTradeTs,
                open:first price,
                high:max price,
                low:min price,
                close:last price,
                volume:sum qty,
                dollarVol:sum dollarVol,
                tickCount:count i,
                buyVol:sum qty where bt=1,
                sellVol:sum qty where bt=-1,
                vwap:qty wavg price,
                thetaAtTrigger:sum signedDollar,
                threshold:finalThreshold,
                isWarmUp:1b
                by sym, barNum from completedTrades
        ];
        
        / State includes avgPrice for price adjustment
        state:([] sym:enlist s; date:enlist dt; barNum:enlist finalBarNum; theta:enlist finalTheta;
               E_T:enlist E_T; E_absTheta:enlist E_absTheta; threshold:enlist finalThreshold;
               ticksInBar:enlist ticksInPartial; sumAbsTheta:enlist sumAbsThetaPartial;
               avgPrice:enlist anchorPrice);
        
        / Anchor stores the calibrated threshold and price for mean-reversion
        anchor:([] sym:enlist s; threshold:enlist finalThreshold; avgPrice:enlist anchorPrice);
        
        -1 "RE: ",string[s]," - ",string[count bars]," bars, anchor threshold=",string[finalThreshold],
           ", anchor price=",string[anchorPrice];
        
        `bars`state`anchor!(bars;state;anchor)
    }[trades;targetBarsPerDay;dt] each .re.cfg.syms;
    
    / Combine results
    allBars:raze results`bars;
    allStates:raze results`state;
    allAnchors:raze results`anchor;
    
    / Store results
    `.re.dibBars upsert allBars;
    `.re.dibState upsert 2!allStates;
    `.re.anchor upsert 1!allAnchors;
    
    -1 "RE: Warm-up complete. Total bars: ",string count allBars;
    -1 "RE: Anchors established:";
    show .re.anchor;
    
    `bars`state`anchor!(allBars;allStates;allAnchors)
    };

/ =============================================================================
/ Adaptive DIB Processing (Day 2+)
/ With Target Anchoring and Price Adjustment
/ =============================================================================

.re.processAdaptiveDay:{[dt;span]
    alpha:2.0 % span + 1;
    kappa:.re.cfg.anchorStrength;
    
    -1 "RE: Processing adaptive DIB for date ",string[dt]," (span=",string[span],", alpha=",string[alpha],", kappa=",string[kappa],")";
    
    / Load all trades for the day
    trades:.re.getTradesAllSyms[dt];
    -1 "RE: Loaded ",string[count trades]," ticks";
    
    / Add trade direction and dollar values
    trades:update bt:.re.getBt[buyerIsMaker] from trades;
    trades:update dollarVol:price*qty, signedDollar:bt*price*qty, absSignedDollar:abs price*qty from trades;
    
    / Process each symbol
    results:{[trades;alpha;kappa;dt;s]
        symTrades:select from trades where sym=s;
        
        / Get previous state
        prevStates:select from .re.dibState where sym=s;
        
        $[0=count prevStates;
            [
                -1 "RE: ERROR - No previous state for ",string s;
                :([] sym:`symbol$(); barNum:`long$(); barStart:`timestamp$(); barEnd:`timestamp$();
                   open:`float$(); high:`float$(); low:`float$(); close:`float$();
                   volume:`float$(); dollarVol:`float$(); tickCount:`long$();
                   buyVol:`float$(); sellVol:`float$(); vwap:`float$();
                   thetaAtTrigger:`float$(); threshold:`float$(); isWarmUp:`boolean$())
            ];
            prevState:last prevStates
        ];
        
        / Get anchor for this symbol
        anchorRow:exec first threshold, first avgPrice from .re.anchor where sym=s;
        anchorThreshold:anchorRow`threshold;
        anchorPrice:anchorRow`avgPrice;
        
        / Calculate today's average price for price adjustment
        todayAvgPrice:avg symTrades`price;
        
        / Price adjustment ratio: if price doubled, threshold should double
        priceRatio:todayAvgPrice % anchorPrice;
        
        -1 "RE: ",string[s]," - Price ratio: ",string[priceRatio]," (today=",string[todayAvgPrice],", anchor=",string[anchorPrice],")";
        
        / Extract state values
        startBarNum:prevState`barNum;
        startTheta:prevState`theta;
        E_T:prevState`E_T;
        E_absTheta:prevState`E_absTheta;
        currentThreshold:prevState`threshold;
        ticksInBar:prevState`ticksInBar;
        sumAbsTheta:prevState`sumAbsTheta;
        
        / Price-adjusted anchor for today
        priceAdjAnchor:anchorThreshold * priceRatio;
        
        -1 "RE: ",string[s]," - Starting from barNum=",string[startBarNum],
           ", threshold=",string[currentThreshold],", price-adj anchor=",string[priceAdjAnchor];
        
        / Adaptive bar assignment with target anchoring and price adjustment
        / State vector: (theta; barNumState; E_T; E_absTheta; threshold; ticksInBar; sumAbsTheta; barNumOutput; thresholdOutput; thetaAtTrigger)
        / Index:         0      1            2     3           4          5           6            7             8                9
        initState:(startTheta; startBarNum; E_T; E_absTheta; currentThreshold; ticksInBar; sumAbsTheta; startBarNum; currentThreshold; startTheta);
        
        / The scan applies:
        / 1. Sqrt(E_T) scaling (random walk formula)
        / 2. Target anchoring (mean-reversion toward price-adjusted anchor)
        adaptiveScan:{[a;k;priceAdjAnch;state;row]
            theta:state[0] + row`signedDollar;
            ticks:state[5] + 1;
            sumAbs:state[6] + row`absSignedDollar;
            currentBarNum:state[1];
            currentThreshold:state[4];
            
            $[abs[theta] >= currentThreshold;
                / Bar complete - apply adaptation
                [
                    oldET:state[2];
                    / 1. Update E_T with EWMA
                    newET:(a * ticks) + (1-a) * oldET;
                    
                    / 2. Sqrt scaling (random walk formula)
                    rawThreshold:currentThreshold * sqrt newET % oldET;
                    
                    / 3. Target anchoring: pull toward price-adjusted anchor
                    / newThreshold = rawThreshold + kappa × (anchor - rawThreshold)
                    / This is equivalent to: newThreshold = (1-kappa) × rawThreshold + kappa × anchor
                    newThreshold:rawThreshold + k * (priceAdjAnch - rawThreshold);
                    
                    newEAbsTheta:newThreshold % sqrt newET;
                    / Output theta at trigger (full accumulated theta including carried)
                    (0f; currentBarNum+1; newET; newEAbsTheta; newThreshold; 0j; 0f; currentBarNum; currentThreshold; theta)
                ];
                / Continue accumulating - output current running theta
                (theta; currentBarNum; state[2]; state[3]; currentThreshold; ticks; sumAbs; currentBarNum; currentThreshold; theta)
            ]
        };
        
        / Run the scan
        allStates:initState adaptiveScan[alpha;kappa;priceAdjAnchor]\ ([] signedDollar:symTrades`signedDollar; absSignedDollar:symTrades`absSignedDollar);
        
        / Extract barNumOutput, thresholdOutput, and thetaAtTrigger for each tick
        symTrades:update barNum:allStates[;7], thresholdUsed:allStates[;8], thetaFull:allStates[;9] from symTrades;
        
        / Get final state
        lastState:last allStates;
        finalBarNum:lastState[1];
        finalTheta:lastState[0];
        finalET:lastState[2];
        finalEAbsTheta:lastState[3];
        finalThreshold:lastState[4];
        finalTicksInBar:lastState[5];
        finalSumAbsTheta:lastState[6];
        
        / Completed bars
        completedTrades:select from symTrades where barNum < finalBarNum;
        
        bars:$[0=count completedTrades;
            ([] sym:`symbol$(); barNum:`long$(); barStart:`timestamp$(); barEnd:`timestamp$();
               open:`float$(); high:`float$(); low:`float$(); close:`float$();
               volume:`float$(); dollarVol:`float$(); tickCount:`long$();
               buyVol:`float$(); sellVol:`float$(); vwap:`float$();
               thetaAtTrigger:`float$(); threshold:`float$(); isWarmUp:`boolean$());
            0!select
                barStart:first exchTradeTs,
                barEnd:last exchTradeTs,
                open:first price,
                high:max price,
                low:min price,
                close:last price,
                volume:sum qty,
                dollarVol:sum dollarVol,
                tickCount:count i,
                buyVol:sum qty where bt=1,
                sellVol:sum qty where bt=-1,
                vwap:qty wavg price,
                thetaAtTrigger:last thetaFull,
                threshold:first thresholdUsed,
                isWarmUp:0b
                by sym, barNum from completedTrades
        ];
        
        / State includes avgPrice for next day
        state:([] sym:enlist s; date:enlist dt; barNum:enlist finalBarNum; theta:enlist finalTheta;
               E_T:enlist finalET; E_absTheta:enlist finalEAbsTheta; threshold:enlist finalThreshold;
               ticksInBar:enlist finalTicksInBar; sumAbsTheta:enlist finalSumAbsTheta;
               avgPrice:enlist todayAvgPrice);
        
        -1 "RE: ",string[s]," - ",string[count bars]," bars, threshold: ",string[currentThreshold]," -> ",string[finalThreshold];
        
        `bars`state!(bars;state)
    }[trades;alpha;kappa;dt] each .re.cfg.syms;
    
    / Combine results
    allBars:raze results`bars;
    allStates:raze results`state;
    
    / Store results
    `.re.dibBars upsert allBars;
    `.re.dibState upsert 2!allStates;
    
    -1 "RE: Day complete. Bars added: ",string count allBars;
    `bars`state!(allBars;allStates)
    };

/ =============================================================================
/ Main Orchestration
/ =============================================================================

.re.runDIB:{[warmUpDate;startDate;endDate;targetBarsPerDay;span]
    -1 "RE: ========================================";
    -1 "RE: Starting DIB Generation Pipeline (v2)";
    -1 "RE: Warm-up date: ",string warmUpDate;
    -1 "RE: Date range: ",string[startDate]," to ",string endDate;
    -1 "RE: Target bars/day: ",string targetBarsPerDay;
    -1 "RE: Span: ",string span;
    -1 "RE: Anchor strength (kappa): ",string .re.cfg.anchorStrength;
    -1 "RE: Symbols: ",", " sv string .re.cfg.syms;
    -1 "RE: ========================================";
    
    / Initialize tables
    .re.initState[];
    .re.initBars[];
    .re.initAnchor[];
    
    / Phase 1: Warm-up (establishes anchors)
    -1 "RE: Phase 1 - Warm-up calibration (establishing anchors)";
    .re.calibrateWarmUp[warmUpDate; targetBarsPerDay];
    
    / Phase 2: Adaptive processing
    -1 "RE: Phase 2 - Adaptive DIB generation (with anchoring + price adjustment)";
    dates:startDate + til 1 + endDate - startDate;
    
    {[span;d]
        .re.processAdaptiveDay[d;span]
    }[span] each dates;
    
    / Summary
    -1 "RE: ========================================";
    -1 "RE: Pipeline Complete";
    -1 "RE: Total bars generated: ",string count .re.dibBars;
    -1 "RE: Bars per symbol:";
    show select bars:count i by sym from .re.dibBars;
    -1 "RE: Threshold drift summary:";
    show select 
        anchorTh:first threshold, 
        minTh:min threshold, 
        maxTh:max threshold,
        finalTh:last threshold,
        driftPct:100*(last[threshold]-first[threshold])%first threshold
        by sym from .re.dibBars;
    -1 "RE: ========================================";
    
    .re.dibBars
    };

/ =============================================================================
/ Convenience function with defaults
/ =============================================================================

.re.run:{[]
    .re.runDIB[2026.01.17; 2026.01.18; 2026.01.23; .re.cfg.targetBarsPerDay; .re.cfg.span]
    };

/ =============================================================================
/ Save/Load Functions
/ =============================================================================

.re.save:{[]
    -1 "RE: Saving DIB bars, state, and anchors...";
    (hsym `$"dibBars") set .re.dibBars;
    (hsym `$"dibState") set .re.dibState;
    (hsym `$"dibAnchor") set .re.anchor;
    -1 "RE: Saved to dibBars, dibState, and dibAnchor files";
    };

.re.load:{[]
    -1 "RE: Loading DIB bars, state, and anchors...";
    .re.dibBars:get hsym `$"dibBars";
    .re.dibState:get hsym `$"dibState";
    .re.anchor:get hsym `$"dibAnchor";
    -1 "RE: Loaded ",string[count .re.dibBars]," bars";
    };

/ =============================================================================
/ Diagnostic Functions
/ =============================================================================

.re.showState:{[]
    show .re.dibState;
    };

.re.showAnchor:{[]
    show .re.anchor;
    };

.re.showBars:{[n]
    show n sublist .re.dibBars;
    };

.re.barStats:{[]
    select 
        bars:count i,
        avgTickCount:avg tickCount,
        minTickCount:min tickCount,
        maxTickCount:max tickCount,
        avgThreshold:avg threshold,
        minThreshold:min threshold,
        maxThreshold:max threshold
        by sym, isWarmUp from .re.dibBars
    };

.re.thresholdEvolution:{[s]
    select barNum, threshold, tickCount, thetaAtTrigger from .re.dibBars where sym=s
    };

/ Threshold drift analysis
.re.thresholdDrift:{[]
    -1 "RE: Threshold drift analysis:";
    result:select 
        anchorTh:first threshold,
        minTh:min threshold,
        maxTh:max threshold,
        finalTh:last threshold,
        driftPct:100*(last[threshold]-first[threshold])%first threshold,
        maxDriftPct:100*(max[threshold]-first[threshold])%first threshold
        by sym from .re.dibBars;
    show result;
    result
    };

/ =============================================================================
/ Verification Functions
/ =============================================================================

/ Check that |thetaAtTrigger| >= threshold for each bar
.re.verifyTriggerConsistency:{[]
    -1 "RE: Verifying trigger consistency...";
    result:select 
        bars:count i,
        valid:sum abs[thetaAtTrigger]>=threshold*0.99,
        invalid:sum abs[thetaAtTrigger]<threshold*0.99
        by sym from .re.dibBars;
    show result;
    $[0=exec sum invalid from result;
        -1 "RE: PASS - All bars have |theta| >= threshold";
        -1 "RE: FAIL - Some bars have |theta| < threshold"];
    result
    };

/ Check VWAP calculation
.re.verifyVWAP:{[]
    -1 "RE: VWAP verification requires tick-level data. Use .re.verifyVWAPSample[sym;barNum] with loaded ticks.";
    };
