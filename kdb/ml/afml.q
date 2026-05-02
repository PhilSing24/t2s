/ afml.q - Dollar Imbalance Bars Implementation

/ Three modes:
/   dib   - Pure AFML DIB:   th = E[T] × E[d] × |2P-1|
/   drb   - Pure AFML DRB:   th = E[T] × E[d] × max(P, 1-P)
/   rwdib - Random Walk DIB: th = σ × √E[T] + anchoring + price adjustment

/ AFML modes have issues on high-frequency crypto data (threshold explosion)
/ RWDIB is stable and recommended for crypto

/ =============================================================================
/ Configuration
/ =============================================================================

.afml.cfg.hdb:`$":/home/philippe/t2s/hdb_binancedata";
.afml.cfg.syms:enlist `BTCUSDT;
.afml.cfg.target:200;
.afml.cfg.span:100;
.afml.cfg.mindol:50;   / minimum dollar value per trade
.afml.cfg.kappa:0.02;  / anchor strength (2% mean-reversion per bar)
.afml.cfg.verbose:0b;  / print progress messages

/ Logging helper
.afml.log:{[msg] if[.afml.cfg.verbose; -1 msg]};

/ =============================================================================
/ HDB Connection
/ =============================================================================

.afml.load:{[]
    @[system; "l ", 1_ string .afml.cfg.hdb; {-1 "AFML: Failed - ",x}]
    };

/ =============================================================================
/ Data Retrieval
/ =============================================================================

.afml.get:{[s;d] 
    t:select from trade where date=d, sym=s;
    idx:where (t[`price]*t`qty) >= .afml.cfg.mindol;
    t idx
    };
.afml.getall:{[d] 
    t:select from trade where date=d, sym in .afml.cfg.syms;
    idx:where (t[`price]*t`qty) >= .afml.cfg.mindol;
    t idx
    };

/ =============================================================================
/ Trade Direction
/ =============================================================================

.afml.bt:{[bim] 1 - 2*bim};

/ =============================================================================
/ Threshold (Pure AFML)
/ =============================================================================

/ DIB: |2P - 1|
.afml.threshdib:{[et;pb;ed] et * ed * abs -1 + 2*pb};

/ DRB: max(P, 1-P)
.afml.threshdrb:{[et;pb;ed] et * ed * pb | 1-pb};

/ =============================================================================
/ Threshold (Random Walk)
/ =============================================================================

/ RWDIB: σ × √E[T] - no E[d], no P term
.afml.threshrw:{[sigma;et] sigma * sqrt et};

/ =============================================================================
/ Scan Functions (DIB vs DRB)
/ =============================================================================

/ DIB: continuous accumulation (signs cancel)
/ state: (theta;bn;tib;bib;dib)
/ tr: (sd;isbuy;dvol)
.afml.scndib:{[th;st;tr]
    theta:st[0] + tr 0;
    tib:st[2]+1;
    bib:st[3]+tr 1;
    dib:st[4]+tr 2;
    $[th<=abs theta;
        (0f;st[1]+1;0j;0j;0f);
        (theta;st[1];tib;bib;dib)]
    };

/ DRB: reset on direction change
.afml.scndrb:{[th;st;tr]
    sd:tr 0;
    ptheta:st 0;
    / Reset if sign changes, else accumulate
    theta:$[(ptheta>0)=sd>0; ptheta+sd; sd];
    tib:st[2]+1;
    bib:st[3]+tr 1;
    dib:st[4]+tr 2;
    $[th<=abs theta;
        (0f;st[1]+1;0j;0j;0f);
        (theta;st[1];tib;bib;dib)]
    };

/ =============================================================================
/ Adaptive Scan Functions (with EWMA updates)
/ State: (theta;bn;et;pb;ed;th;tib;bib;dib;bnout;thout;thetaout)
/ Index:   0    1  2  3  4  5  6   7   8   9     10    11
/ =============================================================================

/ Adaptive DIB: continuous accumulation
.afml.scndiba:{[a;st;tr]
    theta:st[0] + tr 0;
    tib:st[6]+1;
    bib:st[7]+tr 1;
    dib:st[8]+tr 2;
    cth:st 5;
    
    $[cth<=abs theta;
        [
            / Bar done - EWMA update
            oet:st 2; opb:st 3; oed:st 4;
            net:(a*tib)+(1-a)*oet;
            npb:(a*bib%tib)+(1-a)*opb;
            ned:(a*dib%tib)+(1-a)*oed;
            nth:.afml.threshdib[net;npb;ned];
            (0f;st[1]+1;net;npb;ned;nth;0j;0j;0f;st 1;cth;theta)
        ];
        (theta;st 1;st 2;st 3;st 4;st 5;tib;bib;dib;st 1;st 5;theta)
    ]
    };

/ Adaptive DRB: reset on direction change
.afml.scndrba:{[a;st;tr]
    sd:tr 0;
    ptheta:st 0;
    / Reset if sign changes, else accumulate
    theta:$[(ptheta>0)=sd>0; ptheta+sd; sd];
    tib:st[6]+1;
    bib:st[7]+tr 1;
    dib:st[8]+tr 2;
    cth:st 5;
    
    $[cth<=abs theta;
        [
            / Bar done - EWMA update
            oet:st 2; opb:st 3; oed:st 4;
            net:(a*tib)+(1-a)*oet;
            npb:(a*bib%tib)+(1-a)*opb;
            ned:(a*dib%tib)+(1-a)*oed;
            nth:.afml.threshdrb[net;npb;ned];
            (0f;st[1]+1;net;npb;ned;nth;0j;0j;0f;st 1;cth;theta)
        ];
        (theta;st 1;st 2;st 3;st 4;st 5;tib;bib;dib;st 1;st 5;theta)
    ]
    };

/ =============================================================================
/ Random Walk Adaptive Scan (with √E[T] + anchoring)
/ State: (theta;bn;et;sigma;th;anchor;tib;dib;bnout;thout;thetaout)
/ Index:   0    1  2  3     4  5      6   7   8     9     10
/ =============================================================================

/ Adaptive RWDIB: √E[T] scaling + target anchoring
/ a = EWMA alpha, k = kappa (anchor strength)
.afml.scnrwdiba:{[a;k;st;tr]
    theta:st[0] + tr 0;
    tib:st[6]+1;
    dib:st[7]+tr 1;
    cth:st 4;
    
    $[cth<=abs theta;
        [
            / Bar done - update E[T] and threshold
            oet:st 2;
            sigma:st 3;
            anchor:st 5;
            
            / 1. EWMA update for E[T]
            net:(a*tib)+(1-a)*oet;
            
            / 2. √E[T] scaling
            rawth:cth * sqrt net % oet;
            
            / 3. Target anchoring: pull toward anchor
            nth:rawth + k * (anchor - rawth);
            
            (0f;st[1]+1;net;sigma;nth;anchor;0j;0f;st 1;cth;theta)
        ];
        (theta;st 1;st 2;st 3;st 4;st 5;tib;dib;st 1;st 4;theta)
    ]
    };

/ =============================================================================
/ State & Output Tables
/ =============================================================================

.afml.initstate:{[]
    .afml.state:([sym:`symbol$()] 
        date:`date$();
        bn:`long$();
        theta:`float$();
        et:`float$();
        pb:`float$();
        ed:`float$();
        th:`float$();
        tib:`long$();
        bib:`long$();
        dib:`float$();
        sigma:`float$();       / std of signed dollar (for rwdib)
        anchor:`float$();      / anchor threshold (for rwdib)
        anchorprice:`float$()  / anchor price (for rwdib price adjustment)
    );
    };

.afml.initbars:{[]
    .afml.bars:([]
        sym:`symbol$();
        bn:`long$();
        barstart:`timestamp$();
        barend:`timestamp$();
        open:`float$();
        high:`float$();
        low:`float$();
        close:`float$();
        qty:`float$();
        dqty:`float$();
        ticks:`long$();
        buys:`long$();
        bqty:`float$();
        sqty:`float$();
        vwap:`float$();
        theta:`float$();
        et:`float$();
        pb:`float$();
        ed:`float$();
        th:`float$();
        warmup:`boolean$()
    );
    };

.afml.empty:{[]
    ([] sym:`symbol$(); bn:`long$(); barstart:`timestamp$(); barend:`timestamp$();
       open:`float$(); high:`float$(); low:`float$(); close:`float$();
       qty:`float$(); dqty:`float$(); ticks:`long$(); buys:`long$();
       bqty:`float$(); sqty:`float$(); vwap:`float$(); theta:`float$();
       et:`float$(); pb:`float$(); ed:`float$(); th:`float$(); warmup:`boolean$())
    };

/ =============================================================================
/ Warm-Up Calibration (Day 1)
/ =============================================================================

.afml.warmup:{[dt;target;mode]
    .afml.log "AFML: ========================================";
    .afml.log "AFML: Warm-up for ",string[dt]," mode=",string mode;
    
    trades:.afml.getall[dt];
    .afml.log "AFML: Loaded ",string[count trades]," ticks";
    
    trades:update bt:.afml.bt[buyerIsMaker], dvol:price*qty from trades;
    trades:update sd:bt*dvol from trades;
    
    / Branch based on mode
    res:$[mode=`rwdib;
        .afml.warmuprw[trades;target;dt];
        .afml.warmupafml[trades;target;dt;mode]
    ];
    
    allbars:raze res`bars;
    allstate:raze res`state;
    
    `.afml.bars upsert allbars;
    `.afml.state upsert 1!allstate;
    
    .afml.log "AFML: Warm-up done. Bars: ",string count allbars;
    .afml.log "AFML: ========================================";
    
    `bars`state!(allbars;allstate)
    };

/ AFML warmup (dib/drb modes)
.afml.warmupafml:{[trades;target;dt;mode]
    scn:$[mode=`drb; .afml.scndrb; .afml.scndib];
    thfn:$[mode=`drb; .afml.threshdrb; .afml.threshdib];
    
    {[trades;target;dt;scn;thfn;mode;s]
        idx:where trades[`sym]=s;
        t:trades idx;
        n:count t;
        
        / Initial parameters
        et:n % target;
        pb:avg 1 - t`buyerIsMaker;
        ed:avg t`dvol;
        th:thfn[et;pb;ed];
        
        .afml.log "AFML: ",string[s]," params:";
        .afml.log "AFML:   et=",string[et]," pb=",string[pb]," ed=",string ed;
        pterm:$[mode=`drb; pb|1-pb; abs -1+2*pb];
        .afml.log "AFML:   pterm=",string[pterm]," th=",string th;
        
        / Run scan - pass (sd;bt=1;dvol) per tick
        dat:flip (t`sd;t[`bt]=1;t`dvol);
        st0:(0f;0j;0j;0j;0f);
        sts:st0 scn[th]\ dat;
        
        t:update bn:sts[;1] from t;
        
        lst:last sts;
        fbn:lst 1;
        ftheta:lst 0;
        ftib:lst 2;
        fbib:lst 3;
        fdib:lst 4;
        
        .afml.log "AFML: ",string[s]," - ",string[fbn]," bars (target:",string[target],")";
        
        / Build completed bars
        ctidx:where t[`bn]<fbn;
        ct:t ctidx;
        
        bars:$[0=count ct;
            .afml.empty[];
            0!select
                barstart:first exchTradeTs,
                barend:last exchTradeTs,
                open:first price,
                high:max price,
                low:min price,
                close:last price,
                qty:sum qty,
                dqty:sum dvol,
                ticks:count i,
                buys:sum bt=1,
                bqty:sum qty where bt=1,
                sqty:sum qty where bt=-1,
                vwap:qty wavg price,
                theta:sum sd,
                et:et,
                pb:pb,
                ed:ed,
                th:th,
                warmup:1b
                by sym,bn from ct
        ];
        
        state:([] 
            sym:enlist s; date:enlist dt; bn:enlist fbn; theta:enlist ftheta;
            et:enlist et; pb:enlist pb; ed:enlist ed; th:enlist th;
            tib:enlist ftib; bib:enlist fbib; dib:enlist fdib;
            sigma:enlist 0f; anchor:enlist 0f; anchorprice:enlist 0f);
        
        `bars`state!(bars;state)
    }[trades;target;dt;scn;thfn;mode] each .afml.cfg.syms
    };

/ Random Walk warmup (rwdib mode)
.afml.warmuprw:{[trades;target;dt]
    {[trades;target;dt;s]
        idx:where trades[`sym]=s;
        t:trades idx;
        n:count t;
        
        / Calculate parameters
        et:n % target;
        sigma:dev t`sd;
        avgprice:avg t`price;
        
        / Warmup scan function
        scn:{[th;st;tr]
            theta:st[0] + tr 0;
            tib:st[2]+1;
            dib:st[3]+tr 1;
            $[th<=abs theta;
                (0f;st[1]+1;0j;0f);
                (theta;st[1];tib;dib)]
            };
        
        dat:flip (t`sd;t`dvol);
        st0:(0f;0j;0j;0f);
        
        / === Pass 1: Initial estimate ===
        th1:.afml.threshrw[sigma;et];
        sts1:st0 scn[th1]\ dat;
        pass1bars:last[sts1] 1;
        
        .afml.log "AFML: ",string[s]," Pass 1: th=",string[th1]," -> ",string[pass1bars]," bars";
        
        / === Pass 2: Adjust threshold ===
        / If we got too many bars, increase threshold by √(actual/target)
        adjfactor:sqrt pass1bars % target;
        th:.afml.threshrw[sigma;et] * adjfactor;
        
        .afml.log "AFML: ",string[s]," Pass 2: adj=",string[adjfactor]," th=",string th;
        
        / Run final scan
        sts:st0 scn[th]\ dat;
        t:update bn:sts[;1] from t;
        
        lst:last sts;
        fbn:lst 1;
        ftheta:lst 0;
        ftib:lst 2;
        fdib:lst 3;
        
        / Update et based on actual bars
        finalET:n % max(1;fbn);
        
        .afml.log "AFML: ",string[s]," - ",string[fbn]," bars (target:",string[target],")";
        .afml.log "AFML:   et=",string[finalET]," sigma=",string[sigma]," avgprice=",string avgprice;
        
        / Build completed bars
        ctidx:where t[`bn]<fbn;
        ct:t ctidx;
        
        bars:$[0=count ct;
            .afml.empty[];
            0!select
                barstart:first exchTradeTs,
                barend:last exchTradeTs,
                open:first price,
                high:max price,
                low:min price,
                close:last price,
                qty:sum qty,
                dqty:sum dvol,
                ticks:count i,
                buys:sum bt=1,
                bqty:sum qty where bt=1,
                sqty:sum qty where bt=-1,
                vwap:qty wavg price,
                theta:sum sd,
                et:finalET,
                pb:0f,
                ed:0f,
                th:th,
                warmup:1b
                by sym,bn from ct
        ];
        
        / State includes anchor info for rwdib
        state:([] 
            sym:enlist s; date:enlist dt; bn:enlist fbn; theta:enlist ftheta;
            et:enlist finalET; pb:enlist 0f; ed:enlist 0f; th:enlist th;
            tib:enlist ftib; bib:enlist 0j; dib:enlist fdib;
            sigma:enlist sigma; anchor:enlist th; anchorprice:enlist avgprice);
        
        `bars`state!(bars;state)
    }[trades;target;dt] each .afml.cfg.syms
    };

/ =============================================================================
/ Adaptive Processing (Day 2+)
/ =============================================================================

.afml.proc:{[dt;span;mode]
    alpha:2.0 % span+1;
    
    .afml.log "AFML: Processing ",string[dt]," alpha=",string alpha;
    
    trades:.afml.getall[dt];
    .afml.log "AFML: Loaded ",string[count trades]," ticks";
    
    trades:update bt:.afml.bt[buyerIsMaker], dvol:price*qty from trades;
    trades:update sd:bt*dvol from trades;
    
    / Get state for each symbol
    stbl:0!.afml.state;
    
    / Branch based on mode
    res:$[mode=`rwdib;
        .afml.procrw[trades;alpha;dt;stbl];
        .afml.procafml[trades;alpha;dt;stbl;mode]
    ];
    
    allbars:raze res`bars;
    allstate:raze res`state;
    
    `.afml.bars upsert allbars;
    `.afml.state upsert 1!allstate;
    
    .afml.log "AFML: Day done. Bars: ",string count allbars;
    
    `bars`state!(allbars;allstate)
    };

/ AFML adaptive processing (dib/drb modes)
.afml.procafml:{[trades;alpha;dt;stbl;mode]
    scn:$[mode=`drb; .afml.scndrba; .afml.scndiba];
    
    {[trades;alpha;dt;scn;sprev]
        s:sprev`sym;
        idx:where trades[`sym]=s;
        t:trades idx;
        
        / Previous state
        sbn:sprev`bn;
        stheta:sprev`theta;
        oet:sprev`et;
        spb:sprev`pb;
        sed:sprev`ed;
        sth:sprev`th;
        stib:sprev`tib;
        sbib:sprev`bib;
        sdib:sprev`dib;
        
        .afml.log "AFML: ",string[s]," start: bn=",string[sbn]," th=",string sth;
        
        / Adaptive scan
        / State: (theta;bn;et;pb;ed;th;tib;bib;dib;bnout;thout;thetaout)
        / Index:   0    1  2  3  4  5  6   7   8   9     10    11
        
        st0:(stheta;sbn;oet;spb;sed;sth;stib;sbib;sdib;sbn;sth;stheta);
        
        dat:flip (t`sd;t[`bt]=1;t`dvol);
        sts:st0 scn[alpha]\ dat;
        
        t:update bn:sts[;9], thused:sts[;10], thetafull:sts[;11] from t;
        
        lst:last sts;
        fbn:lst 1;
        ftheta:lst 0;
        fet:lst 2;
        fpb:lst 3;
        fed:lst 4;
        fth:lst 5;
        ftib:lst 6;
        fbib:lst 7;
        fdib:lst 8;
        
        nbars:fbn-sbn;
        .afml.log "AFML: ",string[s]," - ",string[nbars]," bars";
        .afml.log "AFML:   et:",string[oet],"->",string[fet]," pb:",string[spb],"->",string[fpb]," ed:",string[sed],"->",string fed;
        .afml.log "AFML:   th:",string[sth],"->",string fth;
        
        / Completed bars
        ctidx:where (t[`bn]<fbn) and t[`bn]>=sbn;
        ct:t ctidx;
        
        bars:$[0=count ct;
            .afml.empty[];
            0!select
                barstart:first exchTradeTs,
                barend:last exchTradeTs,
                open:first price,
                high:max price,
                low:min price,
                close:last price,
                qty:sum qty,
                dqty:sum dvol,
                ticks:count i,
                buys:sum bt=1,
                bqty:sum qty where bt=1,
                sqty:sum qty where bt=-1,
                vwap:qty wavg price,
                theta:last thetafull,
                et:fet,
                pb:fpb,
                ed:fed,
                th:first thused,
                warmup:0b
                by sym,bn from ct
        ];
        
        state:([] 
            sym:enlist s; date:enlist dt; bn:enlist fbn; theta:enlist ftheta;
            et:enlist fet; pb:enlist fpb; ed:enlist fed; th:enlist fth;
            tib:enlist ftib; bib:enlist fbib; dib:enlist fdib;
            sigma:enlist 0f; anchor:enlist 0f; anchorprice:enlist 0f);
        
        `bars`state!(bars;state)
    }[trades;alpha;dt;scn] each stbl
    };

/ Random Walk adaptive processing (rwdib mode)
.afml.procrw:{[trades;alpha;dt;stbl]
    kappa:.afml.cfg.kappa;
    
    {[trades;alpha;kappa;dt;sprev]
        s:sprev`sym;
        idx:where trades[`sym]=s;
        t:trades idx;
        
        / Previous state
        sbn:sprev`bn;
        stheta:sprev`theta;
        oet:sprev`et;
        sth:sprev`th;
        stib:sprev`tib;
        sdib:sprev`dib;
        sigma:sprev`sigma;
        anchor:sprev`anchor;
        anchorprice:sprev`anchorprice;
        
        / Price adjustment: scale anchor by today's price
        todayprice:avg t`price;
        pratio:todayprice % anchorprice;
        adjanchor:anchor * pratio;
        
        .afml.log "AFML: ",string[s]," start: bn=",string[sbn]," th=",string sth;
        .afml.log "AFML:   price ratio=",string[pratio]," adj anchor=",string adjanchor;
        
        / Adaptive scan for RWDIB
        / State: (theta;bn;et;sigma;th;anchor;tib;dib;bnout;thout;thetaout)
        / Index:   0    1  2  3     4  5      6   7   8     9     10
        
        st0:(stheta;sbn;oet;sigma;sth;adjanchor;stib;sdib;sbn;sth;stheta);
        
        dat:flip (t`sd;t`dvol);
        sts:st0 .afml.scnrwdiba[alpha;kappa]\ dat;
        
        t:update bn:sts[;8], thused:sts[;9], thetafull:sts[;10] from t;
        
        lst:last sts;
        fbn:lst 1;
        ftheta:lst 0;
        fet:lst 2;
        fth:lst 4;
        ftib:lst 6;
        fdib:lst 7;
        
        nbars:fbn-sbn;
        .afml.log "AFML: ",string[s]," - ",string[nbars]," bars";
        .afml.log "AFML:   et:",string[oet],"->",string[fet];
        .afml.log "AFML:   th:",string[sth],"->",string fth;
        
        / Completed bars
        ctidx:where (t[`bn]<fbn) and t[`bn]>=sbn;
        ct:t ctidx;
        
        bars:$[0=count ct;
            .afml.empty[];
            0!select
                barstart:first exchTradeTs,
                barend:last exchTradeTs,
                open:first price,
                high:max price,
                low:min price,
                close:last price,
                qty:sum qty,
                dqty:sum dvol,
                ticks:count i,
                buys:sum bt=1,
                bqty:sum qty where bt=1,
                sqty:sum qty where bt=-1,
                vwap:qty wavg price,
                theta:last thetafull,
                et:fet,
                pb:0f,
                ed:0f,
                th:first thused,
                warmup:0b
                by sym,bn from ct
        ];
        
        / State preserves anchor info
        state:([] 
            sym:enlist s; date:enlist dt; bn:enlist fbn; theta:enlist ftheta;
            et:enlist fet; pb:enlist 0f; ed:enlist 0f; th:enlist fth;
            tib:enlist ftib; bib:enlist 0j; dib:enlist fdib;
            sigma:enlist sigma; anchor:enlist anchor; anchorprice:enlist anchorprice);
        
        `bars`state!(bars;state)
    }[trades;alpha;kappa;dt] each stbl
    };

/ =============================================================================
/ Main
/ =============================================================================

.afml.run:{[wdate;sdate;edate;target;span;mode]
    .afml.log "AFML: ========================================";
    modestr:$[mode=`rwdib; "Random Walk DIB"; "Pure AFML ",string mode];
    .afml.log "AFML: ",modestr;
    formula:$[mode=`drb; "th = et * ed * max(pb, 1-pb)"; 
              mode=`rwdib; "th = sigma * sqrt(et) + anchoring";
              "th = et * ed * |2*pb - 1|"];
    .afml.log "AFML: ",formula;
    .afml.log "AFML: ========================================";
    .afml.log "AFML: Mode: ",string mode;
    .afml.log "AFML: Warmup: ",string wdate;
    .afml.log "AFML: Range: ",string[sdate]," to ",string edate;
    .afml.log "AFML: Target: ",string target;
    .afml.log "AFML: Span: ",string span;
    if[mode=`rwdib; .afml.log "AFML: Kappa: ",string .afml.cfg.kappa];
    .afml.log "AFML: Syms: ",", " sv string .afml.cfg.syms;
    .afml.log "AFML: ========================================";
    
    .afml.initstate[];
    .afml.initbars[];
    
    .afml.log "AFML: Phase 1 - Warmup";
    .afml.warmup[wdate;target;mode];
    
    .afml.log "AFML: Phase 2 - Adaptive";
    dates:sdate + til 1 + edate - sdate;
    {[span;mode;d] .afml.proc[d;span;mode]}[span;mode] each dates;
    
    .afml.log "AFML: ========================================";
    .afml.log "AFML: Done. Total bars: ",string count .afml.bars;
    .afml.log "AFML: Bars per day:";
    if[.afml.cfg.verbose; show select n:count i by dt:barstart.date from .afml.bars];
    .afml.log "AFML: Parameter evolution:";
    if[.afml.cfg.verbose; show select n:count i, th0:first th, th1:last th, pct:100*(last[th]-first th)%first th by sym from .afml.bars];
    .afml.log "AFML: ========================================";
    
    .afml.bars
    };

/ =============================================================================
/ Convenience
/ =============================================================================

.afml.go:{[mode] 
    if[not `trade in tables[]; .afml.load[]];
    .afml.run[2026.01.14;2026.01.15;2026.01.25;.afml.cfg.target;.afml.cfg.span;mode]
    };

/ =============================================================================
/ Clean Table (filter warmup, renumber bn, remove internal params)
/ =============================================================================

.afml.clean:{[]
    t:select from .afml.bars where not warmup;
    t:update bn:1+i from t;
    .afml.tbl:`bn xcols delete warmup,et,pb,ed,th from t;
    .afml.log "AFML: Clean table created. ",string[count .afml.tbl]," bars";
    .afml.tbl
    };

/ =============================================================================
/ Diagnostics
/ =============================================================================

.afml.showstate:{[] show .afml.state};
.afml.showbars:{[n] show n sublist .afml.bars};
.afml.stats:{[] select n:count i, avgticks:avg ticks, avgth:avg th, minpb:min pb, maxpb:max pb by sym,warmup from .afml.bars};
.afml.evo:{[s] select bn,th,et,pb,ed,ticks,theta from .afml.bars where sym=s};
.afml.chk:{[] r:select th0:first th, thmax:max th, ratio:max[th]%first th by sym from .afml.bars; show r; $[any r`ratio>10; -1"AFML: WARN - explosion"; -1"AFML: OK"]; r};

/ =============================================================================
/ Save/Load
/ =============================================================================

.afml.save:{[] (hsym`$"afml_bars")set .afml.bars; (hsym`$"afml_state")set .afml.state; -1"AFML: Saved"};
.afml.ld:{[] .afml.bars:get hsym`$"afml_bars"; .afml.state:get hsym`$"afml_state"; -1"AFML: Loaded ",string[count .afml.bars]," bars"};
