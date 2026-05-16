/ kdb/ml/labels.q - Triple-Barrier Labeling
/ Implements de Prado AFML Ch. 3.4.

/ For each bar in the input, simulate a trade entered at that bar's close.
/ Walk forward up to h bars and check three barriers:
/   Upper (profit-take): close * (1 + pt * sigma)
/   Lower (stop-loss):   close * (1 - sl * sigma)
/   Vertical (time):     bar + h
/ The first barrier hit determines the label (+1 upper, -1 lower).
/ If neither price barrier hits, label is sign(return at vertical) per AFML.
/ If both price barriers hit in the SAME bar, label is 0 (conservative
/ tiebreak - OHLC alone can't determine intra-bar ordering).

/ ============================================================================
/ Configuration
/ ============================================================================

.lbl.cfg.pt:        2.0;    / profit-take multiplier
.lbl.cfg.sl:        1.0;    / stop-loss multiplier
.lbl.cfg.h:         50;     / vertical horizon (bars)
.lbl.cfg.sigmaSpan: 100;    / EWMA span for volatility estimator
.lbl.cfg.verbose:   0b;

.lbl.log:{[msg] if[.lbl.cfg.verbose; -1 msg]};

/ ============================================================================
/ Volatility Estimator
/ ============================================================================

/ EWMA of |log returns|, span n. Returns vector same length as input,
/ first element null (no return for first bar). Per AFML the default span
/ is 100 bars - long enough to be stable, short enough to track regime.
.lbl.sigma:{[closes;n]
    rets:    1_ log ratios closes;
    absRets: abs rets;
    if[0 = count absRets; :enlist 0n];
    alpha: 2.0 % n + 1;
    / q lambdas don't capture enclosing locals, so we project alpha into the
    / scan lambda. f[alpha] returns a 2-arg function {[x;y] ...} for scan.
    f: {[alpha; x; y] (alpha * y) + (1 - alpha) * x}[alpha];
    ewma: absRets[0] f\ absRets;
    0n, ewma
    };

/ ============================================================================
/ First-Hit Detection
/ ============================================================================

/ Find first barrier hit in a windowed slice of highs/lows.
/ Returns (offset; label) or (0N; 0N) if no hit in this slice.
/ Same-bar (high>=upper AND low<=lower) returns label 0 (conservative).
.lbl.firstHit:{[upperPx; lowerPx; highs; lows]
    if[0 = count highs; :(0N; 0N)];

    upHits: highs >= upperPx;
    dnHits: lows  <= lowerPx;
    anyHit: upHits or dnHits;
    if[not any anyHit; :(0N; 0N)];

    firstIdx: first where anyHit;
    u: upHits firstIdx;
    d: dnHits firstIdx;

    label: $[u and d; 0;
            u;        1;
                     -1];
    (firstIdx; `long$label)
    };

/ ============================================================================
/ Per-Bar Labeling
/ ============================================================================

/ Compute label for bar i. Returns a 6-tuple:
/   (endBn; upperPx; lowerPx; label; retAtTouch; reason)
/ where reason in {`upper, `lower, `sameBar, `vertical, `noSigma, `noFuture}.
/ `v` is a dict bundling closes/highs/lows/bns to stay within q's 8-param limit.
.lbl.labelBar:{[v; sigmas; pt; sl; h; n; i]
    sigma: sigmas i;
    if[null sigma; :(0N; 0n; 0n; 0N; 0n; `noSigma)];

    cp:      v[`closes] i;
    upperPx: cp * 1 + pt * sigma;
    lowerPx: cp * 1 - sl * sigma;

    endIdx: (n - 1) & i + h;
    if[endIdx <= i; :(0N; upperPx; lowerPx; 0N; 0n; `noFuture)];

    slice:       (i + 1) + til endIdx - i;
    sliceHighs:  v[`highs] slice;
    sliceLows:   v[`lows]  slice;

    hit:      .lbl.firstHit[upperPx; lowerPx; sliceHighs; sliceLows];
    firstOff: hit 0;
    labelHit: hit 1;

    hitIdx:   $[null firstOff; endIdx; i + 1 + firstOff];
    closeRet: (v[`closes][hitIdx] - cp) % cp;

    labelFinal: $[null firstOff;
        / Vertical: sign of close return per AFML
        `long$$[closeRet > 0; 1; closeRet < 0; -1; 0];
        / Price barrier hit: use the hit label
        labelHit];

    reason: $[null firstOff;       `vertical;
              labelFinal = 0;      `sameBar;
              labelFinal = 1;      `upper;
                                   `lower];

    / retAtTouch follows AFML limit-order semantics:
    /   upper   -> fill at upperPx,   ret =  pt * sigma  (positive)
    /   lower   -> fill at lowerPx,   ret = -sl * sigma  (negative)
    /   sameBar -> ambiguous fill,    ret = 0            (conservative)
    /   vertical-> exit at close,     ret = close return
    ret: $[reason = `vertical;  closeRet;
           reason = `upper;     pt * sigma;
           reason = `lower;     neg sl * sigma;
                                0f];

    (v[`bns] hitIdx; upperPx; lowerPx; labelFinal; ret; reason)
    };

/ ============================================================================
/ Per-Symbol Labeling
/ ============================================================================

/ Compute labels for all bars of one symbol. `symb` is the symbol; `barsT`
/ is the full bars table (will be filtered).
.lbl.computeOne:{[barsT; symb; pt; sl; h; sigmaSpan]
    bars:   `bn xasc select from barsT where sym = symb;
    closes: bars`close;
    highs:  bars`high;
    lows:   bars`low;
    bns:    bars`bn;
    n:      count bars;

    sigmas: .lbl.sigma[closes; sigmaSpan];

    .lbl.log raze ("labels: "; string symb; " - "; string n; " bars");

    / Pack vectors into a dict to stay within q's 8-param function limit.
    v: `closes`highs`lows`bns ! (closes; highs; lows; bns);
    results: .lbl.labelBar[v; sigmas; pt; sl; h; n] each til n;

    ([] sym:        n # symb;
        bn:         bns;
        close:      closes;
        sigma:      sigmas;
        endBn:      results[; 0];
        upperPx:    results[; 1];
        lowerPx:    results[; 2];
        label:      results[; 3];
        retAtTouch: results[; 4];
        reason:     results[; 5])
    };

/ ============================================================================
/ Public API
/ ============================================================================

.lbl.run:{[barsT]
    .lbl.log raze ("labels: pt=";        string .lbl.cfg.pt;
                   " sl=";               string .lbl.cfg.sl;
                   " h=";                string .lbl.cfg.h;
                   " sigmaSpan=";        string .lbl.cfg.sigmaSpan);

    syms: exec distinct sym from barsT;

    / Use globals so the inner lambda can reference shared values without
    / projection. q lambdas don't capture enclosing locals.
    .lbl._bt:: barsT;
    .lbl._pt:: .lbl.cfg.pt;
    .lbl._sl:: .lbl.cfg.sl;
    .lbl._h::  .lbl.cfg.h;
    .lbl._ss:: .lbl.cfg.sigmaSpan;

    results: {[symb]
        .lbl.computeOne[.lbl._bt; symb; .lbl._pt; .lbl._sl; .lbl._h; .lbl._ss]
        } each syms;

    .lbl.labels: raze results;
    .lbl.log raze ("labels: done. "; string count .lbl.labels; " rows");

    .lbl.labels
    };

/ Convenience: run on .afml.tbl (the cleaned bars output)
.lbl.runOnAfml:{[]
    if[not `tbl in key `.afml; '"labels: .afml.tbl not present - run .afml.clean[] first"];
    .lbl.run .afml.tbl
    };

/ ============================================================================
/ Diagnostics
/ ============================================================================

.lbl.distribution:{[]
    if[not `labels in key `.lbl; '"labels: no labels computed yet"];
    -1 "Label distribution (excluding noSigma/noFuture):";
    show select n:count i, avgRet:avg retAtTouch
        by sym, label from .lbl.labels where not reason in `noSigma`noFuture;
    -1 "";
    -1 "Reason distribution:";
    show select n:count i by sym, reason from .lbl.labels;
    };

.lbl.summary:{[]
    if[not `labels in key `.lbl; '"labels: no labels computed yet"];
    t: select from .lbl.labels where not reason in `noSigma`noFuture;
    select labelable: count i,
           avgRet:    avg retAtTouch,
           avgSigma:  avg sigma,
           avgHorizon: avg endBn - bn,
           upPct:     100 * (sum reason = `upper)    % count i,
           dnPct:     100 * (sum reason = `lower)    % count i,
           sbPct:     100 * (sum reason = `sameBar)  % count i,
           vtPct:     100 * (sum reason = `vertical) % count i
        by sym from t
    };
