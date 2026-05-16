/ tests/test_labels.q

/ Real-data tests for kdb/ml/labels.q (triple-barrier labeling).
/ Generates bars by running afml on BTCUSDT trades from hdb_binancedata at
/ 2026.01.14 through 2026.01.16, then runs labels and checks invariants.
/ Skips cleanly if HDB or required partitions are missing.

/ Run via:
/   q tests/test_labels.q
/ Or as part of the suite:
/   ./tests/run_tests.sh

\c 50 300

/ ----------------------------------------------------------------------------
/ Assertion helpers
/ ----------------------------------------------------------------------------

.t.pass: 0
.t.fail: 0
.t.cur: `none

.t.case: {[name]
    .t.cur:: name;
    -1 raze ("  -- "; string name)
    };

.t.ok: {[cond; msg]
    $[cond;
        [.t.pass +: 1];
        [.t.fail +: 1; -1 raze ("    FAIL ["; string .t.cur; "]: "; msg)]]
    };

.t.eq: {[actual; expected; msg]
    .t.ok[actual ~ expected;
        raze (msg; " (got "; .Q.s1 actual; ", expected "; .Q.s1 expected; ")")]
    };

.t.within: {[actual; lo; hi; msg]
    .t.ok[(actual >= lo) and actual <= hi;
        raze (msg; " (got "; .Q.s1 actual;
              ", expected ["; .Q.s1 lo; ", "; .Q.s1 hi; "])")]
    };

.t.report: {[]
    -1 "";
    -1 "============================================";
    -1 raze ("Pass: "; string .t.pass; "  Fail: "; string .t.fail);
    -1 "============================================";
    system "sleep 0.1";
    $[.t.fail > 0; exit 1; exit 0]
    };

.t.skip: {[msg]
    -2 raze ("test_labels.q: SKIP - "; msg);
    system "sleep 0.1";
    exit 0
    };

/ ----------------------------------------------------------------------------
/ Setup: load modules, check HDB, run AFML to produce bars
/ ----------------------------------------------------------------------------

-1 "test_labels.q: loading afml.q and labels.q...";
\l kdb/ml/afml.q
\l kdb/ml/labels.q

hdbPath: 1_ string .afml.cfg.hdb;
-1 raze ("test_labels.q: HDB path resolved to: "; hdbPath);

pathExists: not () ~ key hsym `$hdbPath;
if[not pathExists;
    .t.skip raze ("HDB directory not found at "; hdbPath)];

.t.loadOK: 1b;
@[{system "l ", x};
  hdbPath;
  {[err] .t.loadOK:: 0b; -2 raze ("test_labels.q: HDB load error: "; err)}];

if[not .t.loadOK;
    .t.skip "HDB failed to load"];

required: 2026.01.14 2026.01.15 2026.01.16;
have:     distinct exec date from select date from trade where date in required;
missing:  required except have;
if[0 < count missing;
    .t.skip raze ("missing HDB partitions: "; " " sv string missing)];

-1 "test_labels.q: HDB loaded, partitions OK";

/ Run afml warmup + adaptive to produce bars. Suppress its progress output.
.afml.cfg.verbose: 0b;
.lbl.cfg.verbose:  0b;

-1 "test_labels.q: running afml warmup + 2 adaptive days (~30s)...";
.afml.initstate[];
.afml.initbars[];
.afml.warmup[2026.01.14; 200; `rwdib];
.afml.proc[2026.01.15; 100; `rwdib];
.afml.proc[2026.01.16; 100; `rwdib];
.afml.clean[];

-1 raze ("test_labels.q: produced "; string count .afml.tbl; " bars");
-1 "";

/ ----------------------------------------------------------------------------
/ Run labeling
/ ----------------------------------------------------------------------------

labels: .lbl.run .afml.tbl;
-1 raze ("test_labels.q: labeled "; string count labels; " rows");
-1 "";

/ Filter to "labelable" rows (excluding noSigma/noFuture) for most tests.
labelable: select from labels where not reason in `noSigma`noFuture;
-1 raze ("test_labels.q: "; string count labelable; " labelable rows (excluding noSigma/noFuture)");
-1 "";

/ ============================================================================
/ Test 1: Output shape and column types
/ ============================================================================

.t.case `output_shape;

expectedCols: `sym`bn`close`sigma`endBn`upperPx`lowerPx`label`retAtTouch`reason;
.t.eq[asc cols labels; asc expectedCols; "output schema mismatch"];

.t.eq[count labels; count .afml.tbl; "labels count != bars count"];

m: meta labels;
.t.eq[m[`sym;       `t]; "s"; "sym not symbol type"];
.t.eq[m[`bn;        `t]; "j"; "bn not long type"];
.t.eq[m[`endBn;     `t]; "j"; "endBn not long type"];
.t.eq[m[`label;     `t]; "j"; "label not long type"];
.t.eq[m[`reason;    `t]; "s"; "reason not symbol type"];

/ ============================================================================
/ Test 2: Label domain - all labels in {-1, 0, +1, null}
/ ============================================================================

.t.case `label_domain;
invalid: select from labels where not null label, not label in -1 0 1;
.t.eq[count invalid; 0; "labels outside {-1, 0, +1, null}"];

.t.ok[0 < count labelable; "no labelable rows produced"];
.t.ok[all not null labelable`label; "labelable rows have null labels"];

/ ============================================================================
/ Test 3: Reason domain
/ ============================================================================

.t.case `reason_domain;
validReasons: `upper`lower`sameBar`vertical`noSigma`noFuture;
badReasons: select from labels where not reason in validReasons;
.t.eq[count badReasons; 0; "rows with reason outside expected set"];

/ ============================================================================
/ Test 4: Barriers straddle entry (upperPx > close > lowerPx)
/ ============================================================================
/ Only meaningful where sigma is non-null and positive.

.t.case `barriers_straddle_entry;
sigOK: select from labels where not null sigma, sigma > 0f;
.t.ok[0 < count sigOK; "no rows with positive sigma"];
.t.ok[all sigOK[`upperPx] > sigOK`close; "upperPx <= close in some rows"];
.t.ok[all sigOK[`lowerPx] < sigOK`close; "lowerPx >= close in some rows"];

/ ============================================================================
/ Test 5: Forward-looking and within horizon
/ ============================================================================
/ endBn must be > bn (no zero/negative-duration trades) and endBn - bn <= h
/ (vertical horizon respected).

.t.case `forward_and_within_horizon;
h: .lbl.cfg.h;
.t.ok[all labelable[`endBn] > labelable`bn; "endBn <= bn in some labelable rows"];
.t.ok[all (labelable[`endBn] - labelable`bn) <= h;
    raze ("endBn - bn exceeded horizon "; string h; " in some rows")];

/ ============================================================================
/ Test 6: Return sign matches label for unambiguous price hits
/ ============================================================================
/ For label = +1 (upper hit), retAtTouch must be positive.
/ For label = -1 (lower hit), retAtTouch must be negative.
/ Same-bar (label = 0 from straddle) and vertical hits have separate logic.

.t.case `return_sign_matches_label;
ups: select from labels where reason = `upper;
dns: select from labels where reason = `lower;
.t.ok[(0 = count ups) or all ups[`retAtTouch]    > 0f; "upper hits with non-positive return"];
.t.ok[(0 = count dns) or all dns[`retAtTouch]    < 0f; "lower hits with non-negative return"];

/ Vertical: label must equal sign of return.
/ When verts is empty, both sides are empty and ~ returns true (vacuously passes).
verts: select from labels where reason = `vertical;
signedLabels: `long$?[verts[`retAtTouch] > 0f; 1; ?[verts[`retAtTouch] < 0f; -1; 0]];
.t.ok[verts[`label] ~ signedLabels; "vertical label != sign(retAtTouch)"];

/ ============================================================================
/ Test 7: Label distribution is non-degenerate
/ ============================================================================
/ On real BTCUSDT data with pt=2, sl=1, h=50, expect a mix of +1 and -1.
/ 0 labels are rare (only same-bar straddle or exact-zero vertical return).

.t.case `label_distribution;
nUp:   count select from labelable where label = 1;
nDn:   count select from labelable where label = -1;
nZero: count select from labelable where label = 0;

-1 raze ("    label counts: +1="; string nUp; " -1="; string nDn; " 0="; string nZero);

.t.ok[nUp > 0; "no +1 labels in labelable set"];
.t.ok[nDn > 0; "no -1 labels in labelable set"];
.t.ok[(nUp + nDn) > 10 * nZero; "label 0 dominates (suspicious)"];

/ ============================================================================
/ Test 8: Sigma is non-negative
/ ============================================================================

.t.case `sigma_non_negative;
sigVals: select from labels where not null sigma;
.t.ok[all sigVals[`sigma] >= 0f; "negative sigma encountered"];

/ ============================================================================
/ Test 9: noSigma rows are only at the beginning, noFuture at the end
/ ============================================================================
/ Structural sanity: the EWMA warmup gap is at the front; the horizon-truncation
/ gap is at the back.

.t.case `null_reasons_localized;
sortedLabels: `bn xasc labels;

noSig: select from sortedLabels where reason = `noSigma;
.t.ok[(0 = count noSig) or all noSig[`bn] <= 5;
    "noSigma rows appeared past the first few bars"];

noFut: select from sortedLabels where reason = `noFuture;
lastBn: exec max bn from sortedLabels;
.t.ok[(0 = count noFut) or all noFut[`bn] >= (lastBn - 2);
    "noFuture rows appeared before the last few bars"];

/ ============================================================================
/ Test 10: Barrier monotonicity in pt/sl
/ ============================================================================
/ For the same bars and sigma, larger pt should give a higher upperPx,
/ and changing pt should NOT affect lowerPx. Regression catch for math bugs.

.t.case `barrier_monotonicity;
prevPt: .lbl.cfg.pt;
prevSl: .lbl.cfg.sl;

.lbl.cfg.pt: 1.0;
.lbl.cfg.sl: 1.0;
labels1: .lbl.run .afml.tbl;

.lbl.cfg.pt: 3.0;
.lbl.cfg.sl: 1.0;
labels2: .lbl.run .afml.tbl;

/ Restore before assertions so a failure doesn't leak state.
.lbl.cfg.pt: prevPt;
.lbl.cfg.sl: prevSl;

/ Both filterings use the same sigma column (same bars, same span), so the
/ surviving bns are identical and we can compare element-wise.
sigOK1: select bn, upperPx, lowerPx from labels1 where not null sigma, sigma > 0f;
sigOK2: select bn, upperPx, lowerPx from labels2 where not null sigma, sigma > 0f;

.t.eq[sigOK1`bn; sigOK2`bn; "bn vectors differ between label runs"];
.t.ok[all sigOK2[`upperPx] > sigOK1`upperPx; "raising pt did not raise upperPx"];
.t.ok[all sigOK2[`lowerPx] = sigOK1`lowerPx; "changing pt should not affect lowerPx"];

/ ============================================================================
/ Report
/ ============================================================================

.t.report[];
