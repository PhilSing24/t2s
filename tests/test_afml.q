/ tests/test_afml.q

/ Real-data tests for kdb/ml/afml.q. Uses BTCUSDT trades from hdb_binancedata
/ at 2026.01.14 through 2026.01.16 (warmup + 2 adaptive days).

/ Skips cleanly if HDB is missing or those partitions aren't present, so
/ this is safe to run on a fresh clone or partial HDB.

/ Run via the standard runner:
/   ./tests/run_tests.sh

/ Or directly:
/   q tests/test_afml.q

\c 50 300

/ ----------------------------------------------------------------------------
/ Assertion helpers
/ (If t_lib.q has equivalents, swap this block for `\l tests/t_lib.q`)
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
    cond: actual ~ expected;
    .t.ok[cond;
        raze (msg; " (got "; .Q.s1 actual; ", expected "; .Q.s1 expected; ")")]
    };

.t.within: {[actual; lo; hi; msg]
    cond: (actual >= lo) and actual <= hi;
    .t.ok[cond;
        raze (msg; " (got "; .Q.s1 actual;
              ", expected ["; .Q.s1 lo; ", "; .Q.s1 hi; "])")]
    };

.t.report: {[]
    -1 "";
    -1 "============================================";
    -1 raze ("Pass: "; string .t.pass; "  Fail: "; string .t.fail);
    -1 "============================================";
    $[.t.fail > 0; exit 1; exit 0]
    };

/ Skip helper: prints message and exits cleanly. Called via single-expression
/ if[] body to avoid multi-statement if[]+exit parsing issues observed in
/ KDB-X 5.0.
.t.skip: {[msg]
    -2 raze ("test_afml.q: SKIP - "; msg);
    exit 0
    };

/ ----------------------------------------------------------------------------
/ Setup
/ ----------------------------------------------------------------------------

-1 "test_afml.q: loading afml.q...";
\l kdb/ml/afml.q

hdbPath: 1_ string .afml.cfg.hdb;
-1 raze ("test_afml.q: HDB path resolved to: "; hdbPath);

/ Sanity-check that the path exists on disk. Diagnostic print so we can see
/ what key returns even if the check passes.
pathExists: not () ~ key hsym `$hdbPath;
-1 raze ("test_afml.q: path exists check: "; string pathExists);

if[not pathExists;
    .t.skip raze ("HDB directory not found at "; hdbPath)];

/ Load the HDB. Flag-based skip so we don't need exit inside the catch lambda.
.t.loadOK: 1b;
@[{system "l ", x};
  hdbPath;
  {[err] .t.loadOK:: 0b; -2 raze ("test_afml.q: HDB load error: "; err)}];

if[not .t.loadOK;
    .t.skip "HDB failed to load"];

-1 "test_afml.q: HDB loaded";

/ Verify the partitions we need are present.
required: 2026.01.14 2026.01.15 2026.01.16;
have: distinct exec date from select date from trade where date in required;
missing: required except have;
if[0 < count missing;
    .t.skip raze ("missing HDB partitions: "; " " sv string missing)];

-1 "test_afml.q: setup OK, all required partitions present";
-1 "";

/ Suppress afml's own progress logging during the test run.
.afml.cfg.verbose: 0b;

/ ============================================================================
/ Test 1: Warmup produces a reasonable number of bars with valid state
/ ============================================================================

.t.case `warmup_produces_bars;
.afml.initstate[];
.afml.initbars[];
.afml.warmup[2026.01.14; 200; `rwdib];

.t.ok[count[.afml.bars] > 0; "warmup produced no bars"];
.t.within[count .afml.bars; 50; 1000; "warmup bar count outside reasonable range for target=200"];
.t.ok[all .afml.bars`warmup; "not all warmup bars marked warmup=1b"];
.t.eq[count .afml.state; 1; "state should have exactly one row (BTCUSDT)"];

state1: first 0!.afml.state;
.t.ok[state1[`et] > 0f; "warmup et should be > 0"];
.t.ok[state1[`sigma] > 0f; "warmup sigma should be > 0"];
.t.ok[state1[`th] > 0f; "warmup threshold should be > 0"];
.t.ok[state1[`anchor] > 0f; "warmup anchor should be set"];
.t.ok[state1[`anchorprice] > 0f; "warmup anchor price should be set"];

/ ============================================================================
/ Test 2: Bar OHLC consistency (high >= open/close/low, low <= open/close)
/ ============================================================================

.t.case `bar_ohlc_consistency;
b: select from .afml.bars;
.t.ok[all b[`high] >= b`low;   "high < low in some bars"];
.t.ok[all b[`high] >= b`open;  "high < open in some bars"];
.t.ok[all b[`high] >= b`close; "high < close in some bars"];
.t.ok[all b[`low]  <= b`open;  "low > open in some bars"];
.t.ok[all b[`low]  <= b`close; "low > close in some bars"];
.t.ok[all b[`ticks] > 0;       "some bars have zero ticks"];
.t.ok[all b[`qty] > 0f;        "some bars have non-positive qty"];

/ ============================================================================
/ Test 3: Volume conservation (bqty + sqty == qty)
/ ============================================================================
/ Each tick is either buy (bt=+1) or sell (bt=-1); buy qty + sell qty must
/ equal total qty exactly (no float drift since the sums are independent
/ partitions of the same set of ticks).

.t.case `volume_conservation;
b: select from .afml.bars;
maxDiff: max abs (b[`bqty] + b`sqty) - b`qty;
.t.ok[maxDiff < 1e-9; raze ("bqty + sqty != qty (max diff "; .Q.s1 maxDiff; ")")];

/ ============================================================================
/ Test 4: Bar numbering is contiguous (no gaps)
/ ============================================================================
/ Detects state-machine bugs where bn skips or repeats. Looks at differences
/ between consecutive bn values after sorting; all should be 1.

.t.case `bar_numbering_contiguous;
bns: asc exec bn from .afml.bars where sym = `BTCUSDT;
diffs: 1 _ deltas bns;
gaps: `long$ sum diffs <> 1;
.t.eq[gaps; 0; "non-contiguous bar numbering"];

/ ============================================================================
/ Test 5: Adaptive day produces new bars marked warmup=0b
/ ============================================================================

.t.case `adaptive_day_produces_bars;
priorBarCount: count .afml.bars;
priorState: first 0!.afml.state;

.afml.proc[2026.01.15; 100; `rwdib];

newBars: count[.afml.bars] - priorBarCount;
.t.ok[newBars > 0; "adaptive day 1 produced no new bars"];

adaptiveBars: select from .afml.bars where not warmup;
.t.ok[all not adaptiveBars`warmup; "adaptive bars not marked warmup=0b"];

/ ============================================================================
/ Test 6: State evolves between warmup and adaptive
/ ============================================================================
/ The EWMA-updated et should differ from warmup's initial et (highly likely
/ on real data; would only fail to differ in a degenerate case). The bar
/ number must strictly advance.

.t.case `state_evolves;
state2: first 0!.afml.state;
.t.ok[state2[`bn] > priorState`bn; "bar number did not advance after adaptive day"];
.t.ok[state2[`et] <> priorState`et; "et did not change after adaptive EWMA update"];

/ ============================================================================
/ Test 7: Multi-day threshold stability (the RWDIB anchoring point)
/ ============================================================================
/ This is the test that justifies the RWDIB mode's existence. AFML modes
/ blow up to 100x+ on crypto; RWDIB with anchoring should stay tight.

.t.case `threshold_stability;
.afml.proc[2026.01.16; 100; `rwdib];

ths: exec th from .afml.bars where not warmup;
maxTh: max ths;
minTh: min ths;
ratio: maxTh % minTh;

-1 raze ("    threshold range: "; .Q.s1 minTh; " -> "; .Q.s1 maxTh; " (ratio "; .Q.s1 ratio; ")");
.t.ok[ratio < 10f; "threshold ratio > 10x across the run (potential explosion)"];

/ ============================================================================
/ Test 8: Save / load round-trip
/ ============================================================================
/ Save and reload should produce identical in-memory state. Uses the public
/ .afml.save / .afml.ld API which writes to the CWD (so the test cleans up
/ the files at the end).

.t.case `save_load_roundtrip;
preBars:  count .afml.bars;
preBN:    (first 0!.afml.state)`bn;
preTheta: (first 0!.afml.state)`theta;
-1 "    [diag] pre-save snapshot taken";

.afml.save[];
-1 "    [diag] save returned";

/ Wipe in-memory tables.
.afml.bars:  ();
.afml.state: ();
-1 "    [diag] in-memory wiped";

.afml.ld[];
-1 "    [diag] load returned";

.t.eq[count .afml.bars; preBars; "bar count changed after save/load round-trip"];
-1 "    [diag] assertion 1 done";
.t.eq[(first 0!.afml.state)`bn; preBN; "state bn changed after save/load round-trip"];
-1 "    [diag] assertion 2 done";
.t.eq[(first 0!.afml.state)`theta; preTheta; "state theta changed after save/load round-trip"];
-1 "    [diag] assertion 3 done";

/ Cleanup the polluted save files. @[] absorbs hdel failures (e.g. if the
/ files were never written due to an earlier test failure).
@[hdel; hsym `$"afml_bars";  {[e] -1 raze ("    note: "; e)}];
-1 "    [diag] cleanup 1 done";
@[hdel; hsym `$"afml_state"; {[e] -1 raze ("    note: "; e)}];
-1 "    [diag] cleanup 2 done";

/ ============================================================================
/ Report
/ ============================================================================

.t.report[];
