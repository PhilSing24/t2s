/ t_lib.q - Shared assertion and setup helpers for the test suite.
/ Loaded at the top of each test_*.q file via \l tests/t_lib.q
/ Named t_lib (not test_*) so the runner glob does not pick it up.
/ Note on string handling in KDB-X 5.0:
/ "string" of an atom can return an enlist'd char list rather than a
/ flat char list. Joining that with another char list using "," produces
/ a generic list (type 0h), not a flat string. To stay safe across kdb
/ versions, all message construction in this lib uses "raze" over a list
/ of pieces, which always produces a flat char list.

.t.passed:0;
.t.failed:0;
.t.testName:"";

/ Build a message from a list of string pieces, flat-safe across versions.
.t.msg:{[pieces] raze pieces};

.t.start:{[name]
  .t.testName:$[10h=type name; name; -11h=type name; string name; -3!name];
  .t.passed:0;
  .t.failed:0;
  -1 "===========================================";
  -1 .t.msg ("TEST: "; .t.testName);
  -1 "===========================================";
  };

.t.assert:{[label;cond]
  $[cond;
    [.t.passed+:1; -1 .t.msg ("  PASS: "; label)];
    [.t.failed+:1; -1 .t.msg ("  FAIL: "; label)]];
  };

.t.assertEq:{[label;expected;actual]
  ok:expected~actual;
  $[ok;
    [.t.passed+:1; -1 .t.msg ("  PASS: "; label)];
    [.t.failed+:1;
     -1 .t.msg ("  FAIL: "; label);
     -1 .t.msg ("        expected: "; -3!expected);
     -1 .t.msg ("        actual:   "; -3!actual)]];
  };

.t.finish:{[]
  -1 "-------------------------------------------";
  -1 .t.msg ("RESULT: "; .t.testName; " - passed "; string .t.passed; ", failed "; string .t.failed);
  -1 "===========================================";
  if[.t.failed > 0; exit 1];
  exit 0;
  };

/ -------------------------------------------------------
/ Sandbox helpers (used by integration tests later)
/ -------------------------------------------------------

.t.sandboxDir:":tests/sandbox";

.t.setupSandbox:{[]
  system "rm -rf tests/sandbox";
  system "mkdir -p tests/sandbox";
  };

.t.teardownSandbox:{[]
  system "rm -rf tests/sandbox";
  };

/ -------------------------------------------------------
/ Port allocation: production + 10000
/ -------------------------------------------------------

.t.ports.tp:15010;
.t.ports.wdb:15011;
.t.ports.sig:15012;
.t.ports.ctp:15014;
.t.ports.rte:15015;
.t.ports.tel:15016;
.t.ports.rdb:15017;
.t.ports.pnl:15018;
