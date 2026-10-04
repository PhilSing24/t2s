/ t_guard.q - isolation guard for a sandboxed TP or WDB process.
/ -
/ Connects to the process over IPC, reads back the configuration it has
/ actually resolved (not what the test asked for), makes every path absolute
/ against the process's own working directory, and exits 1 if any path lies
/ outside the sandbox or any port lies outside the test range.
/ -
/ Driven by tests/t_lib.sh (t2s_guard). Inputs via environment:
/   T2S_GUARD_PROC      tp | wdb
/   T2S_GUARD_PORT      port the process listens on
/   T2S_GUARD_SANDBOX   absolute sandbox root
/   T2S_GUARD_PORT_MIN  lowest allowed port
/   T2S_GUARD_PORT_MAX  highest allowed port

proc:`$getenv `T2S_GUARD_PROC;
port:"J"$getenv `T2S_GUARD_PORT;
sandbox:getenv `T2S_GUARD_SANDBOX;
portMin:"J"$getenv `T2S_GUARD_PORT_MIN;
portMax:"J"$getenv `T2S_GUARD_PORT_MAX;

fail:{[msg] -2 raze ("GUARD FAIL: "; msg); system "sleep 0.1"; exit 1};

if[not proc in `tp`wdb; fail "T2S_GUARD_PROC must be tp or wdb"];
if[null port; fail "T2S_GUARD_PORT not set"];
if[0 = count sandbox; fail "T2S_GUARD_SANDBOX not set"];

/ Canonical absolute form of a path (resolves ., .. and symlinks, no need
/ for the path to exist).
canon:{[p] first system raze ("realpath -m '"; p; "'")};
sandboxAbs:canon sandbox;

h:@[hopen; (`$":localhost:",string port; 3000); {[e] fail raze ("cannot connect to port "; string port; ": "; e)}];

/ Working directory of the sandboxed process, for resolving relative paths.
cwd:h "system \"cd\"";
if[0h = type cwd; cwd:first cwd];

/ Fetch a variable from the remote process as a string path. Accepts
/ strings, symbols and file handles (`:/path).
remotePath:{[h;name]
  v:h name;
  v:$[-11h = type v; string v; 10h = type v; v; -1h = type v; string v; .Q.s1 v];
  if[(count v) and ":" = first v; v:1 _ v];
  v};

absolutize:{[cwd;p] canon $[(count p) and "/" = first p; p; raze (cwd; "/"; p)]};

/ Which variables to check, per process.
pathVars:$[proc = `tp;
  (".tp.cfg.logDir"; ".tp.logFile"; ".tp.cfg.seqFile"; ".tp.cfg.wdbCheckpointFile"; ".tp.cfg.hdbDir"; ".tp.cfg.tmpDir");
  (".wdb.cfg.hdbDir"; ".wdb.cfg.checkpointFile"; ".wdb.tmpDir"; ".wdb.tmpPath .wdb.today[]"; ".wdb.replay.tmpFile")];
/ Test-only clock overrides. Allowed ONLY in a sandboxed process: if one is
/ set, every path above must already have passed, which this guard
/ enforces; it is reported here so a fake date never goes unnoticed.
clockVar:$[proc = `tp; ".tp.clock.fixed"; ".wdb.clock.fixed"];
portVars:$[proc = `tp;
  enlist ".tp.cfg.port";
  (".wdb.cfg.port"; ".wdb.cfg.tpPort")];

violations:0;

-1 raze ("GUARD: "; string proc; " on port "; string port; " cwd="; cwd);
-1 raze ("GUARD: sandbox root "; sandboxAbs);

{[h;cwd;sandboxAbs;name]
  raw:remotePath[h; name];
  absPath:absolutize[cwd; raw];
  / Equality, or the sandbox root followed by a path separator. A bare
  / prefix test would wrongly accept /x/sandbox2 for /x/sandbox.
  ok:(absPath ~ sandboxAbs) or (raze (sandboxAbs; "/")) ~ (1 + count sandboxAbs)#absPath;
  -1 raze ("GUARD: path "; name; " = "; raw; " -> "; absPath; $[ok; "  ok"; "  OUTSIDE SANDBOX"]);
  if[not ok; violations::violations+1];
 }[h;cwd;sandboxAbs] each pathVars;

fixedDate:h clockVar;
if[not null fixedDate;
  -1 raze ("GUARD: clock "; clockVar; " = "; string fixedDate; "  (FAKE DATE - permitted only because every path is inside the sandbox)")];

{[h;portMin;portMax;name]
  v:h name;
  ok:(v >= portMin) and v <= portMax;
  -1 raze ("GUARD: port "; name; " = "; string v; $[ok; "  ok"; "  OUTSIDE TEST RANGE"]);
  if[not ok; violations::violations+1];
 }[h;portMin;portMax] each portVars;

hclose h;

if[violations > 0;
  if[not null fixedDate;
    -1 raze ("GUARD: a fake date is set on a process that is NOT fully sandboxed - never do this outside tests")];
  fail raze (string violations; " violation(s) for "; string proc; " - test run aborted")];

-1 raze ("GUARD: "; string proc; " isolated");
/ Let stdout flush before exit (KDB-X 5.0 can drop trailing output otherwise).
system "sleep 0.1";
exit 0
