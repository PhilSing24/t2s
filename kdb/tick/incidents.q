/ incidents.q - a short memory of problems, for "what went wrong RECENTLY".
/ -
/ Cumulative counters say that something happened since the process (or
/ the handler) started; they keep a resolved problem on the attention list
/ for days. Each process therefore also records every problem as a
/ timestamped incident, and .health[] / status.sh flag only the incidents
/ of the last T2S_ALERT_WINDOW_SEC seconds (default 3600). The counters
/ stay where they were, as totals.
/ -
/   .inc.add[src; kind; n]   record n occurrences of `kind` for `src` now
/   .inc.recent[]            ([src; kind] n; lastTime) within the window
/   .inc.recentDict[]        "src.kind" ! n, flat, for .health[]
/   .inc.count[kinds]        total n within the window for those kinds
/ Loaded by tp.q and wdb.q.

.inc.windowSec:$[count v:getenv `T2S_ALERT_WINDOW_SEC; "J"$v; 3600];
.inc.maxRows:20000;
.inc.log:([] time:`timestamp$(); src:`symbol$(); kind:`symbol$(); n:`long$());

.inc.add:{[src; kind; n]
  if[n <= 0; :()];
  `.inc.log insert (.z.p; src; kind; n);
  if[.inc.maxRows < count .inc.log; .inc.log:neg[.inc.maxRows div 2] # .inc.log];
  };
.inc.since:{[] .z.p - 1000000000 * .inc.windowSec};
.inc.recent:{[] select n:sum n, lastTime:max time by src, kind from .inc.log where time > .inc.since[]};
.inc.recentDict:{[] r:0!.inc.recent[]; (`$(string[r `src] ,' "." ,' string r `kind)) ! r `n};
.inc.count:{[kinds] exec sum n from .inc.log where time > .inc.since[], kind in kinds};
