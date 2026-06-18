\c 50 300

// ============================================================================
// backtest.q - Top-level orchestrator for a single backtest run (v0.1)
//
// Wires the 4-layer framework together for one strategy on one date range:
//   1. Load framework files (framework, pretrade, execution, position, replay)
//   2. Load the chosen strategy
//   3. Run replay over the configured date range
//   4. Print position report and framework status
//
// USAGE
//   q kdb/framework/backtest.q
//   q kdb/framework/backtest.q -strategy kdb/strategies/momentum.q \
//                              -ns .strat.momentum                 \
//                              -sym BTCUSDT                        \
//                              -start 2026.06.06 -end 2026.06.06   \
//                              -table aggTrade_fut                 \
//                              -fastN 10000 -slowN 50000           \
//                              -tradeQty 0.1
//
// Strategy-specific args (-fastN, -slowN, -tradeQty) are forwarded to the
// strategy's init function via the cfg dict. The strategy reads them with
// fallback to its own defaults if not provided.
//
// All arguments are optional; sensible defaults are provided so a bare
// `q kdb/framework/backtest.q` runs the example momentum strategy on
// 2026-06-06 BTCUSDT aggTrades from hdb_binancedata/.
// ============================================================================


// ----------------------------------------------------------------------------
// Defaults (override via command-line args)
// ----------------------------------------------------------------------------
.runner.cfg.hdbRoot:      "hdb_binancedata";
.runner.cfg.strategyFile: "kdb/strategies/momentum.q";
.runner.cfg.namespace:    `.strat.momentum;
.runner.cfg.sym:          `BTCUSDT;
.runner.cfg.startDate:    2026.06.06;
.runner.cfg.endDate:      2026.06.06;
.runner.cfg.tableName:    `aggTrade_fut;
.runner.cfg.strategyCfg:  ()!();


// ----------------------------------------------------------------------------
// Argument parsing - simple `-key value` style
// ----------------------------------------------------------------------------

// Infer the type of a string CLI value for the strategy cfg dict.
// Order matters: try long first, then float, else keep as string.
//   "10000"   -> 10000j   (long)
//   "1.5"     -> 1.5f     (float)
//   "1.0"     -> 1.0f     (float - has a decimal point, not a clean long)
//   "BTCUSDT" -> "BTCUSDT" (string)
// Note: a round value like "5" becomes a long 5j, not 5f. A strategy that
// treats a parameter as float should cast it defensively in its init.
.runner.inferType: {[v]
  / Long iff the value round-trips through long parsing unchanged.
  if[v ~ string "J"$v; :"J"$v];
  / Float iff it parses to a non-null float (covers "1.5", "1.0", "1e3").
  if[not null "F"$v; :"F"$v];
  / Fallback: leave as the original string.
  v
 };

.runner.parseArgs: {[args]
  if[0 = count args; :()];
  / Walk pairs. Runner-owned flags are handled explicitly; ANY other -flag is
  / treated as a strategy parameter and forwarded to strategyCfg with an
  / inferred type. This means adding a new strategy with new parameters never
  / requires editing the runner. Strategy-cfg pairs are collected into a list
  / of (key;value) tuples; the dict is assembled at the end so q doesn't lock
  / the value list to a single type during incremental insertion.
  scPairs: ();
  i: 0;
  while[i < count args;
    k: args i;
    if[(i + 1) >= count args; '"missing value for ", k];
    v: args i+1;
    $[k ~ "-strategy"; .runner.cfg.strategyFile: v;
      k ~ "-ns";       .runner.cfg.namespace:    `$v;
      k ~ "-sym";      .runner.cfg.sym:          `$v;
      k ~ "-start";    .runner.cfg.startDate:    "D"$v;
      k ~ "-end";      .runner.cfg.endDate:      "D"$v;
      k ~ "-table";    .runner.cfg.tableName:    `$v;
      k ~ "-hdb";      .runner.cfg.hdbRoot:      v;
      / default: any other -flag -> strategy parameter (strip leading '-')
      [ if[not "-" = k 0; '"unexpected arg (no leading dash): ", k];
        pname: `$ 1 _ k;
        scPairs,: enlist (pname; .runner.inferType v) ]
    ];
    i+: 2;
  ];
  if[count scPairs;
    .runner.cfg.strategyCfg: (first each scPairs)!last each scPairs;
  ];
 };


// ----------------------------------------------------------------------------
// Boot - load framework files in dependency order, then strategy
// ----------------------------------------------------------------------------
.runner.bootFramework: {[]
  / All framework files live next to this runner. We loaded the runner from
  / some path; the others sit alongside it.
  base: "kdb/framework/";
  -1 raze ("RUNNER: loading framework from "; base);
  system raze ("l "; base; "position.q");
  system raze ("l "; base; "pretrade.q");
  system raze ("l "; base; "execution.q");
  system raze ("l "; base; "replay.q");
  system raze ("l "; base; "framework.q");
 };

.runner.bootStrategy: {[]
  .fw.loadStrategy[.runner.cfg.strategyFile; .runner.cfg.namespace; .runner.cfg.strategyCfg];
 };


// ----------------------------------------------------------------------------
// Run
// ----------------------------------------------------------------------------
.runner.run: {[]
  .runner.bootFramework[];
  / Show effective strategy cfg before loading - helps verify CLI overrides took
  if[count .runner.cfg.strategyCfg;
    -1 raze ("RUNNER: strategy cfg overrides: "; .Q.s1 .runner.cfg.strategyCfg);
   ];
  .runner.bootStrategy[];

  / Configure framework virtual-clock timer (0 = disabled)
  / Could be a runner.cfg.timerSec parameter if a strategy uses onTimer.

  .replay.run[.runner.cfg.hdbRoot;
              .runner.cfg.sym;
              .runner.cfg.startDate;
              .runner.cfg.endDate;
              .runner.cfg.tableName];

  / Final report
  -1 "";
  show .fw.status[];
  -1 "";
  .pos.report[];
 };


// ----------------------------------------------------------------------------
// Entry point
// ----------------------------------------------------------------------------
.runner.parseArgs .z.x;
.runner.run[];
exit 0;
