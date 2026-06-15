\c 50 300

// ============================================================================
// replay.q - Historical replay driver (v0.1)
//
// Reads historical market events from hdb_binancedata/ and drives the
// framework as if they were arriving live. Same framework code, different
// driver - in a future live runner, CTP subscription would take this file's
// place.
//
// Inputs (from caller):
//   hdbRoot   - path to historical HDB (e.g. "hdb_binancedata")
//   sym       - symbol to backtest (single-symbol in v0.1)
//   startDate - q date
//   endDate   - q date (inclusive)
//   table     - `aggTrade_fut or `trade_fut (which granularity to replay)
//
// Side effects:
//   - Loads sym enum file so symbols resolve
//   - Preloads funding history for the date range (cached for clock advance)
//   - Iterates trade rows in time order, calling .fw.onTrade per row
// ============================================================================


// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------

.replay.cfg.hdbRoot:   "hdb_binancedata";
.replay.cfg.fundingTable: `funding;
.replay.state.fundingHistory: ();   / cached funding events for current run


// ----------------------------------------------------------------------------
// Sym enum loading
//
// When we read splay column files directly (without .Q.dpft loading a full
// HDB), the sym column is enum-typed integer indices into the parent sym
// file. Loading the sym file into the global `sym variable makes the enum
// resolve to symbol values for queries.
// ----------------------------------------------------------------------------

.replay.loadSym: {[hdbRoot]
  p: hsym `$ raze (hdbRoot; "/sym");
  if[() ~ key p;
    '"replay: sym file not found at ", string p
  ];
  sym:: get p;
 };


// ----------------------------------------------------------------------------
// Funding preload
//
// Funding data lives in a single splayed table at hdbRoot/funding/. We
// load the entire table once at run start and filter to the relevant
// symbol + date window. Volume is tiny (~3 events/day/symbol) so we keep
// it in memory.
// ----------------------------------------------------------------------------

.replay.loadFunding: {[hdbRoot; wantSym; startDate; endDate]
  p: hsym `$ raze (hdbRoot; "/funding/");
  if[() ~ key p;
    -1 "WARN: no funding table on disk - funding will not be applied";
    .replay.state.fundingHistory: ([] sym:`symbol$(); fundingTime:`timestamp$(); fundingRate:`float$(); markPrice:`float$());
    :()
  ];
  f: get p;
  startTs: startDate;
  endTs:   endDate + 1;
  f: select from f where sym = wantSym, fundingTime >= startTs, fundingTime < endTs;
  f: `fundingTime xasc f;
  .replay.state.fundingHistory: f;
  -1 raze ("REPLAY: loaded "; string count f; " funding events for "; string wantSym;
           " in range "; string startDate; " - "; string endDate);
 };

.replay.fundingBetween: {[fromTs; toTs]
  / Called by framework virtual-clock advancer. Returns funding events with
  / fundingTime in (fromTs, toTs] - exclusive-inclusive so the same event
  / isn't returned twice on successive calls.
  select from .replay.state.fundingHistory where fundingTime > fromTs, fundingTime <= toTs
 };


// ----------------------------------------------------------------------------
// Trade replay
//
// Reads splays one date at a time, iterates rows, calls .fw.onTrade.
// ----------------------------------------------------------------------------

.replay.loadDay: {[hdbRoot; date; table]
  / Build splay path: hdbRoot/<date>/<table>
  p: hsym `$ raze (hdbRoot; "/"; string date; "/"; string table);
  if[() ~ key p;
    -1 raze ("WARN: no splay at "; string p; " - skipping "; string date);
    :([] sym:`symbol$(); price:`float$())
  ];
  t: get p;
  `exchTradeTs xasc t
 };

.replay.perDay: {[hdbRoot; wantSym; tableName; acc; dt]
  -1 raze ("REPLAY: replaying "; string dt);
  t: .replay.loadDay[hdbRoot; dt; tableName];
  / Filter to the requested sym (column name collides with locals in q-sql
  / so we name our param wantSym).
  t: select from t where sym = wantSym;
  if[0 = count t;
    -1 raze ("REPLAY: no rows for "; string dt; " "; string wantSym);
    :acc
  ];
  n: count t;
  -1 raze ("REPLAY: "; string n; " events to dispatch");
  {[r] .fw.onTrade r} each t;
  acc + n
 };

.replay.run: {[hdbRoot; sym; startDate; endDate; tableName]
  -1 "============================================";
  -1 raze ("REPLAY: starting "; string sym; " "; string startDate; " - "; string endDate; " ("; string tableName; ")");
  -1 "============================================";

  .replay.loadSym hdbRoot;
  .replay.loadFunding[hdbRoot; sym; startDate; endDate];

  dates: startDate + til 1 + `int$endDate - startDate;
  startTime: .z.p;

  / Run the fold over dates. Calls .replay.perDay (a namespace function so
  / it's visible from the fold's lambda; kdb lambdas don't capture locals).
  totalEvents: {[hr; s; tn; acc; dt] .replay.perDay[hr; s; tn; acc; dt]}[hdbRoot; sym; tableName]/[0; dates];

  elapsed: `long$(.z.p - startTime) % 1e9;
  -1 "============================================";
  -1 raze ("REPLAY: done. "; string totalEvents; " events in "; string elapsed; "s");
  -1 "============================================";
 };

-1 "replay.q loaded";
