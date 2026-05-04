\c 50 300

// ============================================================================
// fundingLoader.q - Binance funding rate history loader
//
// Fetches historical funding rate events from Binance USD-M futures via
// the fundingRate REST endpoint and stores them in a single splayed table
// at $HDB_BINANCE_DIR/funding/.
//
// Funding events fire every 8 hours (or 4h for some symbols), so volume is
// low: ~3 records/day/symbol. The whole table for one symbol's full history
// fits in memory comfortably; date-partitioning would be wasteful.
//
// CONFIGURATION
//   HDB_BINANCE_DIR  Target HDB root (default: ./hdb_binancedata)
//
// USAGE - INTERACTIVE
//   q kdb/utils/fundingLoader.q
//   q) fetchSinceLast `BTCUSDT
//   q) fetchSinceLastAll `BTCUSDT`ETHUSDT`SOLUSDT
//   q) fetchWindow[`BTCUSDT; 2026.01.01D00; 2026.04.30D23:59]
//   q) fetchAllHistory `BTCUSDT
//
// USAGE - NON-INTERACTIVE
//   q kdb/utils/fundingLoader.q -since-last BTCUSDT,ETHUSDT
//   q kdb/utils/fundingLoader.q -window BTCUSDT 2026.01.01 2026.04.30
//   q kdb/utils/fundingLoader.q -all BTCUSDT
//
// SCHEMA (stored at $HDB_BINANCE_DIR/funding/)
//   sym         - symbol (e.g. `BTCUSDT)
//   fundingTime - timestamp of the funding event (UTC)
//   fundingRate - funding rate as a fraction (e.g. 0.0001 = 0.01%)
//   markPrice   - mark price at the funding event
//
// NOTE on KDB-X 5.0 string concat
//   Same quirk as tradeLoader.q: 'string atom' returns enlist'd char list
//   which breaks chained ',' concatenation. Use 'raze (a;b;c;...)' throughout.
// ============================================================================

// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------

.cfg.hdbRoot: hsym `$ $[count v:getenv `HDB_BINANCE_DIR; v; "hdb_binancedata"];
.cfg.fundingDir: ` sv .cfg.hdbRoot, `funding;
.cfg.apiHost: "fapi.binance.com";
.cfg.apiPath: "/fapi/v1/fundingRate";

// Pagination: Binance returns up to 1000 records per call.
.cfg.batchLimit: 1000;
.cfg.maxPages: 50;          // safety cap, 50 * 1000 = 50,000 events covers years
.cfg.delayMs: 250;          // polite delay between pages within a fetch

// Earliest plausible date for futures funding (Binance launched USD-M perpetuals
// in late 2019). Used as the default start for fetchAllHistory.
.cfg.epochDefault: 2019.01.01D00:00:00.000;

// ----------------------------------------------------------------------------
// Schema
// ----------------------------------------------------------------------------

// In-memory schema for incoming records. The on-disk splayed table uses the
// same shape with sym enumerated (handled by .Q.en).
.schema.funding:([] sym:`$(); fundingTime:`timestamp$(); fundingRate:`float$(); markPrice:`float$());

// ----------------------------------------------------------------------------
// Time helpers
// ----------------------------------------------------------------------------

// Convert q timestamp -> ms since epoch (Binance API expects ms longs).
.fn.tsToMs: {[ts] `long$(ts - 1970.01.01D0) % 1000000};

// Convert ms since epoch -> q timestamp.
.fn.msToTs: {[ms] 1970.01.01D0 + 1000000 * ms};

// ----------------------------------------------------------------------------
// HTTP fetch
// ----------------------------------------------------------------------------

// Build query string from a dict of params.
.fn.buildQuery: {[params]
  if[0 = count params; :""];
  pairs: {[k; v] raze (string k; "="; $[10h = type v; v; string v])}'[key params; value params];
  raze ("?"; "&" sv pairs)
 };

// Fetch a single page of funding rates. Returns the parsed JSON array (a
// list of dicts) or () on failure.
.fn.fetchPage: {[sym; startMs; endMs; limit]
  params: `symbol`startTime`endTime`limit ! (string sym; startMs; endMs; limit);
  url: raze ("https://"; .cfg.apiHost; .cfg.apiPath; .fn.buildQuery params);
  // curl with -f to fail on HTTP errors, -s for silent, -m timeout
  cmd: raze ("curl -s -f -m 30 \""; url; "\"");
  body: system cmd;
  if[0 = count body;
    -1 raze ("ERROR: empty response from "; url);
    :()
  ];
  // body is a list of char-vectors (one per line). Concatenate with no
  // separator to reconstitute the JSON.
  json: raze body;
  records: @[.j.k; json; {[e] -1 raze ("ERROR: JSON parse failed: "; e); ::}];
  if[records ~ (::); :()];
  records
 };

// ----------------------------------------------------------------------------
// Record shaping
// ----------------------------------------------------------------------------

// Convert a list of raw JSON dicts (each with string-typed fundingRate, etc.)
// into a typed table matching .schema.funding.
.fn.shapeRecords: {[records]
  if[0 = count records;
    :.schema.funding
  ];
  // .j.k returns a list of dicts. Project each field across the list.
  // Each record looks like:
  //   `symbol`fundingRate`fundingTime`markPrice ! ("BTCUSDT";"0.0001";1.570608e12;"34287.54")
  // Numbers come back as floats; strings as char vectors.
  syms:  `$ records @\: `symbol;
  rates: "F"$ records @\: `fundingRate;
  marks: "F"$ records @\: `markPrice;
  // fundingTime arrives as a number (ms epoch). Could be float or long depending
  // on JSON parse - cast to long defensively.
  rawTimes: records @\: `fundingTime;
  times: .fn.msToTs `long$ rawTimes;
  ([] sym:syms; fundingTime:times; fundingRate:rates; markPrice:marks)
 };

// ----------------------------------------------------------------------------
// Storage
// ----------------------------------------------------------------------------

// Returns 1b if the on-disk funding table exists.
// Build the splayed-table directory path with required trailing slash.
.fn.fundingPath: {[]
  rootStr: 1 _ string .cfg.hdbRoot;
  hsym `$ raze (rootStr; "/funding/")
 };

.fn.tableExists: {[]
  not () ~ key .cfg.fundingDir
 };

// Load the existing on-disk table (or empty schema if not present).
// The splayed table stores the sym column as enum-int references into
// the parent HDB's sym file. We load that file into the global `sym
// variable so the enum resolves to actual symbols (this is what
// kdb's `\l hdb` does automatically; we replicate it for direct loads).
.fn.loadExisting: {[]
  if[not .fn.tableExists[]; :.schema.funding];
  // Make the enum domain available globally
  symPath: ` sv .cfg.hdbRoot, `sym;
  if[not () ~ key symPath; sym:: get symPath];
  get .fn.fundingPath[]
 };

// Write the table to disk as a splayed table with sym enumerated.
// Replaces any existing table (caller is responsible for merging first).
.fn.writeTable: {[t]
  // Ensure root directory exists
  rootStr: 1 _ string .cfg.hdbRoot;
  if[() ~ key .cfg.hdbRoot;
    system raze ("mkdir -p "; rootStr);
  ];
  // Sort and enumerate against the HDB root's shared sym file. .Q.en
  // updates the parent sym in place and rewrites the table's sym column
  // as enum references into it.
  t: `sym`fundingTime xasc t;
  t: .Q.en[.cfg.hdbRoot; t];
  // Save as splayed table. Path must end with "/" so kdb writes as a
  // directory of column files rather than a single serialized file.
  .fn.fundingPath[] set t;
  count t
 };

// Returns the latest fundingTime stored for a symbol, or 0Np if none.
.fn.lastFundingTime: {[s]
  if[not .fn.tableExists[]; :0Np];
  t: .fn.loadExisting[];
  matches: select fundingTime from t where sym = s;
  if[0 = count matches; :0Np];
  max matches `fundingTime
 };

// ----------------------------------------------------------------------------
// Pagination loop
// ----------------------------------------------------------------------------

// Fetch all funding events for sym in [startMs, endMs] using pagination.
// Returns a list of raw JSON dicts.
.fn.paginateAll: {[sym; startMs; endMs]
  recs: ();
  cursor: startMs;
  page: 0;
  done: 0b;
  while[not done;
    if[page >= .cfg.maxPages;
      -1 raze ("WARN: reached maxPages cap ("; string .cfg.maxPages; "); stopping");
      :recs
    ];
    page+: 1;
    -1 raze ("  Page "; string page; ": fetching from "; string .fn.msToTs cursor);
    batch: .fn.fetchPage[sym; cursor; endMs; .cfg.batchLimit];
    n: count batch;
    -1 raze ("  Page "; string page; ": got "; string n; " records");
    if[0 = n; done: 1b; :recs];
    recs: recs, batch;
    if[n < .cfg.batchLimit; done: 1b; :recs];
    // Advance cursor past the last record's fundingTime
    lastMs: `long$ (last batch) `fundingTime;
    cursor: 1 + lastMs;
    if[cursor > endMs; done: 1b; :recs];
    // Polite delay
    if[.cfg.delayMs > 0;
      system raze ("sleep "; string `float$ .cfg.delayMs % 1000);
    ];
  ];
  recs
 };

// ----------------------------------------------------------------------------
// Public API
// ----------------------------------------------------------------------------

// Fetch funding for a symbol within an explicit time window.
// startTs and endTs are q timestamps (e.g. 2026.01.01D00:00:00).
// Existing records are preserved; new ones merged in.
fetchWindow: {[sym; startTs; endTs]
  -1 raze ("Fetching funding for "; string sym; " from "; string startTs; " to "; string endTs);
  startMs: .fn.tsToMs startTs;
  endMs:   .fn.tsToMs endTs;
  raw: .fn.paginateAll[sym; startMs; endMs];
  if[0 = count raw;
    -1 "  No new records.";
    :0
  ];
  newT: .fn.shapeRecords raw;
  -1 raze ("  Fetched "; string count newT; " records");
  // Merge with existing
  existing: .fn.loadExisting[];
  combined: existing, newT;
  // Dedupe by (sym, fundingTime) - keep last (most recent fetch wins)
  combined: 0!select last fundingRate, last markPrice by sym, fundingTime from combined;
  added: (count combined) - count existing;
  -1 raze ("  Net new rows after dedupe: "; string added);
  .fn.writeTable combined;
  -1 raze ("  Saved. Table now has "; string count combined; " rows");
  added
 };

// Fetch funding for a symbol since the last stored fundingTime.
// If no records exist yet, defaults to the .cfg.epochDefault start.
fetchSinceLast: {[sym]
  lastTs: .fn.lastFundingTime sym;
  startTs: $[null lastTs; .cfg.epochDefault; lastTs + 1];
  endTs: .z.p;
  fetchWindow[sym; startTs; endTs]
 };

// Multi-symbol incremental fetch.
fetchSinceLastAll: {[syms]
  results: ();
  -1 raze ("Fetching since last for "; string count syms; " symbols");
  -1 "";
  i: 0;
  while[i < count syms;
    s: syms i;
    -1 raze ("[" ; string s; "] ("; string i+1; "/"; string count syms; ")");
    n: fetchSinceLast s;
    results: results, enlist `sym`added!(s; n);
    -1 "";
    i+: 1;
  ];
  -1 "============================================";
  -1 "Summary";
  show results;
  -1 "============================================";
  results
 };

// Fetch full history for a symbol from .cfg.epochDefault to now.
fetchAllHistory: {[sym]
  fetchWindow[sym; .cfg.epochDefault; .z.p]
 };

// ----------------------------------------------------------------------------
// Query helper
// ----------------------------------------------------------------------------

// Quick summary of stored funding data per symbol.
status: {[]
  if[not .fn.tableExists[];
    -1 "No funding table exists yet.";
    :()
  ];
  t: .fn.loadExisting[];
  -1 raze ("Funding table at "; 1 _ string .cfg.fundingDir);
  -1 raze ("  Total rows: "; string count t);
  -1 "";
  show select rows: count i, firstTime: min fundingTime, lastTime: max fundingTime by sym from t;
 };

// ----------------------------------------------------------------------------
// Non-interactive entry
// ----------------------------------------------------------------------------

.fn.parseDate: {[s]
  if[not 10 = count s; :0Nd];
  if[not all s[(4 7)] = "."; :0Nd];
  d: "D"$s;
  $[null d; 0Nd; d]
 };

.fn.usage: {[]
  -2 "Usage:";
  -2 "  q kdb/utils/fundingLoader.q                         (interactive mode)";
  -2 "  q kdb/utils/fundingLoader.q -since-last SYMS";
  -2 "  q kdb/utils/fundingLoader.q -window SYM START END";
  -2 "  q kdb/utils/fundingLoader.q -all SYM";
  -2 "";
  -2 "  SYMS         comma-separated, uppercase (e.g. BTCUSDT,ETHUSDT)";
  -2 "  SYM          single symbol (uppercase)";
  -2 "  START, END   YYYY.MM.DD";
  -2 "";
  -2 "Examples:";
  -2 "  q kdb/utils/fundingLoader.q -since-last BTCUSDT,ETHUSDT,SOLUSDT";
  -2 "  q kdb/utils/fundingLoader.q -window BTCUSDT 2026.01.01 2026.04.30";
  -2 "  q kdb/utils/fundingLoader.q -all BTCUSDT";
 };

.fn.runFromArgs: {[args]
  if[0 = count args; .fn.usage[]; exit 1];
  cmd: first args;
  args: 1 _ args;

  if[cmd ~ "-since-last";
    if[1 <> count args; .fn.usage[]; exit 1];
    syms: `$"," vs first args;
    fetchSinceLastAll[syms];
    exit 0
  ];

  if[cmd ~ "-window";
    if[3 <> count args; .fn.usage[]; exit 1];
    sym: `$args 0;
    startDt: .fn.parseDate args 1;
    endDt:   .fn.parseDate args 2;
    if[(null startDt) | null endDt;
      -2 "ERROR: dates must be YYYY.MM.DD";
      .fn.usage[];
      exit 1
    ];
    // End of day for endDt to include all events on that date
    fetchWindow[sym; startDt + 0D00:00; endDt + 0D23:59:59];
    exit 0
  ];

  if[cmd ~ "-all";
    if[1 <> count args; .fn.usage[]; exit 1];
    sym: `$first args;
    fetchAllHistory sym;
    exit 0
  ];

  -2 raze ("ERROR: unknown command: "; cmd);
  .fn.usage[];
  exit 1
 };

// ----------------------------------------------------------------------------
// Banner / dispatch
// ----------------------------------------------------------------------------

if[count .z.x;
  .fn.runFromArgs .z.x;
 ];

-1 "fundingLoader loaded.";
-1 raze ("  HDB root:    "; 1 _ string .cfg.hdbRoot);
-1 raze ("  Funding dir: "; 1 _ string .cfg.fundingDir);
-1 "";
-1 "Usage:";
-1 "  fetchSinceLast `BTCUSDT";
-1 "  fetchSinceLastAll `BTCUSDT`ETHUSDT`SOLUSDT";
-1 "  fetchWindow[`BTCUSDT; 2026.01.01D00:00; 2026.04.30D23:59]";
-1 "  fetchAllHistory `BTCUSDT";
-1 "  status[]";
-1 "";
-1 "Or non-interactively:";
-1 "  q kdb/utils/fundingLoader.q -since-last BTCUSDT,ETHUSDT";
-1 "  q kdb/utils/fundingLoader.q -window BTCUSDT 2026.01.01 2026.04.30";
-1 "  q kdb/utils/fundingLoader.q -all BTCUSDT";
-1 "";
