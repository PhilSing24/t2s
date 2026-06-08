\c 50 300

// ============================================================================
// aggTradeLoaderFut.q - Binance USD-M futures historical aggTrade loader
//
// Downloads daily aggTrade ZIPs from data.binance.vision (USD-M futures
// archive), extracts the CSV, and loads into a kdb+ partition under
// HDB_BINANCE_DIR. Mirrors tradeLoader.q's structure - same env vars,
// same options, same range workflow.
//
// CONFIGURATION (env vars, set in your shell profile):
//   BINANCE_DOWNLOAD_DIR  Where ZIPs/CSVs land (default: ./BinanceMarketData/)
//   HDB_BINANCE_DIR       Target HDB root        (default: ./hdb_binancedata)
//
// USAGE - INTERACTIVE
//   q kdb/utils/aggTradeLoaderFut.q
//   q) downloadAndLoad[2026.01.17; `BTCUSDT]
//   q) downloadAndLoadRange[2026.01.10; 2026.01.20; `BTCUSDT]
//   q) downloadAndLoadRangeOpts[2026.01.10; 2026.01.20; `BTCUSDT;
//                               `delaySec`skipExisting!(5;1b)]
//
// USAGE - NON-INTERACTIVE (script mode)
//   Args: -range START END SYMS [DELAY]
//     START, END  YYYY.MM.DD format
//     SYMS        comma-separated, uppercase (e.g. BTCUSDT)
//     DELAY       optional seconds between dates (default 2)
//
//   q kdb/utils/aggTradeLoaderFut.q -range 2026.01.10 2026.01.20 BTCUSDT
//
// PUBLIC FUNCTIONS
//   downloadAndLoad[date; syms]                   one date end-to-end
//   downloadAndLoadRange[start; end; syms]        date range with defaults
//   downloadAndLoadRangeOpts[start; end; syms; opts]   range with opts dict
//   downloadOnly[date; syms]                      fetch CSVs, no kdb load
//   loadAndSave[date; syms]                       load existing CSVs only
//
// OUTPUT
//   Partitioned splay at HDB_BINANCE_DIR/<date>/aggTrade_fut/
//   Columns: exchTradeTs, exchTradeTimeMs, sym, aggTradeId, firstTradeId,
//            lastTradeId, price, qty, buyerIsMaker
//
// NOTES
//   - Binance USD-M futures aggTrade CSVs have a HEADER ROW (unlike spot
//     trades CSV); we strip it before parsing.
//   - Boolean column in CSV is "true"/"false" lowercase (spot uses "True"/
//     "False" capitalized). The parser handles this.
//   - See ADR-013 for context on the live counterpart (trade_binance_fut).
//   - Schema deliberately omits observation columns (fhRecvTimeUtcNs etc)
//     that live in the production HDB only.
// ============================================================================

// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------

.cfg.downloadDir: $[count v:getenv `BINANCE_DOWNLOAD_DIR; v; "BinanceMarketData/"];
.cfg.partitionDir: hsym `$ $[count v:getenv `HDB_BINANCE_DIR; v; "hdb_binancedata"];
.cfg.baseUrl: "https://data.binance.vision/data/futures/um/daily/aggTrades/";

.cfg.defaultDelaySec: 2;
.cfg.defaultSkipExisting: 1b;

// ----------------------------------------------------------------------------
// Path helpers
// ----------------------------------------------------------------------------

formatDate: {[dt] "-" sv "." vs string dt};

buildUrl: {[sym; dt]
  raze (.cfg.baseUrl; string sym; "/"; string sym; "-aggTrades-"; formatDate dt; ".zip")
 };

zipPath: {[sym; dt]
  raze (.cfg.downloadDir; string sym; "-aggTrades-"; formatDate dt; ".zip")
 };

csvPath: {[sym; dt]
  raze (.cfg.downloadDir; string sym; "-aggTrades-"; formatDate dt; ".csv")
 };

partitionExists: {[dt]
  / The aggTrade_fut subdir is what we write; presence of any sym table here
  / implies this loader already populated this date. Using the subdir rather
  / than just the date dir lets aggTradeLoaderFut and tradeLoaderFut coexist
  / for the same date without one's existence blocking the other.
  p: raze ((1 _ string .cfg.partitionDir); "/"; string dt; "/aggTrade_fut");
  not () ~ key hsym `$p
 };

anyFileExists: {[syms; dt]
  paths: {[s; d] hsym `$(csvPath[s; d])}[; dt] each syms;
  any not () ~/: key each paths
 };

// ----------------------------------------------------------------------------
// Network and IO
// ----------------------------------------------------------------------------

downloadZip: {[sym; dt]
  url: buildUrl[sym; dt];
  zp: zipPath[sym; dt];
  / Ensure download dir exists. mkdir -p is idempotent so this is safe to
  / call on every download. Without it, curl exits with code 23 (write
  / error) when .cfg.downloadDir doesn't yet exist, which kdb's `system`
  / does NOT detect (system only throws if it can't run the shell at all,
  / not on non-zero exit codes from the executed command).
  system raze ("mkdir -p \""; .cfg.downloadDir; "\"");
  / Append `|| exit 1` so a non-zero curl exit propagates to the shell's
  / final exit status. kdb's `system` then sees the shell itself failed
  / and throws 'os, which the protected eval below catches.
  cmd: raze ("curl -s -f -o \""; zp; "\" \""; url; "\" || exit 1");
  ok: @[{system x; 1b}; cmd; {[err] 0b}];
  if[not ok;
    -1 raze ("ERROR: Failed to download "; url; " (file may not be published yet)");
    / Clean up any zero-byte file curl may have left behind
    if[not () ~ key hsym `$zp; hdel hsym `$zp];
    :0b
  ];
  if[() ~ key hsym `$zp;
    -1 raze ("ERROR: Download succeeded but file missing: "; zp);
    :0b
  ];
  1b
 };

extractAndClean: {[sym; dt]
  zp: zipPath[sym; dt];
  cp: csvPath[sym; dt];
  / Extract directly to a header-stripped CSV. Piping `unzip -p` through
  / `tail -n +2` avoids both (a) reading the whole file into kdb memory
  / (which `read0` would do, easily OOMing on multi-GB raw-trade files)
  / and (b) the redundant write of a header-included file we'd rewrite
  / anyway. The resulting CSV is consumed directly by `0:` with a
  / streaming file handle.
  /
  / IMPORTANT: kdb's `system` invokes the command directly via execvp,
  / NOT via a shell. Pipe and redirect operators ('|', '>') are passed
  / as literal arguments to the first program and silently fail (you'd
  / get an empty output file with no error). Explicit `/bin/sh -c` wrap
  / is required so the shell parses the operators.
  inner: raze ("unzip -p \""; zp; "\" | tail -n +2 > \""; cp; "\"");
  system raze ("/bin/sh -c '"; inner; "'");
  hdel hsym `$zp;
 };

// ----------------------------------------------------------------------------
// CSV parsing
//
// Binance futures aggTrade CSV header (verified 2026-06-08):
//   agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
// Types: J F F J J J S  (last column is "true"/"false" lowercase, read as
// symbol then converted to boolean below).
// ----------------------------------------------------------------------------

loadAggTrades: {[filepath]
  epochOffset: "j"$1970.01.01D0;
  / Symbol from filename pattern: "BTCUSDT-aggTrades-YYYY-MM-DD.csv"
  sym: `$first "-" vs last "/" vs string filepath;
  / Header has been pre-stripped by extractAndClean, so we can use `0:`
  / on the file path directly (streams from disk, doesn't load the whole
  / file into memory).
  raw: flip
       `aggTradeId`price`qty`firstTradeId`lastTradeId`exchTradeTimeMs`buyerIsMaker !
       ("JFFJJJS"; ",") 0: filepath;
  select
    exchTradeTs: `timestamp$(1000000 * exchTradeTimeMs) + epochOffset,
    exchTradeTimeMs,
    sym,
    aggTradeId,
    firstTradeId,
    lastTradeId,
    price,
    qty,
    buyerIsMaker: `true = buyerIsMaker
  from raw
 };

// ----------------------------------------------------------------------------
// Per-date primitives (used by both single and range workflows)
// ----------------------------------------------------------------------------

.dl.fetchOneDate: {[dt; syms]
  -1 raze ("  Downloading "; string count syms; " files for "; string dt; "...");
  results: downloadZip[; dt] each syms;
  if[not all results;
    -1 raze ("  ERROR: Some downloads failed for "; string dt);
    :0b
  ];
  -1 "  Extracting...";
  extractAndClean[; dt] each syms;
  1b
 };

.dl.loadOneDate: {[dt; syms]
  paths: {[s; d] hsym `$(csvPath[s; d])}[; dt] each syms;
  missing: paths where () ~/: key each paths;
  if[count missing;
    -1 raze ("  ERROR: Missing CSV files: "; ", " sv string missing);
    :0N
  ];
  -1 "  Loading CSVs...";
  t: raze loadAggTrades each paths;
  t: `sym`exchTradeTs xasc t;
  -1 "  Saving partition...";
  .z.zd: (17; 5; 1);
  `aggTrade_fut set t;
  .Q.dpft[.cfg.partitionDir; dt; `sym; `aggTrade_fut];
  delete aggTrade_fut from `.;
  count t
 };

// ----------------------------------------------------------------------------
// Single-date public API
// ----------------------------------------------------------------------------

downloadAndLoad: {[dt; syms]
  if[partitionExists dt;
    -1 raze ("ERROR: aggTrade_fut partition already exists for "; string dt);
    :()
  ];
  if[anyFileExists[syms; dt];
    -1 raze ("ERROR: CSV file(s) already exist for "; string dt);
    :()
  ];
  if[not .dl.fetchOneDate[dt; syms]; :()];
  n: .dl.loadOneDate[dt; syms];
  if[null n; :()];
  -1 raze ("Done. Loaded "; string n; " aggTrades for "; string dt);
  n
 };

downloadOnly: {[dt; syms]
  if[anyFileExists[syms; dt];
    -1 raze ("ERROR: CSV file(s) already exist for "; string dt);
    :()
  ];
  if[not .dl.fetchOneDate[dt; syms]; :()];
  -1 raze ("Done. CSVs saved to "; .cfg.downloadDir);
  csvPath[; dt] each syms
 };

loadAndSave: {[dt; syms]
  if[partitionExists dt;
    -1 raze ("ERROR: aggTrade_fut partition already exists for "; string dt);
    :()
  ];
  n: .dl.loadOneDate[dt; syms];
  if[null n; :()];
  -1 raze ("Done. Loaded "; string n; " aggTrades for "; string dt);
  n
 };

// ----------------------------------------------------------------------------
// Date-range public API
// ----------------------------------------------------------------------------

.dl.range: {[startDt; endDt; syms; opts]
  if[startDt > endDt;
    -1 raze ("ERROR: startDate ("; string startDt; ") > endDate ("; string endDt; ")");
    :()
  ];
  defaults: `delaySec`skipExisting!(.cfg.defaultDelaySec; .cfg.defaultSkipExisting);
  opts: defaults, opts;

  dates: startDt + til 1 + `int$endDt - startDt;
  -1 raze ("Range: "; string startDt; " to "; string endDt; " ("; string count dates; " dates)");
  -1 raze ("Symbols: "; " " sv string syms);
  -1 "";

  ok: ();
  skipped: ();
  failed: ();
  totalRows: 0;

  i: 0;
  while[i < count dates;
    dt: dates i;
    -1 raze ("["; string dt; "] ("; string i+1; "/"; string count dates; ")");

    skipDate: 0b;
    failedDate: 0b;

    if[partitionExists dt;
      $[opts `skipExisting;
        [-1 "  aggTrade_fut partition exists - skipping"; skipped: skipped, dt; skipDate: 1b];
        [-1 "ERROR: Partition exists and skipExisting=0b; aborting"; :()]
      ];
    ];

    if[not skipDate;
      if[anyFileExists[syms; dt];
        -1 raze ("  Cleaning leftover CSVs for "; string dt);
        paths: {[s; d] hsym `$(csvPath[s; d])}[; dt] each syms;
        {[p] if[not () ~ key p; hdel p]} each paths;
      ];

      if[not .dl.fetchOneDate[dt; syms];
        failed: failed, dt;
        failedDate: 1b;
      ];

      if[not failedDate;
        n: .dl.loadOneDate[dt; syms];
        if[null n;
          failed: failed, dt;
          failedDate: 1b;
        ];
        if[not failedDate;
          -1 raze ("  Loaded "; string n; " aggTrades");
          ok: ok, dt;
          totalRows+: n;
        ];
      ];
    ];

    if[(not skipDate) & (i < (count dates) - 1);
      system raze ("sleep "; string opts `delaySec);
    ];
    i+: 1;
  ];

  -1 "";
  -1 "============================================";
  -1 "Range complete";
  -1 raze ("  Loaded:  "; string count ok; " dates, "; string totalRows; " aggTrades total");
  -1 raze ("  Skipped: "; string count skipped; " dates");
  -1 raze ("  Failed:  "; string count failed; " dates");
  if[count failed;
    -1 raze ("  Failed dates: "; " " sv string failed)
  ];
  -1 "============================================";
  `loaded`skipped`failed`totalRows!(ok; skipped; failed; totalRows)
 };

downloadAndLoadRange: {[startDt; endDt; syms]
  .dl.range[startDt; endDt; syms; ()!()]
 };

downloadAndLoadRangeOpts: {[startDt; endDt; syms; opts]
  .dl.range[startDt; endDt; syms; opts]
 };

// ----------------------------------------------------------------------------
// Non-interactive entry
// ----------------------------------------------------------------------------

.dl.parseDate: {[s]
  if[not 10 = count s; :0Nd];
  if[not all s[(4 7)] = "."; :0Nd];
  d: "D"$s;
  $[null d; 0Nd; d]
 };

.dl.usage: {[]
  -2 "Usage:";
  -2 "  q kdb/utils/aggTradeLoaderFut.q                            (interactive mode)";
  -2 "  q kdb/utils/aggTradeLoaderFut.q -range START END SYMS [DELAY]";
  -2 "";
  -2 "  START, END  YYYY.MM.DD";
  -2 "  SYMS        comma-separated uppercase (e.g. BTCUSDT)";
  -2 "  DELAY       optional, seconds between dates (default 2)";
  -2 "";
  -2 "Examples:";
  -2 "  q kdb/utils/aggTradeLoaderFut.q -range 2026.06.01 2026.06.07 BTCUSDT";
  -2 "  q kdb/utils/aggTradeLoaderFut.q -range 2026.06.06 2026.06.06 BTCUSDT 5";
 };

.dl.runFromArgs: {[args]
  if[not "-range" ~ first args;
    .dl.usage[];
    exit 1
  ];
  args: 1 _ args;
  if[not (count args) within 3 4;
    .dl.usage[];
    exit 1
  ];

  startDt: .dl.parseDate args 0;
  endDt:   .dl.parseDate args 1;
  if[(null startDt) | null endDt;
    -2 "ERROR: dates must be YYYY.MM.DD";
    .dl.usage[];
    exit 1
  ];

  syms: `$"," vs args 2;
  if[any " " in/: string syms;
    -2 "ERROR: symbol list contains whitespace";
    exit 1
  ];

  delaySec: .cfg.defaultDelaySec;
  if[4 = count args;
    raw: args 3;
    if[not all raw in .Q.n;
      -2 raze ("ERROR: DELAY must be a non-negative integer, got: "; raw);
      exit 1
    ];
    delaySec: "J"$raw;
  ];

  result: .dl.range[startDt; endDt; syms; `delaySec`skipExisting!(delaySec;1b)];
  $[count result `failed; exit 2; exit 0]
 };

// ----------------------------------------------------------------------------
// Banner / dispatch
// ----------------------------------------------------------------------------

if[count .z.x;
  .dl.runFromArgs .z.x;
 ];

-1 "aggTradeLoaderFut loaded.";
-1 raze ("  Download dir:  "; .cfg.downloadDir);
-1 raze ("  Partition dir: "; 1 _ string .cfg.partitionDir);
-1 raze ("  Base URL:      "; .cfg.baseUrl);
-1 "";
-1 "Usage:";
-1 "  downloadAndLoad[2026.06.06; `BTCUSDT]";
-1 "  downloadAndLoadRange[2026.06.01; 2026.06.07; `BTCUSDT]";
-1 "  downloadAndLoadRangeOpts[2026.06.01; 2026.06.07; `BTCUSDT;";
-1 "                            `delaySec`skipExisting!(5;1b)]";
-1 "";
-1 "Or non-interactively:";
-1 "  q kdb/utils/aggTradeLoaderFut.q -range 2026.06.01 2026.06.07 BTCUSDT";
-1 "";
