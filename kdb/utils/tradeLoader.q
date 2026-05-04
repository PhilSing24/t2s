\c 50 300

// ============================================================================
// tradeLoader.q - Binance historical trade loader
//
// Downloads daily trade ZIPs from data.binance.vision, extracts the CSV,
// and loads into a kdb+ partition under HDB_BINANCE_DIR.
//
// CONFIGURATION (env vars, set in your shell profile):
//   BINANCE_DOWNLOAD_DIR  Where ZIPs/CSVs land (default: ./BinanceMarketData/)
//   HDB_BINANCE_DIR       Target HDB root        (default: ./hdb_binancedata)
//
// USAGE - INTERACTIVE
//   q kdb/utils/tradeLoader.q
//   q) downloadAndLoad[2026.01.17; `BTCUSDT`ETHUSDT]
//   q) downloadAndLoadRange[2026.01.10; 2026.01.20; `BTCUSDT]
//   q) downloadAndLoadRangeOpts[2026.01.10; 2026.01.20; `BTCUSDT;
//                               `delaySec`skipExisting!(5;1b)]
//
// USAGE - NON-INTERACTIVE (script mode)
//   Args: -range START END SYMS [DELAY]
//     START, END  YYYY.MM.DD format (q date literal)
//     SYMS        comma-separated, uppercase (e.g. BTCUSDT,ETHUSDT)
//     DELAY       optional seconds between dates (default 2)
//
//   q kdb/utils/tradeLoader.q -range 2026.01.10 2026.01.20 BTCUSDT,ETHUSDT
//   q kdb/utils/tradeLoader.q -range 2026.01.18 2026.01.18 BTCUSDT 5
//
// PUBLIC FUNCTIONS
//   downloadAndLoad[date; syms]                   one date end-to-end
//   downloadAndLoadRange[start; end; syms]        date range with defaults
//   downloadAndLoadRangeOpts[start; end; syms; opts]   range with opts dict
//   downloadOnly[date; syms]                      fetch CSVs, no kdb load
//   loadAndSave[date; syms]                       load existing CSVs only
// ============================================================================

// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------

.cfg.downloadDir: $[count v:getenv `BINANCE_DOWNLOAD_DIR; v; "BinanceMarketData/"];
.cfg.partitionDir: hsym `$ $[count v:getenv `HDB_BINANCE_DIR; v; "hdb_binancedata"];
.cfg.baseUrl: "https://data.binance.vision/data/spot/daily/trades/";

.cfg.defaultDelaySec: 2;
.cfg.defaultSkipExisting: 1b;

// ----------------------------------------------------------------------------
// Path helpers
// ----------------------------------------------------------------------------

formatDate: {[dt] "-" sv "." vs string dt};

buildUrl: {[sym; dt]
  s: string sym;
  d: formatDate dt;
  .cfg.baseUrl, s, "/", s, "-trades-", d, ".zip"
 };

zipPath: {[sym; dt] .cfg.downloadDir, (string sym), "-trades-", (formatDate dt), ".zip"};
csvPath: {[sym; dt] .cfg.downloadDir, (string sym), "-trades-", (formatDate dt), ".csv"};

partitionExists: {[dt]
  p: (1 _ string .cfg.partitionDir), "/", string dt;
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
  cmd: "curl -s -f -o \"", zp, "\" \"", url, "\"";
  system cmd;
  if[() ~ key hsym `$zp;
    -1 "ERROR: Failed to download ", url;
    :0b
  ];
  1b
 };

extractAndClean: {[sym; dt]
  zp: zipPath[sym; dt];
  system "unzip -o ", zp, " -d ", .cfg.downloadDir;
  hdel hsym `$zp;
 };

// ----------------------------------------------------------------------------
// CSV parsing
// ----------------------------------------------------------------------------

loadTrades: {[filepath]
  epochOffset: "j"$1970.01.01D0;
  sym: `$first "-" vs last "/" vs string filepath;
  raw: flip `tradeId`price`qty`quoteQty`exchTradeTimeMs`buyerIsMaker`ignore ! ("JFFFJSS"; ",") 0: filepath;
  select
    exchTradeTs: `timestamp$(1000 * exchTradeTimeMs) + epochOffset,
    exchTradeTimeMs,
    sym,
    tradeId,
    price,
    qty,
    buyerIsMaker: `True = buyerIsMaker
  from raw
 };

// ----------------------------------------------------------------------------
// Per-date primitives (used by both single and range workflows)
// ----------------------------------------------------------------------------

// Download all symbol ZIPs for one date and extract.
// Returns 1b on success, 0b on any download failure.
.dl.fetchOneDate: {[dt; syms]
  -1 "  Downloading ", (string count syms), " files for ", string dt, "...";
  results: downloadZip[; dt] each syms;
  if[not all results;
    -1 "  ERROR: Some downloads failed for ", string dt;
    :0b
  ];
  -1 "  Extracting...";
  extractAndClean[; dt] each syms;
  1b
 };

// Load CSVs for one date, save partition. Returns row count, or 0N on failure.
.dl.loadOneDate: {[dt; syms]
  paths: {[s; d] hsym `$(csvPath[s; d])}[; dt] each syms;
  missing: paths where () ~/: key each paths;
  if[count missing;
    -1 "  ERROR: Missing CSV files: ", ", " sv string missing;
    :0N
  ];
  -1 "  Loading CSVs...";
  // Local var to avoid leaking `trade` into the global namespace, which
  // would cause stale state between range iterations.
  t: raze loadTrades each paths;
  t: `sym`exchTradeTs xasc t;
  -1 "  Saving partition...";
  .z.zd: (17; 5; 1);
  // .Q.dpft requires the table at a top-level name; assign and clean up.
  `trade set t;
  .Q.dpft[.cfg.partitionDir; dt; `sym; `trade];
  delete trade from `.;
  count t
 };

// ----------------------------------------------------------------------------
// Single-date public API
// ----------------------------------------------------------------------------

downloadAndLoad: {[dt; syms]
  if[partitionExists dt;
    -1 "ERROR: Partition already exists for ", string dt;
    :()
  ];
  if[anyFileExists[syms; dt];
    -1 "ERROR: CSV file(s) already exist for ", string dt;
    :()
  ];
  if[not .dl.fetchOneDate[dt; syms]; :()];
  n: .dl.loadOneDate[dt; syms];
  if[null n; :()];
  -1 "Done. Loaded ", (string n), " trades for ", string dt;
  n
 };

downloadOnly: {[dt; syms]
  if[anyFileExists[syms; dt];
    -1 "ERROR: CSV file(s) already exist for ", string dt;
    :()
  ];
  if[not .dl.fetchOneDate[dt; syms]; :()];
  -1 "Done. CSVs saved to ", .cfg.downloadDir;
  csvPath[; dt] each syms
 };

loadAndSave: {[dt; syms]
  if[partitionExists dt;
    -1 "ERROR: Partition already exists for ", string dt;
    :()
  ];
  n: .dl.loadOneDate[dt; syms];
  if[null n; :()];
  -1 "Done. Loaded ", (string n), " trades for ", string dt;
  n
 };

// ----------------------------------------------------------------------------
// Date-range public API
// ----------------------------------------------------------------------------

// Download and load a contiguous date range. Per-date failure does not abort
// the range; the loop tracks loaded/skipped/failed and prints a summary.
.dl.range: {[startDt; endDt; syms; opts]
  if[startDt > endDt;
    -1 "ERROR: startDate (", string startDt, ") > endDate (", string endDt, ")";
    :()
  ];
  defaults: `delaySec`skipExisting!(.cfg.defaultDelaySec; .cfg.defaultSkipExisting);
  opts: defaults, opts;

  dates: startDt + til 1 + `int$endDt - startDt;
  -1 "Range: ", string[startDt], " to ", string[endDt], " (", string[count dates], " dates)";
  -1 "Symbols: ", " " sv string syms;
  -1 "";

  ok: ();
  skipped: ();
  failed: ();
  totalRows: 0;

  i: 0;
  while[i < count dates;
    dt: dates i;
    -1 "[", string[dt], "] (", string[i+1], "/", string[count dates], ")";

    skipDate: 0b;
    failedDate: 0b;

    // Skip-existing handling
    if[partitionExists dt;
      $[opts `skipExisting;
        [-1 "  Partition exists - skipping"; skipped: skipped, dt; skipDate: 1b];
        [-1 "ERROR: Partition exists and skipExisting=0b; aborting"; :()]
      ];
    ];

    if[not skipDate;
      // Clean leftover CSVs from prior failed runs
      if[anyFileExists[syms; dt];
        -1 "  Cleaning leftover CSVs for ", string dt;
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
          -1 "  Loaded ", string[n], " trades";
          ok: ok, dt;
          totalRows+: n;
        ];
      ];
    ];

    // Polite delay before next download (skip after a skipped date and after
    // the last date).
    if[(not skipDate) & (i < (count dates) - 1);
      system "sleep ", string opts `delaySec;
    ];
    i+: 1;
  ];

  -1 "";
  -1 "============================================";
  -1 "Range complete";
  -1 "  Loaded:  ", string[count ok], " dates, ", string[totalRows], " trades total";
  -1 "  Skipped: ", string[count skipped], " dates";
  -1 "  Failed:  ", string[count failed], " dates";
  if[count failed; -1 "  Failed dates: ", " " sv string failed];
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
// Non-interactive entry: parse argv, run a command, exit.
// ----------------------------------------------------------------------------

// Validate "YYYY.MM.DD" string and return the parsed date or 0Nd on failure.
.dl.parseDate: {[s]
  if[not 10 = count s; :0Nd];
  if[not all s[(4 7)] = "."; :0Nd];
  d: "D"$s;
  $[null d; 0Nd; d]
 };

.dl.usage: {[]
  -2 "Usage:";
  -2 "  q kdb/utils/tradeLoader.q                            (interactive mode)";
  -2 "  q kdb/utils/tradeLoader.q -range START END SYMS [DELAY]";
  -2 "";
  -2 "  START, END  YYYY.MM.DD";
  -2 "  SYMS        comma-separated uppercase (e.g. BTCUSDT,ETHUSDT)";
  -2 "  DELAY       optional, seconds between dates (default 2)";
  -2 "";
  -2 "Examples:";
  -2 "  q kdb/utils/tradeLoader.q -range 2026.01.10 2026.01.20 BTCUSDT,ETHUSDT";
  -2 "  q kdb/utils/tradeLoader.q -range 2026.01.18 2026.01.18 BTCUSDT 5";
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
      -2 "ERROR: DELAY must be a non-negative integer, got: ", raw;
      exit 1
    ];
    delaySec: "J"$raw;
  ];

  result: .dl.range[startDt; endDt; syms; `delaySec`skipExisting!(delaySec;1b)];
  // Exit non-zero if any date failed
  $[count result `failed; exit 2; exit 0]
 };

// ----------------------------------------------------------------------------
// Banner / dispatch
// ----------------------------------------------------------------------------

if[count .z.x;
  // Argument-driven: run and exit
  .dl.runFromArgs .z.x;
 ];

// Interactive: print banner, define functions, return to REPL
-1 "tradeLoader loaded.";
-1 "  Download dir:  ", .cfg.downloadDir;
-1 "  Partition dir: ", 1 _ string .cfg.partitionDir;
-1 "";
-1 "Usage:";
-1 "  downloadAndLoad[2026.01.17; `BTCUSDT`ETHUSDT`SOLUSDT]";
-1 "  downloadAndLoadRange[2026.01.10; 2026.01.20; `BTCUSDT`ETHUSDT]";
-1 "  downloadAndLoadRangeOpts[2026.01.10; 2026.01.20; `BTCUSDT;";
-1 "                            `delaySec`skipExisting!(5;1b)]";
-1 "";
-1 "Or non-interactively:";
-1 "  q kdb/utils/tradeLoader.q -range 2026.01.10 2026.01.20 BTCUSDT,ETHUSDT";
-1 "";
