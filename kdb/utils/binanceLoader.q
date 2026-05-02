\c 50 300

// Configuration
.cfg.downloadDir: "/home/philippe/BinanceMarketData/";
.cfg.partitionDir: `:/home/philippe/t2s/hdb_binancedata;
.cfg.baseUrl: "https://data.binance.vision/data/spot/daily/trades/";

// Date formatting helper: 2026.01.17 -> "2026-01-17"
formatDate: {[dt] "-" sv "." vs string dt};

// Build download URL for symbol and date
buildUrl: {[sym; dt]
  s: string sym;
  d: formatDate dt;
  .cfg.baseUrl, s, "/", s, "-trades-", d, ".zip"
 };

// Build local file paths
zipPath: {[sym; dt] .cfg.downloadDir, (string sym), "-trades-", (formatDate dt), ".zip"};
csvPath: {[sym; dt] .cfg.downloadDir, (string sym), "-trades-", (formatDate dt), ".csv"};

// Check if partition exists
partitionExists: {[dt]
  p: (1 _ string .cfg.partitionDir), "/", string dt;
  not () ~ key hsym `$p
 };

// Check if any CSV exists for given symbols and date
anyFileExists: {[syms; dt]
  paths: {[s; d] hsym `$(csvPath[s; d])}[; dt] each syms;
  any not () ~/: key each paths
 };

// Download single ZIP file using curl
downloadZip: {[sym; dt]
  url: buildUrl[sym; dt];
  zp: zipPath[sym; dt];
  cmd: "curl -s -f -o \"", zp, "\" \"", url, "\"";
  system cmd;
  // Check if file was created
  if[() ~ key hsym `$zp;
    -1 "ERROR: Failed to download ", url;
    :0b
  ];
  1b
 };

// Extract ZIP and delete it
extractAndClean: {[sym; dt]
  zp: zipPath[sym; dt];
  system "unzip -o ", zp, " -d ", .cfg.downloadDir;
  hdel hsym `$zp;
 };

// Load trades from CSV (enhanced version of original)
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

// Main function
downloadAndLoad: {[dt; syms]
  // Guard: check if partition already exists
  if[partitionExists dt;
    -1 "ERROR: Partition already exists for ", string dt;
    :()
  ];

  // Guard: check if any CSV already exists
  if[anyFileExists[syms; dt];
    -1 "ERROR: CSV file(s) already exist for ", string dt;
    :()
  ];

  // Download all ZIPs
  -1 "Downloading ", (string count syms), " files...";
  results: downloadZip[; dt] each syms;
  if[not all results;
    -1 "ERROR: Some downloads failed. Aborting.";
    :()
  ];

  // Extract and clean up ZIPs
  -1 "Extracting files...";
  extractAndClean[; dt] each syms;

  // Load all CSVs
  -1 "Loading CSVs...";
  paths: {[s; d] hsym `$(csvPath[s; d])}[; dt] each syms;
  trade: raze loadTrades each paths;

  // Sort and save partition
  -1 "Saving partition...";
  `trade set `sym`exchTradeTs xasc trade;
  .z.zd: (17; 5; 1);
  .Q.dpft[.cfg.partitionDir; dt; `sym; `trade];

  -1 "Done. Loaded ", (string count trade), " trades for ", string dt;

  -1 "Done. Loaded ", (string count trade), " trades for ", string dt;
  count trade
 };

// Download and save CSVs only (no kdb+ loading)
downloadOnly: {[dt; syms]
  // Guard: check if any CSV already exists
  if[anyFileExists[syms; dt];
    -1 "ERROR: CSV file(s) already exist for ", string dt;
    :()
  ];

  // Download all ZIPs
  -1 "Downloading ", (string count syms), " files...";
  results: downloadZip[; dt] each syms;
  if[not all results;
    -1 "ERROR: Some downloads failed. Aborting.";
    :()
  ];

  // Extract and clean up ZIPs
  -1 "Extracting files...";
  extractAndClean[; dt] each syms;

  -1 "Done. CSVs saved to ", .cfg.downloadDir;
  paths: csvPath[; dt] each syms;
  paths
 };

// Load existing CSVs and save to partition (no download)
loadAndSave: {[dt; syms]
  // Guard: check if partition already exists
  if[partitionExists dt;
    -1 "ERROR: Partition already exists for ", string dt;
    :()
  ];

  // Check all CSVs exist
  paths: {[s; d] hsym `$(csvPath[s; d])}[; dt] each syms;
  missing: paths where () ~/: key each paths;
  if[count missing;
    -1 "ERROR: Missing CSV files: ", ", " sv string missing;
    :()
  ];

  // Load all CSVs
  -1 "Loading CSVs...";
  `trade set raze loadTrades each paths;

  // Sort and save partition
  -1 "Saving partition...";
  `trade set `sym`exchTradeTs xasc trade;
  .z.zd: (17; 5; 1);
  .Q.dpft[.cfg.partitionDir; dt; `sym; `trade];

  -1 "Done. Loaded ", (string count trade), " trades for ", string dt;
  count trade
 };


// Example usage:
// downloadOnly[2026.01.17; `BTCUSDT`ETHUSDT`SOLUSDT]
// downloadAndLoad[2026.01.17; `BTCUSDT`ETHUSDT`SOLUSDT]
// loadAndSave[2026.01.17; `BTCUSDT`ETHUSDT`SOLUSDT]
