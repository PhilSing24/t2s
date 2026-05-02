// HDB Utilities
// Location: /home/philippe/t2s/kdb/utils/hdbUtils.q

// Current HDB state
.hdb.path: `;
.hdb.loaded: 0b;

// Switch to a different HDB
.hdb.use: {[hdbPath]
  if[not () ~ key hdbPath;
    .hdb.path: hdbPath;
    system "l ", 1 _ string hdbPath;
    .hdb.loaded: 1b;
    -1 "Loaded HDB: ", string hdbPath;
    :1b
  ];
  -1 "ERROR: HDB not found at ", string hdbPath;
  :0b
 };

// Show current HDB
.hdb.current: {[]
  if[not .hdb.loaded; :"No HDB loaded"];
  .hdb.path
 };

// List tables actually on disk in HDB (not just in memory)
.hdb.tables: {[]
  if[not .hdb.loaded; -1 "ERROR: No HDB loaded"; :()];
  firstPart: hsym `$(1 _ string .hdb.path), "/", string first date;
  contents: key firstPart;
  contents where not contents like ".*"  // exclude hidden files
 };

// Get date range (first and last partition)
.hdb.dateRange: {[]
  if[not .hdb.loaded; -1 "ERROR: No HDB loaded"; :()];
  d: date;
  `startDate`endDate ! (min d; max d)
 };

// Row counts by date for a table within date range
.hdb.rowCounts: {[tab; startDt; endDt]
  if[not .hdb.loaded; -1 "ERROR: No HDB loaded"; :()];
  if[not tab in .hdb.tables[]; -1 "ERROR: Table not found: ", string tab; :()];
  dates: date where (date >= startDt) & (date <= endDt);
  counts: {[t; d] count ?[t; enlist (=; `date; d); 0b; ()]}[tab;] each dates;
  flip `date`rows ! (dates; counts)
 };

// Row counts by date for a table within date range, filtered by sym
.hdb.rowCountsBySym: {[tab; symFilter; startDt; endDt]
  if[not .hdb.loaded; -1 "ERROR: No HDB loaded"; :()];
  if[not tab in .hdb.tables[]; -1 "ERROR: Table not found: ", string tab; :()];
  dates: date where (date >= startDt) & (date <= endDt);
  counts: {[t; sf; d] count ?[t; ((=; `date; d); (in; `sym; enlist sf)); 0b; ()]}[tab; symFilter;] each dates;
  flip `date`sym`rows ! (dates; count[dates]#symFilter; counts)
 };


// Compression stats for a table within date range
// Returns: date, rows, compressed size (MB), logical size (MB), ratio
.hdb.compression: {[tab; startDt; endDt]
  if[not .hdb.loaded; -1 "ERROR: No HDB loaded"; :()];
  if[not tab in .hdb.tables[]; -1 "ERROR: Table not found: ", string tab; :()];
  dates: date where (date >= startDt) & (date <= endDt);
  basePath: 1 _ string .hdb.path;
  getStats: {[basePath; tab; dt]
    tabPath: hsym `$(basePath, "/", (string dt), "/", string tab);
    colList: key tabPath;
    colPaths: ` sv/: tabPath ,/: colList;
    info: -21!/: colPaths;
    // Handle uncompressed files (empty dict) - use hcount for file size
    getComp: {$[count x; x`compressedLength; hcount y]};
    getLogic: {$[count x; x`uncompressedLength; hcount y]};
    compSize: "f"$sum getComp'[info; colPaths];
    logicSize: "f"$sum getLogic'[info; colPaths];
    ratio: $[compSize > 0f; logicSize % compSize; 1f];
    (dt; compSize; logicSize; ratio)
  };
  stats: getStats[basePath; tab;] each dates;
  flip `date`compressedMB`logicalMB`ratio ! flip {(x 0; (x 1) % 1e6; (x 2) % 1e6; x 3)} each stats
 };

// Load table into memory for date range
.hdb.load: {[tab; startDt; endDt]
  if[not .hdb.loaded; -1 "ERROR: No HDB loaded"; :()];
  if[not tab in .hdb.tables[]; -1 "ERROR: Table not found: ", string tab; :()];
  res: ?[tab; enlist (&; (>=; `date; startDt); (<=; `date; endDt)); 0b; ()];
  -1 "Loaded ", (string count res), " rows from ", (string tab);
  res
 };

// Load table into memory for date range, filtered by sym
.hdb.loadBySym: {[tab; symFilter; startDt; endDt]
  if[not .hdb.loaded; -1 "ERROR: No HDB loaded"; :()];
  if[not tab in .hdb.tables[]; -1 "ERROR: Table not found: ", string tab; :()];
  res: ?[tab; ((>=; `date; startDt); (<=; `date; endDt); (in; `sym; enlist symFilter)); 0b; ()];
  -1 "Loaded ", (string count res), " rows from ", (string tab), " for sym ", string symFilter;
  res
 };

// Example usage:
// .hdb.use `:/home/philippe/t2s/hdb
// .hdb.use `:/home/philippe/t2s/hdb_binancedata
// .hdb.current[]
// .hdb.tables[]
// .hdb.dateRange[]
// .hdb.rowCounts[`trade_binance; 2026.01.01; 2026.01.15]
// .hdb.compression[`trade; 2026.01.18; 2026.01.23]
// myTrade: .hdb.load[`trade; 2026.01.20; 2026.01.22]
// myTradeBySym: .hdb.loadBySym[`trade; `BTCUSDT; 2026.01.20; 2026.01.22]
// infoCount: .hdb.rowCountsBySym[`trade; `BTCUSDT; 2026.01.20; 2026.01.22]
