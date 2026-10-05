// HDB Utilities
// Helpers to switch between HDBs, list tables, query date ranges, and load
// data into memory. Use .hdb.use to point at any HDB directory.

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
  // \l made the HDB the current directory, so the partition is addressed
  // from there: this works whether .hdb.use was given a relative or an
  // absolute path
  firstPart: hsym `$string first date;
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
// .hdb.use `:hdb
// .hdb.use `:hdb_binancedata
// .hdb.current[]
// .hdb.tables[]
// .hdb.dateRange[]
// .hdb.rowCounts[`trade_binance; 2026.01.01; 2026.01.15]
// .hdb.compression[`trade; 2026.01.18; 2026.01.23]
// myTrade: .hdb.load[`trade; 2026.01.20; 2026.01.22]
// myTradeBySym: .hdb.loadBySym[`trade; `BTCUSDT; 2026.01.20; 2026.01.22]
// infoCount: .hdb.rowCountsBySym[`trade; `BTCUSDT; 2026.01.20; 2026.01.22]

// ---------------------------------------------------------------------------
// Clock-corrected rows
//
// A row's `time` is the handler's receive time, the same instant as
// fhRecvTimeUtcNs. While the system clock is behind the exchange (the first
// seconds after a wake from sleep), the handlers take `time` from the
// exchange event time instead and leave the raw clock reading in
// fhRecvTimeUtcNs (cpp/include/row_clock.hpp). So a clock-corrected row is a
// row whose `time` differs from fhRecvTimeUtcNs. These helpers find them, so
// the epoch offset between the two columns never has to be typed by hand.
// ---------------------------------------------------------------------------

// Nanoseconds between the Unix epoch (fhRecvTimeUtcNs) and the kdb+ epoch (time)
.hdb.epochOffsetNs: 946684800000000000j;

// qsql condition: time differs from the receive time
.hdb.clockCorrectedWhere: (<>; `fhRecvTimeUtcNs; (+; .hdb.epochOffsetNs; ($; enlist `long; `time)));

// Add what a corrected row was corrected from and by how much:
//   fhRecvTime   the original receive time (the stale clock reading) as a timestamp
//   clockLagMs   how far the clock was behind: time minus fhRecvTime, in ms
.hdb.withClockLag: {[t]
  update fhRecvTime: `timestamp$fhRecvTimeUtcNs - .hdb.epochOffsetNs,
         clockLagMs: ((`long$time) - fhRecvTimeUtcNs - .hdb.epochOffsetNs) div 1000000
    from t
 };

// Clock-corrected rows of an in-memory table (any table with time and fhRecvTimeUtcNs)
.hdb.clockCorrectedRows: {[t] .hdb.withClockLag ?[t; enlist .hdb.clockCorrectedWhere; 0b; ()]};

// Clock-corrected rows of an HDB table for one date
.hdb.clockCorrected: {[tab; dt]
  if[not .hdb.loaded; -1 "ERROR: No HDB loaded"; :()];
  if[not tab in .hdb.tables[]; -1 "ERROR: Table not found: ", string tab; :()];
  .hdb.withClockLag ?[tab; ((=; `date; dt); .hdb.clockCorrectedWhere); 0b; ()]
 };

// Example usage:
// .hdb.clockCorrected[`quote_binance; 2026.10.06]
// select rows: count i, maxLagMs: max clockLagMs, first time, last time by sym from .hdb.clockCorrected[`trade_binance; 2026.10.06]
// .hdb.clockCorrectedRows select from trade_binance      / on an in-memory table, e.g. in the WDB
