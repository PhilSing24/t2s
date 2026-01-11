/ logmgr.q - Log Manager (Cleanup and Diagnostics)
/ Lightweight process for log file maintenance

/ =============================================================================
/ Configuration
/ =============================================================================

.log.cfg.port:5014;
.log.cfg.logDir:"logs";
.log.cfg.retentionDays:7;              / Keep logs for 7 days by default
.log.cfg.cleanupHour:0;                / Run cleanup at midnight

/ Store start time of the process
.proc.startTime:.z.p;

/ =============================================================================
/ Log File Discovery
/ =============================================================================

/ List all log files in the log directory
/ @return table with date, file path, size, chunks
.log.list:{[]
  / Get all files in log directory
  dir:hsym `$.log.cfg.logDir;
  files:key dir;
  
  if[0 = count files; :([] date:`date$(); file:`$(); sizeMB:`float$(); chunks:`long$())];
  
  / Parse filenames: YYYY.MM.DD.log (single log format)
  parsed:{
    parts:"." vs string x;
    / Valid format: 4 parts (YYYY, MM, DD, log)
    if[4 <> count parts; :()];
    if[not "log" ~ parts 3; :()];
    d:"D"$"." sv 3#parts;
    if[null d; :()];
    (d; x)
    } each files;
  
  / Filter valid entries - keep only non-empty results
  / Note: () is different from (::) in q, use count to filter
  valid:parsed where 0 < count each parsed;
  
  if[0 = count valid; :([] date:`date$(); file:`$(); sizeMB:`float$(); chunks:`long$())];
  
  / Build result table with full paths
  dates:valid[;0];
  filenames:valid[;1];
  fullPaths:` sv/: dir,/:filenames;
  
  result:([] 
    date:dates; 
    file:fullPaths
  );
  
  / Add file sizes
  result:update sizeMB:(hcount each file) % 1e6 from result;
  
  / Add chunk counts (validation)
  result:update chunks:{first .log.info[x]} each file from result;
  
  `date xasc result
  };

/ Get info about a specific log file
/ @param f - log file path (hsym)
/ @return (chunks; size; valid)
.log.info:{[f]
  sz:@[hcount; f; -1j];
  if[sz < 0; :(0j; 0j; 0b)];
  / Get chunk count using -11!
  chunks:@[{-11!(-2;x)}; f; 0j];
  (chunks; sz; chunks > 0)
  };

/ =============================================================================
/ Log Cleanup
/ =============================================================================

/ Delete logs older than retention period
/ @param days - retention period in days (default from config)
/ @return count of files deleted
.log.cleanup:{[days]
  if[days ~ (::); days:.log.cfg.retentionDays];
  
  cutoff:.z.D - days;
  -1 "LOG: Cleaning up logs older than ",string[cutoff]," (",string[days]," day retention)";
  
  / Get all logs
  logs:.log.list[];
  
  if[0 = count logs;
    -1 "LOG: No log files found";
    :0j
  ];
  
  / Find files to delete
  toDelete:select from logs where date < cutoff;
  
  if[0 = count toDelete;
    -1 "LOG: No files to delete";
    :0j
  ];
  
  -1 "LOG: Deleting ",string[count toDelete]," files...";
  
  / Delete files
  deleted:0j;
  {
    -1 "  Deleting: ",string[x`file]," (",string[x`date],")";
    @[hdel; x`file; {-1 "  ERROR deleting: ",x}];
    deleted+:1;
  } each toDelete;
  
  -1 "LOG: Cleanup complete - deleted ",string[count toDelete]," files";
  count toDelete
  };

/ =============================================================================
/ Log Diagnostics
/ =============================================================================

/ Get summary of all logs
/ @return table with date, chunks, total size
.log.summary:{[]
  logs:.log.list[];
  
  if[0 = count logs; :([] date:`date$(); chunks:`long$(); sizeMB:`float$())];
  
  select chunks, sizeMB from logs
  };

/ Check log file integrity
/ @param f - log file path (hsym)
/ @return dictionary with status
.log.verify:{[f]
  -1 "LOG: Verifying ",string[f];
  
  if[() ~ key f;
    -1 "  File not found";
    :`status`file`exists!(`notfound; f; 0b)
  ];
  
  info:.log.info[f];
  chunks:info 0;
  size:info 1;
  valid:info 2;
  
  -1 "  Chunks: ",string[chunks];
  -1 "  Size: ",string[size]," bytes";
  -1 "  Valid: ",string[valid];
  
  `status`file`chunks`size`valid!($[valid; `ok; `corrupt]; f; chunks; size; valid)
  };

/ Verify log for a date
/ @param d - date (default today)
/ @return verification result dictionary
.log.verifyDate:{[d]
  if[d ~ (::); d:.z.D];
  
  logFile:hsym `$(.log.cfg.logDir,"/",string[d],".log");
  .log.verify[logFile]
  };

/ =============================================================================
/ Scheduled Cleanup (optional)
/ =============================================================================

/ Check if cleanup should run (called by timer)
.log.checkCleanup:{[]
  / Run cleanup at configured hour
  if[.log.cfg.cleanupHour = `hh$.z.T;
    / Only run once per hour (check minutes = 0)
    if[0 = `mm$.z.T;
      -1 "LOG: Scheduled cleanup starting...";
      .log.cleanup[::];
    ];
  ];
  };

/ =============================================================================
/ Startup
/ =============================================================================

system "p ",string .log.cfg.port;

-1 "=======================================================";
-1 "LOG Manager starting on port ",string[.log.cfg.port];
-1 "=======================================================";
-1 "Configuration:";
-1 "  Log directory: ",.log.cfg.logDir;
-1 "  Log format: single file per day (YYYY.MM.DD.log)";
-1 "  Retention: ",string[.log.cfg.retentionDays]," days";
-1 "  Cleanup hour: ",string[.log.cfg.cleanupHour],":00";

/ Show current log status
-1 "";
-1 "Current logs:";
logs:.log.list[];
if[0 < count logs; show select date, sizeMB, chunks from logs; -1 "  Total: ",string[count logs]," files, ",string[sum logs`sizeMB]," MB"];
if[0 = count logs; -1 "  No log files found"];

-1 "";
-1 "Query interface:";
-1 "  .log.list[]                    / List all log files";
-1 "  .log.summary[]                 / Summary by date";
-1 "  .log.info[`:/path/to/log]      / Get file info";
-1 "  .log.verify[`:/path/to/log]    / Verify file integrity";
-1 "  .log.verifyDate[.z.D]          / Verify today's log";
-1 "  .log.cleanup[]                 / Delete old logs (default retention)";
-1 "  .log.cleanup[3]                / Delete logs older than 3 days";
-1 "";
-1 "LOG Manager ready";
-1 "=======================================================";

/ Optional: Start timer for scheduled cleanup (every minute check)
/ Uncomment to enable automatic cleanup at configured hour
/ .z.ts:{.log.checkCleanup[]};
/ system "t 60000";
