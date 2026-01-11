/ ctl.q - Control process for dashboard buttons
/ Launches and stops the market data pipeline
/ Port: 5000

/ -----------------------------------------------------------------------------
/ Configuration
/ -----------------------------------------------------------------------------

.ctl.baseDir:getenv[`HOME],"/tick-to-signal";
.ctl.startScript:"start_bg.sh";
.ctl.stopScript:"stop.sh";
.ctl.pidDir:.ctl.baseDir,"/logs/processes";

/ -----------------------------------------------------------------------------
/ Process status tracking
/ -----------------------------------------------------------------------------

.ctl.status:`stopped;
.ctl.startTime:0Np;

/ -----------------------------------------------------------------------------
/ Native process control (doesn't rely on scripts)
/ -----------------------------------------------------------------------------

/ Kill process by PID file (graceful then forced)
.ctl.killByPid:{[pidFile]
  fullPath:.ctl.pidDir,"/",pidFile;
  pid:@[{first read0 hsym `$x}; fullPath; ""];
  if[not ""~pid;
    / Try graceful shutdown first (SIGTERM)
    @[system; "kill -15 ",pid," 2>/dev/null"; {}];
    system "sleep 1";
    / Force kill if still alive (SIGKILL)
    @[system; "kill -9 ",pid," 2>/dev/null"; {}];
    -1 "Killed PID ",pid," from ",pidFile;
    @[hdel; hsym `$fullPath; {}];
  ];
  };

/ Kill process by port (graceful then forced)
.ctl.killByPort:{[port]
  / Graceful first (SIGTERM)
  cmd:"lsof -ti:",string[port]," 2>/dev/null | xargs kill -15 2>/dev/null";
  @[system; cmd; {}];
  system "sleep 1";
  / Force kill if still alive (SIGKILL)
  cmd:"lsof -ti:",string[port]," 2>/dev/null | xargs kill -9 2>/dev/null";
  @[system; cmd; {}];
  };

/ Native stop function (doesn't use stop.sh)
.ctl.stopNative:{[]
  -1 "Stopping processes (graceful then forced)...";
  
  / Method 1: Kill by PID files
  .ctl.killByPid each ("tp.pid";"rdb.pid";"rte.pid";"tel.pid";"logmgr.pid";"trade_fh.pid";"quote_fh.pid");
  
  / Method 2: Kill by ports (fallback)
  .ctl.killByPort each 5010 5011 5012 5013 5014;
  
  / Method 3: Kill by process name (fallback) - graceful then forced
  @[system; "pkill -15 -f trade_feed_handler 2>/dev/null"; {}];
  @[system; "pkill -15 -f quote_feed_handler 2>/dev/null"; {}];
  system "sleep 1";
  @[system; "pkill -9 -f trade_feed_handler 2>/dev/null"; {}];
  @[system; "pkill -9 -f quote_feed_handler 2>/dev/null"; {}];
  
  -1 "Native stop complete";
  };

/ -----------------------------------------------------------------------------
/ Control functions (called by dashboard)
/ -----------------------------------------------------------------------------

/ Start all processes
.ctl.start:{[]
  if[.ctl.status=`running; :`already_running];
  cmd:"bash -c 'cd ",.ctl.baseDir," && ./",.ctl.startScript,"'";
  -1 "Executing: ",cmd;
  @[system; cmd; {-1 "Start error: ",x}];
  .ctl.status:`running;
  .ctl.startTime:.z.p;
  -1 "Pipeline started at ",string .ctl.startTime;
  `started
  };

/ Stop all processes (uses native stop, not script)
.ctl.stop:{[]
  if[.ctl.status=`stopped; :`already_stopped];
  
  / Use native stop function instead of script
  .ctl.stopNative[];
  
  .ctl.status:`stopped;
  -1 "Pipeline stopped";
  `stopped
  };

/ Force stop (for when status is wrong)
.ctl.forceStop:{[]
  -1 "Force stopping all processes...";
  .ctl.stopNative[];
  .ctl.status:`stopped;
  -1 "Force stop complete";
  `stopped
  };

/ Get current status
.ctl.getStatus:{[]
  `status`startTime`uptime ! (
    .ctl.status;
    .ctl.startTime;
    $[.ctl.status=`running; .z.p - .ctl.startTime; 0Nn]
  )
  };

/ Check if processes are actually running (by checking ports)
.ctl.healthCheck:{[]
  kdbAlive:{@[{hclose hopen x; 1b}; `$":localhost:",string x; 0b]};
  `tp`rdb`rte`tel`logmgr ! kdbAlive each 5010 5011 5012 5013 5014
  };

/ Returns table for dashboard display
/ Columns: process, port, status, started
.ctl.statusTable:{[]
  h:.ctl.healthCheck[];
  ports:5010 5011 5012 5013 5014;
  getStart:{[port] @[{h:hopen x; r:h".proc.startTime"; hclose h; r}; `$":localhost:",string port; 0Np]};
  starts:getStart each ports;
  flip `process`port`status`started ! (
    (`TP;`RDB;`RTE;`TEL;`LOG);
    ports;
    ?[h`tp`rdb`rte`tel`logmgr;`Live;`Down];
    starts
  )
  };

/ -----------------------------------------------------------------------------
/ Startup
/ -----------------------------------------------------------------------------

\p 5000

-1 "Control process started on port 5000";
-1 "Functions:";
-1 "  .ctl.start[]       - Start all processes";
-1 "  .ctl.stop[]        - Stop all processes (graceful)";
-1 "  .ctl.forceStop[]   - Force stop (ignores status flag)";
-1 "  .ctl.getStatus[]   - Get status info";
-1 "  .ctl.healthCheck[] - Check which ports are alive";
-1 "  .ctl.statusTable[] - Display status table";
