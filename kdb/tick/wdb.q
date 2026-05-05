/ wdb.q - Write-only RDB with intraday writedown
/ Based on w.q pattern - writes to disk when MAXROWS exceeded
/ At EOD: sorts on disk, moves to HDB partition

/ -------------------------------------------------------
/ Configuration
/ -------------------------------------------------------

.wdb.cfg.port:5011;
.wdb.cfg.tpPort:5010;
/ HDB directory: read from env var, fall back to a relative path.
/ Override at launch with T2S_HDB_DIR=/path/to/hdb (recommended: absolute path).
.wdb.cfg.hdbDir:hsym `$ $[count v:getenv `T2S_HDB_DIR; v; "../hdb"];
.wdb.cfg.maxRows:50000;

/ Enable compression for HDB writes
/ zstd, 2^17 block, level 1
.z.zd:(17;5;1);

/ Connection resilience
.wdb.conn.handle:0N;                   / TP connection handle
.wdb.conn.state:`disconnected;         / `disconnected`connecting`connected
.wdb.conn.lastAttempt:0Np;             / Last connection attempt time
.wdb.conn.retryCount:0;                / Consecutive failed attempts
.wdb.conn.cfg.baseDelayMs:1000;        / Initial retry delay (1 sec)
.wdb.conn.cfg.maxDelayMs:30000;        / Max retry delay (30 sec)
.wdb.conn.cfg.backoffMultiplier:1.5;   / Exponential backoff factor

/ Timer interval for reconnection checks
.wdb.cfg.timerMs:5000;

system "g 0";

.wdb.epochOffset:neg "j"$1970.01.01D0;
.proc.startTime:.z.p;

/ Statistics
.wdb.stats.flushCount:0j;
.wdb.stats.rowsWritten:0j;
.wdb.stats.tradesReceived:0j;
.wdb.stats.quotesReceived:0j;

/ -------------------------------------------------------
/ Phase 4: Replay-on-reconnect state
/ -------------------------------------------------------
/ WDB persists the highest tpSeqNo it has successfully flushed to disk.
/ On reconnect, it asks TP to replay everything since that point. This
/ closes the gap between TP's durability log and WDB's HDB partition
/ across disconnects (TP restart, WDB restart, network blip).

/ Checkpoint file: small file persisted alongside the tmp directory.
/ Uses T2S_TMP_DIR if set (same env var as TMPSAVE) for consistency.
.wdb.cfg.checkpointFile:hsym `$ raze ($[count v:getenv `T2S_TMP_DIR; v; "../"]; "wdb.lastTpSeqNo");

/ Highest tpSeqNo successfully flushed to disk. Loaded from checkpoint
/ on startup, advanced after each flush, persisted on each successful flush.
.wdb.lastTpSeqNo:0j;

/ Phase 4 stats (separate from the existing stats group)
.wdb.stats.replayRowsApplied:0j;
.wdb.stats.replayDuplicatesFiltered:0j;

/ Buffers used during reconnect window: between subscribing live and
/ finishing replay, incoming live messages accumulate here so they
/ aren't lost while we're catching up. Cleared after drain.
.wdb.replayLiveBuffer.trade_binance:();
.wdb.replayLiveBuffer.quote_binance:();

/ True while in the replay-and-catchup phase. While true, upd() stashes
/ messages into replayLiveBuffer instead of processing them. Cleared by
/ .wdb.runReplay once replay completes.
.wdb.replayMode:0b;

/ Cutoff captured at reconnect time. Live messages with tpSeqNo <= cutoff
/ are duplicates of replay output and get filtered when draining the buffer.
.wdb.replayCutoff:0j;

/ -------------------------------------------------------
/ TMPSAVE - temporary directory for intraday writes
/ Read tmp dir prefix from env var, fall back to a relative path.
/ Override with T2S_TMP_DIR=/path/to/tmp/ (trailing slash; absolute recommended).
/ -------------------------------------------------------

.wdb.tmpDir:$[count v:getenv `T2S_TMP_DIR; v; "../"];
.wdb.getTmpSave:{`$":",.wdb.tmpDir,"tmp.",string[.z.i],".",string x}
TMPSAVE:.wdb.getTmpSave .z.d

/ -------------------------------------------------------
/ Table schemas (loaded from shared definition)
/ WDB receives data from TP with TP's stamp, then appends its own.
/ -------------------------------------------------------

\l ../schemas.q

trade_binance:.schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo`wdbRecvTimeUtcNs];
quote_binance:.schema.extend[.schema.quote; `tpRecvTimeUtcNs`tpSeqNo`wdbRecvTimeUtcNs];

/ -------------------------------------------------------
/ Phase 4: Field positions (computed from cols at startup)
/ -------------------------------------------------------
/ When upd() receives a row from TP it has 14 elements (TP stamps
/ tpRecvTimeUtcNs and tpSeqNo, but our wdbRecvTimeUtcNs hasn't been added
/ yet at that point). The position of tpSeqNo in our 15-column table is
/ the same as the position in the incoming 14-element row, since
/ wdbRecvTimeUtcNs is appended at the end.
/ Both schemas have tpSeqNo at the same relative position so we use trade.
.wdb.idx.tpSeqNo:(cols trade_binance)?`tpSeqNo;

/ -------------------------------------------------------
/ Utility Functions
/ -------------------------------------------------------

.wdb.tsToNs:{[ts] .wdb.epochOffset+"j"$ts};

/ -------------------------------------------------------
/ Phase 4: Checkpoint persistence
/ -------------------------------------------------------

/ Load checkpoint from disk. Returns 0 if no checkpoint exists yet.
.wdb.loadCheckpoint:{[]
  if[() ~ key .wdb.cfg.checkpointFile;
    -1 "WDB: no checkpoint file, starting from tpSeqNo=0";
    :0j
  ];
  v: @[get; .wdb.cfg.checkpointFile; {[err]
    -1 raze ("WDB: ERROR reading checkpoint - "; err);
    0j
  }];
  -1 raze ("WDB: loaded checkpoint tpSeqNo="; string v);
  v
 };

/ Persist checkpoint atomically. Writes to a temp file then renames so
/ a crash mid-write doesn't corrupt the checkpoint file.
.wdb.saveCheckpoint:{[seq]
  / Build the temp file path by appending ".tmp" to the checkpoint path.
  / Cannot use ` sv (...; `tmp) - that treats the checkpoint as a directory.
  tmpStr: 1 _ string .wdb.cfg.checkpointFile;
  tmpFile: hsym `$ raze (tmpStr; ".tmp");
  / Use .[func; argList; errHandler] for multi-arg protected eval.
  / @[func; arg; errHandler] is the SINGLE-arg form and would treat
  / our (tmpFile; seq) as a single list arg to set.
  / Sentinel: error handler returns the symbol `error`; success path
  / returns whatever set returns (the seq value). We check by type.
  result: .[set; (tmpFile; seq); {[err]
    -1 raze ("WDB: ERROR writing checkpoint tmp - "; err);
    `error
  }];
  if[result ~ `error; :()];
  / Atomic rename via shell mv (kdb has no rename primitive)
  cmd: raze ("mv "; tmpStr; ".tmp "; tmpStr);
  @[system; cmd; {[err] -1 raze ("WDB: ERROR renaming checkpoint - "; err)}];
 };

/ -------------------------------------------------------
/ Connection Management (Resilient)
/ -------------------------------------------------------

/ Calculate next retry delay with exponential backoff
.wdb.conn.getDelay:{[]
  delay:.wdb.conn.cfg.baseDelayMs * `long$.wdb.conn.cfg.backoffMultiplier xexp .wdb.conn.retryCount;
  delay & .wdb.conn.cfg.maxDelayMs  / Cap at max
  };

/ Check if enough time has passed since last attempt
.wdb.conn.canRetry:{[]
  if[null .wdb.conn.lastAttempt; :1b];
  elapsed:`long$(.z.p - .wdb.conn.lastAttempt) % 1000000;  / ms
  elapsed >= .wdb.conn.getDelay[]
  };

/ -------------------------------------------------------
/ Phase 4: Replay-on-reconnect logic
/ -------------------------------------------------------
/ The replay protocol runs once per connect. It catches up the WDB on
/ messages persisted in TP's durability log that we missed during a
/ disconnect (TP restart, WDB restart, network blip).
/ ----
/ Protocol (called from .wdb.connect after subscribe):
/   1. Capture TP's current tpSeqNo as cutoff (separates replay from live)
/   2. Ask TP to replay rows with tpSeqNo > lastTpSeqNo (our checkpoint)
/   3. Apply each replayed row through append() - fills tables, advances
/      checkpoint via flush
/   4. Drain replayLiveBuffer (live messages that arrived during steps 1-3),
/      filtering out any with tpSeqNo <= cutoff (those are duplicates)
/   5. Clear replayMode so future upd() calls go through the normal path

/ Apply a replayed row. Same as live upd() but accepts pre-shaped row data.
/ Stamps wdbRecvTimeUtcNs at replay-time (we don't have the original).
.wdb.applyReplayRow:{[tbl;row]
  row: row, .wdb.tsToNs[.z.p];
  .wdb.stats.replayRowsApplied+: 1;
  $[tbl = `trade_binance; .wdb.stats.tradesReceived+:1;
    tbl = `quote_binance; .wdb.stats.quotesReceived+:1;
    ()];
  append[tbl;row];
 };

/ Drain the live-buffer accumulated during replay window. Each entry is a
/ pre-WDB-stamp row (14 elements). Filter out duplicates of replay output
/ (tpSeqNo <= replayCutoff) and apply the rest as new live messages.
.wdb.drainLiveBuffer:{[tbl]
  buf: .wdb.replayLiveBuffer[tbl];
  if[0 = count buf; :()];

  / Extract tpSeqNo from each row and split duplicates from new
  bufSeqs: {x[.wdb.idx.tpSeqNo]} each buf;
  isDup: bufSeqs <= .wdb.replayCutoff;
  dupCount: sum isDup;
  newRows: buf where not isDup;

  -1 raze ("WDB: drained "; string count buf; " buffered "; string tbl;
           " (filtered "; string dupCount; " dups, "; string count newRows; " new)");

  .wdb.stats.replayDuplicatesFiltered+: dupCount;

  / Apply new rows via the normal live path (adds wdbRecvTimeUtcNs)
  {[tbl; row]
    row: row, .wdb.tsToNs[.z.p];
    $[tbl = `trade_binance; .wdb.stats.tradesReceived+:1;
      tbl = `quote_binance; .wdb.stats.quotesReceived+:1;
      ()];
    append[tbl; row];
  } [tbl;] each newRows;

  / Clear the buffer
  .wdb.replayLiveBuffer[tbl]: ();
 };

/ Run the full replay protocol against an open TP handle. Called by
/ .wdb.connect AFTER the subscribe completes.
.wdb.runReplay:{[h]
  -1 raze ("WDB: starting replay from tpSeqNo "; string .wdb.lastTpSeqNo + 1);

  / Capture cutoff. Live messages already arriving (since we subscribed)
  / are stashed in replayLiveBuffer; on drain, any with tpSeqNo > cutoff
  / are new and processed, others are duplicates of replay output.
  .wdb.replayCutoff: h ".tp.currentSeqNo[]";
  -1 raze ("WDB: replay cutoff = "; string .wdb.replayCutoff);

  if[.wdb.replayCutoff < .wdb.lastTpSeqNo + 1;
    -1 "WDB: nothing to replay (TP seqno is behind our checkpoint - probably TP restart with no log)";
  ];

  if[.wdb.replayCutoff >= .wdb.lastTpSeqNo + 1;
    {[h; tbl]
      replayRows: h(`.tp.replayFrom; tbl; .wdb.lastTpSeqNo + 1);
      n: count replayRows;
      -1 raze ("WDB: replaying "; string n; " "; string tbl; " rows");
      / Each iteration of the table yields a dict row; convert to list
      / via `value` to match the shape live upd() receives.
      {[tbl; rowDict] .wdb.applyReplayRow[tbl; value rowDict]} [tbl;] each replayRows;
    }[h;] each `trade_binance`quote_binance;
  ];

  / Drain live buffer (filtering duplicates against cutoff)
  .wdb.drainLiveBuffer each `trade_binance`quote_binance;

  / Clear replay mode - normal upd() resumes
  .wdb.replayMode: 0b;
  .wdb.replayCutoff: 0j;

  -1 "WDB: replay complete, switching to live mode";
 };

/ Status helper for observability
.wdb.replayStatus:{[]
  `lastTpSeqNo`replayMode`replayCutoff`replayRowsApplied`replayDupsFiltered`bufferTrades`bufferQuotes!(
    .wdb.lastTpSeqNo;
    .wdb.replayMode;
    .wdb.replayCutoff;
    .wdb.stats.replayRowsApplied;
    .wdb.stats.replayDuplicatesFiltered;
    count .wdb.replayLiveBuffer.trade_binance;
    count .wdb.replayLiveBuffer.quote_binance)
 };

/ Main connection function - NEVER THROWS
.wdb.connect:{[]
  / Guard: already connected
  if[not null .wdb.conn.handle; :1b];
  
  / Guard: backoff not elapsed
  if[not .wdb.conn.canRetry[]; :0b];
  
  .wdb.conn.state:`connecting;
  .wdb.conn.lastAttempt:.z.p;
  
  -1 "WDB: Connecting to TP on port ",string[.wdb.cfg.tpPort],
     " (attempt ",string[.wdb.conn.retryCount + 1],")...";
  
  / Protected connection attempt
  h:@[hopen; `$"::",string[.wdb.cfg.tpPort]; {[err] -1 "WDB: Connection failed - ",err; 0N}];
  
  if[null h;
    .wdb.conn.retryCount+:1;
    .wdb.conn.state:`disconnected;
    nextDelay:.wdb.conn.getDelay[];
    -1 "WDB: Will retry in ",string[nextDelay],"ms";
    :0b
  ];
  
  / Connection successful - subscribe and replay (Phase 4)
  / Order matters:
  /   1. Set replayMode=true so live messages get buffered (not processed)
  /   2. Subscribe (live messages start arriving; they're buffered)
  /   3. Run replay protocol (capture cutoff, replay missed range, drain buffer)
  /   4. runReplay clears replayMode; normal processing resumes
  subResult:@[{[h]
    .wdb.replayMode:: 1b;
    res:h(`pubsub.subscribe;`trade_binance;`);
    -1 "WDB: Subscribed to ",string first first res;
    res:h(`pubsub.subscribe;`quote_binance;`);
    -1 "WDB: Subscribed to ",string first first res;
    .wdb.runReplay[h];
    1b
  }; h; {[err]
    .wdb.replayMode:: 0b;
    -1 "WDB: Subscription/replay failed - ",err;
    0b
  }];
  
  if[not subResult;
    @[hclose; h; {}];  / Clean up failed connection
    .wdb.conn.retryCount+:1;
    .wdb.conn.state:`disconnected;
    :0b
  ];
  
  / Success - update state
  .wdb.conn.handle:h;
  .wdb.conn.state:`connected;
  .wdb.conn.retryCount:0;
  -1 "WDB: Connected successfully (handle ",string[h],")";
  1b
  };

/ Disconnect handler
.z.pc:{[h]
  if[h = .wdb.conn.handle;
    -1 "WDB: TP connection lost (handle ",string[h],")";
    .wdb.conn.handle:0N;
    .wdb.conn.state:`disconnected;
    .wdb.conn.retryCount:0;  / Reset backoff on disconnect
    -1 "WDB: Will attempt reconnection on next timer tick";
  ];
  };

/ -------------------------------------------------------
/ Disk Sort - efficient on-disk sorting by sym
/ -------------------------------------------------------

disksort:{[t;c;a]
  if[not`s~attr(t:hsym t)c;
    if[count t;
      ii:iasc iasc flip c!t c,:();
      if[not$[(0,-1+count ii)~(first;last)@\:ii;@[{`s#x;1b};ii;0b];0b];
        {v:get y;
          if[not$[all(fv:first v)~/:256#v;all fv~/:v;0b];
            v[x]:v;
            y set v];
        }[ii] each ` sv't,'get ` sv t,`.d
      ]
    ];
    @[t;first c;a]
  ];
  t}

/ -------------------------------------------------------
/ Append - insert and write to disk if MAXROWS exceeded
/ -------------------------------------------------------

append:{[t;data]
  t insert data;
  if[.wdb.cfg.maxRows<count value t;
    -1"WDB: Flushing ",string[t]," to disk (",string[count value t]," rows)";

    / Phase 4: capture max tpSeqNo BEFORE flushing so we can advance the
    / checkpoint after a successful flush.
    maxSeq: max value[t]`tpSeqNo;

    / Append enumerated buffer to disk. Phase 4 also addresses review issue #6
    / (unprotected intraday flush) - wrap in protected eval so a write failure
    / (disk full, permissions, etc.) doesn't tear down the process and leaves
    / the in-memory buffer intact for retry on the next append call.
    flushOk:@[{
        .[` sv TMPSAVE,x,`;();,;.Q.en[.wdb.cfg.hdbDir]`. x];
        1b
      }; t; {[err] -1 raze ("WDB: ERROR intraday flush - "; err); 0b}];

    if[flushOk;
      .wdb.stats.flushCount+:1;
      .wdb.stats.rowsWritten+:count value t;
      / Phase 4: advance and persist checkpoint after successful flush
      if[maxSeq > .wdb.lastTpSeqNo;
        .wdb.lastTpSeqNo: maxSeq;
        .wdb.saveCheckpoint[maxSeq];
      ];
      / Clear buffer
      @[`.;t;0#];
    ];
  ]};

/ -------------------------------------------------------
/ Update Handler
/ -------------------------------------------------------

upd:{[tbl;data]
  / Phase 4: during replay-mode, stash live messages into a buffer to be
  / drained after replay completes. This prevents losing live data that
  / arrives during the replay window.
  if[.wdb.replayMode;
    if[tbl in `trade_binance`quote_binance;
      .wdb.replayLiveBuffer[tbl],: enlist data;
    ];
    :();
  ];

  / Normal live processing
  / Add WDB receive timestamp
  data:data,.wdb.tsToNs[.z.p];

  / Track statistics
  $[tbl = `trade_binance; .wdb.stats.tradesReceived+:1;
    tbl = `quote_binance; .wdb.stats.quotesReceived+:1;
    ()];

  / Append (will flush to disk if needed)
  append[tbl;data];
  };

/ -------------------------------------------------------
/ Health Check
/ -------------------------------------------------------

.health:{[]
  memMB:(`long$.Q.w[][`used]) % 1000000;
  
  / Determine status based on connection state
  st:$[.wdb.conn.state = `connected; `ok;
       .wdb.conn.state = `connecting; `degraded;
       `disconnected];
  
  `process`port`uptime`status`connState`memMB`tradesRecv`quotesRecv`flushes`rowsWritten`bufferTrades`bufferQuotes!(
    `wdb;
    .wdb.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    .wdb.conn.state;
    memMB;
    .wdb.stats.tradesReceived;
    .wdb.stats.quotesReceived;
    .wdb.stats.flushCount;
    .wdb.stats.rowsWritten;
    count trade_binance;
    count quote_binance
  )
  };

/ -------------------------------------------------------
/ Status Query
/ -------------------------------------------------------

.wdb.status:{[]
  `port`tpPort`connected`maxRows`tmpSave`hdbDir`flushes`rowsWritten`bufferTrades`bufferQuotes`memMB!(
    .wdb.cfg.port;
    .wdb.cfg.tpPort;
    .wdb.conn.state = `connected;
    .wdb.cfg.maxRows;
    TMPSAVE;
    .wdb.cfg.hdbDir;
    .wdb.stats.flushCount;
    .wdb.stats.rowsWritten;
    count trade_binance;
    count quote_binance;
    (`long$.Q.w[][`used]) % 1000000
  )
  };

/ -------------------------------------------------------
/ End-of-Day Helpers (separate functions, no closures over locals)
/ -------------------------------------------------------

/ Flush one table to TMPSAVE; return 1b on success, 0b on failure.
/ Reads TMPSAVE and .wdb.cfg.hdbDir as globals (avoids closure-capture issues).
.wdb.eod.flushTable:{[t]
  cnt:count value t;
  if[0 = cnt; :1b];
  -1 .wdb.eod.msg ("Flushing final "; string t; " ("; string cnt; " rows)");
  ok:@[{[t]
    .[` sv TMPSAVE,t,`;();,;.Q.en[.wdb.cfg.hdbDir]`. t];
    1b
  }; t; {[t;err] -1 .wdb.eod.msg ("ERROR flushing "; string t; " - "; err); 0b}[t]];
  if[ok;
    .wdb.stats.rowsWritten+:cnt;
    @[`.;t;0#];
  ];
  ok
  };

.wdb.eod.sortTable:{[t]
  ok:@[{[t] disksort[` sv TMPSAVE,t,`;`sym;`p#]; 1b};
       t;
       {[t;err] -1 .wdb.eod.msg ("ERROR sorting "; string t; " - "; err); 0b}[t]];
  ok
  };

.wdb.eod.resubscribe:{[]
  if[null .wdb.conn.handle; :()];
  @[{[h]
    h(`pubsub.subscribe;`trade_binance;`);
    -1 "WDB: Resubscribed to trade_binance";
    h(`pubsub.subscribe;`quote_binance;`);
    -1 "WDB: Resubscribed to quote_binance";
  }; .wdb.conn.handle; {[err] -1 "WDB: Resubscription failed - ",err}];
  };

/ raze-based message builder, robust against KDB-X 5.0 string-of-atom quirk
.wdb.eod.msg:{[pieces] "WDB: ",raze pieces};

.wdb.eod.resetForNewDay:{[]
  TMPSAVE::.wdb.getTmpSave .z.d;
  .wdb.stats.flushCount:0j;
  .wdb.stats.rowsWritten:0j;
  .wdb.stats.tradesReceived:0j;
  .wdb.stats.quotesReceived:0j;
  };

/ -------------------------------------------------------
/ End-of-Day Handler
/ -------------------------------------------------------

endofday:{[]
  d:-1 + .z.d;
  -1 .wdb.eod.msg ("EOD processing for "; string d);

  / Capture closing TMPSAVE before any reset
  closingTmp:TMPSAVE;
  closingTmpStr:1_string closingTmp;

  / Tables with sym column
  t:tables`.;
  t@:where 11h=type each t@\:`sym;
  -1 .wdb.eod.msg ("Tables to process: "; ", " sv string t);

  / Step 1: flush remaining buffers
  flushOk:all .wdb.eod.flushTable each t;
  if[not flushOk;
    -1 .wdb.eod.msg ("ABORTING EOD - flush failures, data preserved in "; closingTmpStr);
    :();
  ];

  / Step 2: pre-flight - did anything actually get written?
  if[() ~ key closingTmp;
    -1 "WDB: No temp partition to move (no data flushed today) - skipping HDB write";
    .wdb.eod.resetForNewDay[];
    .wdb.eod.resubscribe[];
    -1 "WDB: EOD complete (no data)";
    :();
  ];

  / Step 3: sort on disk by sym, set `p# attribute
  -1 "WDB: Sorting on disk...";
  sortOk:all .wdb.eod.sortTable each t;
  if[not sortOk;
    -1 .wdb.eod.msg ("ABORTING EOD - sort failures, data preserved in "; closingTmpStr);
    :();
  ];

  / Step 4: move temp partition into HDB partition
  dest:.Q.par[.wdb.cfg.hdbDir;d;`];
  destStr:-1_1_string dest;

  -1 .wdb.eod.msg ("Moving "; closingTmpStr; " -> "; destStr);

  if[not () ~ key dest;
    -1 .wdb.eod.msg ("ERROR destination "; destStr; " already exists - aborting move");
    :();
  ];

  moveCmd:"mv ",closingTmpStr," ",destStr;
  moveOk:@[{[cmd] system cmd; 1b};
           moveCmd;
           {[err] -1 "WDB: ERROR moving partition - ",err; 0b}];

  if[not moveOk;
    -1 .wdb.eod.msg ("ABORTING EOD - move failed, data preserved in "; closingTmpStr);
    :();
  ];

  if[() ~ key dest;
    -1 .wdb.eod.msg ("ERROR move appeared to succeed but destination missing - "; destStr);
    :();
  ];

  -1 .wdb.eod.msg ("HDB partition created: "; destStr);

  / Step 5: reset for new day, resubscribe
  .wdb.eod.resetForNewDay[];
  .wdb.eod.resubscribe[];

  -1 "WDB: EOD complete";
  };

/ -------------------------------------------------------
/ Exit Handler - SAVE data instead of destroying it
/ -------------------------------------------------------

.z.exit:{[x]
  -1 "WDB: Exit signal received (code: ",string[x],") - emergency flush...";
  
  t:tables `.;
  t@:where 11h=type each t@\:`sym;
  
  totalFlushed:0j;
  
  {[t]
    cnt:count value t;
    if[cnt > 0;
      -1 "WDB: Emergency flushing ",string[t]," (",string[cnt]," rows)";
      / Protected write - try to save even if something fails
      @[{[t;cnt]
        .[` sv TMPSAVE,t,`;();,;.Q.en[.wdb.cfg.hdbDir]`. t];
        -1 "WDB: Successfully flushed ",string[t];
      }[t;cnt]; ::; {[t;err] -1 "WDB: ERROR flushing ",string[t]," - ",err}[t]];
    ];
  } each t;
  
  -1 "WDB: Emergency flush complete - data preserved in ",string[TMPSAVE];
  -1 "WDB: To recover, check ",string[TMPSAVE]," directory";
  };

/ -------------------------------------------------------
/ Manual Flush (for testing/maintenance)
/ -------------------------------------------------------

.wdb.flush:{[]
  -1 "WDB: Manual flush requested";
  t:tables `.;
  t@:where 11h=type each t@\:`sym;

  {
    if[0 < count value x;
      / Phase 4: capture max tpSeqNo before flushing so we can advance
      / the checkpoint after a successful flush, same as the auto-flush
      / path in append.
      maxSeq: max value[x]`tpSeqNo;
      -1 "WDB: Flushing ",string[x]," (",string[count value x]," rows)";
      flushOk:@[{
          .[` sv TMPSAVE,x,`;();,;.Q.en[.wdb.cfg.hdbDir]`. x];
          1b
        }; x; {[err] -1 raze ("WDB: ERROR manual flush - "; err); 0b}];
      if[flushOk;
        .wdb.stats.flushCount+:1;
        .wdb.stats.rowsWritten+:count value x;
        if[maxSeq > .wdb.lastTpSeqNo;
          .wdb.lastTpSeqNo: maxSeq;
          .wdb.saveCheckpoint[maxSeq];
        ];
        @[`.;x;0#];
      ];
    ];
  } each t;

  -1 "WDB: Manual flush complete";
  };

/ -------------------------------------------------------
/ Timer - reconnection
/ -------------------------------------------------------

.z.ts:{[]
  / Attempt reconnection if disconnected
  if[null .wdb.conn.handle; .wdb.connect[]];
  };

/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system"p ",string .wdb.cfg.port;

-1"=======================================================";
-1"WDB (Write-only RDB) starting on port ",string[.wdb.cfg.port];
-1"=======================================================";
-1"Configuration:";
-1"  TP port: ",string[.wdb.cfg.tpPort];
-1"  MAXROWS: ",string[.wdb.cfg.maxRows];
-1"  TMPSAVE: ",string[TMPSAVE];
-1"  HDB: ",string[.wdb.cfg.hdbDir];
-1"";
-1"Connection Settings:";
-1"  Base retry delay: ",string[.wdb.conn.cfg.baseDelayMs],"ms";
-1"  Max retry delay: ",string[.wdb.conn.cfg.maxDelayMs],"ms";
-1"";

/ Phase 4: load checkpoint before connecting so replay knows where to start
.wdb.lastTpSeqNo: .wdb.loadCheckpoint[];

/ Attempt initial connection (non-blocking; runReplay fires inside connect)
connected:.wdb.connect[];

/ Start timer for reconnection
system "t ",string .wdb.cfg.timerMs;

-1"";
-1"Query Interface:";
-1"  .health[]            / Standardized health check";
-1"  .wdb.status[]        / Full status";
-1"  .wdb.replayStatus[]  / Phase 4 replay state";
-1"  .wdb.flush[]         / Manual flush to disk";
-1"";
-1"Tables: trade_binance quote_binance";
-1"";

$[connected; -1 "WDB: Ready and processing"; -1 "WDB: Started in DEGRADED mode - waiting for TP connection"];
-1"=======================================================";
