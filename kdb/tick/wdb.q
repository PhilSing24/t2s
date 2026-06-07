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
.wdb.stats.aggTradesReceived:0j;
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

/ Highest tpSeqNo successfully flushed to disk PER TABLE. Loaded from
/ checkpoint on startup, advanced after each table's flush, persisted on
/ each successful flush.

/ DESIGN NOTE: this must be per-table, not a single global counter. With
/ a global cursor, flushing one table (say trade_binance reaches its 50k
/ buffer first) would advance the checkpoint past unflushed rows of the
/ other table (quote_binance with seqnos in the same range still in memory).
/ A crash here loses those rows on replay since they're below the new
/ checkpoint floor. Tracking per-table fixes this: trade's flush only
/ advances trade's cursor, and quote replay still picks up its pending
/ rows from the right point.
.wdb.lastTpSeqNo: `trade_binance`trade_binance_fut`quote_binance ! 0 0 0j;

/ Phase 4 stats (separate from the existing stats group)
.wdb.stats.replayRowsApplied:0j;
.wdb.stats.replayDuplicatesFiltered:0j;

/ Buffers used during reconnect window: between subscribing live and
/ finishing replay, incoming live messages accumulate here so they
/ aren't lost while we're catching up. Cleared after drain.
.wdb.replayLiveBuffer.trade_binance:();
.wdb.replayLiveBuffer.trade_binance_fut:();
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
/ Path is per-DATE only (no PID). A restarted WDB process inherits the
/ existing day's temp partition and appends to it, so a mid-day crash
/ doesn't strand data in a PID-named directory that EOD won't move.
/ Cross-day stragglers are handled by the orphan-recovery pass at startup.
/ -------------------------------------------------------

.wdb.tmpDir:$[count v:getenv `T2S_TMP_DIR; v; "../"];
.wdb.getTmpSave:{`$":",.wdb.tmpDir,"tmp.",string x}
TMPSAVE:.wdb.getTmpSave .z.d

/ -------------------------------------------------------
/ Table schemas (loaded from shared definition)
/ WDB receives data from TP with TP's stamp, then appends its own.
/ -------------------------------------------------------

\l ../schemas.q

trade_binance:.schema.extend[.schema.trade; `tpRecvTimeUtcNs`tpSeqNo`wdbRecvTimeUtcNs];
trade_binance_fut:.schema.extend[.schema.aggTrade; `tpRecvTimeUtcNs`tpSeqNo`wdbRecvTimeUtcNs];
quote_binance:.schema.extend[.schema.quote; `tpRecvTimeUtcNs`tpSeqNo`wdbRecvTimeUtcNs];

/ -------------------------------------------------------
/ Phase 4: Field positions (computed from cols at startup)
/ -------------------------------------------------------
/ When upd() receives a row from TP it has N-1 elements (TP stamps
/ tpRecvTimeUtcNs and tpSeqNo, but our wdbRecvTimeUtcNs hasn't been added
/ yet at that point). The position of tpSeqNo in our N-column table is
/ the same as the position in the incoming row, since wdbRecvTimeUtcNs is
/ appended at the end.
/ All three schemas have tpSeqNo at the same relative position from the
/ left (second-to-last in the incoming row), so a single index works for
/ all of them.
.wdb.idx.tpSeqNo:(cols trade_binance)?`tpSeqNo;

/ -------------------------------------------------------
/ Utility Functions
/ -------------------------------------------------------

.wdb.tsToNs:{[ts] .wdb.epochOffset+"j"$ts};

/ -------------------------------------------------------
/ Phase 4: Checkpoint persistence
/ -------------------------------------------------------

/ Load checkpoint from disk. Returns per-table dict, with backward-compat
/ handling for the pre-fix scalar format. The scalar gets promoted to a
/ per-table dict by initializing all tables to that value. This prevents
/ NEW losses going forward but does NOT recover rows that were already
/ silently skipped under the old global-cursor design - any such rows are
/ unrecoverable from this checkpoint and would need to be replayed from the
/ TP durability log (which is what the per-table fix prevents from
/ happening again).
/ ADR-013 backward-compat: legacy 2-table checkpoints (just
/ trade_binance + quote_binance, from before futures was wired) are
/ migrated to a 3-table dict with trade_binance_fut starting at 0.
.wdb.loadCheckpoint:{[]
  defaults: `trade_binance`trade_binance_fut`quote_binance ! 0 0 0j;
  if[() ~ key .wdb.cfg.checkpointFile;
    -1 "WDB: no checkpoint file, starting fresh (trade=0, aggTrade=0, quote=0)";
    :defaults
  ];
  v: @[get; .wdb.cfg.checkpointFile; {[err]
    -1 raze ("WDB: ERROR reading checkpoint - "; err);
    defaults
  }];
  / Legacy scalar format: a single long applied to both tables. This was
  / the buggy original design - see DESIGN NOTE on .wdb.lastTpSeqNo above.
  if[-7h = type v;
    -1 raze ("WDB: migrating legacy scalar checkpoint "; string v; " -> per-table dict");
    :`trade_binance`trade_binance_fut`quote_binance ! (v;0j;v)
  ];
  / Pre-ADR-013 dict format had only trade_binance + quote_binance.
  / Add trade_binance_fut starting at 0 if absent (no futures data
  / has been written under that cursor, so 0 is correct).
  if[not `trade_binance_fut in key v;
    -1 "WDB: migrating legacy 2-table checkpoint -> 3-table dict (trade_binance_fut=0)";
    v: v, (enlist `trade_binance_fut)!enlist 0j;
  ];
  -1 raze ("WDB: loaded checkpoint - trade="; string v`trade_binance;
           " aggTrade="; string v`trade_binance_fut;
           " quote="; string v`quote_binance);
  v
 };

/ Persist checkpoint atomically. Writes the current .wdb.lastTpSeqNo dict
/ to a temp file then renames so a crash mid-write doesn't corrupt the
/ checkpoint file. Takes no args - always persists the current global.
.wdb.saveCheckpoint:{[]
  / Build the temp file path by appending ".tmp" to the checkpoint path.
  / Cannot use ` sv (...; `tmp) - that treats the checkpoint as a directory.
  tmpStr: 1 _ string .wdb.cfg.checkpointFile;
  tmpFile: hsym `$ raze (tmpStr; ".tmp");
  / Use .[func; argList; errHandler] for multi-arg protected eval.
  / @[func; arg; errHandler] is the SINGLE-arg form and would treat
  / our (tmpFile; dict) as a single list arg to set.
  / Sentinel: error handler returns the symbol `error`; success path
  / returns whatever set returns. We check by type.
  result: .[set; (tmpFile; .wdb.lastTpSeqNo); {[err]
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
  / Cast to long AFTER the multiplication (was: cast multiplier first, which
  / truncated 1.5^N to int, producing degenerate sequence 1s, 1s, 2s, 3s, 5s
  / instead of smooth 1s, 1.5s, 2.25s, 3.375s, ...)
  delay:`long$ .wdb.conn.cfg.baseDelayMs * .wdb.conn.cfg.backoffMultiplier xexp .wdb.conn.retryCount;
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
  $[tbl = `trade_binance;     .wdb.stats.tradesReceived+:1;
    tbl = `trade_binance_fut; .wdb.stats.aggTradesReceived+:1;
    tbl = `quote_binance;     .wdb.stats.quotesReceived+:1;
    ()];
  append[tbl;row];
 };

/ Drain the live-buffer accumulated during replay window. Each entry is a
/ pre-WDB-stamp row. Filter out duplicates of replay output
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
    $[tbl = `trade_binance;     .wdb.stats.tradesReceived+:1;
      tbl = `trade_binance_fut; .wdb.stats.aggTradesReceived+:1;
      tbl = `quote_binance;     .wdb.stats.quotesReceived+:1;
      ()];
    append[tbl; row];
  } [tbl;] each newRows;

  / Clear the buffer
  .wdb.replayLiveBuffer[tbl]: ();
 };

/ Run the full replay protocol against an open TP handle. Called by
/ .wdb.connect AFTER the subscribe completes.
.wdb.runReplay:{[h]
  -1 raze ("WDB: starting replay - checkpoint state: trade=";
           string .wdb.lastTpSeqNo`trade_binance;
           " aggTrade="; string .wdb.lastTpSeqNo`trade_binance_fut;
           " quote=";    string .wdb.lastTpSeqNo`quote_binance);

  / Capture cutoff. Live messages already arriving (since we subscribed)
  / are stashed in replayLiveBuffer; on drain, any with tpSeqNo > cutoff
  / are new and processed, others are duplicates of replay output.
  .wdb.replayCutoff: h ".tp.currentSeqNo[]";
  -1 raze ("WDB: replay cutoff = "; string .wdb.replayCutoff);

  / Per-table replay. Each iteration reads its own table's cursor fresh,
  / so a flush triggered DURING trade replay (which advances trade's
  / cursor) doesn't drag quote's starting point forward. This is critical
  / - the previous global-cursor design lost quote rows because trade's
  / replay would push the shared cursor past quote's checkpoint.
  {[h; tbl]
    fromSeq: .wdb.lastTpSeqNo[tbl] + 1;
    if[.wdb.replayCutoff < fromSeq;
      -1 raze ("WDB: nothing to replay for "; string tbl;
               " (cutoff "; string .wdb.replayCutoff;
               " < fromSeq "; string fromSeq; ")");
      :()
    ];
    replayRows: h(`.tp.replayFrom; tbl; fromSeq);
    n: count replayRows;
    -1 raze ("WDB: replaying "; string n; " "; string tbl;
             " rows from tpSeqNo "; string fromSeq);
    / Each iteration of the table yields a dict row; convert to list
    / via `value` to match the shape live upd() receives.
    {[tbl; rowDict] .wdb.applyReplayRow[tbl; value rowDict]} [tbl;] each replayRows;
  }[h;] each `trade_binance`trade_binance_fut`quote_binance;

  / Drain live buffer (filtering duplicates against cutoff)
  .wdb.drainLiveBuffer each `trade_binance`trade_binance_fut`quote_binance;

  / Clear replay mode - normal upd() resumes
  .wdb.replayMode: 0b;
  .wdb.replayCutoff: 0j;

  -1 "WDB: replay complete, switching to live mode";
 };

/ Status helper for observability
.wdb.replayStatus:{[]
  `lastTpSeqNoTrade`lastTpSeqNoAggTrade`lastTpSeqNoQuote`replayMode`replayCutoff`replayRowsApplied`replayDupsFiltered`bufferTrades`bufferAggTrades`bufferQuotes!(
    .wdb.lastTpSeqNo`trade_binance;
    .wdb.lastTpSeqNo`trade_binance_fut;
    .wdb.lastTpSeqNo`quote_binance;
    .wdb.replayMode;
    .wdb.replayCutoff;
    .wdb.stats.replayRowsApplied;
    .wdb.stats.replayDuplicatesFiltered;
    count .wdb.replayLiveBuffer.trade_binance;
    count .wdb.replayLiveBuffer.trade_binance_fut;
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
    res:h(`pubsub.subscribe;`trade_binance_fut;`);
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

    / Capture max tpSeqNo for THIS table BEFORE flushing so we can advance
    / the per-table checkpoint after a successful flush.
    maxSeq: max value[t]`tpSeqNo;

    / Append enumerated buffer to disk. Wrap in protected eval so a write
    / failure (disk full, permissions, etc.) doesn't tear down the process
    / and leaves the in-memory buffer intact for retry on the next call.
    flushOk:@[{
        .[` sv TMPSAVE,x,`;();,;.Q.en[.wdb.cfg.hdbDir]`. x];
        1b
      }; t; {[err] -1 raze ("WDB: ERROR intraday flush - "; err); 0b}];

    if[flushOk;
      .wdb.stats.flushCount+:1;
      .wdb.stats.rowsWritten+:count value t;
      / Per-table checkpoint: only this table's cursor advances. Quote's
      / cursor stays where it was, so a crash between this flush and the
      / next quote flush still replays the right pending quote rows.
      if[maxSeq > .wdb.lastTpSeqNo[t];
        .wdb.lastTpSeqNo[t]: maxSeq;
        .wdb.saveCheckpoint[];
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
    if[tbl in `trade_binance`trade_binance_fut`quote_binance;
      .wdb.replayLiveBuffer[tbl],: enlist data;
    ];
    :();
  ];

  / Normal live processing
  / Add WDB receive timestamp
  data:data,.wdb.tsToNs[.z.p];

  / Track statistics
  $[tbl = `trade_binance;     .wdb.stats.tradesReceived+:1;
    tbl = `trade_binance_fut; .wdb.stats.aggTradesReceived+:1;
    tbl = `quote_binance;     .wdb.stats.quotesReceived+:1;
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
  
  `process`port`uptime`status`connState`memMB`tradesRecv`aggTradesRecv`quotesRecv`flushes`rowsWritten`bufferTrades`bufferAggTrades`bufferQuotes!(
    `wdb;
    .wdb.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    .wdb.conn.state;
    memMB;
    .wdb.stats.tradesReceived;
    .wdb.stats.aggTradesReceived;
    .wdb.stats.quotesReceived;
    .wdb.stats.flushCount;
    .wdb.stats.rowsWritten;
    count trade_binance;
    count trade_binance_fut;
    count quote_binance
  )
  };

/ -------------------------------------------------------
/ Status Query
/ -------------------------------------------------------

.wdb.status:{[]
  `port`tpPort`connected`maxRows`tmpSave`hdbDir`flushes`rowsWritten`bufferTrades`bufferAggTrades`bufferQuotes`memMB!(
    .wdb.cfg.port;
    .wdb.cfg.tpPort;
    .wdb.conn.state = `connected;
    .wdb.cfg.maxRows;
    TMPSAVE;
    .wdb.cfg.hdbDir;
    .wdb.stats.flushCount;
    .wdb.stats.rowsWritten;
    count trade_binance;
    count trade_binance_fut;
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
  / Capture max seq BEFORE flush so we can advance the per-table checkpoint
  / on success. If we crash between this flush and the partition move, restart
  / will see this checkpoint and won't ask TP to replay rows already on disk.
  maxSeq: max value[t]`tpSeqNo;
  ok:@[{[t]
    .[` sv TMPSAVE,t,`;();,;.Q.en[.wdb.cfg.hdbDir]`. t];
    1b
  }; t; {[t;err] -1 .wdb.eod.msg ("ERROR flushing "; string t; " - "; err); 0b}[t]];
  if[ok;
    .wdb.stats.rowsWritten+:cnt;
    @[`.;t;0#];
    / Advance and persist this table's checkpoint.
    if[maxSeq > .wdb.lastTpSeqNo[t];
      .wdb.lastTpSeqNo[t]: maxSeq;
      .wdb.saveCheckpoint[];
    ];
  ];
  ok
  };

.wdb.eod.sortTable:{[t]
  / If this table was empty all day, flushTable never wrote a splay to
  / TMPSAVE and disksort would fail on the missing path. Treat absence
  / as success (nothing to sort, nothing to fail). Pre-ADR-013 this code
  / was never exercised against an empty table because spot + quote were
  / always both populated; the futures table can legitimately be empty
  / when running --markets=spot.
  splayPath:` sv TMPSAVE,t,`;
  if[() ~ key splayPath;
    -1 .wdb.eod.msg ("No splay for "; string t; " - skipping sort (empty table)");
    :1b
  ];
  ok:@[{[p] disksort[p;`sym;`p#]; 1b};
       splayPath;
       {[t;err] -1 .wdb.eod.msg ("ERROR sorting "; string t; " - "; err); 0b}[t]];
  ok
  };

.wdb.eod.resubscribe:{[]
  if[null .wdb.conn.handle; :()];
  @[{[h]
    h(`pubsub.subscribe;`trade_binance;`);
    -1 "WDB: Resubscribed to trade_binance";
    h(`pubsub.subscribe;`trade_binance_fut;`);
    -1 "WDB: Resubscribed to trade_binance_fut";
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
  .wdb.stats.aggTradesReceived:0j;
  .wdb.stats.quotesReceived:0j;
  };

/ -------------------------------------------------------
/ Orphan Recovery
/ -------------------------------------------------------
/ Scans .wdb.tmpDir at startup for tmp.YYYY.MM.DD directories from past
/ dates (stale temp partitions left behind by previous crashes) and moves
/ them into the HDB. Without this, a WDB crash between EOD-flush and
/ partition-move could strand a complete day's data in a temp directory
/ that nothing else will ever look at, while the checkpoint claims those
/ rows were durably persisted.

/ Parse a directory entry of form "tmp.YYYY.MM.DD" into a date.
/ Returns 0Nd if the entry doesn't match the expected shape.
.wdb.recovery.parseDate:{[entryStr]
  if[14 <> count entryStr; :0Nd];
  if[not "tmp." ~ 4#entryStr; :0Nd];
  "D"$ 4_ entryStr
  };

/ Sort + move one orphan temp directory into the HDB partition for its date.
/ Returns 1b on success, 0b on failure. Failure leaves the orphan in place
/ for inspection.
.wdb.recovery.recoverOne:{[entryStr; d]
  fullPath: .wdb.tmpDir, entryStr;
  tmpSym: hsym `$ fullPath;
  tabs: @[key; tmpSym; {[err]
    -1 raze ("WDB: cannot read orphan dir - "; err);
    `symbol$()
  }];
  if[0 = count tabs;
    -1 raze ("WDB: orphan "; entryStr; " is empty - leaving in place");
    :0b
  ];

  -1 raze ("WDB: sorting orphan "; entryStr; " (tables: "; ", " sv string tabs; ")");
  sortOk: all .wdb.recovery.sortOrphanTable[tmpSym] each tabs;
  if[not sortOk;
    -1 raze ("WDB: ABORTING recovery of "; entryStr; " - sort failures, data preserved");
    :0b
  ];

  / Move into HDB partition for the orphan's date.
  dest: .Q.par[.wdb.cfg.hdbDir; d; `];
  destStr: -1_1_string dest;
  if[not () ~ key dest;
    -1 raze ("WDB: ERROR HDB partition "; destStr;
             " already exists - leaving orphan "; entryStr; " for manual review");
    :0b
  ];

  cmd: "mv ", fullPath, " ", destStr;
  moveOk: @[{[c] system c; 1b};
            cmd;
            {[err] -1 raze ("WDB: ERROR moving orphan - "; err); 0b}];
  if[moveOk;
    -1 raze ("WDB: recovered orphan "; entryStr; " -> "; destStr);
  ];
  moveOk
  };

/ disksort one table within an orphan dir. Returns 1b/0b.
.wdb.recovery.sortOrphanTable:{[tmpSym; t]
  @[{[p] disksort[p; `sym; `p#]; 1b};
    ` sv tmpSym, t, `;
    {[err] -1 raze ("WDB: ERROR sorting orphan table - "; err); 0b}]
  };

/ Top-level: scan tmpDir, find tmp.YYYY.MM.DD entries with date < today,
/ recover each. Called once at startup before .wdb.connect[].
.wdb.recovery.run:{[]
  -1 "WDB: scanning for orphan temp partitions...";
  tmpDirSym: hsym `$ .wdb.tmpDir;
  entries: @[key; tmpDirSym; {[err]
    -1 raze ("WDB: cannot list tmp dir - "; err);
    `symbol$()
  }];
  if[0 = count entries;
    -1 "WDB: tmp dir empty, no orphan scan needed";
    :()
  ];

  / Build (entryStr; date) pairs for entries matching tmp.YYYY.MM.DD.
  / Discard malformed entries (returns 0Nd from parseDate).
  entryStrs: string entries;
  dates: .wdb.recovery.parseDate each entryStrs;
  validIdx: where not null dates;
  if[0 = count validIdx;
    -1 "WDB: no tmp.YYYY.MM.DD entries found";
    :()
  ];

  / Orphans are entries with date strictly before today. Today's dir (if
  / any) is owned by this process and will be appended to as live data
  / arrives.
  orphanIdx: validIdx where dates[validIdx] < .z.d;
  if[0 = count orphanIdx;
    -1 "WDB: no past-date temp partitions to recover";
    :()
  ];

  -1 raze ("WDB: found "; string count orphanIdx; " orphan partition(s)");

  / Process oldest first so partitions land in HDB in date order.
  / Use a projection on a top-level lambda - q lambdas don't capture
  / enclosing locals, so entryStrs/dates must be bound via projection
  / rather than referenced from the closure.
  ord: orphanIdx iasc dates orphanIdx;
  {[i; strs; dts] .wdb.recovery.recoverOne[strs i; dts i]}[; entryStrs; dates] each ord;

  -1 "WDB: orphan recovery scan complete";
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

  / Tables with sym column. This auto-discovers trade_binance_fut once
  / declared above - no name needs to be hardcoded here.
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
      / Capture max tpSeqNo for this table before flushing so we can advance
      / its checkpoint after a successful flush. Same pattern as the auto-flush
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
        if[maxSeq > .wdb.lastTpSeqNo[x];
          .wdb.lastTpSeqNo[x]: maxSeq;
          .wdb.saveCheckpoint[];
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

/ Recover any orphan tmp.YYYY.MM.DD partitions from prior crashes. Runs
/ before connect so any stranded past-day data flows into HDB before we
/ start accepting new live messages.
.wdb.recovery.run[];

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
-1"Tables: trade_binance trade_binance_fut quote_binance";
-1"";

$[connected; -1 "WDB: Ready and processing"; -1 "WDB: Started in DEGRADED mode - waiting for TP connection"];
-1"=======================================================";
