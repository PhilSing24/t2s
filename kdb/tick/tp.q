/ tp.q - Tickerplant with KDB-X pubsub module

/ -------------------------------------------------------
/ Configuration
/ -------------------------------------------------------

.tp.cfg.port:5010;
.tp.cfg.logDir:"logs";
.tp.cfg.logEnabled:1b;

system "g 0";

.tp.epochOffset:neg"j"$1970.01.01D0;
.proc.startTime:.z.p;

/ -------------------------------------------------------
/ Table schema (must exist before pubsub init)
/ -------------------------------------------------------

trade_binance:([]
  time:`timestamp$();
  sym:`symbol$();
  tradeId:`long$();
  price:`float$();
  qty:`float$();
  buyerIsMaker:`boolean$();
  exchEventTimeMs:`long$();
  exchTradeTimeMs:`long$();
  fhRecvTimeUtcNs:`long$();
  fhParseUs:`long$();
  fhSendUs:`long$();
  fhSeqNo:`long$();
  tpRecvTimeUtcNs:`long$();
  tpSeqNo:`long$()         / NEW: monotonic per-TP sequence (Phase 4 - replay support)
  );

quote_binance:([]
  time:`timestamp$();
  sym:`symbol$();
  bidPrice1:`float$();
  bidPrice2:`float$();
  bidPrice3:`float$();
  bidPrice4:`float$();
  bidPrice5:`float$();
  bidQty1:`float$();
  bidQty2:`float$();
  bidQty3:`float$();
  bidQty4:`float$();
  bidQty5:`float$();
  askPrice1:`float$();
  askPrice2:`float$();
  askPrice3:`float$();
  askPrice4:`float$();
  askPrice5:`float$();
  askQty1:`float$();
  askQty2:`float$();
  askQty3:`float$();
  askQty4:`float$();
  askQty5:`float$();
  isValid:`boolean$();
  exchEventTimeMs:`long$();
  fhRecvTimeUtcNs:`long$();
  fhParseUs:`long$();
  fhSendUs:`long$();
  fhSeqNo:`long$();
  tpRecvTimeUtcNs:`long$();
  tpSeqNo:`long$()         / NEW: monotonic per-TP sequence (Phase 4 - replay support)
  );

health_feed_handler:([]
  time:`timestamp$();
  handler:`symbol$();
  startTimeUtc:`timestamp$();
  uptimeSec:`long$();
  msgsReceived:`long$();
  msgsPublished:`long$();
  lastMsgTimeUtc:`timestamp$();
  lastPubTimeUtc:`timestamp$();
  connState:`symbol$();
  symbolCount:`int$()
  );

/ -------------------------------------------------------
/ Pub/Sub - KDB-X module (must be named 'pubsub' for IPC)
/ -------------------------------------------------------

pubsub:use`di.pubsub

pubsub.init[]

/ -------------------------------------------------------
/ Logging
/ -------------------------------------------------------

.tp.logHandle:0N;
.tp.logFile:`;
.tp.logCount:0j;

.tp.logFilePath:{[] hsym`$(.tp.cfg.logDir,"/",string[.z.d],".log")};

.tp.initLog:{[f]
  if[0=@[hcount;f;0j];f set()];
  hopen f
  };

.tp.openLog:{[]
  if[not .tp.cfg.logEnabled;:()];
  system"mkdir -p ",.tp.cfg.logDir;
  .tp.logFile:.tp.logFilePath[];
  .tp.logHandle:.tp.initLog[.tp.logFile];
  .tp.logCount:@[{-11!(-2;x)};.tp.logFile;0j];
  -1"TP: Log file: ",string[.tp.logFile]," (",string[.tp.logCount]," chunks)";
  };

.tp.closeLog:{[]
  if[not .tp.cfg.logEnabled;:()];
  if[not null .tp.logHandle;@[hclose;.tp.logHandle;{}];.tp.logHandle:0N];
  };

.tp.log:{[tbl;data]
  if[not .tp.cfg.logEnabled;:()];
  if[tbl=`health_feed_handler;:()];
  .tp.logHandle enlist(`upd;tbl;data);
  .tp.logCount+:1;
  };

.tp.rotate:{[]
  -1"TP: Rotating log...";
  .tp.closeLog[];
  .tp.openLog[];
  };

/ -------------------------------------------------------
/ Sequence Tracking & Gap Detection (Phase 4)
/ -------------------------------------------------------

/ Field index for fhSeqNo, derived from the table schemas. This is the
/ position of fhSeqNo in the INCOMING FH row (before TP appends
/ tpRecvTimeUtcNs and tpSeqNo). Since TP always appends to the end, the
/ index is the same whether we're looking at a live FH row or a logged
/ post-TP row, as long as fhSeqNo's position in the schema doesn't change.
/ Deriving from cols means schema column reordering is picked up automatically
/ (was: hardcoded 11 / 27, which silently breaks gap detection if the schema
/ shifts. Fixes review finding #4).
.tp.idx.tradeSeq:(cols trade_binance)?`fhSeqNo;
.tp.idx.quoteSeq:(cols quote_binance)?`fhSeqNo;

/ Per-side state: last fhSeqNo seen from each FH
.tp.seq.trade:0N;
.tp.seq.quote:0N;

/ Per-side gap counters
.tp.gaps.trade:0j;
.tp.gaps.quote:0j;
.tp.missed.trade:0j;
.tp.missed.quote:0j;

/ Per-side restart counters (when fhSeqNo went backwards far enough to look like restart)
.tp.restarts.trade:0j;
.tp.restarts.quote:0j;

/ Per-side duplicate counters (Phase 4 - bumped when FH heartbeat resends already-seen seqs)
.tp.dups.trade:0j;
.tp.dups.quote:0j;

/ Monotonic per-TP sequence number stamped on every accepted message (Phase 4).
/ Used by subscribers (WDB primarily) to track "what's the latest message I've
/ processed" and to request replay from a specific point on reconnect.
.tp.tpSeqNo:0j;

/ Threshold for treating a backward jump as "FH restart" rather than "duplicate".
/ A duplicate is an in-flight resend of a recent message - typically off by 1-100.
/ A restart is the FH's own seqno being reset to 0 or 1 (whole-process restart).
.tp.cfg.restartThresh:1000;

/ Check fhSeqNo for the given side. Returns one of:
/   `accept           - new in-sequence message, log + publish
/   `accept_with_gap  - new message but gap detected; still log + publish (we already
/                       lost the missing ones, accepting this one moves us forward)
/   `drop_duplicate   - message already seen (FH heartbeat-driven resend); drop silently
/                       (skip log + publish; just return)
.tp.checkSeq:{[side; seq]
  lastSeq: .tp.seq[side];

  / First message - initialize, accept
  if[null lastSeq;
    .tp.seq[side]: seq;
    :`accept
  ];

  / Normal sequential case
  if[seq = lastSeq + 1;
    .tp.seq[side]: seq;
    :`accept
  ];

  / Forward jump - gap detected, missed messages
  if[seq > lastSeq + 1;
    missed: seq - lastSeq - 1;
    .tp.gaps[side]+: 1;
    .tp.missed[side]+: missed;
    -1 raze ("TP: "; string side; " gap - expected "; string lastSeq+1;
             " got "; string seq; " (missed "; string missed; ")");
    .tp.seq[side]: seq;
    :`accept_with_gap
  ];

  / Backward jump - either duplicate (small step backward) or FH restart (big jump)
  / Duplicate: typical case from heartbeat-driven resend, just drop
  if[(lastSeq - seq) < .tp.cfg.restartThresh;
    .tp.dups[side]+: 1;
    :`drop_duplicate
  ];

  / Big backward jump - assume FH restarted, accept and resync
  .tp.restarts[side]+: 1;
  -1 raze ("TP: "; string side; " FH restart detected - seq from ";
           string lastSeq; " to "; string seq);
  .tp.seq[side]: seq;
  `accept
 };

/ -------------------------------------------------------
/ Update handling (Phase 4 - tpSeqNo stamping + dup suppression)
/ -------------------------------------------------------

.tp.tsToNs:{[ts] .tp.epochOffset+"j"$ts};

upd:{[tbl;data]
  / Health messages bypass sequence checks and durability log entirely.
  / They are best-effort by design (operational visibility, not durable data).
  if[tbl=`health_feed_handler;
    pubsub.publish[tbl;data];
    :();
  ];

  / Sequence check returns one of `accept | `accept_with_gap | `drop_duplicate.
  / On duplicate, we skip log + publish entirely.
  result: $[tbl=`trade_binance;
              .tp.checkSeq[`trade; data .tp.idx.tradeSeq];
            tbl=`quote_binance;
              .tp.checkSeq[`quote; data .tp.idx.quoteSeq];
            `accept];     / unknown tables: just pass through

  if[result = `drop_duplicate; :()];

  / Append TP-side fields: tpRecvTimeUtcNs, tpSeqNo
  / The schema's last two columns are tpRecvTimeUtcNs and tpSeqNo so they
  / must be appended to the row in that exact order.
  .tp.tpSeqNo+: 1;
  data: data, (.tp.tsToNs[.z.p]; .tp.tpSeqNo);

  / Log first (durability), then publish (best-effort fanout).
  / If logging throws, we don't publish (consistent durable view).
  .tp.log[tbl;data];
  pubsub.publish[tbl;data];
 };

/ Alias for feed handlers that call .u.upd
.u.upd:upd;

/ -------------------------------------------------------
/ Phase 4 Public API
/ -------------------------------------------------------

/ Returns the highest fhSeqNo accepted from this FH for the given side.
/ Returns 0 (not 0N) if no messages received yet, so FH can compare
/ against its local fhSeqNo_ unambiguously.
/ Called by FH heartbeat to detect TP-side gaps and trigger resends.
.tp.lastAccepted:{[side]
  s: .tp.seq[side];
  $[null s; 0; s]
 };

/ Current monotonic tpSeqNo. Used by subscribers at reconnect time to
/ capture a clean cutoff between replay (everything <= cutoff) and live
/ (everything > cutoff), so the replay/live boundary is well-defined.
.tp.currentSeqNo:{[] .tp.tpSeqNo}

/ Replay support: read the durability log and return rows for `tbl`
/ with tpSeqNo >= fromSeq. Used by subscribers (WDB) on reconnect to
/ catch up on data missed during disconnect before subscribing live.
/ ----
/ Implementation note: kdb's -11! reads a log file by replaying its
/ entries via a callable named `upd`. We temporarily redefine upd to
/ accumulate into a scratch table, run the replay, then restore. The
/ scratch table is held under .tp.replayScratch to avoid polluting
/ the global namespace.
.tp.replayFrom:{[tbl; fromSeq]
  logFile: .tp.logFilePath[];
  / If no log exists yet, return an empty table of the right shape
  if[() ~ key logFile; :0#value tbl];

  / Initialize scratch + stash target table in globals so the inner upd
  / lambda below can see them (q lambdas do not capture local closures,
  / so we cannot reference outer-scope `tbl` inside the inner function).
  .tp.replayScratch:: 0#value tbl;
  .tp.replayTarget:: tbl;
  oldUpd:: upd;
  / Only accept rows whose width matches the current schema. Older log
  / entries from before Phase 4 (no tpSeqNo column) get silently skipped.
  upd:: {[t;d]
    if[t = .tp.replayTarget;
      if[(count d) = count cols value t;
        .tp.replayScratch,:: enlist d
      ]
    ]
  };

  / Replay - protect so any error restores upd
  .[{-11!x}; enlist logFile; {[err]
    -1 raze ("TP: replayFrom error: "; err);
  }];

  / Restore original upd
  upd:: oldUpd;

  / Filter to requested seq range and return
  result: select from .tp.replayScratch where tpSeqNo >= fromSeq;
  delete replayScratch from `.tp;
  delete replayTarget from `.tp;
  result
 };

/ Recover .tp.tpSeqNo AND per-side .tp.seq.{trade,quote} from the durability
/ log on startup. Reads through the log, tracking the maximum tpSeqNo (global)
/ and the maximum fhSeqNo per side. Seeds the corresponding state so newly
/ accepted messages continue the sequence and gap detection correctly flags
/ messages missed during the TP-down window.
/
/ Critical: must be called BEFORE the log is reopened for new writes,
/ otherwise the new TP-process's first messages would re-use existing seqs.
/
/ Without per-side FH recovery, a TP restart while FH continues silently
/ swallowed gap detection: the first post-restart FH message hit checkSeq's
/ "first message - initialize, accept" branch and the missing messages
/ between TP's last accepted seq and the FH's current seq were never
/ surfaced as gaps. (Fixes review finding #6.)
.tp.recoverSeqNo:{[]
  logFile: .tp.logFilePath[];
  if[() ~ key logFile;
    -1 "TP: no existing log, starting tpSeqNo from 0";
    :()
  ];

  / Track max per-stream sequence numbers during replay.
  .tp.recoverTpMax::    0j;
  .tp.recoverTradeMax:: 0N;
  .tp.recoverQuoteMax:: 0N;

  oldUpd:: upd;
  upd:: {[t;d]
    / Defensive: skip pre-Phase-4 log entries (no tpSeqNo column)
    if[t in `trade_binance`quote_binance;
      if[(count d) = count cols value t;
        / tpSeqNo is the last column of the persisted row
        tpSeq: last d;
        if[tpSeq > .tp.recoverTpMax; .tp.recoverTpMax:: tpSeq];
        / fhSeqNo position is the same in persisted rows as in live rows
        / since TP appends to the end (tpRecvTimeUtcNs, tpSeqNo).
        if[t = `trade_binance;
          fhSeq: d .tp.idx.tradeSeq;
          if[(null .tp.recoverTradeMax) or fhSeq > .tp.recoverTradeMax;
            .tp.recoverTradeMax:: fhSeq
          ]
        ];
        if[t = `quote_binance;
          fhSeq: d .tp.idx.quoteSeq;
          if[(null .tp.recoverQuoteMax) or fhSeq > .tp.recoverQuoteMax;
            .tp.recoverQuoteMax:: fhSeq
          ]
        ]
      ]
    ]
  };

  .[{-11!x}; enlist logFile; {[err]
    -1 raze ("TP: recoverSeqNo error: "; err);
  }];

  upd:: oldUpd;

  / Seed live state from recovered maxes. tpSeqNo continues monotonically;
  / per-side fhSeqs are the floor for the next valid message (anything <= is
  / treated as duplicate or restart per checkSeq's existing logic).
  .tp.tpSeqNo:   .tp.recoverTpMax;
  .tp.seq.trade: .tp.recoverTradeMax;
  .tp.seq.quote: .tp.recoverQuoteMax;

  -1 raze ("TP: recovered tpSeqNo="; string .tp.tpSeqNo;
           " tradeSeq=";              .Q.s1 .tp.seq.trade;
           " quoteSeq=";              .Q.s1 .tp.seq.quote);

  delete recoverTpMax    from `.tp;
  delete recoverTradeMax from `.tp;
  delete recoverQuoteMax from `.tp;
 };

/ -------------------------------------------------------
/ Status and Monitoring
/ -------------------------------------------------------

/ Standardized health check (consistent across all processes)
.health:{[]
  st:$[(.tp.gaps.trade > 0) | .tp.gaps.quote > 0; `degraded; `ok];
  `process`port`uptime`status`memMB`msgsIn`msgsOut!(
    `tp;
    .tp.cfg.port;
    `second$.z.p - .proc.startTime;
    st;
    (`long$.Q.w[][`used]) % 1000000;
    .tp.logCount;
    .tp.logCount)
  }

.tp.status:{[]
  flip `metric`value!(
    `port`uptime`logFile`logChunks`logSizeMB`tradeGaps`tradeMissed`tradeRestarts`tradeDups`lastTradeSeq`quoteGaps`quoteMissed`quoteRestarts`quoteDups`lastQuoteSeq`tpSeqNo;
    (.tp.cfg.port;
     `second$.z.p-.proc.startTime;
     .tp.logFile;
     .tp.logCount;
     0.01*`long$100*(@[hcount;.tp.logFile;0j])%1e6;
     .tp.gaps.trade; .tp.missed.trade; .tp.restarts.trade; .tp.dups.trade; .tp.seq.trade;
     .tp.gaps.quote; .tp.missed.quote; .tp.restarts.quote; .tp.dups.quote; .tp.seq.quote;
     .tp.tpSeqNo))
  };

/ Compact status as dictionary (for programmatic use)
.tp.statusDict:{[]
  `port`uptime`logChunks`tradeGaps`tradeMissed`tradeDups`quoteGaps`quoteMissed`quoteDups`tpSeqNo!
   (.tp.cfg.port;
    `second$.z.p-.proc.startTime;
    .tp.logCount;
    .tp.gaps.trade; .tp.missed.trade; .tp.dups.trade;
    .tp.gaps.quote; .tp.missed.quote; .tp.dups.quote;
    .tp.tpSeqNo)
  };

/ Log status (unchanged for compatibility)
.tp.logStatus:{[]
  ([]file:enlist .tp.logFile;chunks:enlist .tp.logCount;sizeMB:enlist(@[hcount;.tp.logFile;0j])%1e6)
  };

/ -------------------------------------------------------
/ End-of-Day
/ -------------------------------------------------------

.tp.endOfDay:{[]
  -1 raze ("TP: EOD - chunks:"; string .tp.logCount;
           " tradeGaps:"; string .tp.gaps.trade;
           " quoteGaps:"; string .tp.gaps.quote;
           " tpSeqNo:"; string .tp.tpSeqNo);
  pubsub.callendofday[];
  .tp.rotate[];
  delete from `trade_binance;
  delete from `quote_binance;
  delete from `health_feed_handler;
  .tp.logCount:0j;
  / Reset per-day operational counters
  .tp.gaps.trade:0j;
  .tp.missed.trade:0j;
  .tp.restarts.trade:0j;
  .tp.dups.trade:0j;
  .tp.gaps.quote:0j;
  .tp.missed.quote:0j;
  .tp.restarts.quote:0j;
  .tp.dups.quote:0j;
  / Keep last fhSeqNo for each side (FH doesn't reset on EOD).
  / Keep .tp.tpSeqNo monotonic across days so subscribers' cursors remain
  / valid across midnight. Note: replayFrom currently reads only today's
  / log; cross-midnight replay is not supported in v1 (TODO Phase 5).
 };

/ -------------------------------------------------------
/ EOD - Midnight UTC Detection
/ -------------------------------------------------------

.tp.currentDate:.z.d;

.tp.checkEOD:{[]
  if[.z.d > .tp.currentDate;
    -1 "TP: Midnight UTC detected - triggering EOD";
    .tp.endOfDay[];
    .tp.currentDate:.z.d;
  ];
  };

/ Timer - check every 60 seconds for date rollover
.z.ts:{[] .tp.checkEOD[] };



/ -------------------------------------------------------
/ Startup
/ -------------------------------------------------------

system"p ",string .tp.cfg.port;

/ Phase 4: recover tpSeqNo from existing log BEFORE opening for new writes,
/ so newly accepted messages continue the sequence rather than re-using
/ existing seq numbers.
.tp.recoverSeqNo[];

.tp.openLog[];

system "t 1000";   / Check every 1 second

-1"=======================================================";
-1"TP (KDB-X module) starting on port ",string[.tp.cfg.port];
-1"=======================================================";
-1"Tables: trade_binance quote_binance health_feed_handler";
-1"";
-1"Monitoring:";
-1"  .health[]            / Standardized health check";
-1"  .tp.status[]         / Full status table";
-1"  .tp.statusDict[]     / Status as dictionary";
-1"  .tp.logStatus[]      / Log file status";
-1"";
-1"Phase 4 API (acks/replay):";
-1"  .tp.lastAccepted[`trade]   / Highest fhSeqNo accepted for trades";
-1"  .tp.lastAccepted[`quote]   / Highest fhSeqNo accepted for quotes";
-1"  .tp.currentSeqNo[]         / Current monotonic tpSeqNo (replay cutoff)";
-1"  .tp.replayFrom[`trade_binance; fromSeq]   / Replay subset from log";
-1"";
-1"Operations:";
-1"  .tp.endOfDay[]    / Trigger end-of-day";
-1"";
-1"TP ready";
-1"=======================================================";
