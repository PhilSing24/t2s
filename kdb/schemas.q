/ schemas.q - Single source of truth for tickerplant table schemas
/ Loaded by every process in the pipeline; defines the base schemas plus
/ a small helper to extend a schema with extra receive-time columns.
/ Usage:
/   \l ../schemas.q
/   / Process that just receives the upstream schema as-is:
/   trade_binance:.schema.trade;
/   trade_binance_fut:.schema.aggTrade;
/   quote_binance:.schema.quote;          / layout generated from config/shared.json
/   health_feed_handler:.schema.health;
/   / Process that adds its own receive-time stamp(s):
/   trade_binance:.schema.extend[.schema.trade; enlist `tpRecvTimeUtcNs];
/   trade_binance_fut:.schema.extend[.schema.aggTrade; enlist `tpRecvTimeUtcNs];
/   quote_binance:.schema.extend[.schema.quote; `tpRecvTimeUtcNs`rdbRecvTimeUtcNs];

/ -------------------------------------------------------
/ Shared configuration: config/shared.json
/ -------------------------------------------------------
/ One file read by all four feed handlers and by every q process:
/   symbols      the instruments every handler subscribes to
/   quote_depth  book levels per side published by the quote handlers
/ The depth drives the quote table layout here, the row the handlers build
/ and the width they announce to TP. CHANGING IT CHANGES THE TABLE LAYOUT:
/ see the README section "Symbols and quote depth" before editing it.
/ Location: T2S_SHARED_CONFIG if set, else config/shared.json found by
/ walking up from the main script's directory, then from the current directory.
.schema.cfg.find:{[]
  if[count v:getenv `T2S_SHARED_CONFIG; :v];
  up:{[start] {[d] first system raze ("dirname '"; d; "'")}\[5; start]};
  dirs:$[count string .z.f; up first system raze ("dirname \"$(realpath '"; string .z.f; "')\""); ()];
  dirs:dirs, up first system "pwd";
  hit:dirs where {[d] not () ~ key hsym `$ raze (d; "/config/shared.json")} each dirs;
  $[count hit; raze (first hit; "/config/shared.json"); ""]};
.schema.cfg.die:{[msg] -2 raze ("schemas.q: "; msg); system "sleep 0.1"; exit 1};
.schema.cfg.file:.schema.cfg.find[];
if[0 = count .schema.cfg.file;
  .schema.cfg.die "config/shared.json not found (set T2S_SHARED_CONFIG or run from inside the repo)"];
.schema.cfg.raw:@[{[f] .j.k raze read0 hsym `$f}; .schema.cfg.file;
  {[e] .schema.cfg.die raze ("cannot read "; .schema.cfg.file; ": "; e)}];
if[not all `symbols`quote_depth in key .schema.cfg.raw;
  .schema.cfg.die raze (.schema.cfg.file; " must define symbols and quote_depth")];
.schema.cfg.minDepth:1;
.schema.cfg.maxDepth:50;     / same bounds as cpp/include/config.hpp
.schema.depth:`long$ .schema.cfg.raw `quote_depth;
if[(not .schema.depth = .schema.cfg.raw `quote_depth) or (.schema.depth < .schema.cfg.minDepth) or .schema.depth > .schema.cfg.maxDepth;
  .schema.cfg.die raze (.schema.cfg.file; ": quote_depth "; .Q.s1 .schema.cfg.raw `quote_depth; " must be a whole number from 1 to 50")];
.schema.symbols:`$ upper each .schema.cfg.raw `symbols;
if[0 = count .schema.symbols; .schema.cfg.die raze (.schema.cfg.file; ": symbols is empty")];

/ -------------------------------------------------------
/ Base schemas
/ -------------------------------------------------------

/ Trade feed handler output (12 base columns) — Binance spot @trade stream.
/ Carries the per-symbol tradeId from the exchange for gap detection.
.schema.trade:([]
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
  fhSeqNo:`long$()
  );

/ Futures aggTrade feed handler output (14 base columns) — Binance USDT-M
/ @aggTrade stream. Carries aggTradeId (the per-symbol monotonic id for
/ gap detection) plus firstTradeId / lastTradeId describing the range of
/ underlying fills aggregated into this event. Identical to .schema.trade
/ except for the three id columns at positions 2-4. See ADR-013.
.schema.aggTrade:([]
  time:`timestamp$();
  sym:`symbol$();
  aggTradeId:`long$();
  firstTradeId:`long$();
  lastTradeId:`long$();
  price:`float$();
  qty:`float$();
  buyerIsMaker:`boolean$();
  exchEventTimeMs:`long$();
  exchTradeTimeMs:`long$();
  fhRecvTimeUtcNs:`long$();
  fhParseUs:`long$();
  fhSendUs:`long$();
  fhSeqNo:`long$()
  );

/ Quote feed handler output: the top .schema.depth levels of the book per
/ side, plus flags and timing. The layout is GENERATED from the depth in
/ config/shared.json (8 + 4*depth columns: 28 at depth 5):
/   time, sym, bidPrice1..N, bidQty1..N, askPrice1..N, askQty1..N,
/   isValid, exchEventTimeMs, <extra>, fhRecvTimeUtcNs, fhParseUs, fhSendUs, fhSeqNo
/ The C++ side builds its row from the same depth (cpp/include/quote_row.hpp).
.schema.levelCols:{[n] raze {[n;p] `$p ,/: string 1 + til n}[n] each ("bidPrice"; "bidQty"; "askPrice"; "askQty")};
.schema.mkQuote:{[n; extra]
  c:`time`sym, .schema.levelCols[n], `isValid`exchEventTimeMs, extra, `fhRecvTimeUtcNs`fhParseUs`fhSendUs`fhSeqNo;
  t:"ps", ((4 * n)#"f"), "bj", ((count extra)#"j"), "jjjj";
  flip c ! t $\: ()};
.schema.quote:.schema.mkQuote[.schema.depth; `symbol$()];

/ Tables holding quote rows; their depth is checked against existing data
/ at start-up (see .schema.requireDepth below).
.schema.quoteTables:enlist `quote_binance;

/ Per-process health snapshot (10 columns)
.schema.health:([]
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
/ Helper: extend a base schema with extra long-typed columns
/ -------------------------------------------------------
/ Used to append receive-time stamps. Each downstream process
/ adds its own column so timing through the pipeline is preserved.
/   .schema.extend[.schema.trade; enlist `tpRecvTimeUtcNs]
/   .schema.extend[.schema.trade; `tpRecvTimeUtcNs`rdbRecvTimeUtcNs]

.schema.extend:{[base;extraCols]
  base,'flip extraCols!(count[extraCols])#enlist `long$()
  };

/ -------------------------------------------------------
/ Depth guard: existing data must have the configured depth
/ -------------------------------------------------------
/ A quote table written at one depth cannot be extended at another: the
/ column sets differ, so a partitioned HDB would hold two layouts under one
/ table name and a tmp.<date> dir would get mismatched appends. TP and WDB
/ therefore refuse to start when any existing copy of a quote table (every
/ HDB date partition, every tmp.<date> dir) has a depth other than
/ .schema.depth. Nothing is modified; the operator decides (README).

/ Depth of a splayed quote table = number of bidPrice* columns in its .d
.schema.splayDepth:{[dir] d:@[get; ` sv dir, `.d; {[e] `symbol$()}]; sum (string d) like "bidPrice*"};

/ hdbDir, tmpDir: strings. One line per quote table copy with another depth.
.schema.depthMismatches:{[hdbDir; tmpDir]
  ls:{[d] p:key hsym `$d; $[11h = type p; p; `symbol$()]};
  hp:ls hdbDir; hp:hp where {[n] not null "D"$ string n} each hp;
  tp:ls tmpDir; tp:tp where (string tp) like "tmp.[0-9]*";
  roots:({[d;n] hsym `$ raze (d; "/"; string n)}[hdbDir] each hp), {[d;n] hsym `$ raze (d; "/"; string n)}[tmpDir] each tp;
  raze {[r]
    raze {[r;t]
      dir:` sv r, t;
      if[() ~ key dir; :()];
      n:.schema.splayDepth dir;
      $[n = .schema.depth; (); enlist raze (1 _ string dir; " has depth "; string n)]
     }[r] each .schema.quoteTables
   } each roots};

.schema.requireDepth:{[proc; hdbDir; tmpDir]
  m:.schema.depthMismatches[hdbDir; tmpDir];
  if[0 = count m; :(::)];
  -2 raze (proc; ": REFUSING TO START - quote_depth is "; string .schema.depth; " in "; .schema.cfg.file;
           " but "; string count m; " existing quote table dir(s) have another depth:");
  {[l] -2 raze ("  "; l)} each 10 sublist m;
  if[10 < count m; -2 raze ("  ... and "; string (count m) - 10; " more")];
  -2 "  A different depth is a different table layout. Set quote_depth back, or move";
  -2 "  those partitions / tmp dirs out of the HDB first (README: Symbols and quote depth).";
  system "sleep 0.1";
  exit 1};
