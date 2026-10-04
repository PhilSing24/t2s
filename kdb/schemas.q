/ schemas.q - Single source of truth for tickerplant table schemas
/ Loaded by every process in the pipeline; defines the base schemas plus
/ a small helper to extend a schema with extra receive-time columns.
/ Usage:
/   \l ../schemas.q
/   / Process that just receives the upstream schema as-is:
/   trade_binance:.schema.trade;
/   trade_binance_fut:.schema.aggTrade;
/   quote_binance:.schema.quote;          / layout generated from config/shared.json
/   quote_binance_fut:.schema.quoteFut;
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

/ Futures aggTrade feed handler output (15 base columns) — Binance USDT-M
/ @aggTrade stream. Carries aggTradeId (the per-symbol monotonic id for
/ gap detection) plus firstTradeId / lastTradeId describing the range of
/ underlying fills aggregated into this event. Identical to .schema.trade
/ except for the three id columns at positions 2-4 and qtyExRpi. See ADR-013.
/ qty is the event's `q` ("quantity with all the market trades"); qtyExRpi
/ is its `nq` ("normal quantity without the trades involving RPI orders",
/ RPI = Retail Price Improvement), in the stream since 2025-12-31:
/   https://developers.binance.com/docs/derivatives/change-log
/   https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
/ qty - qtyExRpi is the part of the aggregate that traded against RPI orders.
/ Null if an event arrived without the field, and in rows stored before it
/ was added.
.schema.aggTrade:([]
  time:`timestamp$();
  sym:`symbol$();
  aggTradeId:`long$();
  firstTradeId:`long$();
  lastTradeId:`long$();
  price:`float$();
  qty:`float$();
  qtyExRpi:`float$();
  buyerIsMaker:`boolean$();
  exchEventTimeMs:`long$();
  exchTradeTimeMs:`long$();
  fhRecvTimeUtcNs:`long$();
  fhParseUs:`long$();
  fhSendUs:`long$();
  fhSeqNo:`long$()
  );

/ Quote feed handler output: the top .schema.depth levels of the book per
/ side, plus flags, exchange update ids and timing. The layout is GENERATED
/ from the depth in config/shared.json (10 + 4*depth columns: 30 at depth 5):
/   time, sym, bidPrice1..N, bidQty1..N, askPrice1..N, askQty1..N,
/   isValid, exchEventTimeMs, <extra>, fhRecvTimeUtcNs, fhParseUs, fhSendUs, fhSeqNo
/ Update ids (null on an invalid row). A row covers the exchange events
/ applied since the previous published row of the same symbol:
/   exchFirstUpdateId  U of the first of those events
/   exchUpdateId       u of the last: the update id the book is at. A REST
/                      depth snapshot with lastUpdateId = exchUpdateId shows
/                      exactly this row's levels.
/ A heartbeat row repeats the ids of the row before it. With these, sequence
/ continuity can be verified from the stored rows alone
/ (kdb/utils/check_quote_seq.q).
/ The C++ side builds its row from the same depth (cpp/include/quote_row.hpp).
.schema.levelCols:{[n] raze {[n;p] `$p ,/: string 1 + til n}[n] each ("bidPrice"; "bidQty"; "askPrice"; "askQty")};
.schema.mkQuote:{[n; extra]
  c:`time`sym, .schema.levelCols[n], `isValid`exchEventTimeMs, extra, `fhRecvTimeUtcNs`fhParseUs`fhSendUs`fhSeqNo;
  t:"ps", ((4 * n)#"f"), "bj", ((count extra)#"j"), "jjjj";
  flip c ! t $\: ()};
.schema.quote:.schema.mkQuote[.schema.depth; `exchFirstUpdateId`exchUpdateId];

/ USD-M futures quote handler output: the same layout plus two fields only
/ the futures depth event has (12 + 4*depth columns: 32 at depth 5):
/   exchTransactTimeMs  `T`, transaction time, right after exchEventTimeMs (`E`)
/   exchPrevUpdateId    `pu` of the first event of the row's range, i.e. the
/                       `u` of the event before it. Futures ids are not
/                       consecutive, so this is what chains one row to the
/                       previous row's exchUpdateId.
.schema.quoteFut:.schema.mkQuote[.schema.depth; `exchTransactTimeMs`exchFirstUpdateId`exchUpdateId`exchPrevUpdateId];

/ Tables holding quote rows; their depth is checked against existing data
/ at start-up (see .schema.requireDepth below).
.schema.quoteTables:`quote_binance`quote_binance_fut;

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
/ Stored layout of the live tables
/ -------------------------------------------------------
/ What WDB writes for each table: the feed-handler columns, TP's two
/ stamps, then WDB's own.
.schema.live:`trade_binance`trade_binance_fut`quote_binance`quote_binance_fut ! (.schema.trade; .schema.aggTrade; .schema.quote; .schema.quoteFut);
.schema.stamps:`tpRecvTimeUtcNs`tpSeqNo`wdbRecvTimeUtcNs;
.schema.stored:{[t] .schema.extend[.schema.live t; .schema.stamps]};
.schema.storedCols:{[t] cols .schema.stored t};

/ -------------------------------------------------------
/ Layout guard: existing data must match the schema
/ -------------------------------------------------------
/ A table written with one column list cannot be extended with another:
/ a partitioned HDB would hold two layouts under one name, and WDB's
/ appends to a tmp.<date> dir would not line up. TP and WDB therefore
/ refuse to start when any existing copy of a live table (every HDB date
/ partition, every tmp.<date> dir) differs from the schema:
/   - another quote depth: nothing can fix that in place (README, "Symbols
/     and quote depth")
/   - columns added to the schema since the data was written:
/     kdb/utils/hdb_migrate.q adds them as nulls
/ Nothing is modified here; the operator decides.

/ Column list of a splayed table dir (its .d file), or empty
.schema.splayCols:{[dir] @[get; ` sv dir, `.d; {[e] `symbol$()}]};
/ Depth of a splayed quote table = number of bidPrice* columns
.schema.splayDepth:{[dir] sum (string .schema.splayCols dir) like "bidPrice*"};

/ Date partitions under hdbDir and tmp.<date> dirs under tmpDir, as hsyms
.schema.dataRoots:{[hdbDir; tmpDir]
  ls:{[d] p:key hsym `$d; $[11h = type p; p; `symbol$()]};
  hp:ls hdbDir; hp:hp where {[n] not null "D"$ string n} each hp;
  tp:ls tmpDir; tp:tp where (string tp) like "tmp.[0-9]*";
  noSlash:{[d] $[(1 < count d) and "/" = last d; -1 _ d; d]};
  `hdb`tmp ! ({[d;n] hsym `$ raze (d; "/"; string n)}[noSlash hdbDir] each hp; {[d;n] hsym `$ raze (d; "/"; string n)}[noSlash tmpDir] each tp)};

/ One dictionary per existing copy of a live table that differs from the
/ schema: dir, table, kind (`depth or `columns), detail.
.schema.layoutMismatches:{[hdbDir; tmpDir]
  roots:raze value .schema.dataRoots[hdbDir; tmpDir];
  raze {[r]
    raze {[r;t]
      dir:` sv r, t;
      if[() ~ key dir; :()];
      have:.schema.splayCols dir; want:.schema.storedCols t;
      if[have ~ want; :()];
      if[(t in .schema.quoteTables) and not .schema.depth = n:.schema.splayDepth dir;
        :enlist `dir`table`kind`detail!(dir; t; `depth; raze (1 _ string dir; " has depth "; string n))];
      miss:want except have; extra:have except want;
      enlist `dir`table`kind`detail!(dir; t; `columns;
        raze (1 _ string dir; ": ";
              $[count miss; raze ("lacks "; " " sv string miss); ""];
              $[(count miss) and count extra; "; "; ""];
              $[count extra; raze ("has unknown "; " " sv string extra); ""];
              $[(0 = count miss) and 0 = count extra; "same columns in another order"; ""]))
     }[r] each key .schema.live
   } each roots};

.schema.requireLayout:{[proc; hdbDir; tmpDir]
  m:.schema.layoutMismatches[hdbDir; tmpDir];
  if[0 = count m; :(::)];
  show10:{[l] {[x] -2 raze ("  "; x)} each 10 sublist l; if[10 < count l; -2 raze ("  ... and "; string (count l) - 10; " more")]};
  dm:m[;`detail] where m[;`kind] = `depth;
  cm:m[;`detail] where m[;`kind] = `columns;
  if[count dm;
    -2 raze (proc; ": REFUSING TO START - quote_depth is "; string .schema.depth; " in "; .schema.cfg.file;
             " but "; string count dm; " existing quote table dir(s) have another depth:");
    show10 dm;
    -2 "  A different depth is a different table layout. Set quote_depth back, or move";
    -2 "  those partitions / tmp dirs out of the HDB first (README: Symbols and quote depth)."];
  if[count cm;
    -2 raze (proc; ": REFUSING TO START - "; string count cm; " existing table dir(s) do not have the schema's columns:");
    show10 cm;
    -2 "  Columns added to the schema must be added to the stored data first:";
    -2 "    q kdb/utils/hdb_migrate.q          (dry run: shows what would be done)";
    -2 "    q kdb/utils/hdb_migrate.q -apply   (adds the columns as nulls; existing column files are not touched)"];
  system "sleep 0.1";
  exit 1};
