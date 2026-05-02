/ schemas.q - Single source of truth for tickerplant table schemas
/ Loaded by every process in the pipeline; defines the base schemas plus
/ a small helper to extend a schema with extra receive-time columns.
/ Usage:
/   \l ../schemas.q
/   / Process that just receives the upstream schema as-is:
/   trade_binance:.schema.trade;
/   quote_binance:.schema.quote;
/   health_feed_handler:.schema.health;
/   / Process that adds its own receive-time stamp(s):
/   trade_binance:.schema.extend[.schema.trade; enlist `tpRecvTimeUtcNs];
/   quote_binance:.schema.extend[.schema.quote; `tpRecvTimeUtcNs`rdbRecvTimeUtcNs];

/ -------------------------------------------------------
/ Base schemas
/ -------------------------------------------------------

/ Trade feed handler output (12 base columns)
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

/ Quote feed handler output (28 base columns: L5 book + flags + timing)
.schema.quote:([]
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
  fhSeqNo:`long$()
  );

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
