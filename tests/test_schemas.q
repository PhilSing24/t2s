/ test_schemas.q - Verify schemas.q defines the expected tables with
/ the expected columns. Catches accidental schema changes that would
/ break the rest of the pipeline.
/ Run from project root: q tests/test_schemas.q

\l tests/t_lib.q
\l kdb/schemas.q

.t.start "schemas";

/ -------------------------------------------------------
/ Existence
/ -------------------------------------------------------
.t.assert["base trade schema exists"; not () ~ key `.schema.trade];
.t.assert["base quote schema exists"; not () ~ key `.schema.quote];
.t.assert["base health schema exists"; not () ~ key `.schema.health];
.t.assert["extend helper exists"; not () ~ key `.schema.extend];

/ -------------------------------------------------------
/ Column counts
/ -------------------------------------------------------
.t.assertEq["trade has 12 base columns"; 12; count cols .schema.trade];
.t.assertEq["quote depth comes from config/shared.json"; 5; .schema.depth];
.t.assert["shared config was found inside the repo"; .schema.cfg.file like "*/config/shared.json"];
.t.assertEq["symbols come from config/shared.json"; `BTCUSDT`ETHUSDT`SOLUSDT; .schema.symbols];
.t.assertEq["quote has 10 + 4*depth base columns"; 10 + 4 * .schema.depth; count cols .schema.quote];
.t.assertEq["quote stores U and u after exchEventTimeMs";
  `exchEventTimeMs`exchFirstUpdateId`exchUpdateId`fhRecvTimeUtcNs;
  (cols .schema.quote) (til 4) + (cols .schema.quote)?`exchEventTimeMs];
.t.assertEq["futures quote stores T, U, u, pu after exchEventTimeMs";
  `exchEventTimeMs`exchTransactTimeMs`exchFirstUpdateId`exchUpdateId`exchPrevUpdateId`fhRecvTimeUtcNs;
  (cols .schema.quoteFut) (til 6) + (cols .schema.quoteFut)?`exchEventTimeMs];
.t.assertEq["quote at depth 5 keeps the layout it had before depth was configurable";
  `time`sym`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5`bidQty1`bidQty2`bidQty3`bidQty4`bidQty5`askPrice1`askPrice2`askPrice3`askPrice4`askPrice5`askQty1`askQty2`askQty3`askQty4`askQty5`isValid`exchEventTimeMs`fhRecvTimeUtcNs`fhParseUs`fhSendUs`fhSeqNo;
  cols .schema.mkQuote[5; `symbol$()]];
.t.assertEq["futures quote = quote + exchTransactTimeMs and exchPrevUpdateId";
  (cols .schema.quote) ~ (cols .schema.quoteFut) except `exchTransactTimeMs`exchPrevUpdateId; 1b];
.t.assertEq["futures quote has 12 + 4*depth base columns"; 12 + 4 * .schema.depth; count cols .schema.quoteFut];
.t.assertEq["exchTransactTimeMs follows exchEventTimeMs";
  1 + (cols .schema.quoteFut)?`exchEventTimeMs; (cols .schema.quoteFut)?`exchTransactTimeMs];
.t.assertEq["both quote tables are depth-guarded"; `quote_binance`quote_binance_fut; .schema.quoteTables];
.t.assertEq["quote at depth 3 has 20 columns"; 20; count cols .schema.mkQuote[3; `symbol$()]];
.t.assertEq["quote at depth 3 column order";
  `time`sym`bidPrice1`bidPrice2`bidPrice3`bidQty1`bidQty2`bidQty3`askPrice1`askPrice2`askPrice3`askQty1`askQty2`askQty3`isValid`exchEventTimeMs`fhRecvTimeUtcNs`fhParseUs`fhSendUs`fhSeqNo;
  cols .schema.mkQuote[3; `symbol$()]];
.t.assertEq["quote column types"; "psffffbjjjjj"; exec t from meta .schema.mkQuote[1; `symbol$()]];
.t.assertEq["an extra long column goes after exchEventTimeMs";
  `isValid`exchEventTimeMs`exchTransactTimeMs`fhRecvTimeUtcNs;
  (cols .schema.mkQuote[2; enlist `exchTransactTimeMs]) 10 11 12 13];
.t.assertEq["health has 10 columns"; 10; count cols .schema.health];

/ -------------------------------------------------------
/ Specific columns the rest of the pipeline depends on
/ -------------------------------------------------------
tradeRequiredCols:`time`sym`tradeId`price`qty`buyerIsMaker`fhSeqNo`fhParseUs`fhSendUs;
quoteRequiredCols:`time`sym`bidPrice1`bidQty1`askPrice1`askQty1`bidQty5`askQty5`isValid`fhSeqNo`fhParseUs`fhSendUs;
.t.assert["trade has all required columns";
  all tradeRequiredCols in cols .schema.trade];
.t.assert["quote has all required columns";
  all quoteRequiredCols in cols .schema.quote];

/ -------------------------------------------------------
/ Type sanity (catches a class of typos)
/ -------------------------------------------------------
.t.assertEq["trade.sym is symbol"; "s"; .Q.t abs type .schema.trade `sym];
.t.assertEq["trade.price is float"; "f"; .Q.t abs type .schema.trade `price];
.t.assertEq["trade.fhSeqNo is long"; "j"; .Q.t abs type .schema.trade `fhSeqNo];
.t.assertEq["quote.bidPrice1 is float"; "f"; .Q.t abs type .schema.quote `bidPrice1];
.t.assertEq["quote.isValid is boolean"; "b"; .Q.t abs type .schema.quote `isValid];

/ -------------------------------------------------------
/ Extend helper round-trip
/ -------------------------------------------------------
ext1:.schema.extend[.schema.trade; enlist `tpRecvTimeUtcNs];
.t.assertEq["extend with one col adds one column";
  1 + count cols .schema.trade;
  count cols ext1];
.t.assert["extend with one col preserves all base columns";
  all (cols .schema.trade) in cols ext1];
.t.assertEq["extended col has long type"; "j"; .Q.t abs type ext1 `tpRecvTimeUtcNs];

ext2:.schema.extend[.schema.trade; `tpRecvTimeUtcNs`rdbRecvTimeUtcNs];
.t.assertEq["extend with two cols adds two columns";
  2 + count cols .schema.trade;
  count cols ext2];

/ -------------------------------------------------------
/ Index derivation (the "no magic numbers" guarantee)
/ -------------------------------------------------------
.t.assertEq["fhSeqNo is at expected position in extended trade";
  11; (cols ext1)?`fhSeqNo];
.t.assertEq["sym is at position 1"; 1; (cols .schema.trade)?`sym];
.t.assertEq["price is at position 3"; 3; (cols .schema.trade)?`price];
.t.assertEq["fhParseUs is at position 9"; 9; (cols .schema.trade)?`fhParseUs];

.t.finish[];
