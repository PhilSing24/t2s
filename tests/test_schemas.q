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
.t.assertEq["quote has 28 base columns"; 28; count cols .schema.quote];
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
