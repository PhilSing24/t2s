/ check_quote_seq.q - verify, from stored rows alone, that the exchange
/ update ids of each quote table are continuous per symbol.
/ -
/   q kdb/utils/check_quote_seq.q -date D            the HDB partition for D
/   q kdb/utils/check_quote_seq.q -dir PATH          any dir holding the quote
/                                                    splays (e.g. tmp/tmp.<today>)
/ -
/ Every valid quote row stores the range of exchange depth events applied
/ since the previous published row of its symbol (see kdb/schemas.q):
/   exchFirstUpdateId  U of the first event of the range
/   exchUpdateId       u of the last
/   exchPrevUpdateId   futures only: pu of the first event
/ A heartbeat row repeats the previous ids. So, per symbol in tpSeqNo order,
/ each row must continue the row before it:
/   quote_binance      spot ids are consecutive:
/                        exchFirstUpdateId <= previous exchUpdateId + 1
/                        and exchUpdateId >= previous exchUpdateId
/   quote_binance_fut  futures ids are not consecutive; events chain by pu:
/                        exchPrevUpdateId = previous exchUpdateId
/                        (or the row repeats the previous ids: a heartbeat)
/ A row that does not is a BREAK: exchange events are missing between the
/ two rows. Breaks are legitimate when the handler lost the stream and said
/ so; they are classified:
/   marked     an invalid row (isValid=0b) sits between the two rows - the
/              handler published it on a sequence gap, a lost connection or
/              exhausted depth - or fhSeqNo restarted (the handler process
/              was restarted)
/   UNMARKED   nothing in the data explains the break: quotes were lost
/              silently. This must never happen.
/ -
/ Environment: T2S_HDB_DIR (default <repo>/hdb), used for -date and for the
/ sym file. Read-only.
/ Exit code 0 if there is no UNMARKED break, 1 if there is one, 2 on usage.

\c 60 250

.qs.dir:first system "dirname ",string .z.f;
system "l ",.qs.dir,"/../schemas.q";

.qs.hdbDir:$[count v:getenv `T2S_HDB_DIR; v; .qs.dir,"/../../hdb"];
.qs.opt:.Q.opt .z.x;
.qs.arg:{[k] $[k in key .qs.opt; first .qs.opt k; ""]};
if[not any `date`dir in key .qs.opt;
  -2 "usage: q kdb/utils/check_quote_seq.q -date YYYY.MM.DD | -dir PATH"; exit 2];
.qs.root:$[count d:.qs.arg `dir; d; raze (.qs.hdbDir; "/"; .qs.arg `date)];
if[() ~ key hsym `$.qs.root; -2 raze ("QSEQ: no such directory: "; .qs.root); exit 2];
.qs.say:{[msg] -1 "QSEQ: ",msg};

/ The splays are enumerated against the HDB's sym file
sym:@[get; hsym `$ raze (.qs.hdbDir; "/sym"); {[e] `symbol$()}];

/ -------------------------------------------------------
/ One table
/ -------------------------------------------------------
/ Returns `table`rows`valid`invalid`noIds`checked`badRange`breaks, where
/ breaks is a table of the rows that do not continue their predecessor.
.qs.check:{[t]
  dir:hsym `$ raze (.qs.root; "/"; string t);
  empty:`table`rows`valid`invalid`noIds`checked`badRange`breaks!(t; 0Nj; 0j; 0j; 0j; 0j; 0j; ());
  if[() ~ key dir; :empty];
  have:.schema.splayCols dir;
  fut:t = `quote_binance_fut;
  need:`time`sym`isValid`exchFirstUpdateId`exchUpdateId`fhSeqNo`tpSeqNo, $[fut; enlist `exchPrevUpdateId; `symbol$()];
  if[not all need in have;
    .qs.say raze (string t; ": columns missing ("; " " sv string need except have; ") - run kdb/utils/hdb_migrate.q");
    :@[empty; `rows; :; -1j]];
  x:flip need ! {[dir;c] get ` sv dir, c}[dir] each need;
  x:update sym:value sym from x;                 / de-enumerate
  if[not fut; x:update exchPrevUpdateId:0Nj from x];
  x:`sym`tpSeqNo xasc x;
  v:select from x where isValid, not null exchUpdateId;
  v:update pFirst:prev exchFirstUpdateId, pU:prev exchUpdateId, pFh:prev fhSeqNo, pTp:prev tpSeqNo, pTime:prev time by sym from v;
  v:$[fut;
      update cont:(exchPrevUpdateId = pU) or (exchFirstUpdateId = pFirst) and exchUpdateId = pU from v;
      update cont:(exchFirstUpdateId <= pU + 1) and exchUpdateId >= pU from v];
  b:select from v where not null pTp, not cont;
  invRows:select sym, tpSeqNo from x where not isValid;
  b:$[count b;
      update restart:fhSeqNo < pFh,
             invalidBetween:{[ir;s;lo;hi] any (ir[`sym] = s) and (ir[`tpSeqNo] > lo) and ir[`tpSeqNo] < hi}[invRows]'[sym; pTp; tpSeqNo] from b;
      update restart:`boolean$(), invalidBetween:`boolean$() from b];
  b:update kind:?[invalidBetween or restart; `marked; `UNMARKED] from b;
  `table`rows`valid`invalid`noIds`checked`badRange`breaks!(t; count x; sum x `isValid; sum not x `isValid;
     exec count i from x where isValid, null exchUpdateId; count v;
     exec count i from v where exchFirstUpdateId > exchUpdateId; b)};

/ -------------------------------------------------------
/ Report
/ -------------------------------------------------------
.qs.report:{[r]
  t:string r `table;
  if[null r `rows; .qs.say raze (t; ": not present in "; .qs.root); :0];
  if[-1 = r `rows; :0];
  b:r `breaks;
  nUn:$[count b; sum b[`kind] = `UNMARKED; 0];
  .qs.say raze (t; ": "; string r `rows; " rows ("; string r `valid; " valid, "; string r `invalid; " invalid); ";
                string r `checked; " rows with update ids checked; breaks "; string count b;
                " ("; string (count b) - nUn; " marked, "; string nUn; " UNMARKED)");
  if[r[`noIds] > 0;
    .qs.say raze ("  "; string r `noIds; " valid rows have no update ids (stored before the columns existed): not checkable")];
  if[(r[`checked] = 0) and r[`valid] > 0; .qs.say "  NOT CHECKED: no row carries update ids"];
  if[r[`badRange] > 0; .qs.say raze ("  "; string r `badRange; " rows have exchFirstUpdateId > exchUpdateId")];
  if[count b;
    -1 "";
    show select time, sym, kind, why:?[restart; `handlerRestart; ?[invalidBetween; `invalidRow; `$"-"]],
                prevTime:pTime, prevU:pU, firstU:exchFirstUpdateId, u:exchUpdateId, pu:exchPrevUpdateId,
                holeSec:(`long$(time - pTime)) % 1e9 from b;
    -1 ""];
  nUn + r `badRange};

.qs.say raze ("checking "; .qs.root);
res:.qs.check each .schema.quoteTables;
bad:sum .qs.report each res;
.qs.say $[bad > 0; raze ("FAILED - "; string bad; " unmarked break(s) or bad range(s): quotes were lost without a marker");
          0 = sum res[;`checked]; "NOTHING CHECKED - no row carries update ids";
          "OK - no unmarked break"];
exit `long$ bad > 0
