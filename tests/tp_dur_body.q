/ tp_dur_body.q - q steps for tests/test_tp_durability.sh
/ -
/ One step per invocation, like tests/wdb_dur_body.q:
/   q tests/tp_dur_body.q -step <name> [args] -q
/ -
/ Steps:
/   publish            register session -session S on a fresh handle (unless
/                      -session is omitted), then publish -rows N rows of
/                      -table dated -date. fhSeqNo continues per table.
/   publish_seq        register -session S, then publish ONE -table row with
/                      fhSeqNo = -seq (counter untouched): an out-of-order
/                      resend
/   fhseq_reset        reset the fhSeqNo counter for -table (a new handler)
/   register_bad       register -table with -width; must be REJECTED with a
/                      "width mismatch" error
/   publish_bad_width  send one -table row with one column too many; must be
/                      rejected (not logged) and counted
/   tp_status          .tp.statusDict[][-key] (then .health[]) formatted with
/                      string equals -value
/   tp_seq_save        save .tp.currentSeqNo[] under -name in the sandbox
/   tp_seq_assert_gt   assert .tp.currentSeqNo[] > the value saved under -name
/   tp_seq_assert_ge   assert .tp.currentSeqNo[] >= the value saved under -name
/                      (after a migration the counter equals the last number
/                      handed out; the next one is still fresh)
/   assert_log_monotone  tpSeqNo over ALL sandbox TP logs (log order) is
/                      strictly increasing, hence unique
/   assert_log_rows    -table has -rows rows across all sandbox TP logs
/   assert_fh_sessions fhSeqNo for -table across the logs restarts from 1
/                      exactly -restarts times and has no gaps inside a run
/ -
/ Exit code 0 on success, 1 on any failed assertion.

\l tests/t_dur_lib.q

.d.sessionArg:{[] s:.d.arg `session; $[0 = count s; 0Nj; "J"$s]};

/ Register a session for table t on handle h announcing the next fhSeqNo.
.d.register:{[h;t;sid]
  nextSeq:1 + .d.peekSeq .d.side t;
  r:h (".tp.registerSession"; t; sid; nextSeq; .d.width t);
  if[not r ~ `ok; .d.fail raze ("registration returned "; .Q.s1 r)];
  -1 raze ("  registered session "; string sid; " for "; string t; " (next fhSeqNo "; string nextSeq; ")")};

if[.d.step ~ "publish";
  t:`$.d.arg `table; n:"J"$.d.arg `rows; d:"D"$.d.arg `date; sid:.d.sessionArg[];
  h:.d.open .d.tpPort;
  if[not null sid; .d.register[h;t;sid]];
  .d.publishRows[h;t;n;d];
  hclose h;
  -1 raze ("  published "; string n; " "; string t; " rows dated "; string d; $[null sid; " (unregistered)"; ""]);
  .d.done[]];

if[.d.step ~ "publish_seq";
  t:`$.d.arg `table; seq:"J"$.d.arg `seq; d:"D"$.d.arg `date; sid:.d.sessionArg[];
  h:.d.open .d.tpPort;
  if[not null sid; .d.register[h;t;sid]];
  h (`upd; t; .d.mkRow[t; .d.baseTs d; 0; seq]);
  hclose h;
  -1 raze ("  published one "; string t; " row with fhSeqNo "; string seq); .d.done[]];

if[.d.step ~ "fhseq_reset";
  t:`$.d.arg `table; .d.resetSeq .d.side t;
  -1 raze ("  fhSeqNo counter reset for "; string t); .d.done[]];

if[.d.step ~ "register_bad";
  t:`$.d.arg `table; w:"J"$.d.arg `width;
  h:.d.open .d.tpPort;
  r:@[h; (".tp.registerSession"; t; 999j; 1j; w); {[e] "ERR:",e}];
  hclose h;
  if[not 10h = type r; .d.fail raze ("registration with width "; string w; " was ACCEPTED: "; .Q.s1 r)];
  if[not (r like "ERR:*") and 0 < count ss[r; "width mismatch"]; .d.fail raze ("unexpected rejection message: "; r)];
  .d.pass raze ("registration with width "; string w; " rejected: "; 4 _ r); .d.done[]];

if[.d.step ~ "publish_bad_width";
  t:`$.d.arg `table; d:"D"$.d.arg `date;
  h:.d.open .d.tpPort;
  row:.d.mkRow[t; .d.baseTs d; 0; 0j], 0j;   / one column too many
  h (`upd; t; row);
  hclose h;
  -1 raze ("  sent one "; string t; " row with "; string count row; " columns"); .d.done[]];

if[.d.step ~ "tp_status";
  k:`$.d.arg `key; want:.d.arg `value;
  h:.d.open .d.tpPort;
  sd:h ".tp.statusDict[]"; hd:h ".health[]"; hclose h;
  v:$[k in key sd; sd k; k in key hd; hd k; .d.fail raze ("unknown TP status key "; string k)];
  got:.d.fmt v;
  if[not got ~ want; .d.fail raze ("TP "; string k; " = "; got; ", expected "; want)];
  .d.pass raze ("TP "; string k; " = "; got); .d.done[]];

.d.seqSaveFile:{[name] hsym `$ .d.sbTmp, "../tpseq_", name};

if[.d.step ~ "tp_seq_save";
  name:.d.arg `name;
  h:.d.open .d.tpPort; v:h ".tp.currentSeqNo[]"; hclose h;
  (.d.seqSaveFile name) set v;
  -1 raze ("  tpSeqNo now "; string v; " (saved as "; name; ")"); .d.done[]];

if[.d.step ~ "tp_seq_assert_gt";
  name:.d.arg `name;
  saved:get .d.seqSaveFile name;
  h:.d.open .d.tpPort; v:h ".tp.currentSeqNo[]"; hclose h;
  if[not v > saved; .d.fail raze ("tpSeqNo "; string v; " is not above "; name; " = "; string saved)];
  .d.pass raze ("tpSeqNo "; string v; " > "; name; " = "; string saved); .d.done[]];

if[.d.step ~ "tp_seq_assert_ge";
  name:.d.arg `name;
  saved:get .d.seqSaveFile name;
  h:.d.open .d.tpPort; v:h ".tp.currentSeqNo[]"; hclose h;
  if[not v >= saved; .d.fail raze ("tpSeqNo "; string v; " is below "; name; " = "; string saved)];
  .d.pass raze ("tpSeqNo "; string v; " >= "; name; " = "; string saved); .d.done[]];

if[.d.step ~ "assert_log_monotone";
  s:.d.tpLogAllSeqs[];
  if[0 = count s; .d.fail "no rows in any TP log"];
  bad:where not (1 _ s) > (-1 _ s);
  if[count bad; .d.fail raze ("tpSeqNo not strictly increasing across the TP logs at "; string count bad; " place(s), e.g. "; .Q.s1 s (first bad), 1 + first bad)];
  if[count[s] <> count distinct s; .d.fail "duplicate tpSeqNo in the TP logs"];
  .d.pass raze ("tpSeqNo strictly increasing and unique over "; string count s; " logged rows ("; string count .d.tpLogFiles[]; " log file(s)), range "; .Q.s1 (min s; max s));
  .d.done[]];

if[.d.step ~ "assert_log_rows";
  t:`$.d.arg `table; n:"J"$.d.arg `rows;
  got:count .d.tpLogSeqs t;
  if[n <> got; .d.fail raze (string t; ": "; string got; " rows in the TP logs, expected "; string n)];
  .d.pass raze (string t; ": "; string got; " rows in the TP logs"); .d.done[]];

if[.d.step ~ "assert_fh_sessions";
  t:`$.d.arg `table; want:"J"$.d.arg `restarts;
  fh:last .d.tpLogRows t;
  if[0 = count fh; .d.fail raze ("no "; string t; " rows in the TP logs")];
  restarts:count where (1 _ fh) < (-1 _ fh);
  gaps:count where (1 _ fh) > 1 + (-1 _ fh);
  if[restarts <> want; .d.fail raze (string t; ": "; string restarts; " fhSeqNo restart(s) in the logs, expected "; string want)];
  if[gaps > 0; .d.fail raze (string t; ": "; string gaps; " fhSeqNo gap(s) inside a run")];
  .d.pass raze (string t; ": "; string restarts; " fhSeqNo restart(s), no gaps inside runs, "; string count fh; " rows"); .d.done[]];

.d.fail raze ("unknown step: "; .d.step);
