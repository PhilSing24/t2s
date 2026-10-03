/ tp_replay_body.q - q steps for tests/test_tp_replay.sh (replay that seeks)
/ -
/ One step per invocation:  q tests/tp_replay_body.q -step <name> [args] -q
/ -
/ Steps:
/   publish_timed      register -session S and publish -rows N rows of
/                      -table dated -date, timing every synchronous upd;
/                      fails if any single call exceeds -maxms (TP must
/                      stay live while WDB replays)
/   corrupt_log        overwrite 32 bytes with zeros -tailbytes before the
/                      end of the sandbox log for -date (simulated disk
/                      corruption inside the region a replay must read)
/   checkpoint_save    save the WDB checkpoint file contents under -name
/   checkpoint_same    assert the WDB checkpoint equals the saved -name
/   assert_replay      .wdb.replayStatus[]: lastReplayLogs count = -logs,
/                      lastReplaySeekOffset > 8 if -seeked 1 (else = 8),
/                      lastReplaySegments >= -minsegs, replayFailures = -failures
/   assert_index       the sandbox index file for -date has at least -min
/                      entries with strictly increasing tpSeqNo and offsets
/   assert_tp_live     .health[] of TP reachable and status = -value
/ -
/ Exit code 0 on success, 1 on any failed assertion.

\l tests/t_dur_lib.q

.d.sessionArg:{[] s:.d.arg `session; $[0 = count s; 0Nj; "J"$s]};
.d.register:{[h;t;sid]
  nextSeq:1 + .d.peekSeq .d.side t;
  r:h (".tp.registerSession"; t; sid; nextSeq; .d.width t);
  if[not r ~ `ok; .d.fail raze ("registration returned "; .Q.s1 r)]};

if[.d.step ~ "publish_timed";
  t:`$.d.arg `table; n:"J"$.d.arg `rows; d:"D"$.d.arg `date; sid:.d.sessionArg[]; maxMs:"J"$.d.arg `maxms;
  h:.d.open .d.tpPort;
  if[not null sid; .d.register[h;t;sid]];
  base:.d.baseTs d;
  lat:{[h;t;base;i]
    row:.d.mkRow[t; base + i*0D00:00:00.001; i; .d.nextSeq .d.side t];
    t0:.z.p; h (`upd; t; row); `long$(.z.p - t0) % 1000000}[h;t;base] each til n;
  hclose h;
  -1 raze ("  published "; string n; " "; string t; " rows; upd latency max "; string max lat; " ms, median "; string med lat; " ms");
  if[(max lat) > maxMs; .d.fail raze ("TP upd latency "; string max lat; " ms exceeded "; string maxMs; " ms during WDB replay")];
  .d.pass raze ("TP stayed live during replay: "; string n; " upd calls, max "; string max lat; " ms"); .d.done[]];

if[.d.step ~ "corrupt_log";
  d:"D"$.d.arg `date; tail:"J"$.d.arg `tailbytes;
  f:hsym `$ .d.tpLogDir, "/", string[d], ".log";
  n:hcount f;
  off:n - tail;
  if[off < 8; .d.fail "corrupt_log: log too small"];
  / Overwrite in place at a byte offset (q cannot seek-write, so use dd)
  cmd:raze ("dd if=/dev/zero of='"; 1 _ string f; "' bs=1 seek="; string off; " count=32 conv=notrunc status=none");
  system cmd;
  -1 raze ("  zeroed 32 bytes at offset "; string off; " of "; string f; " (size "; string n; ")"); .d.done[]];

.d.cpSaveFile:{[name] hsym `$ .d.sbTmp, "../cp_", name};

if[.d.step ~ "checkpoint_save";
  name:.d.arg `name;
  v:get .d.checkpoint;
  (.d.cpSaveFile name) set v;
  -1 raze ("  checkpoint saved as "; name; ": "; .Q.s1 v `seq); .d.done[]];

if[.d.step ~ "checkpoint_same";
  name:.d.arg `name;
  saved:get .d.cpSaveFile name;
  v:get .d.checkpoint;
  if[not (v `seq) ~ saved `seq; .d.fail raze ("checkpoint changed: "; .Q.s1 v `seq; " vs saved "; .Q.s1 saved `seq)];
  .d.pass raze ("checkpoint unchanged: "; .Q.s1 v `seq); .d.done[]];

if[.d.step ~ "assert_replay";
  nLogs:"J"$.d.arg `logs; seeked:"J"$.d.arg `seeked; minSegs:"J"$.d.arg `minsegs; failures:"J"$.d.arg `failures;
  h:.d.open .d.wdbPort; rs:h ".wdb.replayStatus[]"; hclose h;
  if[(not null nLogs) and nLogs <> count rs `lastReplayLogs; .d.fail raze ("lastReplayLogs = "; .Q.s1 rs `lastReplayLogs; ", expected "; string nLogs; " log(s)")];
  if[not null seeked;
    so:rs `lastReplaySeekOffset;
    if[seeked = 1; if[not so > 8; .d.fail raze ("replay did not seek: seek offset "; string so)]];
    if[seeked = 0; if[not so = 8; .d.fail raze ("replay seeked unexpectedly: offset "; string so)]]];
  if[(not null minSegs) and (rs `lastReplaySegments) < minSegs; .d.fail raze ("lastReplaySegments = "; string rs `lastReplaySegments; ", expected >= "; string minSegs)];
  if[(not null failures) and failures <> rs `replayFailures; .d.fail raze ("replayFailures = "; string rs `replayFailures; ", expected "; string failures)];
  .d.pass raze ("replay: logs "; .Q.s1 rs `lastReplayLogs; ", seek offset "; string rs `lastReplaySeekOffset;
                ", segments "; string rs `lastReplaySegments; ", bytes "; string rs `lastReplayBytes;
                ", rows "; string rs `lastReplayRows; ", "; string rs `lastReplayMs; " ms, failures "; string rs `replayFailures);
  .d.done[]];

if[.d.step ~ "assert_index";
  d:"D"$.d.arg `date; mn:"J"$.d.arg `min;
  f:hsym `$ .d.tpLogDir, "/", string[d], ".idx";
  if[() ~ key f; .d.fail raze ("index file missing: "; string f)];
  idx:get f;
  if[mn > count idx; .d.fail raze ("index has "; string count idx; " entries, expected >= "; string mn)];
  if[not all (1 _ idx `tpSeqNo) > (-1 _ idx `tpSeqNo); .d.fail "index tpSeqNo not strictly increasing"];
  if[not all (1 _ idx `offset) > (-1 _ idx `offset); .d.fail "index offsets not strictly increasing"];
  .d.pass raze ("index "; string f; ": "; string count idx; " entries, first "; .Q.s1 first idx; ", last "; .Q.s1 last idx); .d.done[]];

if[.d.step ~ "assert_tp_live";
  want:.d.arg `value;
  h:.d.open .d.tpPort; hd:h ".health[]"; hclose h;
  got:string hd `status;
  if[not got ~ want; .d.fail raze ("TP status = "; got; ", expected "; want)];
  .d.pass raze ("TP status = "; got; ", diskFreeMB "; string hd `diskFreeMB); .d.done[]];

.d.fail raze ("unknown step: "; .d.step);
