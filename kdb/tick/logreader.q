/ logreader.q - seeking reader for TP durability logs. Shared by wdb.q
/ (replay) and kdb/utils/logmgr.q (retention). Loaded with \l from kdb/tick.
/ -
/ Log format (what -11! reads): an 8-byte file header (0xff01 + 6 zero
/ bytes) followed by serialized messages back to back, with NO per-chunk
/ length prefix. Seeking therefore relies on the index TP writes next to
/ each log (<date>.idx): a table of (tpSeqNo; offset; chunk) recorded every
/ .tp.cfg.indexEvery rows, where offset is the byte position of the chunk
/ carrying that tpSeqNo. A byte range cut at two such offsets, prefixed with
/ the file header, is a valid log file in its own right, so a segment is
/ replayed by copying that range to a temp file and running the native -11!
/ on it. -11!(-2;f) validates a file first: it returns a plain chunk count
/ when every byte is good and (goodChunks; goodBytes) when it is not.

.lr.header:0xff01000000000000;
.lr.emptyIdx:([] tpSeqNo:`long$(); offset:`long$(); chunk:`long$());

/ `:dir/2026.10.03.log -> `:dir/2026.10.03.idx
.lr.idxPath:{[logFile] hsym `$ (-4 _ 1 _ string logFile),".idx"};

.lr.loadIdx:{[logFile]
  f:.lr.idxPath logFile;
  if[() ~ key f; :.lr.emptyIdx];
  r:@[get; f; {[e] `error}];
  $[(not r ~ `error) and 98h = type r; r; .lr.emptyIdx]};

/ Last index entry at or below fromSeq: (offset; chunk), or (0N; 0N) when
/ the index has nothing at or below it (then the log must be read from the
/ start).
.lr.seek:{[idx; fromSeq]
  c:select from idx where tpSeqNo <= fromSeq;
  $[count c; (last c `offset; last c `chunk); (0Nj; 0Nj)]};

/ Dates of the daily logs in a dir, oldest first.
.lr.listLogs:{[dir]
  files:@[key; hsym `$ dir; {[e] `symbol$()}];
  if[0 = count files; :`date$()];
  names:string files;
  logs:names where names like "????.??.??.log";
  dates:"D"$ 10#' logs;
  asc dates where not null dates};

.lr.logPath:{[dir; d] hsym `$ dir,"/",string[d],".log"};

/ Validate a log-format file: returns (goodChunks; goodBytes).
.lr.validate:{[f]
  r:-11!(-2; f);
  $[-7h = type r; (r; hcount f); r]};

/ Replay a log-format file through fn[tbl; row] for every chunk, restoring
/ the global upd afterwards even on error. Returns the chunk count.
.lr.replayWith:{[f; fn]
  .lr.fn::fn;
  old:upd;
  upd::{[t; d] .lr.fn[t; d]};
  r:.[{-11! x}; enlist f; {[e] (`error; e)}];
  upd::old;
  if[(0h = type r) and (first r) ~ `error; '"logreader: replay failed: ", last r];
  r};

/ Replay the byte range [startOffset, endOffset) of logFile, which must be
/ cut at chunk boundaries (index offsets, or the file end), through fn. The
/ range is copied to tmpFile with the file header and validated before it
/ is replayed. Throws on any corruption or misalignment; returns the chunk
/ count.
.lr.replaySegment:{[logFile; startOffset; endOffset; tmpFile; fn]
  n:endOffset - startOffset;
  if[n < 0; '"logreader: negative segment"];
  if[n = 0; :0j];
  bytes:read1 (logFile; startOffset; n);
  if[n <> count bytes; '"logreader: short read: wanted ", string[n], " bytes at ", string[startOffset], ", got ", string count bytes];
  tmpFile 1: .lr.header, bytes;
  v:.lr.validate tmpFile;
  if[(v 1) <> 8 + n;
    '"logreader: corrupt segment in ", string[logFile], " at offset ", string[startOffset + (v 1) - 8],
     " (", string[v 0], " good chunks, ", string[8 + n - v 1], " bad bytes)"];
  .lr.replayWith[tmpFile; fn]};

/ tpSeqNo of the first row in a log (any table), or 0N if the log is empty.
/ Reads only a bounded prefix.
.lr.firstSeq:{[logFile; tmpFile]
  n:hcount logFile;
  if[n <= 8; :0Nj];
  pre:read1 (logFile; 0; n & 1048576);
  tmpFile 1: pre;
  v:.lr.validate tmpFile;
  if[(v 1) < count pre; tmpFile 1: (v 1)#pre];
  .lr.first::0Nj;
  .lr.replayWith[tmpFile; {[t; d] if[null .lr.first; .lr.first::last d]}];
  .lr.first};

/ Replay logFile from fromSeq up to endOffset (the committed length), using
/ the index to seek. fn[tbl; row] receives every row from the seek point
/ on, including rows below fromSeq that share its segment: the caller
/ filters. afterSegment[] (may be ::) is called after each segment, which
/ a test hook uses to slow the walk down. Returns a dict of counters.
.lr.replayFrom:{[logFile; fromSeq; endOffset; tmpFile; fn; afterSegment]
  idx:.lr.loadIdx logFile;
  sk:.lr.seek[idx; fromSeq];
  startOffset:$[null sk 0; 8; sk 0];
  bounds:asc distinct (exec offset from idx where offset > startOffset, offset < endOffset), endOffset;
  starts:startOffset, -1 _ bounds;
  chunks:0j; segs:0j;
  {[logFile; tmpFile; fn; afterSegment; s; e]
    .lr.segChunks::.lr.replaySegment[logFile; s; e; tmpFile; fn];
    if[not (::) ~ afterSegment; afterSegment[]];
   }[logFile; tmpFile; fn; afterSegment]'[starts; bounds];
  `seekOffset`seekChunk`endOffset`segments`bytes!(startOffset; sk 1; endOffset; count bounds; endOffset - startOffset)};
