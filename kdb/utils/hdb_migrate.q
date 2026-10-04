/ hdb_migrate.q - bring stored data up to the current schema, without
/ touching any existing column file.
/ -
/   q kdb/utils/hdb_migrate.q            dry run: list what would be done
/   q kdb/utils/hdb_migrate.q -apply     do it
/ -
/ When a column is added to a live table in kdb/schemas.q, data written
/ before the change lacks it. A partitioned HDB takes a table's column list
/ from its newest partition, so an HDB-wide query on the new column fails on
/ the older partitions, and WDB cannot append to an older tmp.<date> dir.
/ TP and WDB refuse to start in that state (.schema.requireLayout). This
/ tool fixes it. For every HDB date partition and every tmp.<date> dir:
/ -
/   add columns   a live table that lacks schema columns gets each of them
/                 as a new file of typed NULLS (0b for a boolean), one per
/                 row, compressed like WDB writes; its .d file is rewritten
/                 to the schema's column order
/   create table  a live table missing from an HDB partition gets an empty
/                 splay, so HDB-wide queries on that table work for every
/                 date (tmp dirs are left alone: WDB creates what it needs)
/ -
/ It never rewrites, moves or deletes an existing column file: it only
/ creates files that do not exist yet and rewrites the small .d column
/ list. It refuses a table it cannot fix that way (another quote depth,
/ columns the schema does not know) and exits 1 without changing anything.
/ -
/ Environment: T2S_HDB_DIR (default <repo>/hdb), T2S_TMP_DIR (default
/ <repo>/tmp/). Run it with the pipeline stopped.
/ Exit code 0 when everything matches the schema afterwards (or, in a dry
/ run, could be made to), 1 if something cannot be migrated, 2 on usage.

\c 60 300

.mg.dir:first system "dirname ",string .z.f;
system "l ",.mg.dir,"/../schemas.q";

.mg.hdbDir:$[count v:getenv `T2S_HDB_DIR; v; .mg.dir,"/../../hdb"];
.mg.tmpDir:$[count v:getenv `T2S_TMP_DIR; v; .mg.dir,"/../../tmp/"];
.mg.hdb:hsym `$ .mg.hdbDir;
.mg.apply:any .z.x ~\: "-apply";
if[count bad:.z.x where not .z.x ~\: "-apply"; -2 "usage: q kdb/utils/hdb_migrate.q [-apply]"; exit 2];
.z.zd:(17;5;1);   / same compression as wdb.q

.mg.say:{[msg] -1 "MIGRATE: ",msg};
.mg.path:{[dir] 1 _ string dir};

/ -------------------------------------------------------
/ Plan: one row per (root, table) that needs something
/ -------------------------------------------------------
.mg.rowCount:{[dir; have] $[0 = count have; 0j; count get ` sv dir, first have]};

.mg.planOne:{[kind; r; t]
  dir:` sv r, t;
  want:.schema.storedCols t;
  if[() ~ key dir;
    :$[kind = `hdb; enlist `root`table`rows`action`ncols`note!(r; t; 0j; `createEmpty; want; ""); ()]];
  have:.schema.splayCols dir;
  if[have ~ want; :()];
  n:.mg.rowCount[dir; have];
  if[(t in .schema.quoteTables) and not .schema.depth = d:.schema.splayDepth dir;
    :enlist `root`table`rows`action`ncols`note!(r; t; n; `CANNOT; `symbol$(); raze ("quote depth "; string d; ", schema has "; string .schema.depth))];
  extra:have except want; miss:want except have;
  if[count extra;
    :enlist `root`table`rows`action`ncols`note!(r; t; n; `CANNOT; extra; "columns the schema does not know")];
  present:miss where {[dir;c] not () ~ key ` sv dir, c}[dir] each miss;
  if[count present;
    :enlist `root`table`rows`action`ncols`note!(r; t; n; `CANNOT; present; "column file exists but is not listed in .d")];
  enlist `root`table`rows`action`ncols`note!(r; t; n; $[count miss; `addNullCols; `reorderD]; miss; "")};

.mg.plan:{[]
  roots:.schema.dataRoots[.mg.hdbDir; .mg.tmpDir];
  raze {[kind; rs] raze {[kind; r] raze .mg.planOne[kind; r] each key .schema.live}[kind] each rs}'[key roots; value roots]};

/ -------------------------------------------------------
/ Apply
/ -------------------------------------------------------
/ n nulls of the type of column c of table t (0b for boolean; an enumerated
/ null for a symbol column)
.mg.nulls:{[t; c; n]
  proto:(.schema.stored t) c;
  $[11h = type proto; (.Q.en[.mg.hdb] ([] x:n#`)) `x; n#proto]};

.mg.doAdd:{[p]
  dir:` sv p[`root], p `table;
  {[dir; t; n; c]
    f:` sv dir, c;
    if[not () ~ key f; '"refusing to overwrite existing file ", 1 _ string f];
    f set .mg.nulls[t; c; n];
   }[dir; p `table; p `rows] each p `ncols;
  (` sv dir, `.d) set .schema.storedCols p `table;
  };

.mg.doCreate:{[p]
  dir:` sv p[`root], p `table;
  if[not () ~ key dir; '"refusing to overwrite existing table dir ", 1 _ string dir];
  (` sv dir, `) set update `p#sym from .Q.en[.mg.hdb] .schema.stored p `table;
  };

.mg.doReorder:{[p] (` sv p[`root], p[`table], `.d) set .schema.storedCols p `table};

.mg.run:{[p]
  $[p[`action] = `addNullCols; .mg.doAdd p;
    p[`action] = `createEmpty; .mg.doCreate p;
    p[`action] = `reorderD;    .mg.doReorder p;
    '"unexpected action"]};

/ -------------------------------------------------------
/ Entry
/ -------------------------------------------------------
.mg.say raze ($[.mg.apply; "APPLY"; "DRY RUN"]; " - HDB "; .mg.hdbDir; ", tmp "; .mg.tmpDir);
.mg.say raze ("schema: "; ", " sv {[t] raze (string t; " "; string count .schema.storedCols t; " cols")} each key .schema.live;
              "; quote depth "; string .schema.depth);
pl:.mg.plan[];
if[0 = count pl; .mg.say "nothing to do: every stored table matches the schema"; exit 0];

.mg.show:{[pl]
  show select dir:{[r;t] raze (.mg.path r; "/"; string t)}'[root; table], rows, action, columns:{[a;c] $[a = `createEmpty; "(all, no rows)"; count c; " " sv string c; ""]}'[action; ncols], note from pl};
.mg.show pl;
can:pl where not pl[;`action] = `CANNOT;
cannot:pl where pl[;`action] = `CANNOT;
.mg.say raze ("to do: "; string sum can[;`action] = `addNullCols; " table dir(s) get null columns (";
              string count raze (can where can[;`action] = `addNullCols)[;`ncols]; " new column files, ";
              string 0j + sum 0j, (can where can[;`action] = `addNullCols)[;`rows]; " rows covered); ";
              string sum can[;`action] = `createEmpty; " empty table(s) created; ";
              string sum can[;`action] = `reorderD; " .d reordered");
.mg.say "existing column files rewritten, moved or deleted: 0 (by construction)";
if[count cannot;
  .mg.say raze (string count cannot; " table dir(s) CANNOT be migrated (see the note column) - nothing was changed");
  exit 1];
if[not .mg.apply; .mg.say "dry run - nothing written (add -apply)"; exit 0];

res:{[p] @[{[p] .mg.run p; `ok}; p; {[e] `$"ERROR: ",e}]} each can;
if[any not res = `ok;
  .mg.say "ERRORS:";
  {[p;r] if[not r = `ok; .mg.say raze ("  "; .mg.path p `root; "/"; string p `table; " - "; string r)]}'[can; res];
  exit 1];
.mg.say raze ("applied "; string count can; " change(s)");
left:.mg.plan[];
if[count left; .mg.say "after applying, these still differ:"; .mg.show left; exit 1];
.mg.say "re-scan: every stored table now matches the schema";
exit 0
