/ rebuild_check.q - shape check of a rebuilt splay (used by tests/test_rebuild_day.sh)
/ REBUILT_TABLE=<dir of the table splay>. Exit 0 if the rows are marked as
/ rebuilt (null wdbRecvTimeUtcNs), the column order matches WDB's and sym is
/ parted; 1 otherwise.
p:hsym `$ getenv `REBUILT_TABLE;
c:get ` sv p,`.d;
w:get ` sv p,`wdbRecvTimeUtcNs;
s:attr get ` sv p,`sym;
-1 "columns: ",.Q.s1 c;
-1 "wdbRecvTimeUtcNs all null: ",string all null w;
-1 "sym attr: ",.Q.s1 s;
ok:(all null w) and (`wdbRecvTimeUtcNs = last c) and s = `p;
system "sleep 0.05";
exit $[ok; 0; 1]
