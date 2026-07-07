# n1k1 — a five-minute demo

You have a pile of JSON files on your drive — plus some CSV, YAML, and a
couple of gzipped logs. How do you slice and dice them? `jq` incantations?
Spin up DuckDB? Load a database?

**Meet `n1k1`:** a single pure-Go binary. Point it at a directory and query
your files with SQL++ (N1QL) — à la `sqlite` / `duckdb`, but for the JSON (and
CSV/YAML/gzip/Parquet/…) already sitting on disk.

Every command below is copy-pasteable and was run against the sample data in
[`examples/`](examples/). The output shown is real.

```sh
make cli        # builds ./n1k1  (needs GOPRIVATE='github.com/couchbase/*')
```

---

## 1. Point it at a directory

A datastore is just a directory tree: `<dir>/<namespace>/<keyspace>/…`. The
namespace defaults to `default`, so `FROM orders` reads
`examples/shop/default/orders/` — one JSON doc per file here.

`.tables` lists the keyspaces, each with a copy-paste starter query:

```sh
$ echo ".tables" | ./n1k1 examples/shop
2 keyspaces — copy/paste to try:
  customers   SELECT COUNT(*) FROM customers;
  orders      SELECT * FROM orders LIMIT 3;
```

`.schema` samples a keyspace's shape — field names, JSON types, and a
ready-made `WHERE` example per column:

```sh
$ echo ".schema orders" | ./n1k1 examples/shop
orders  (sampled 20 docs):
┌──────────┬────────┬──────────┬──────────────────────────────────────────────┐
│ field    │ types  │ distinct │ example                                      │
├──────────┼────────┼──────────┼──────────────────────────────────────────────┤
│ customer │ string │        4 │ SELECT * FROM orders WHERE customer IN [...]; │
│ status   │ string │        3 │ SELECT * FROM orders WHERE status IN [...];   │
│ total    │ number │      16+ │ SELECT * FROM orders WHERE total = 129.5;    │
│ ...      │        │          │                                              │
└──────────┴────────┴──────────┴──────────────────────────────────────────────┘
```

Your first `SELECT`. At an interactive terminal, output is a `box` table;
piped or under `-c` it defaults to JSON lines. Force `box` explicitly with
`-mode box`:

```sh
$ ./n1k1 -mode box -c "SELECT id, customer, total, status FROM orders LIMIT 4" examples/shop
┌──────┬──────────┬───────┬─────────┐
│   id │ customer │ total │ status  │
├──────┼──────────┼───────┼─────────┤
│ 1001 │ alice    │ 129.5 │ shipped │
│ 1002 │ bob      │ 49.99 │ pending │
│ 1003 │ alice    │   210 │ shipped │
│ 1004 │ carol    │ 18.75 │ shipped │
└──────┴──────────┴───────┴─────────┘
4 row(s) · 4 column(s)
```

---

## 2. Filter, project, compute

Real expressions in `SELECT` and `WHERE` — arithmetic, `ROUND`, `ORDER BY`,
`LIMIT`:

```sh
$ ./n1k1 -mode box -c "SELECT id, customer, total, ROUND(total*0.08, 2) AS tax
                       FROM orders WHERE total > 150 ORDER BY total DESC" examples/shop
┌──────┬──────────┬────────┬──────┐
│   id │ customer │  total │  tax │
├──────┼──────────┼────────┼──────┤
│ 1020 │ alice    │ 389.99 │ 31.2 │
│ 1019 │ carol    │    245 │ 19.6 │
│ 1003 │ alice    │    210 │ 16.8 │
│ 1018 │ bob      │ 156.25 │ 12.5 │
└──────┴──────────┴────────┴──────┘
```

**Output modes** — same query, pick the shape (`box`, `jsonlines`, `json`,
`csv`, `markdown`, `line`, `list`; add `|pretty` to indent JSON):

```sh
$ ./n1k1 -mode csv -c "SELECT id, customer, total FROM orders WHERE total>150 ORDER BY total DESC" examples/shop
id,customer,total
1020,alice,389.99
1019,carol,245
1003,alice,210
1018,bob,156.25

$ ./n1k1 -mode markdown -c "SELECT status, COUNT(*) AS n FROM orders GROUP BY status ORDER BY n DESC" examples/shop
| status | n |
| --- | --- |
| shipped | 16 |
| pending | 3 |
| cancelled | 1 |
```

Pipe it straight into `jq` or a CSV tool — `jsonlines` is the default when the
output isn't a terminal.

---

## 3. JOIN, GROUP BY, aggregate

Join two keyspaces and roll up. `orders` join `customers`, revenue by city:

```sh
$ ./n1k1 -mode box -c "SELECT c.city, COUNT(*) AS orders,
                              ROUND(SUM(o.total),2) AS revenue,
                              ROUND(AVG(o.total),2) AS avg_order
                       FROM orders o JOIN customers c ON o.customer = c.id
                       GROUP BY c.city ORDER BY revenue DESC" examples/shop
┌─────────┬────────┬─────────┬───────────┐
│ city    │ orders │ revenue │ avg_order │
├─────────┼────────┼─────────┼───────────┤
│ Seattle │     11 │ 1352.93 │    122.99 │
│ Austin  │      9 │  596.43 │     66.27 │
└─────────┴────────┴─────────┴───────────┘
```

---

## 4. Any format, same SQL

Point at a different directory — the file *format* is discovered per file. No
config, no `CREATE TABLE`.

**CSV** (`examples/finance` — header row → field names, light type inference):

```sh
$ ./n1k1 -mode box -c "SELECT currency, COUNT(*) AS n, ROUND(SUM(amount),2) AS total
                       FROM txns GROUP BY currency ORDER BY total DESC" examples/finance
┌──────────┬───┬────────┐
│ currency │ n │  total │
├──────────┼───┼────────┤
│ EUR      │ 1 │    210 │
│ USD      │ 3 │ 197.74 │
└──────────┴───┴────────┘
```

**YAML** (`examples/infra` — `---` multi-doc streams, nested paths, arrays):

```sh
$ ./n1k1 -mode box -c "SELECT name, team, resources.cpu AS cpu, resources.mem AS mem,
                              ARRAY_LENGTH(ports) AS nports
                       FROM services ORDER BY name" examples/infra
┌─────────────┬──────────┬───────┬───────┬────────┐
│ name        │ team     │ cpu   │ mem   │ nports │
├─────────────┼──────────┼───────┼───────┼────────┤
│ api-gateway │ platform │ 500m  │ 512Mi │      2 │
│ auth        │ identity │ 250m  │ 256Mi │      1 │
│ indexer     │ search   │ 1000m │ 2Gi   │      0 │
│ mailer      │ platform │ 100m  │ 128Mi │      1 │
└─────────────┴──────────┴───────┴───────┴────────┘
```

**gzip** (`examples/archive` — transparent `.jsonl.gz`, decompressed on the
fly; revenue by year via a `SUBSTR` on the timestamp):

```sh
$ ./n1k1 -mode box -c "SELECT SUBSTR(ts,0,4) AS year, COUNT(*) AS orders, ROUND(SUM(total),2) AS revenue
                       FROM orders GROUP BY SUBSTR(ts,0,4) ORDER BY year" examples/archive
┌──────┬────────┬─────────┐
│ year │ orders │ revenue │
├──────┼────────┼─────────┤
│ 2025 │      3 │  237.25 │
│ 2026 │      2 │  328.99 │
└──────┴────────┴─────────┘
```

**Parquet** (`examples/warehouse` — columnar files transposed to rows; 240
sales rows across two parts):

```sh
$ ./n1k1 -mode box -c "SELECT region, COUNT(*) AS n, ROUND(SUM(amount),2) AS revenue
                       FROM sales GROUP BY region ORDER BY revenue DESC" examples/warehouse
┌────────┬────┬─────────┐
│ region │  n │ revenue │
├────────┼────┼─────────┤
│ east   │ 60 │  3677.6 │
│ north  │ 60 │ 3667.56 │
│ ...    │    │         │
└────────┴────┴─────────┘
```

**Documents** (`examples/kb` — a pure-Go extract provider pulls text out of
PDF / DOCX / PPTX / RTF; query it like any other column):

```sh
$ ./n1k1 -mode box -c "SELECT filename, kind FROM docs WHERE text LIKE '%vacation%'" examples/kb
┌────────────────┬──────┐
│ filename       │ kind │
├────────────────┼──────┤
│ deck.pptx      │ pptx │
│ handbook.pdf   │ pdf  │
│ memo.rtf       │ rtf  │
│ q1-report.docx │ docx │
└────────────────┴──────┘
```

---

## 5. Secondary indexes

Declare an index and n1k1 uses it automatically — no query rewrite. `.explain`
shows the plan switching from a full scan to an index scan + fetch:

```sh
$ printf '.index create ix_cust on orders (customer)
.explain on
SELECT id, total FROM orders WHERE customer = "alice" ORDER BY total DESC LIMIT 3;
' | ./n1k1 examples/shop
  ✓ default:orders.ix_cust  20 entries, 128.0KB
created ix_cust
n1k1 plan / op tree:
project  [labelPath, labelPath]
  order-offset-limit  [.["id"] .["total"] .["orders"]]
    project  [(`orders`.`id`), (`orders`.`total`), labelPath]
      filter  ((`orders`.`customer`) = "alice")
        datastore-fetch  [.["orders"] ^id]
          datastore-scan-index  [^id]          ← index scan, not a full scan
{"id":"1020","total":389.99}
{"id":"1003","total":210.00}
{"id":"1001","total":129.50}
```

Index definitions live in `<dir>/.n1k1/catalog.json` and are built lazily on
first use (or eagerly with `-index eager`; `-index off` A/B-baselines a full
scan). Full-text (`bleve`) indexes are also supported via `SEARCH()`.
(`rm -rf examples/shop/.n1k1` to clean up the built index.)

---

## 6. One-liners & pipes

No datastore needed for constant expressions — n1k1 is a handy SQL++
calculator:

```sh
$ ./n1k1 -c "SELECT ARRAY_AVG([2,4,6]) AS avg, UPPER('n1k1') AS name, DATE_PART_STR('2026-07-06','month') AS m"
{"avg":4,"name":"N1K1","m":7}
```

Pipe statements in on stdin (batch mode):

```sh
$ echo "SELECT customer, COUNT(*) AS n FROM orders GROUP BY customer ORDER BY n DESC" | ./n1k1 examples/shop
{"customer":"alice","n":6}
{"customer":"bob","n":5}
{"customer":"carol","n":5}
{"customer":"dave","n":4}
```

And a couple of showpieces — `sparkline()`/`histogram()` render a group's
numbers as a unicode chart (great for eyeballing time series):

```sh
$ ./n1k1 -mode box -c "SELECT host, COUNT(*) AS n, sparkline(\`value\`) AS trend
                       FROM cpu GROUP BY host ORDER BY host" examples/metrics
┌───────┬────┬──────────────────────────────────────────────────┐
│ host  │  n │ trend                                            │
├───────┼────┼──────────────────────────────────────────────────┤
│ hostA │ 48 │ ▂▂▁▁▁▁▁▂▂▃▃▄▄▅▅▅▆▅▅▅▄▄▃▃▄▃▃▃▂▃▃▃▄▅▅▆▇▇█████▇▇▆▅▅ │
│ hostB │ 24 │ ▁▂▂█▁▁▁▂▂█▁▁▁▂▂█▁▁▁▁▂█▁▁                         │
│ hostC │ 24 │ ▁▂▂▂▃▃▄▄▅▅▆▆▇▇██████████                         │
└───────┴────┴──────────────────────────────────────────────────┘
```

Pure-SQL++ `WITH RECURSIVE` can even do real iterative computation — the
Mandelbrot set as ASCII art, no data source required:

```sh
$ ./n1k1 -f examples/queries/mandelbrot.sql++ | jq -r '.line'
                        ..... ......:@#@@@@@@@@@@@@@@@@@@-..
                       ..:..........+@@@@@@@@@@@@@@@@@@@@=%-
                       ..:=@::-:....-@@@@@@@@@@@@@@@@@@@@@@..
                       ...:@=@@@=:::@@@@@@@@@@@@@@@@@@@@@@:.
                  ....=.:-@@@@@@@@@+@@@@@@@@@@@@@@@@@@@@@@:.
                    ...(trimmed)...
```

The REPL (just `./n1k1 examples/shop`) adds arrow-key history, tab-friendly
dot-commands (`.mode .timer .explain .schema .index .output …`), and colorized
output at a TTY.

---

## How it works, and why it's neat

n1k1 reuses Couchbase's `couchbase/query` (cbq) engine to **parse and plan**
SQL++, then executes with its own operators tuned to avoid garbage.

**Anti-boxing, byte-oriented execution.** A value is a `[]byte` (`base.Val`),
not a boxed `interface{}` / `map[string]interface{}`. JSON fields are read in
place with `jsonparser` (slices *into* the source document, no unmarshal),
commonly-accessed fields get promoted to positional "registers", and the native
expression library operates directly on bytes. The result is near-zero
per-row allocation. Measured (`bench_expr_arith_test.go`, M2 Pro): native `a+b`
is **31 ns, 0 B/op, 0 allocs/op** vs the cbq fallback's **190 ns, 384 B/op,
8 allocs/op** — ~6× faster with zero garbage per eval.

You can *see* which lane a query takes with `-stats`. An all-native predicate
reports no expression boxing:

```sh
$ ./n1k1 -stats=final -c "SELECT id FROM orders WHERE total > 100 AND status = 'shipped'" examples/shop
... runtime: 419.3KB allocated · 2.3K allocs · heap 3.4MB · 1 GCs · 5 goroutines
                       (no "expr:" line → every expression stayed on the native byte path)
```

A not-yet-ported function falls back to cbq's boxing engine — n1k1 reports
exactly how much boxed work happened:

```sh
$ ./n1k1 -stats=final -c "SELECT id FROM orders WHERE REGEXP_CONTAINS(customer, '^a')" examples/shop
... expr: 1/2 exprs boxed · 20 boxed evals
```

The cbq fallback stays forever as a correctness backstop; the native library
grows underneath it (see [DESIGN-exprs.md](DESIGN-exprs.md)).

**Push-based operators.** Data flows scan → filter → project as plain function
calls (`yield`), not channel sends or `HasNext()` polling — shorter codepaths,
inlinable, no per-row iterator overhead.

**One codebase generates *both* the interpreter and the compiler.** The
`engine/*.go` operators are written in a disciplined Go subset marked with an
`lz` ("lazy" / late-bound) naming convention. Run them directly and you get an
**interpreter**. Feed the same source through the `intermed_build` codegen tool
and the `lz`-marked lines become `printf`s that **emit Go source** — a
**compiler** for the query plan, specialized per query, from the identical
logic. No second implementation to keep in sync. (See
[DESIGN.md](DESIGN.md), "The way the n1k1 compiler works".)

**Spill to disk.** Joins, `DISTINCT`, `GROUP BY` (hashmaps), and `ORDER BY`
(max-heaps) spill from memory to temporary files when a working set gets too
big, so a query bigger than RAM still completes.

---

## Try it yourself

```sh
make cli                                 # builds ./n1k1
./n1k1 examples/shop                     # open a REPL, then: .tables  .schema orders
./n1k1 -c "SELECT 1+1"                   # calculator mode
./n1k1 examples/metrics                  # sparklines, recursive CTEs, and more
```

More sample datasets and their layouts are catalogued in
[`examples/README.md`](examples/README.md); the design docs (`DESIGN*.md`)
cover the internals in depth.
