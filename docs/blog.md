# Introducing n1k1 -- SQL (and more) for JSON, CSV, YAML, logs, Parquet, Office files and more

**n1k1** is a single pure-Go binary. Point it at a directory and query
your files with SQL++ (SQL with JSON operators). It's like
`jq`/`sqlite`/`DuckDB`, but aimed at the semi-structured files (beyond
basic JSON and tabular data) that's sitting on your local drive — no
`CREATE TABLE`, no import step, and file data formats are discovered
per file.

```sh
$ ./n1k1 -c "SELECT id, customer, total, status FROM orders LIMIT 4" examples/shop 
{"id":"1001","customer":"alice","total":129.50,"status":"shipped"}
{"id":"1002","customer":"bob","total":49.99,"status":"pending"}
{"id":"1003","customer":"alice","total":210.00,"status":"shipped"}
{"id":"1004","customer":"carol","total":18.75,"status":"shipped"}

$ ./n1k1 -c "SELECT id, customer, total, status FROM orders LIMIT 4" -mode box examples/shop
┌──────┬──────────┬───────┬─────────┐
│   id │ customer │ total │ status  │
├──────┼──────────┼───────┼─────────┤
│ 1001 │ alice    │ 129.5 │ shipped │
│ 1002 │ bob      │ 49.99 │ pending │
│ 1003 │ alice    │   210 │ shipped │
│ 1004 │ carol    │ 18.75 │ shipped │
└──────┴──────────┴───────┴─────────┘
```

A "datastore" like `examples/shop` is a directory tree:
`<dir>/<namespace>/<keyspace>/…`. So `FROM orders` reads the files
under `examples/shop/default/orders/`.

## The 1 minute tour

`.tables` lists the keyspaces with example queries.

`.schema` samples a keyspace's shape.

```sh
$ echo ".tables" | ./n1k1 examples/shop
2 keyspaces — copy/paste to try:
  customers   SELECT COUNT(*) FROM customers;
  orders      SELECT * FROM orders LIMIT 3;

$ echo ".schema orders" | ./n1k1 examples/shop
orders  (sampled 20 docs):
┌──────────┬────────┬──────────┬───────────────────────────────────────────────┐
│ field    │ types  │ distinct │ example                                       │
├──────────┼────────┼──────────┼───────────────────────────────────────────────┤
│ customer │ string │        4 │ SELECT * FROM orders WHERE customer IN [...]; │
│ status   │ string │        3 │ SELECT * FROM orders WHERE status IN [...];   │
│ total    │ number │      16+ │ SELECT * FROM orders WHERE total = 129.5;     │
└──────────┴────────┴──────────┴───────────────────────────────────────────────┘
```

Use SQL: `WHERE`, `ORDER BY`, `LIMIT`:

```sh
$ ./n1k1 -mode box -c "SELECT id, customer, total, ROUND(total*0.08, 2) AS tax
                       FROM orders
                       WHERE total > 150
                       ORDER BY total DESC" examples/shop
┌──────┬──────────┬────────┬──────┐
│   id │ customer │  total │  tax │
├──────┼──────────┼────────┼──────┤
│ 1020 │ alice    │ 389.99 │ 31.2 │
│ 1019 │ carol    │    245 │ 19.6 │
└──────┴──────────┴────────┴──────┘
```

And joins + roll-ups across two keyspaces — revenue by city:

```sh
$ ./n1k1 -mode box -c "SELECT c.city, COUNT(*) AS orders,
                              ROUND(SUM(o.total),2) AS revenue,
                              ROUND(AVG(o.total),2) AS avg_order
                       FROM orders o
                       JOIN customers c ON o.customer = c.id
                       GROUP BY c.city
                       ORDER BY revenue DESC" examples/shop
┌─────────┬────────┬─────────┬───────────┐
│ city    │ orders │ revenue │ avg_order │
├─────────┼────────┼─────────┼───────────┤
│ Seattle │     11 │ 1352.93 │    122.99 │
│ Austin  │      9 │  596.43 │     66.27 │
└─────────┴────────┴─────────┴───────────┘
```

## File formats

**CSV** — header row becomes field names, with light type inference:

```sh
$ ./n1k1 -mode box -c "SELECT currency, COUNT(*) AS n, ROUND(SUM(amount),2) AS total
                       FROM txns
                       GROUP BY currency
                       ORDER BY total DESC" examples/finance
```

**YAML** — including `---` multi-doc streams, nested paths, and arrays:

```sh
$ ./n1k1 -mode box -c "SELECT name, team, resources.cpu AS cpu, resources.mem AS mem,
                              ARRAY_LENGTH(ports) AS nports
                       FROM services
                       ORDER BY name" examples/infra
┌─────────────┬──────────┬───────┬───────┬────────┐
│ name        │ team     │ cpu   │ mem   │ nports │
├─────────────┼──────────┼───────┼───────┼────────┤
│ api-gateway │ platform │ 500m  │ 512Mi │      2 │
│ indexer     │ search   │ 1000m │ 2Gi   │      0 │
└─────────────┴──────────┴───────┴───────┴────────┘
```

**gzip** — transparent `.jsonl.gz`, decompressed on the fly:

```sh
$ ./n1k1 -mode box -c "SELECT SUBSTR(ts,0,4) AS year, COUNT(*) AS orders,
                              ROUND(SUM(total),2) AS revenue
                       FROM orders
                       GROUP BY SUBSTR(ts,0,4)
                       ORDER BY year" examples/archive
```

**Parquet files**:

```sh
$ ./n1k1 -mode box -c "SELECT region, COUNT(*) AS n,
                              ROUND(SUM(amount),2) AS revenue
                       FROM sales
                       GROUP BY region
                       ORDER BY revenue DESC" examples/warehouse
```

**Office documents** — extracts text out of PDF / DOCX / PPTX / RTF,
so you can grep a knowledge base with `LIKE`:

```sh
$ ./n1k1 -mode box -c "SELECT filename, kind
                       FROM docs
                       WHERE text LIKE '%vacation%'" examples/kb
┌────────────────┬──────┐
│ filename       │ kind │
├────────────────┼──────┤
│ handbook.pdf   │ pdf  │
│ q1-report.docx │ docx │
└────────────────┴──────┘
```

Of note: SQL++ has `UNNEST`, nested-path access, and array functions,
so you don't have to pre-flatten anything.

## Output formats

By default, n1k1 outputs -mode `box` table format in a terminal and
newline-delimited JSON (JSONL) when piped — so drops straight into
`awk`/CSV pipelines.

The `-mode` output options: `box`, `jsonlines`, `json`, `csv`,
`markdown`, `line`, `list`; add a `|pretty` suffix for JSON
pretty-printing (e.g., `box|pretty`).

```sh
$ ./n1k1 -mode csv -c "SELECT id, customer, total
                       FROM orders
                       WHERE total>150" examples/shop
id,customer,total
1020,alice,389.99
1019,carol,245
```

Feed statements on stdin for batch work, or use `-c` as a SQL++ / JSON
calculator with no datastore at all:

```sh
$ ./n1k1 -c "SELECT ARRAY_AVG([2,4,6]) AS avg,
                    UPPER('n1k1') AS name,
                    DATE_PART_STR('2026-07-06','month') AS m"
{"avg":4,"name":"N1K1","m":7}
```

For eyeballing time series straight in the terminal, `sparkline()` and
`histogram()` render a group's numbers as a unicode chart:

```sh
$ ./n1k1 -mode box -c "SELECT host, COUNT(*) AS n, sparkline(\`value\`) AS trend
                       FROM cpu GROUP BY host ORDER BY host" examples/metrics
┌───────┬────┬──────────────────────────────────────────────────┐
│ host  │  n │ trend                                            │
├───────┼────┼──────────────────────────────────────────────────┤
│ hostA │ 48 │ ▂▂▁▁▁▁▁▂▂▃▃▄▄▅▅▅▆▅▅▅▄▄▃▃▄▃▃▃▂▃▃▃▄▅▅▆▇▇█████▇▇▆▅▅ │
│ hostC │ 24 │ ▁▂▂▂▃▃▄▄▅▅▆▆▇▇██████████                         │
└───────┴────┴──────────────────────────────────────────────────┘
```

The interactive REPL (just `./n1k1 <dir>`) supports arrow-key history,
colorized output, and dot-commands (`.help`, `.mode`, `.timer`, `.explain`,
`.schema`, `.index`, `.output`, …).

## Use cases

A few scenarios where you might want to "SQL over the files":

- **Support / diagnostic bundles.** A dump or tree of JSON, logs, and
  configs. Point n1k1 at it and ask real questions — counts, filters,
  group-bys — instead of hand-rolling `jq` per file.  
- **Log triage.** Gzipped `.jsonl.gz` logs are decompressed on the
  fly; `GROUP BY` an error code or `SUBSTR` a timestamp into an hour
  bucket to find the spike.
- **Config audits.** Across a directory of YAML manifests: "which services
  request > 1 CPU?", "who has no readiness port?" — one query, all files.
- **Metrics at a glance.** `sparkline()`/`histogram()` turn a column into a
  terminal chart for a quick read before reaching for a dashboard.
- **Office docs.** PDF's, DOCX's, PPT's, Markdown, etc.

## Indexing

- **Secondary indexes.** Declare an index and n1k1 switches
  the plan from a full scan to an index scan + fetch — no query rewrite. Handy
  when you'll hit the same directory repeatedly.

  ```sh
  $ printf '.index create ix_cust on orders (customer)
  .explain on
  SELECT id, total FROM orders WHERE customer = "alice" ORDER BY total DESC LIMIT 3;
  ' | ./n1k1 examples/shop
  ...
          datastore-scan-index  [^id]          ← index scan, not a full scan
  ```

- **Full-text indexes** are supported too, via `SEARCH()`.

## Other features

- **n1k1 reuses Couchbase's SQL++ engine to parse and plan SQL++ queries**
- **Large queries.** n1k1 will dynamically spill to temporary files as
  needed when a working set grows large, so your joins, `DISTINCT`,
  `GROUP BY`, and `ORDER BY` won't die from OOM (out of memory) errors.
- **S3 / Object Store access.** - including S3/GCS/Azure support.
- **Vector Queries** - n1k1 supports vectorizing data using embedding
  models and storing vectors into Parquet for VECTOR_DISTANCE() queries.
- **Parquet / Iceberg** - directly query columnar data.
- **WINDOW functions** - for leaderboards, running totals, `grep` use cases, etc.
- **Recursive CTE's (common table expressions)**
- **JavaScript UDF's (user defined functions)** - scalar functions
  and streaming table-valued functions supported and custom aggregate functions.
- **JavaScript Data/File Extractors** - got a weird log file format --
  you can provide optional, custom JavaScript 'extractor' code to parse
  and frame your file's data into records.
- **JavaScript hygentic macros** - to go beyond simple UDF's.
- **VARIANT / EJSON enabled**: n1k1 supports the "superset of JSON" from
  modern Parquet and its nested, hierarchical VARIANT data type. Also,
  n1k1 supports extended JSON (EJSON).
- **Multi-Query Optimization** - see the `.multi` dot command for
  fusing multiple queries together to leverage shared scans and common
  subexpression elimination.
- **ASOF joins** - for temporal data processing via K-way merge sorted datasources.
- **LATERAL joins**
- **Time travel queries** - with Parquet data files.
- **TEMP keyspaces**
- **INSERT INTO foo.jsonl** - n1k1 supports new file creation only at
  the moment for JSONL and Parquet file formats.
- **n1k1's codebase is both a SQL++ interpreter and a SQL++compiler.** Written
  in a subset of disciplined Golang, n1k1 can optionally run in 'codegen' mode
  to emit the fused Golang for a given query plan. See: `.prepare <query>`.
- **Pure Go, CGO-free, cross-compiles** to Linux/macOS/Windows as a single static
  binary — no extra dependencies for your target host.
- **WASM support** - for running entirely in a web browser / no server needed.
- **BSL licensed**

## Performance features

- **GC Reduction** - n1k1 is designed to reduce Golang's GC issues by
  avoiding garbage creation.
- **Byte-oriented, anti-boxing execution** - data is handled as direct byte
  buffers or slices (`[]byte`), so JSON data is handle as raw bytes. JSON
  fields are read *in place* (slices into the source JSON buffers, avoiding
  full unmarshal's). SQL++ expression evaluation therefore works directly
  on bytes, leading to near-zero per-row allocations, even for aggregates.
  A native `a + b` benchmarks at **31 ns, 0 allocs**. On a compute-bound
  workloads n1k1 runs **~6–9× faster using ~6–26× less memory** than the
  boxed execution engines.
- **Register based** - tuple field accesses are optimized into positional lookups
  into a fixed-sized array of vals (registers), rather than performing string-based
  hashmap / dictionary lookups.
- **Contiguous, in-place map/heap data structures** - map (dictionaries) and heap
  data structures are backed by contiguous byte buffers (`[]byte`) rather than
  allocating individual key, bucket, and value items or list nodes, for
  improved memory reuse, garbage reduction, and faster GC scanning.
- **Push / Callback based** - instead of graph of iterators
  calling Next() down the tower to drive more data access,
  n1k1 a direct function callbacks.
- **Hybrid Row and Columnar Execution** - if a backend datasource, like Parquet / Iceberg,
  provides columnar data (contiguous vector of scalars, etc), n1k1 optimizes
  via vectorized expression evaluation (e.g., aggregates like SUM(float), etc.),
  and along with leveraging any associated nulls-bitmap metadata.
- **Push-down** - n1k1 can push expressions down to advanced backend datasources,
  like Parquet / Iceberg, for evaluating expressions as close as possible to the data. And,
  advanced datasources, like Parquet / Iceberg, which provide metadata (e.g., min/max values)
  allow n1k1 to skip entire files during query execution, including COUNT() optimizations.

