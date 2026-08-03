# n1k1 — SQL++ for local files and more

Query your local files & data — JSON, JSONL, CSV, YAML, TOML, Parquet,
Iceberg, gzip, even PDFs and Office documents — with SQL++, from a
single self-contained binary. No server to run, no import step, no
schemas to declare.

It's like `sqlite3` / `duckdb` with a query language (SQL++, also
known as N1QL) built for **nested JSON** and a scan layer that treats
a directory tree of mixed file formats as queryable tables
(keyspaces).

[![CI](https://github.com/couchbase/n1k1/actions/workflows/ci.yml/badge.svg)](https://github.com/couchbase/n1k1/actions/workflows/ci.yml)

## Try it

**In your browser:** [the SQL++
playground](https://couchbase.github.io/n1k1/play/) runs client-side —
load a sample dataset or drop in your own files, and query
them. Powered by WASM.

Locally:

```sh
make cli # builds ./n1k1 (pure Go, CGO-free; make install-cli to install)
```

Point it at a directory and query the files inside it:

```
$ ./n1k1 -mode box -c "SELECT id, customer, total FROM orders WHERE total > 200 ORDER BY total DESC" examples/shop
┌──────┬──────────┬────────┐
│   id │ customer │  total │
├──────┼──────────┼────────┤
│ 1020 │ alice    │ 389.99 │
├──────┼──────────┼────────┤
│ 1019 │ carol    │    245 │
├──────┼──────────┼────────┤
│ 1003 │ alice    │    210 │
└──────┴──────────┴────────┘
3 row(s) · 3 column(s)
```

Or with a single file, the keyspace is just the filename:

```
$ ./n1k1 -c "SELECT action, COUNT(*) AS n FROM events GROUP BY action ORDER BY n DESC" events.jsonl
{"action":"login","n":3}
{"action":"purchase","n":2}
```

Aggregate data from CSV's, or from directories of Parquet files:

```sh
./n1k1 -c "SELECT currency, ROUND(SUM(amount), 2) AS total FROM txns GROUP BY currency" examples/finance
./n1k1 -c "SELECT region, ROUND(SUM(amount), 2) AS amount FROM sales GROUP BY region" examples/warehouse
```

Run with no directory at all to use it as an expression sandbox (`./n1k1 -c "SELECT 1+1"`),
or pipe SQL++ statements in on stdin, or run a `.sql++` script with `./n1k1 -f <filename.sql++>`.

Output will be a box table at a terminal and JSON Lines when piped, so it
composes with `jq`, `grep` and friends; The `-mode` flag controls the output
format.

Interactive REPL with history -- use commands like `.tables`, `.schema`,
`.timer`, `.explain`, and more — `.help` lists available commands.

## SQL++

SQL++ is SQL generalized over JSON, so nested data is a first-class citizen instead of
having specialized JSON syntax and functions:

```
$ ./n1k1 -c 'SELECT o.id, t AS tag FROM [{"id":1,"tags":["red","blue"]}] AS o UNNEST o.tags AS t'
{"id":1,"tag":"red"}
{"id":1,"tag":"blue"}
```

In SQL++, `UNNEST` explodes nested arrays into rows, `MISSING` is distinct from `NULL`, and
objects/arrays have a rich function library (`OBJECT_PAIRS`, `ARRAY_AGG`, `WITHIN` for
recursive descent, for array and object comprehensions, …).

**[The SQL++ recipes doc](https://couchbase.github.io/n1k1/)**
provides an online recipe book of common JSON slice-and-dice tasks, with SQL++
examples shown side by side compared to SQL (Postgres,
DuckDB), JavaScript, Python, MongoDB, and jq.

## Data formats

| | |
|---|---|
| **Text records** | JSON (one doc per file, or multi-doc), JSONL/NDJSON, CSV/TSV (header keys, light type inference), YAML (incl. `---` streams), TOML |
| **Columnar** | Parquet, Apache Iceberg — projection / predicate / partition pushdown, time travel, `VARIANT` |
| **Compressed** | `.gz` |
| **Documents** | text extracted from `.pdf`, `.docx`/`.pptx`/`.xlsx`, `.txt`/`.log`/`.md`, `.rtf`; metadata for images and video |
| **Clouds / Remote** | `s3://`, `gs://` (Google), `abfs://` (Azure) — Iceberg tables and Parquet, read via ranged GETs |

Files and directories become keyspaces (tables) by convention — a flat
directory, a `<namespace>/<keyspace>/` tree, or nested subdirectories
all work. `.tables` lists what was discovered, `.schema <keyspace>`
prints a sampled shape, and `-formats` restricts what gets scanned.

## Beyond ad-hoc queries

- **JOIN's window functions, set operations.** Nested-loop and hash joins (inner/outer,
  `ON KEYS`), `UNNEST`/`NEST`, `GROUP BY`/`HAVING`, a full window-function suite (ranking,
  navigation, `ROWS`/`RANGE`/`GROUPS` frames), `UNION`/`INTERSECT`/`EXCEPT`, subqueries.
- **Programmable.** JavaScript UDFs and aggregates, `*.extract.js` recipes that frame
  arbitrary text/log formats into records, and `*.macro.js` macros that expand `@name(...)`
  into SQL++ — all loaded with `-ext`. Scalar, aggregate and streaming table-valued-functions
  supported.
- **Shared scans.** The `.multi` runs a whole directory of multiple, tagged `*.sql++` queries over
  one shared, optimized scan instead of re-reading the data per query — useful for running
  a suite of queries across a large dataset. Common subexpression elimination (CSE) and
  other optimizations are automatically applied. See `.help multi`.
- **Vector / semantic search.** Embedding generation plus `VECTOR_DISTANCE` for similarity
  queries. See `.help vectors`.
- **Staged pipelines.** `CREATE TEMP KEYSPACE … AS SELECT …` materializes intermediate
  results (in memory to start, spilling to disk when large) for your later queries to read.
- **Bigger than memory.** Hash tables and sort heaps spill as needed to temporary files, so joins,
  `DISTINCT`, `GROUP BY`, `ORDER BY`, and large window partitions are not RAM-bound.
- **Secondary indexes.** Secondary indexes and full-text search (FTS) indexes can be built
  lazily on first use — `.index suggest` proposes candidates from a sampling of data.
- **Query compilation.** Besides interpreting plans, n1k1 can generate Go source for a query.
  See `-prepare`, `.prepare`.
- **Runs in a browser.** `GOOS=js/wasm` builds the SQL++ engine into the
  [playground](https://couchbase.github.io/n1k1/play/); see `web/`.

## Not yet

n1k1 is not a database server (at least not yet): **no `UPDATE`/`DELETE` and
no transactions**, and writing is limited to materializing results (`CREATE TEMP KEYSPACE`,
or `INSERT INTO` a `.jsonl` / parquet file).

## Build & test

The n1k1 project favors pure Go (1.25+; `CGO_ENABLED=0`) and cross-compiles to Linux, macOS, and
Windows on amd64 and arm64.

```sh
make cli          # ./n1k1
make cli-trim     # ./n1k1-trim -- dropping Parquet/Iceberg and cloud/object-store access
make              # regenerate intermed/ + run the core tests
make test         # core + the SQL++ conformance suite
make test-all     # the full test sweep (with glue/CLI tests + data-backed tests)
make bench        # engine throughput + allocations
make recipes      # regenerate docs/recipes.{md,html} from docs/recipes.yaml
```

Github Actions: see [`.github/README.md`](.github/README.md).

## Docs

- **[SQL++ recipes](https://couchbase.github.io/n1k1/)** — the same JSON task in SQL++ vs
  SQL, JavaScript, Python, MongoDB, and jq ([source](docs/recipes.yaml))
- [`examples/README.md`](examples/README.md) — the sample data trees used above, with a
  runnable query for each on-disk layout
- [`docs/design/`](docs/design/) — [`DESIGN.md`](docs/design/DESIGN.md) (the
  compiler and performance approach), [`DESIGN-cli.md`](docs/design/DESIGN-cli.md),
  [`DESIGN-data.md`](docs/design/DESIGN-data.md) (how files become keyspaces), and more
- `./n1k1 -h`, and `.help` in the REPL — the authoritative command reference
- [`TODO.md`](TODO.md) — what is planned, and what is known to be missing

## License

Business Source License 1.1.
