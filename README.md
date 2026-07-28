# n1k1 — SQL++ for local files

Query the files you already have — JSON, JSONL, CSV, YAML, TOML, Parquet, Iceberg,
gzip, even PDFs and Office documents — with SQL++ (Couchbase N1QL), from a single
self-contained binary. No server to run, no import step, no schema to declare.

Think `sqlite3` / `duckdb`, but with a query language built for **nested JSON**, and a
scan layer that treats a directory tree of mixed file formats as queryable tables.

[![CI](https://github.com/couchbase/n1k1/actions/workflows/ci.yml/badge.svg)](https://github.com/couchbase/n1k1/actions/workflows/ci.yml)

## Try it

**In your browser, with nothing installed:** the engine also compiles to WebAssembly, so
**[the SQL++ playground](https://couchbase.github.io/n1k1/play/)** runs the real thing
client-side — load a sample dataset or drop in your own files, and query them. Nothing is
uploaded anywhere.

Locally:

```sh
make cli      # builds ./n1k1 (pure Go, CGO-free; make install-cli to install)
```

Point it at a directory and query the files inside it:

```
$ ./n1k1 -c "SELECT id, customer, total FROM orders WHERE total > 200 ORDER BY total DESC" examples/shop
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

…or at one file, where the keyspace is just the filename:

```
$ ./n1k1 -c "SELECT action, COUNT(*) AS n FROM events GROUP BY action ORDER BY n DESC" events.jsonl
{"action":"login","n":3}
{"action":"purchase","n":2}
```

Aggregate straight out of a CSV, or a directory of Parquet parts — same query language:

```sh
./n1k1 -c "SELECT currency, ROUND(SUM(amount), 2) AS total FROM txns GROUP BY currency" examples/finance
./n1k1 -c "SELECT region, ROUND(SUM(amount), 2) AS amount FROM sales GROUP BY region" examples/warehouse
```

Run with no directory at all to use it as an expression sandbox (`./n1k1 -c "SELECT 1+1"`),
pipe statements in on stdin, or run a `.sql++` script with `-f`. Output is a box table at a
terminal and JSON Lines when piped, so it composes with `jq` and friends; `-mode` overrides.
With no `-c`/`-f` you get a REPL with history, `.tables`, `.schema`, `.timer`, `.explain`,
and more — `.help` lists them.

## Why SQL++ rather than SQL

SQL++ is SQL generalized over JSON, so nested data is a first-class citizen instead of
something you reach into with vendor-specific JSON functions:

```
$ ./n1k1 -c 'SELECT o.id, t AS tag FROM [{"id":1,"tags":["red","blue"]}] AS o UNNEST o.tags AS t'
{"id":1,"tag":"red"}
{"id":1,"tag":"blue"}
```

`UNNEST` explodes nested arrays into rows, `MISSING` is distinct from `NULL`, and
objects/arrays have a rich function library (`OBJECT_PAIRS`, `ARRAY_AGG`, `WITHIN` for
recursive descent, array and object comprehensions, …).

**[The SQL++ recipes doc](https://couchbase.github.io/n1k1/)** is the fastest way to judge
whether the language suits you: ~50 common JSON slice-and-dice tasks, each shown in SQL++
side by side with Postgres, DuckDB, JavaScript, Python, MongoDB, and jq — and each with a
**Try It Now** link that opens it in the browser playground, data preloaded.

## What it reads

| | |
|---|---|
| **Text records** | JSON (one doc per file, or multi-doc), JSONL/NDJSON, CSV/TSV (header keys, light type inference), YAML (incl. `---` streams), TOML |
| **Columnar** | Parquet, Apache Iceberg — projection / predicate / partition pushdown, time travel, `VARIANT` |
| **Compressed** | transparent `.gz` |
| **Documents** | text extracted from `.pdf`, `.docx`/`.pptx`/`.xlsx`, `.txt`/`.log`/`.md`, `.rtf`; metadata for images and video |
| **Remote** | `s3://`, `gs://`, `abfs://` (Azure) — Iceberg tables and Parquet, read over ranged GETs |

Files and directories become keyspaces (tables) by convention — a flat directory, a
`<namespace>/<keyspace>/` tree, or nested subdirectories all work. `.tables` lists what was
discovered, `.schema <keyspace>` prints a sampled shape, and `-formats` restricts what gets
scanned.

## Beyond ad-hoc queries

- **Joins, window functions, set operations.** Nested-loop and hash joins (inner/outer,
  `ON KEYS`), `UNNEST`/`NEST`, `GROUP BY`/`HAVING`, a full window-function suite (ranking,
  navigation, `ROWS`/`RANGE`/`GROUPS` frames), `UNION`/`INTERSECT`/`EXCEPT`, subqueries.
- **Bigger than memory.** Hash tables and sort heaps spill to temporary files, so joins,
  `DISTINCT`, `GROUP BY`, `ORDER BY`, and large window partitions are not RAM-bound.
- **Multi-query packs.** `.multi` runs a whole directory of tagged `*.sql++` queries over
  one shared scan instead of re-reading the data per query — useful for running a suite of
  detectors across a large bundle. See `.help multi`.
- **Programmable.** JavaScript UDFs and aggregates, `*.extract.js` recipes that frame
  arbitrary text/log formats into rows, and `*.macro.js` macros that expand `@name(...)`
  into SQL++ — all loaded with `-ext`.
- **Semantic search.** Embedding generation plus `VECTOR_DISTANCE` for similarity queries
  (`.help vectors`).
- **Staged pipelines.** `CREATE TEMP KEYSPACE … AS SELECT …` materializes intermediate
  results (in memory, spilling when large) for a later query to read.
- **Secondary indexes.** Secondary/FTS indexes declared in an on-disk catalog and built
  lazily on first use — `.index suggest` proposes candidates from a doc sample (`.index`).
- **A query compiler.** Besides interpreting plans, n1k1 can generate Go source for a query
  and run that (`-prepare`, `.prepare`) — the origin of the project.
- **Runs in a browser.** `GOOS=js/wasm` builds the whole engine into the
  [playground](https://couchbase.github.io/n1k1/play/); see `web/`.

## Scope

Read-oriented, and deliberately not a database server: there is **no `UPDATE`/`DELETE` and
no transactions**, and writing is limited to materializing results (`CREATE TEMP KEYSPACE`,
or `INSERT INTO` a `.jsonl` file). SQL++ coverage is a large **subset** — the CLI answers
`Unsupported: …` when a plan needs something the engine does not implement yet.

n1k1 began as a research prototype for compiling N1QL query plans to Go, and grew into the
file-querying tool described here. It reuses `couchbase/query` for SQL++ parsing and
planning, then executes with its own operators. It is not a supported Couchbase product.

## Build & test

Go 1.25+. Everything is pure Go (`CGO_ENABLED=0`) and cross-compiles to Linux, macOS, and
Windows on amd64 and arm64.

```sh
make cli          # ./n1k1
make cli-trim     # ./n1k1-trim -- ~41MB instead of ~102MB, dropping Parquet/Iceberg
                  #                and object-store reads; everything else stays
make              # regenerate intermed/ + run the core tests
make test         # core + the SQL++ conformance suite
make test-all     # the full sweep (adds the glue/CLI tests + the data-backed corpus)
make bench        # engine throughput + allocations
make recipes      # regenerate docs/recipes.{md,html} from docs/recipes.yaml
```

If a build ever fails while loading the module graph — `go.mod` pins a few placeholder
enterprise modules that resolve only inside Couchbase — run `make bootstrap` once: it stubs
them locally, creates `test/tmp/`, and regenerates `intermed/`. Do **not** run `go mod
tidy`: `couchbase/query` is reached only through the `n1ql`-tagged `glue/` package, so tidy
would prune it.

CI runs these on all three OSes; see [`.github/README.md`](.github/README.md).

## Docs

- **[SQL++ recipes](https://couchbase.github.io/n1k1/)** — the same JSON task in SQL++ vs
  SQL, JavaScript, Python, MongoDB, and jq ([source](docs/recipes.yaml))
- [`examples/README.md`](examples/README.md) — the sample data trees used above, with a
  runnable query for each on-disk layout
- [`docs/design/`](docs/design/) — internals: [`DESIGN.md`](docs/design/DESIGN.md) (the
  compiler and performance approach), [`DESIGN-cli.md`](docs/design/DESIGN-cli.md),
  [`DESIGN-data.md`](docs/design/DESIGN-data.md) (how files become keyspaces), and more
- [`TODO.md`](TODO.md) — what is planned, and what is known to be missing
- `./n1k1 -h`, and `.help` in the REPL — the authoritative command reference

## License

Business Source License 1.1.
