# cbq-vs-n1k1 benchmark

Apples-to-apples head-to-head over the **same directory of `*.json` files** — the
classic cbq file-datastore layout `<root>/<namespace>/<keyspace>/<key>.json` that
**both** engines read. Both use cbq's parser+planner (identical plan); what differs
is the execution engine: n1k1's `[]byte` byte-engine vs cbq's boxed
`value.AnnotatedValue` executor. `gen.py` writes a deterministic dataset; `bench.py`
builds n1k1, runs a query set warm (median of N reps), and reports per-query time.

## Run

    python3 bench.py                              # n1k1-only (time + allocations)
    NDOCS=20000 REPS=25 python3 bench.py
    N1K1=/path/to/n1k1 python3 bench.py           # skip building n1k1

## The cbq column (real cbq executor over the SAME local files)

cbq's executor is driven over the `dir:` file datastore via cbq's own no-cluster
harness (`test/filestore`: `Start` → `resolver.NewDatastore("dir:"+path)`, `Run` →
full parse→plan→execute via `server.ServiceRequest`). A small `localbench` program
wraps it. Build it once from the **`local-benchmark`** branch of the `n1k1-query`
fork, then point `CBQ_LOCALBENCH` at the binary:

    # in the n1k1-query fork worktree on branch local-benchmark:
    CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go build -o /tmp/localbench ./cmd/localbench

    # then:
    CBQ_LOCALBENCH=/tmp/localbench NDOCS=20000 python3 bench.py

That branch makes cbq's `server`+`execution` build pure-Go by stubbing the
cluster/GSI/FTS datastore modules at the module boundary (all irrelevant to the
`dir:` file datastore) — see its commit for the full recipe. Full-scan / filter /
projection / aggregate execution runs through cbq's real, unmodified executor.

Alternatively, `CBQ_URL=http://host:port/query/service` (+ `CBQ_CREDS=user:pass`)
benchmarks a live cbq's server-side `metrics.executionTime`. Note a live Couchbase
reads a **KV bucket**, not local files — not apples-to-apples for file access; the
`localbench` path is the correct one. (A standalone `cbq-engine -datastore dir:` is
the obvious alternative, but the shipped 7.6.x binary won't start its query service
without cbauth — hence the filestore harness.)

## Results (representative, this machine)

    20000 docs (I/O-bound: a filtered scan opens 20k files per run, ~common to both)
    query            n1k1 ms   n1k1 MB    cbq ms   cbq/n1k1
    count+filter      3404      35.6      3492      1.03x
    expr-heavy        3506      25.5      4243      1.21x

    500 docs (files cached: execution, not file I/O, dominates)
    query            n1k1 ms   n1k1 MB    cbq ms   cbq/n1k1
    count+filter      19.97     0.96      21.48     1.08x
    filter+project    14.58     0.67      17.03     1.17x
    group+agg         16.66     0.76      18.98     1.14x

**Reading it:** for one-doc-per-file data, wall time is dominated by `os.Open` of
every file (a cost both engines pay), so at large N the engines are within ~1.0–1.2×.
When file I/O isn't the whole story (small/cached data, or heavier per-row work like
`expr-heavy`), n1k1's byte engine is consistently **~1.1–1.2× faster** and allocates
little (the `n1k1 MB` column; cbq's boxed `value.AnnotatedValue` model allocates far
more per row — visible in n1k1's own boxed-fallback stats, harder to read out of the
cbq harness). Net: n1k1 wins across the board, widening as the I/O share shrinks or
the compute grows.

## Files

- `gen.py`     — deterministic dataset generator (idempotent).
- `bench.py`   — the driver (n1k1 always; cbq via `CBQ_LOCALBENCH` or `CBQ_URL`).
- The cbq `localbench` binary lives in the `n1k1-query` fork's `local-benchmark`
  branch (`cmd/localbench`), not here.
