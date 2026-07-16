# cbq-vs-n1k1 benchmark

Apples-to-apples head-to-head over the **same directory of `*.json` files** — the
classic cbq file-datastore layout `<root>/<namespace>/<keyspace>/<key>.json` that
**both** engines read. Both use cbq's parser+planner (identical plan); what differs
is the execution engine: n1k1's `[]byte` byte-engine vs cbq's boxed
`value.AnnotatedValue` executor. `gen.py` writes a deterministic dataset; `bench.py`
builds n1k1, runs a query set warm (median of N reps), and reports per-query **time
and allocated memory** for both engines.

## Dataset (`gen.py`)

    <root>/default/orders/order-000000.json ...   (NDOCS docs; has an `items` array)
    <root>/default/cust/c0.json ...               (37 docs, key = custId, for joins)

Each order carries a `custId` referencing a `cust` doc by key (for `JOIN ... ON
KEYS`) and an `items[]` array (for `UNNEST`). Deterministic (seed 42), idempotent.

## Query set

scan/filter/project, group+agg, sort+limit, an arithmetic-heavy projection, a
key-lookup **join** (`orders JOIN cust ON KEYS o.custId`), a join+group, and two
**UNNEST** queries over `o.items`.

## Run

    python3 bench.py                              # n1k1-only (time + allocations)
    NDOCS=50000 REPS=15 python3 bench.py          # ramp up
    N1K1=/path/to/n1k1 python3 bench.py           # skip building n1k1

Defaults: `NDOCS=20000`, `REPS=11`. Note one-doc-per-file at large N is I/O-heavy
(a scan opens every file), so big runs take minutes.

## The cbq column (real cbq executor over the SAME local files)

cbq's executor is driven over the `dir:` file datastore via cbq's own no-cluster
harness (`test/filestore`: `Start` → `resolver.NewDatastore("dir:"+path)`, `Run` →
full parse→plan→execute via `server.ServiceRequest`). A small `localbench` program
wraps it and reports median ms **and median allocated MB** (`runtime.MemStats`
TotalAlloc delta per query). Build it once from the **`local-benchmark`** branch of
the `n1k1-query` fork, then set `CBQ_LOCALBENCH`:

    # in the n1k1-query fork worktree on branch local-benchmark:
    CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go build -o /tmp/localbench ./cmd/localbench
    # then:
    CBQ_LOCALBENCH=/tmp/localbench python3 bench.py

That branch makes cbq's `server`+`execution` build pure-Go by stubbing the
cluster/GSI/FTS datastore modules at the module boundary (irrelevant to the `dir:`
file datastore) — see its commit for the recipe. Full-scan/filter/projection/
aggregate/join/unnest execution runs through cbq's real, unmodified executor.

Alternatively `CBQ_URL=http://host:port/query/service` (+ `CBQ_CREDS=user:pass`)
times a live cbq's server-side `executionTime` (no memory column). Note a live
Couchbase reads a **KV bucket**, not local files — not apples-to-apples for file
access; `localbench` is the correct path. (Standalone `cbq-engine -datastore dir:`
7.6.x won't start its query service without cbauth — hence the filestore harness.)

## Results (representative, this machine)

    20000 docs, warm median of 7 reps
    query           n1k1 ms  cbq ms  x(t)   n1k1 MB  cbq MB   x(m)
    count+filter     3129    3218   1.03x     35.7    74.4    2.1x
    filter+project   3188    3274   1.03x     25.5    97.9    3.8x
    group+agg        3233    3283   1.02x     24.6   103.2    4.2x
    sort+limit       3143    3232   1.03x     24.5   119.7    4.9x
    expr-heavy       3193    3215   1.01x     25.5   107.1    4.2x
    join-count       3227    3283   1.02x     39.0    94.2    2.4x
    join+group       3169    3292   1.04x     39.1   110.4    2.8x
    unnest-count     3196    3290   1.03x     27.2   172.9    6.4x
    unnest+project   3174    3350   1.06x     30.0   232.6    7.8x

**Time:** one-doc-per-file is I/O-bound — a filtered scan opens every file, a cost
*both* engines pay (the `os.Open` bottleneck), so wall time is within ~1.0–1.1×
(n1k1 consistently a hair faster). At small/cached N (e.g. `NDOCS=500`) where file
I/O isn't the whole story, n1k1 is a clearer ~1.1–1.2× faster.

**Memory is where the value-model gap shows:** cbq allocates **2–8× more per query**
— it boxes every doc into `value.AnnotatedValue` (map + `SimpleUnmarshal`) and each
row/field into a `value.Value`, while n1k1 works on the raw `[]byte` with jsonparser
and reuses buffers. The gap is worst for **UNNEST** (7.8×; every array element
becomes a boxed value) and for sort/group (materialize many boxed rows). This is the
same cost the DESIGN docs attribute to the boxed lane, now measured against cbq
itself.

## Files

- `gen.py`     — deterministic dataset generator (orders + cust; items array).
- `bench.py`   — the driver (n1k1 always; cbq via `CBQ_LOCALBENCH` or `CBQ_URL`).
- The cbq `localbench` binary lives in the `n1k1-query` fork's `local-benchmark`
  branch (`cmd/localbench`), not here.
