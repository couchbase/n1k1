# cbq-vs-n1k1 benchmark

Apples-to-apples head-to-head over the **same directory of `*.json` files** — the
classic cbq file-datastore layout `<root>/<namespace>/<keyspace>/<key>.json` that
**both** engines read. Both use cbq's parser+planner (identical plan); what differs
is the execution engine: n1k1's `[]byte` byte-engine vs cbq's boxed
`value.AnnotatedValue` executor.

**Both columns are measured the same way** — the FULL `parse→plan→execute` per query,
warm (median of REPS, first few dropped), reporting median **ms** and median
**allocated MB**:

- `n1k1` — the `n1k1` CLI itself, driven over one warm REPL session. Its footer
  reports `Result.RunElapsed` (the whole `Session.Run`: parse+plan+convert+execute),
  and `.stats` reports allocated bytes/query (`/gc/heap/allocs` delta over the run) —
  so the CLI already emits the fair, apples-to-apples numbers; no separate runner.
- `cbq`  — the fork's `cmd/localbench` (`test/filestore` over the same `dir:`
  datastore, timing `filestore.Run` + `runtime.MemStats` TotalAlloc); build from the
  `n1k1-query` **`local-benchmark`** branch and pass `CBQ_LOCALBENCH=<binary>`.

(Measuring full-run matters: the CLI footer used to time only ExecOp, which for tiny
SQL is fine but excludes parse+plan — negligible for these file queries, but it would
mislead on a large inline literal. `Result.RunElapsed` fixes that at the source.)

## Two scenarios

- **files** — `orders`/`cust`, one JSON doc per file. Realistic, but **I/O-bound**:
  a filtered scan opens *every* file (the `os.Open` cost both engines pay), so wall
  time is close; the difference shows in allocations.
- **bulk** — a few docs each holding a large `items[]` array, driven by **UNNEST**.
  The volume lives *inside* documents, so file I/O is trivial and per-row execution
  dominates — this is where the engine/value-model gap shows in both time and memory.

## Run

    CBQ_LOCALBENCH=/tmp/localbench python3 bench.py     # both engines
    python3 bench.py                                    # n1k1 only
    NDOCS=50000 BULK_ITEMS=50000 REPS=15 python3 bench.py

Build the cbq runner once (in the fork worktree on branch `local-benchmark`):

    CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go build -o /tmp/localbench ./cmd/localbench

`gen.py` writes a deterministic dataset (orders one-per-file with `items` + `custId`;
a `cust` keyspace keyed by custId for `JOIN ... ON KEYS`; a `bulk` keyspace of a few
big-array docs). Defaults `NDOCS=20000`, `BULK_ITEMS=20000`, `REPS=11`.

## Results (representative, this machine)

    files: 3000 docs (one per file -- I/O-bound)
    query           n1k1 ms  cbq ms  x(t)   n1k1 MB  cbq MB   x(m)
    count+filter     481     511    1.06x     5.2    11.1    2.2x
    group+agg        481     486    1.01x     3.7    15.4    4.2x
    sort+limit       481     498    1.04x     3.6    17.9    5.0x
    join+group       470     497    1.06x     5.9    16.6    2.8x
    unnest-count     486     505    1.04x     4.1    25.9    6.2x

    bulk: 4 docs x 20000-elem arrays (UNNEST -- compute-bound)
    query           n1k1 ms  cbq ms  x(t)   n1k1 MB  cbq MB   x(m)
    unnest+group      62     524    8.40x    17.8   469.2   26.4x
    unnest+filter     41     311    7.50x    17.0   352.2   20.7x
    unnest+expr       60     378    6.29x    17.8   365.5   20.6x
    unnest+sort      125     604    4.85x    88.5   507.4    5.7x
    unnest+join      121     696    5.74x    74.8   522.5    7.0x

**files:** wall time ~1× (I/O-bound), but n1k1 allocates **2–6× less**.
**bulk:** with I/O out of the way, n1k1 is **5–8× faster** and uses **5–26× less
memory** — cbq boxes every array element into a `value.Value` (`SimpleUnmarshal`
+ map), while n1k1 UNNESTs and evaluates on the raw `[]byte`. `unnest+join` shows
the same holds for joins against a second keyspace.

## n1k1 stays native (no boxed fallback)

`EXPLAIN <query>` on every benchmark query shows **zero `⟨boxed⟩` markers** — all
project/filter/join/UNNEST expressions run on n1k1's native byte path, none fall
back to cbq's boxed `expression.Evaluate`. So the wins above are genuine
native-vs-boxed, not measurement artifacts.

## Why not `WITH`/CTE inline arrays?

An inline-array CTE (`WITH d AS ([{...},{...}]) SELECT ... FROM d`) is I/O-free, but
it does **not** isolate n1k1's advantage: (1) both engines share the *same cbq
parser*, and parsing a large array literal dominates the run (≈ equal); (2) n1k1
boxes a CTE/subquery FROM-source per row (`EXPLAIN` shows `⟨boxed source⟩`), same as
cbq. Measured fairly, the two come out ~even. Putting the bulk data *inside
documents* (the `bulk` scenario) is the correct I/O-free comparison — there both
engines read their native way and n1k1's byte path wins decisively.

## Files

- `gen.py`       — deterministic dataset generator (orders + cust + bulk).
- `bench.py`     — the driver (builds & drives the `n1k1` CLI; cbq via `CBQ_LOCALBENCH`).
