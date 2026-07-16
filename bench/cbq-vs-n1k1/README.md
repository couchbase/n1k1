# cbq-vs-n1k1 benchmark

Apples-to-apples head-to-head over the **same directory of `*.json` files** — the
classic cbq file-datastore layout `<root>/<namespace>/<keyspace>/<key>.json` that
**both** engines read. Both use cbq's parser+planner (so the plan is identical);
what differs is the execution engine.

## Run

    python3 bench.py                 # n1k1-only columns (time + allocations)
    NDOCS=20000 REPS=25 python3 bench.py
    N1K1=/path/to/n1k1 python3 bench.py   # skip building n1k1

`gen.py` writes a deterministic dataset (seed 42) and is idempotent. `bench.py`
builds the n1k1 CLI if `N1K1` isn't given, generates the data, and reports the
**warm median** (first few reps dropped) per query.

## The `cbq` column (the real head-to-head)

Point `CBQ_URL` at a real cbq `/query/service` endpoint **loaded with the same
data**, and `bench.py` adds a `cbq` column (median server-side
`metrics.executionTime`) plus a `cbq/n1k1` ratio:

    CBQ_URL=http://127.0.0.1:8093/query/service CBQ_CREDS=Administrator:password \
      python3 bench.py

### Why there's no auto-launched cbq here

Getting cbq's *executor* to run over a bare local dir in this repo is blocked two
ways (both investigated):

1. **Standalone `cbq-engine -datastore dir:PATH`.** The shipped 7.6.2 binary
   (`/Applications/Couchbase Server.app/.../bin/cbq-engine`) starts, but its
   **query HTTP service never binds** without cbauth — it loops on
   `cbauth environment variable CBAUTH_REVRPC_URL is not set` (metakv/FTS settings
   init). No flag bypasses it. Older (6.x/early-7.x) builds ran standalone; 7.6.x
   is coupled to cluster auth.

2. **Building cbq's `test/filestore` harness from source** (cbq's own no-cluster,
   stub-config query harness — the ideal in-process cbq column). Blocked by the
   build graph: the `server`/`execution` packages require the closed **EE modules**
   (`eventing-ee`, `query-ee`, `regulator`, `plasma`, `bhive`, `gocbcrypto`), which
   are **empty** in both the `n1k1-query` fork's siblings (no `go.mod`) and the
   partial `couchbase-server.master` checkout. Stubbing their `go.mod` gets past the
   module graph but then needs `go mod tidy` + **real** `regulator`/`eventing`/
   `plasma` symbols (server/execution import them for real) + cgo — i.e. a full
   ee-stubs bootstrap, not a quick hack.

### How to enable the `cbq` column

Easiest is a **live Couchbase** (already installed here):

1. Start Couchbase Server, initialize the cluster, create a bucket `orders-bench`.
2. Load the same rows (`gen.py` writes `<data>/default/orders/*.json`; upsert those
   docs into the bucket, doc-key = filename stem).
3. `CREATE PRIMARY INDEX ON \`orders-bench\`;`
4. Run with `CBQ_URL=http://127.0.0.1:8093/query/service CBQ_CREDS=user:pass`
   (adjust the queries' keyspace name to the bucket, or alias it).

Or run a **cbauth-enabled / older cbq-engine** over `dir:<root>` and point
`CBQ_URL` at it.

## Reading the results

- **`n1k1 ms`** — warm median wall time per query (REPL footer; execution phase).
- **`n1k1 MB/query`** — allocated bytes per query (`-stats` runtime line). This is
  the number that exposes the value-model difference: n1k1's byte engine allocates
  little; cbq's boxed `value.AnnotatedValue` model allocates per row/field.
- Absolute times are **I/O-bound** for the one-doc-per-file layout (a filtered scan
  opens every file; ~common to both engines), so the interesting signal is the
  ratio and the allocations, not raw wall time.

## Caveat on `N1K1_FETCH_CBQ`

An earlier idea — force n1k1 through cbq's `keyspace.Fetch` (boxed
`value.AnnotatedValue`) via `N1K1_FETCH_CBQ=1` as a cbq-value-model proxy — was
tried and **rejected**: that switch only affects the nested-loop **Fetch** op, and
n1k1's native records-scan + per-request fetch cache mean these queries don't
diverge measurably. It is not a substitute for a real cbq engine.
