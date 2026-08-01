# DESIGN-vectors.md — embeddings & vector search in n1k1

_Last reviewed: 2026-08-01._

**Status: Phase 0 SHIPPED + Phase 1 largely SHIPPED + Phase 1.5 SHIPPED.** Phase 0 =
`VECTORIZE_BATCH` builtin + `@vectorize_field` macro + brute-force `VECTOR_DISTANCE` search (fake +
real-HTTP, cgo-free). Phase 1 = native byte-lane `VECTOR_DISTANCE`, a columnar float32 kernel over
an Arrow `List<float32>` column, an `INSERT INTO <name>.parquet` vector-column writer, and
computed-qvec lowering to the columnar fast path. Phase 1.5 = k-means centroids + IVF partitioning +
cluster census with NO engine changes (`examples/kmeans/` + two goja UDFs; see §"Phase 1.5"). Phase
2 (remote-source ingest, ANN-index cgo decision) not started. Companion to DESIGN-data.md
(keyspaces, extract), DESIGN-extensions.md (UDFs, macros), DESIGN-col.md (columnar/SIMD).

## Intent

Turn records (log lines, doc fields) into vectors with a **local, swappable** embedding model,
store them, and run **semantic / nearest-neighbour search** — all cgo-free (`CGO_ENABLED=0`) at
dev/debug scale (a cbcollect bundle, 10K–1M rows). Two sub-problems with opposite constraints:

1. **Embedding** (text → `float32[dim]`): heavy (~1–10 ms/item, best batched), external (model in
   ollama/llama.cpp/ONNX/OpenAI), deterministic in `(text, model)`. A cold, throughput-bound ETL
   transform — the opposite of n1k1's ~1 µs/row byte lane.
2. **Search** (distance + top-K): brute-force or an ANN index.

## Grounding facts

- **Distance is already solved, pure-Go, no new code.** cbq's `VECTOR_DISTANCE(field, query,
  metric)` / `APPROX_VECTOR_DISTANCE` evaluate through one pure-Go helper
  (`expression/func_vector.go vectorDistance()`): array iteration + float64 math, metrics
  `l2`/`l2_squared`/`cosine` (returns `1 − cosine_sim`, **lower = closer**)/`dot`. So there is **no
  COSINE_SIM to build** — reuse the grammar's function. `VECTOR_DISTANCE` = exact (Phase 0 uses
  it: honest full scan); `APPROX_VECTOR_DISTANCE` = the forward name for a future ANN tier
  (identical math today, no index).
  - ⚠ **Quirk:** the **first** operand must be a *field reference*, not an array literal (a
    planner/index-eligibility check); the query vector is the 2nd operand.
- ⚠ **FAISS is dark under `CGO_ENABLED=0`.** `go-faiss` is in the module graph (via bleve v2.6.1)
  but is cgo/C++; the cgo-free build gets bleve *text* FTS, **not** the FAISS vector index. The
  central constraint: **no ANN index without a cgo decision** (deferred, below).

## Principles

- **No grammar changes** (can't touch cbq's parser). Vectors are ordinary SQL++ arrays; `dim` is
  never in the SQL type (`VECTOR_DISTANCE` infers it from array length; the columnar side-file
  records the fixed width in its own metadata).
- **No optimizer magic.** Batching is expressed *explicitly* in SQL (GROUP-BY pages), sugared by a
  macro; `.macro expand` shows the honest SQL.
- **Compute-once, materialize.** Embeddings are deterministic → compute once, persist to a
  side-file, skip on re-run.
- **Model-agnostic.** An options object carries `endpoint` + `model` (ollama, llama.cpp-server,
  OpenAI, local ONNX all speak HTTP/JSON).
- **cgo-free at dev scale.** Brute-force distance over a columnar vector column (stored in the
  model's native element type); defer FAISS/ANN + its cgo cost to a later scale tier.

## Search performance — the measured story

Brute-force top-K via the existing function; no index, no cgo. The journey (M2 Pro, 100K × 384,
cosine top-10) settled the plan and confirmed the prize:

| path | time | vs boxed |
|---|---|---|
| jsonl + boxed (cbq re-boxes the 384-elem array into `value.Value` per row) | 11.2s | — |
| jsonl + native byte-lane (`engine/expr_vector.go`, `base.VectorDistanceVals`) | 6.8s | 1.6× |
| **columnar float32 / Parquet** (`base.VectorDistanceVFloat32`) | **~0.4–0.5s** | **~15–29×** |

Findings that shaped the design:
- **Boxing was the *allocation/GC* killer** (native eval cut allocs 100M→2M, ~50×) **but only
  ~1.6× wall-clock** — the residual on jsonl is dominated by **JSON number parsing** (~38M
  `strconv.ParseFloat` for 100K×384). So the headline win needs BOTH native eval AND columnar
  float32 (raw `[]float32`, no `ParseFloat`). Native-on-jsonl alone is the necessary *kernel* the
  columnar column then feeds.
- A pure-Go micro-bench of cosine over 100K×384 *contiguous float32* is **60ms** — the distance
  math is negligible; the new columnar floor is **Parquet decode** (~0.4s for a 163MB file), not
  JSON parse. The bottleneck moved to the reader (attacked separately — DESIGN-benchmark.md arrow
  read-floor).
- ⚠ **The native-JSONL lane only triggers with a literal/const query vector.** A `WITH`-alias /
  `$param` qvec doesn't lower through `ExprTreeOptimize` (boxed scope reference), so on the *jsonl*
  path the whole call falls back to boxed. (Results identical either way; only speed differs.) The
  **columnar Parquet path handles a `WITH`/`$param` qvec** by evaluating it ONCE (row-independent
  hoist) and feeding the same float32 kernel — as fast as a literal there (computed 507ms ≈ literal
  543ms vs 18.6s on the row lane).
- Correctness: native == boxed via a differential test (`glue.TestVectorDistanceNativeMatchesBoxed`,
  toggle `EnableNativeVectorDistance`) across all metrics + edge cases — it caught a real `-0.0` vs
  `0` divergence (dot of orthogonal vectors), now normalized.

## Columnar vector column — mechanism (SHIPPED)

`base.VectorDistanceVFloat32` (base/vector_v.go) is the vectorized byte-lane core (the
`sum_v_float64` analog): maps N packed float32 vectors + a query → N distances. It reads the column
via `base.VecFloat32`, an **`unsafe` zero-copy reinterpret of the borrowed little-endian arrow
buffer as `[]float32`** (no copy, aliases the page — ⚠ **LE host assumed**). Storage is float32 (no
`ParseFloat`) but accumulation promotes to float64, so results are **bit-identical** to the scalar
path (`TestVectorDistanceVFloat32MatchesVals`).

End-to-end pieces: (a) reader `records.VectorBatchSource` borrows the vec list column's contiguous
float32 child + offsets + scalar side cols; (b) executor `glue.VectorColumnarScan` computes
distances then TRANSPOSES to rows so the existing row-lane `order-offset-limit` does the top-K (no
new order/limit op); (c) the conv rewrite `maybeVectorColumnarFuse` fuses
`project(VECTOR_DISTANCE over a parquet vec)+scan` into a `vector-distance-columnar` op when the
qvec is a constant OR a row-independent expression (`WITH` alias / `$param` / `VECTORIZE_BATCH(...)`,
evaluated ONCE) and passthroughs are provably unread; (d) the `INSERT INTO <name>.parquet` writer
`glue.parquetWriter`. Also fixed a pre-existing bug: parquet projection pushdown dropped nested
(list) columns (leaf `vec.list.element` ≠ `vec`).

> **The native unboxed distance (`base.VectorDistanceVals`) takes TWO arbitrary vectors — drop
> cbq's operand restrictions.** cbq requires operand-0 = field ref and the qvec = static: those are
> vector-index eligibility constraints, meaningless for the brute-force path we have. A native n1k1
> distance is pure compute → accept **any two vector expressions** (two fields, two params, two
> computed values), composing more freely than the boxed builtin.

### The vec column type + nullability (⚠ subtle — `TestVecParquetNullContract`)

The column is a variable `List<float32>` with a **NON-nullable element** and a **NULLABLE list
field**:
- ⚠ Parquet has **NO fixed-size-list type** — a `FixedSizeList` round-trips as a variable `List`
  anyway, and pqarrow can't even WRITE a `FixedSizeList` null. So the writer emits a variable
  `List<float32>`.
- ⚠ **Nullability is two independent levels.** The ELEMENT (each coord) must be non-nullable —
  declaring it nullable makes the def-level round-trip mark **every coord NULL** (whole column reads
  back null). The LIST FIELD is nullable — a row's vec is NULL when the source text is
  MISSING/non-string. That row-null is stored natively by the validity bitmap as a **zero-length
  list** — no sentinel vector, no wasted `dim` floats, no fragile "no model emits all-zeros" bet.
- ⚠ **Borrow implication with nulls:** null rows don't advance the offset, so the child buffer holds
  ONLY present vectors (still one contiguous zero-copy `[]float32`). No-null case keeps regular
  offsets `0,dim,2dim,…` (tight `r*dim` fast path); the null case indexes each row via list OFFSETS
  (`child[offs[r]:offs[r+1]]`), yielding NULL for zero-length rows.
- The doc id IS a matching column: the writer emits `id` (the SELECT's KEY / `META().id`)
  row-aligned with `vec`, so a top-K result maps back to documents by reading `id` at the winning
  row indices.

### Writer scope (v1)

A vector-shaped writer, not general JSON→Parquet: flat scalars (number→INT64/DOUBLE, string→UTF8,
bool→BOOLEAN) + a numeric array→`list<float32>`. Strings are first-class typed UTF8 columns
(`WHERE`/`GROUP BY`-queryable). ⚠ **First-row-defines-schema is STRICT:** the first row must carry
every column; a later row with an extra field, conflicting type, or non-numeric vec element ERRORS
(never a silent coerce/drop); a later row missing a field writes NULL. Complex values (nested
objects, non-numeric/nested arrays) error today. *Deferred:* Parquet VARIANT, or stringify-to-UTF8
— neither needed for the vector use case.

## Embedding — a batched callout at ingest, materialized once

**Not** the extract hot-loop and **not** a per-row scalar UDF with fork-per-row. Instead a scalar
**`VECTORIZE_BATCH(array, opts) → array`** (array of texts → parallel array of vectors): **one goja
call + one model round-trip per batch**, backed by a native pure-Go `http.post` to `opts.endpoint`
(`{"endpoint":"http://localhost:11434/api/embed","model":"nomic-embed-text"}`). A `{"fake":true}`
mode returns deterministic pseudo-vectors (hash → unit vector) so the whole pipeline is testable
with **no model and no network** — the key de-risk (and the CI default, since `make test-all` has no
ollama dependency).

**Batching without magic (explicit GROUP-BY pages):** `ARRAY_AGG` of `{id,text}` **objects** (not
two parallel arrays) keeps id/text/vec glued; page size is a user literal (`FLOOR(t.pos/64)`); the
**`@vectorize_field(ks, field => line, batch => 64, opts => {…})`** macro sugars the wall. The
load-bearing plumbing is VERIFIED: `UNNEST` works over a *computed* `ARRAY_AGG` array, the
`GROUP BY page → ARRAY_AGG({id,…}) → UNNEST` round-trips, and paging via `ROW_NUMBER()`
(`FLOOR((rn-1)/N)` for 0-based) or `_meta.pos` works — so the only new code was `VECTORIZE_BATCH`.

Worked example — ingest a text keyspace into a columnar Parquet vec file, then search it (`ollama
pull nomic-embed-text` first; verified end-to-end against a real model, 768-dim, real semantic
ranking):

```sql
-- ingest: embed `line`, materialize a columnar Parquet vec keyspace
INSERT INTO `vecs/data.parquet` (KEY UUID(), VALUE self)
SELECT r.id, r.vec
  FROM @vectorize_field(logs, field => line, id => id, batch => 64,
       opts => {"endpoint":"http://localhost:11434/api/embed","model":"nomic-embed-text"}) AS r;
-- search: embed the query the same way -> top-5 (columnar fast path; qvec computed once)
WITH q AS (VECTORIZE_BATCH([{"text":"disk full"}],
       {"text":"text","endpoint":"http://localhost:11434/api/embed","model":"nomic-embed-text"})[0].vec)
SELECT v.id, VECTOR_DISTANCE(v.vec, q, "cosine") AS d
  FROM vecs v ORDER BY d ASC LIMIT 5;
```
Over `.parquet` the once-computed `q` + kept scalar columns (numeric OR string) take the columnar
fast path; only the vec itself is the list column.

**Field-tested gotchas (n1k1-for-ai team feedback, 2026-08 — each failed QUIETLY; also in
`.help vectorize` and the macro's header):**
- ⚠ **ollama endpoint choice changes vector NORMS.** `/api/embed` (the batch API n1k1 targets)
  returns **unit-normalized** vectors (norm 1.0); the older `/api/embeddings` returns raw
  magnitudes (norm ~23 observed). Nothing downstream errors — but anything assuming
  euclidean ≈ cosine (the kmeans pipeline, dedup-by-distance) quietly becomes about MAGNITUDE.
  `examples/kmeans/kmeans.py fit` now samples norms and warns when they aren't ~1.
- ⚠ **Batch size can crash the model server.** `batch => 256` of ~1.5KB prompts made a local
  ollama reset connections (`/tokenize: connection reset by peer`); 64 was stable. The macro's
  default is now **64** — raise it only after the endpoint proves it out.
- ⚠ **`@vectorize_field` output is `{id, text, vec}`** — the embedded text is ALWAYS named
  `text`, never the source field's name. `SELECT r.line AS txt` yields MISSING, SQL++ silently
  drops the missing column from the projection, and the materialized parquet simply lacks it (no
  error at INSERT, none at query). Check a materialized target directly:
  `SELECT * FROM <target> LIMIT 1`. (`kmeans.py census --text-field` now fails loud when the
  named field has no valued rows.)

## Vector element types (float32 / float64 / int8·int16 / float16 quantized)

Models emit different numeric types; some **quantize** to int8/int16/float16 to shrink ~4×.
**Non-issue for Phase 0 correctness** — a vector is just a SQL++ array of numbers, and
`VECTOR_DISTANCE` promotes every element to float64, so int8/float32/float64 arrays all "just work"
through the boxed path.

It matters for **storage/perf (Phase 1 columnar)**: ⚠ **store the model's emitted type as-is — do
NOT up-convert to float32.** Up-converting an int8 column would 4× the file and throw away the whole
point of quantizing. The side-file records an element-type tag next to `dim`; the native kernel has
per-type variants or dequantizes/promotes at read (fp32 accumulation standard). ⚠ A properly
quantized model often ships a **scale/offset** (dequantization) — carry it in file metadata and
honor it in the kernel (a Phase-1+ nuance; raw-integer distance preserves NN *ranking* well enough
for a first cut, so it doesn't block anything).

⚠ **The type travels as METADATA, not through the value layer.** SQL++/JSON has no int8/float16 — a
vector rides the value layer as float64-boxed numbers, so the element type can't flow as a Go typed
slice. `VECTORIZE_BATCH` is the only component that saw the model's response, so it **reports
`dtype` (+ `dim`, + `scale`/`offset`)** in its return envelope; the writer packs the column in the
declared type, down-casting at write. ⚠ To *preserve* quantization, `VECTORIZE_BATCH` must emit the
**raw integer codes + scale/offset**, not dequantized floats. ⚠ **Do not infer the type from
values** (a float model with integer-valued outputs would be mis-typed) — use the reported `dtype`
or an explicit option.

**Who cracks the API response encoding — `VECTORIZE_BATCH`, always (it's transport, not a value
type).** Embedding APIs may return `encoding_format:"base64"` (raw dtype bytes, base64'd) or
bit-packed integer arrays. `VECTORIZE_BATCH` decodes it (IMPLEMENTED: auto-detects base64-LE-float32
vs a JSON float array per embedding, sends an `encoding` hint; bit-packed integers ride the number
path), so nobody downstream sees it. The representation it decodes to is phase-separated: **Phase
0** decodes → a plain SQL++ numeric array (composable, jsonl storage); **Phase 1** carries raw typed
bytes + dtype end-to-end (a cbq `binaryValue`): `VECTORIZE_BATCH` emits typed bytes → the writer
memcpy-stores them (no re-encode) → the native kernel reads them directly → a boxed consumer that
chains more SQL gets a **lazy decode** to a float64 array on demand. Hot store+search never boxes;
only ad-hoc chaining pays the decode.

**No representation flag on `VECTORIZE_BATCH`.** Model-request knobs go in `opts` (YES): `dimensions`
(MRL truncation), an output-dtype request, an `input_type`/`task` prefix (nomic-embed *requires*
one), normalization — forwarded, with the resulting `dtype`/`dim` reported back; the wire
`encoding_format` is internal. But the n1k1 output *representation* (array vs typed-bytes) is
resolved by the *consumer* (byte-lane↔boxed boundary), NOT a caller flag — so the signature stays
stable `VECTORIZE_BATCH(texts, opts)` across Phase 0→1; only the under-the-hood value evolves.

## Storage, caching, observability

- **Side-file:** a columnar/Parquet vec keyspace (`dim` in the file metadata); reuses the
  Parquet-keyspace work. Vectors are usually far smaller than the source text.
- **Caching mostly already exists.** `INSERT INTO <file>` default mode `"new"` opens
  `O_CREATE|O_EXCL` and **errors if the target exists** (`glue/insert.go`), so the cache check is
  free: the macro/CLI names the destination by a **config-address** of its args
  (`vecs/<source>.<model>.<version>.jsonl`, computable at expand time), and INSERT's own
  error-if-exists *is* the skip. A macro can't touch the FS but doesn't need to. For a data-level
  address ("the source changed"), cbq's existing `HASHBYTES(value,"sha256")` is available (no new
  UDF).
- **Progress:** embedding is the slow step (ms/row), so `.stats on` draws a live per-operator counter
  tree (stderr, updates during the run via `Session.OnStats` at scan checkpoints; see
  DESIGN-stats.md). Caveats: (1) it's a count/throughput readout, **not a %-complete bar** (no total
  denominator wired, though the pieces exist); (2) progress advances **per checkpoint, not during an
  in-flight model call** — a blocking `VECTORIZE_BATCH` HTTP round-trip stalls the footer for that
  batch, so smaller batches give smoother progress (a tuning knob).

## The cgo fork in the road (deferred)

An ANN index at ~10M+ vectors needs one of: (a) an opt-in `CGO_ENABLED=1` FAISS build variant
(breaks the single pure-Go binary), (b) a **pure-Go HNSW** library (cgo-free, slower build), or (c)
a sidecar index process. **Decide later** — brute-force ships first and covers dev/debug scale.
**Phase 1.5 below defers it further**: IVF partitioning gives mid-scale ANN in pure Go with no new
index format at all.

## Phase 1.5 — k-means centroids, IVF partitioning, cluster census (no engine changes)

**Status: SHIPPED as `examples/kmeans/` (driver + SQL++ idioms + two goja UDFs) — engine untouched.**
The question it answers: given vectors in Parquet, can we compute centroids (k-means) to (a)
*partition* for faster search and (b) *summarize* the dataset — and how much of that is expressible
in SQL++ today?

**Prior art (this is well-trodden ground; we're composing, not inventing):**
- *Partitioning by centroids* is the **IVF (inverted file) index**: pgvector's `ivfflat` (k-means at
  build, `probes` at query — inside a SQL database), FAISS `IVFFlat`, **Lance/LanceDB `IVF_PQ`**
  (the closest cousin: a columnar file format with centroid-pruned scans), Milvus, SPANN.
- *k-means in SQL* is academic lineage: Ordonez (KDD'04/TKDE'06), Apache MADlib, BigQuery ML's
  `CREATE MODEL … kmeans`. Assignment = per-row argmin; update = per-cluster `GROUP BY` mean;
  the iteration loop lives in a driver.
- *"What's in the dataset" via centroids* is cluster-based corpus summarization: BERTopic/top2vec
  (cluster embeddings, label clusters, exemplar docs), Nomic Atlas (embedding maps), SemDeDup
  (dedup within k-means clusters by centroid distance), DINOv2-era hierarchical k-means curation.
  "Docs nearest the centroid" (exemplars/medoids) beats random sampling for eyeballing a cluster.

**What shipped** (`examples/kmeans/kmeans.py` — stdlib-only driver, every data-plane step a SQL++
statement through the stock CLI; `gen_vectors.py` — synthetic topic-clustered demo data;
`extensions/functions/vector_nearest{,_dist}.js` — optional goja argmin UDFs):
`fit` (farthest-first init + Lloyd iterations → `centroids.json`), `partition` (per-cluster
`part_<k>/data.parquet` + a queryable `centroids/` keyspace), `census` (sizes + per-centroid
exemplars via `VECTOR_DISTANCE` + per-cluster `TOKENS()` terms), `probe` (nprobe-partition scan,
recall + timing vs brute). The runbook (incl. flags, idioms, gotchas) is `examples/kmeans/README.md`.

**Expressibility verdict — SQL++ can do all of it, with three measured caveats** (20K×64-d, k=8,
M2 Pro):
- ⚠ **`VECTOR_DISTANCE` refuses a non-STATIC query vector** (cbq-inherited validation: constant /
  `$param` / `WITH` alias only), so the k-means argmin *cannot* use it — a comprehension variable
  over the centroid list is rejected. Workarounds that ship: pure-SQL++ arithmetic argmin
  (`ARRAY_SORT` over `[dist, i]` pairs) and the `vector_nearest()` UDF (~4× faster; its boundary
  cost scales with the K×dim argument re-converted per row, ~0.3ms/row at 8×64).
- ⚠ **Derived tables flatten**: a subselect-computed assignment is re-evaluated per (doc, position)
  under an `UNNEST` — dim× the work (18s → 1.4s when restructured; still pays the boxed lane).
  The all-in-one-statement Lloyd (`fit --pure-sql`) is therefore the *expressibility demo*; the
  practical fit does the assignment scan in SQL++ and the tiny K×dim mean in the driver, and
  `partition` materializes the assignment through a real temp Parquet keyspace.
- The UNNEST-by-position update (`GROUP BY cl, p` over N×dim rows) runs the boxed row lane at
  ~3.5µs/(doc·dim) — fine for sampled fits (sampling is standard; pgvector trains on a sample),
  slow for full-data fits at scale.

**The IVF payoff composes with the columnar kernel** (partitions ARE parquet vec files): probe
top-10 at nprobe=2/8 ≈ 0.12s vs 1.9s row-lane brute (recall 1.00; nprobe=1 → 0.90). Honest framing:
at ≤100K vectors a SINGLE parquet file + columnar brute force (~0.08s at 20K; ~0.4–0.5s at
100K×384) is already the answer — partitioning pays when the full-file decode exceeds the latency
budget (millions of vectors), by decoding only `nprobe/K` of the bytes. **This resolves the cgo
fork at mid scale**: no FAISS, no HNSW graph, no new index format — parquet files + a centroids
keyspace.

**Engine seams this motivates (the Phase 1.5 → 2 follow-ups, in value order):**
1. **`VECTOR_AVG(vec)` native aggregate** (rides the existing `RegisterAggregate` ext-agg seam like
   sparkline/histogram): kills the UNNEST-by-position update AND the driver-side mean — full-data
   Lloyd becomes one fast scan per iteration.
2. **Native argmin**: either lift the static-qvec restriction on n1k1's native `VECTOR_DISTANCE`
   lane (it's pure compute — the restriction is a vector-index eligibility rule, meaningless here)
   or add `VECTOR_NEAREST(vec, cents)` as a native function; batch-friendly on the columnar lane.
3. Later: an `nprobe` query rewrite (centroid scan → partition pruning inside one statement),
   auto-partitioned `INSERT` (one pass instead of K filtered copies), and per-cluster PQ/quantized
   codes if N ever demands true IVF_PQ.

## Phased plan

- **Phase 0 — DONE.** `VECTORIZE_BATCH(batch, opts)` (`glue/vectorize.go`: offline deterministic by
  default, real embeddings via pure-Go `net/http` POST to an `endpoint`) + the `@vectorize_field`
  macro (GROUP-BY-page + `ARRAY_AGG` + `UNNEST`). Vectors are plain float64 arrays here. Tests
  `glue/vectorize_test.go` (fake + stub-HTTP).
- **Phase 1 — largely DONE.** base64/bit-packed response decode; content-addressed caching via
  `HASHBYTES` + INSERT-errors-if-exists; native unboxed `VECTOR_DISTANCE` (`base.VectorDistanceVals`,
  differential-verified); the columnar float32 column + kernel (`base.VectorDistanceVFloat32` /
  `VecFloat32` over `VectorBatchSource`, via `VectorColumnarScan` / `maybeVectorColumnarFuse`,
  `glue/vector.go`), its prerequisite `INSERT INTO <name>.parquet` writer, and computed-qvec lowering
  to the columnar fast path.
- **Phase 1.5 — DONE (no engine changes).** k-means centroids + IVF partitioning + cluster census as
  `examples/kmeans/` (SQL++ scans through the stock CLI, optional `vector_nearest{,_dist}` goja
  UDFs) — see the §"Phase 1.5" section above for findings + the engine seams it motivates
  (`VECTOR_AVG` ext-aggregate, native argmin, an nprobe rewrite).
- **Phase 2 — not started.** Remote-source ingest (S3/Box/Drive/HF → local vec side-file), then the
  ANN-index cgo decision only if N demands (mid-scale ANN now covered by Phase 1.5 IVF).

## Future: signal-preserving preprocessing (log templating / dedup)

Raw log lines are dominated by boilerplate (timestamps, hosts, PIDs) and are highly templated —
embed the raw line and the cruft dominates the vector. Later-phase direction, riding existing seams:
extract the `msg` field first (`*.extract.js`, strips boilerplate before embedding — most of the
win); sample→learn→transform (reuse the index advisor's sampling seam to learn low-signal tokens via
IDF / Drain-style template mining, then normalize per row — extract's describe/apply split); and
**dedup by template** (`SELECT DISTINCT normalize(line)` → embed only the distinct set → join each
row back to its template's vector — a quality *and* cost win, riding GROUP BY / DISTINCT +
compute-once/cache).

Prior art: cbq/Couchbase provides the vector *index+search* (bleve+FAISS), embedding is BYO;
DuckDB `FLOAT[N]` + `array_cosine_similarity` + `vss` HNSW; pgvector + ivfflat/hnsw; **LanceDB**
(columnar file + an embedding-function registry applied at write) is the closest model, doable
cgo-free.
