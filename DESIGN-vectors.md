# DESIGN-vectors.md — embeddings & vector search in n1k1

_Status: design / not started. Companion to DESIGN-data.md (keyspaces, extract),
DESIGN-extensions.md (UDFs, `*.stream.js`, macros), DESIGN-col.md (columnar/SIMD)._

## Intent

Let a user turn records (log lines, doc fields, …) into vectors with a **local,
swappable** embedding model, store them, and run **semantic / nearest-neighbour
search** — all in n1k1's pure-Go, `CGO_ENABLED=0` world, at dev/debug scale (a
cbcollect bundle: 10K–1M rows). Two distinct sub-problems with opposite constraints:

1. **Embedding** (text → `float32[dim]`): heavy (~1–10 ms/item, best batched), external
   (model lives in ollama/llama.cpp/ONNX/OpenAI), deterministic in `(text, model)`. A
   **cold, throughput-bound ETL transform** — the opposite of n1k1's ~1 µs/row byte lane.
2. **Search** (distance + top-K): either brute-force or an ANN index.

## Grounding facts (verified 2026)

- **Distance is already solved, pure-Go, no new code.** cbq's `VECTOR_DISTANCE(field,
  query, metric)` and `APPROX_VECTOR_DISTANCE(...)` both evaluate through one pure-Go
  helper (`expression/func_vector.go` `vectorDistance()`): array iteration + float64 math,
  metrics `l2`/`l2_squared`/`cosine` (returns `1 − cosine_sim`, **lower = closer**)/`dot`.
  n1k1's boxed evaluator runs it out of the box:
  `SELECT t.id FROM ks t ORDER BY VECTOR_DISTANCE(t.v, [1,0,0], "cosine") ASC LIMIT 5`
  works **today** (verified: a=0, near=0.006, orthogonal=1). So there is **no COSINE_SIM
  to build** — reuse the grammar's function.
  - Quirk: the **first** operand must be a *field reference*, not an array literal
    (a planner/index-eligibility check); the query vector is the 2nd operand. That matches
    the real shape (stored vec field vs query vec).
  - `VECTOR_DISTANCE` = exact; `APPROX_VECTOR_DISTANCE` = the index-backed name (identical
    math today, since we have no ANN index). Phase 0 uses **VECTOR_DISTANCE** (honest:
    exact, full scan); `APPROX_` is the forward name for a future ANN tier.
- **FAISS is dark under `CGO_ENABLED=0`.** `go-faiss` is in the module graph (via bleve
  v2.6.1) but is cgo/C++; n1k1's cgo-free build gets bleve *text* FTS, **not** the FAISS
  vector index. This is the central constraint: **no ANN index without a cgo decision.**

## Principles

- **No grammar changes** (can't touch cbq's parser). Vectors are ordinary SQL++ arrays;
  `dim` is never in the SQL type — `VECTOR_DISTANCE` infers it from array length; the
  columnar side-file records the fixed width in its own metadata.
- **No optimizer magic.** Batching is expressed *explicitly* in SQL (GROUP-BY pages), not
  hidden in an operator. A macro sugars it; `.macro expand` shows the honest SQL.
- **Compute-once, materialize.** Embeddings are deterministic → compute once, persist to a
  side-file, skip on re-run. Essential for large/remote sources (S3/Box/Drive/HF).
- **Model-agnostic.** No hardcoded ollama. An options object carries `endpoint` + `model`;
  swap freely (ollama, llama.cpp-server, OpenAI, local ONNX all speak HTTP/JSON).
- **cgo-free at dev scale.** Brute-force distance over a columnar vector column (stored in
  the model's native element type); defer FAISS/ANN (and its cgo cost) to a later scale tier.
- **Reuse existing lanes:** the vector functions above, GROUP BY / `ARRAY_AGG` / `UNNEST`,
  the columnar/SIMD lane, `INSERT INTO <file>`, the extension + macro registries.

## Search (Phase 0 — free)

Brute-force top-K via the existing function; no index, no cgo, tens of ms at bundle scale:
```sql
SELECT t.id, t.line FROM ks t
ORDER BY VECTOR_DISTANCE(t.v, $qvec, "cosine") ASC LIMIT 10;
```
Perf later: port `vectorDistance` to the native columnar float32/SIMD lane (DESIGN-col).
ANN index much later, only if N forces it (see cgo decision).

**Boxing / "recycled box" (Phase 1, not Phase 0).** Phase 0 goes through cbq's *boxed*
evaluator: each row parses its stored vector field into a `value.Value` array, and
`vectorDistance` touches ~`dim` boxed `value.Value` elements per row (`.Index(i)` →
`float64`). Fine for correctness, garbage-heavy at scale. But a "recycled cbq `Value`
backed by a reusable `[]float32`" is **not available** — cbq's value model has no native
`[]float32` array (`value/doc.go`: float32 "not used"; arrays are `[]interface{}` of
float64-boxed numbers). So the win is not *recycling a box* but **skipping the box**: store
the vector column as raw bytes in the model's **native element type** (see below — do NOT
up-convert int8/float16 to float32), and run a native distance kernel over a borrowed
`[]byte`→typed-slice view with a **reused** query-vector buffer + scalar accumulators (no
per-row `value.Value`, SIMD-friendly). That is the DESIGN-col native port — the right home
for the reuse instinct.

## Embedding — a batched callout at ingest, materialized once

**Not** the extract hot-loop (native/cheap) and **not** a per-row scalar UDF with
fork-per-row. Instead a scalar **`VECTORIZE_BATCH(array, opts) → array`** (array of texts →
parallel array of vectors): **one goja call + one model round-trip per batch**. Backed by
a native pure-Go `http.post` (no curl subprocess) to `opts.endpoint`, e.g.
`{"endpoint":"http://localhost:11434/api/embed","model":"nomic-embed-text"}`. A
`{"fake":true}` mode returns deterministic pseudo-vectors (hash → unit vector) so the whole
pipeline is testable with **no model and no network** — the key de-risk.

### Batching without magic (explicit GROUP-BY pages)

```sql
INSERT INTO vecs
SELECT u.id, u.vec FROM (
  SELECT VECTORIZE_BATCH(g.batch, {"model":"…"}) AS vecs, g.batch
  FROM ( SELECT ARRAY_AGG({"id":t.id,"text":t.line}) AS batch, FLOOR(t.pos/256) AS page
         FROM ks t GROUP BY FLOOR(t.pos/256) ) g
) e UNNEST e.vecs AS u;
```
- `ARRAY_AGG` of `{id,text}` **objects** (not two parallel arrays) keeps id/text/vec glued.
- Page size (256) is a user-controlled literal. Last page = leftover rows.
- **`@vectorize_field(ks, field => line, model => "…", batch => 256)`** macro sugars this wall.
- **Load-bearing plumbing — VERIFIED (2026):** `UNNEST` works over a *computed*
  `ARRAY_AGG` array (not just a stored field); `GROUP BY page → ARRAY_AGG({id,…}) → UNNEST`
  round-trips; paging via `ROW_NUMBER()` works (use `FLOOR((rn-1)/N)` for 0-based pages),
  or `_meta.pos` for extracted docs. So every existing-engine piece of this shape is
  proven — the only new code is `VECTORIZE_BATCH` itself.

## Vector element types (float32 / float64 / int8·int16 quantized / float16)

Models emit different numeric types: float32 (typical), float64, or **quantized**
int8/int16/float16 (some models quantize to shrink a vector ~4×). This is a **non-issue for
Phase 0 correctness**: a vector is just a SQL++ array of *numbers*, and `VECTOR_DISTANCE`
promotes every element to float64 for the math (embedding values always sit within float32
range), so int8, float32, and float64 arrays all "just work" through the same boxed path,
no special-casing.

It matters only for **storage/perf (Phase 1 columnar)**, and the rule there is: **store the
model's emitted type as-is — do NOT up-convert to float32.** The Parquet/columnar side-file
records an **element-type tag** next to `dim` and keeps the bytes the model returned:
`float32` (4 B/dim, typical), `float16` (2 B/dim), `int16` (2 B/dim), `int8` (1 B/dim — a
4×-smaller quantized column). Up-converting an int8 column to float32 would 4× the file and
throw away the whole point of the model quantizing. The native distance kernel then either
has a per-type variant (`int8·int8`, `f32·f32`) or dequantizes/promotes to a common
precision at read (fp32 accumulation is standard) — the *stored* representation stays
native. Parquet expresses these as a FIXED_SIZE_LIST/FIXED_LEN_BYTE_ARRAY of the right
element type. NOTE: a properly quantized model often ships a **scale/offset**
(dequantization) or pre-normalizes; carry that in the file metadata and honor it in the
kernel — a Phase-1+ nuance (raw-integer distance preserves NN *ranking* well enough for a
first cut, so it doesn't block anything).

**How the type reaches the file (it travels as METADATA, not through the value layer).**
SQL++/JSON has no int8/float16 — a vector rides the value layer as an array of
*float64-boxed numbers*, so the element type cannot flow as a Go typed slice through the
query. `VECTORIZE_BATCH` is the only component that saw the model's response, so it
**reports `dtype` (+ `dim`, + `scale`/`offset` for quantized models) in its return
envelope**; the columnar/Parquet writer reads that and packs the column in the declared
type, down-casting the float64-boxed numbers at write (safe — the model already produced
in-range values). To *preserve* quantization, `VECTORIZE_BATCH` must emit the **raw integer
codes + scale/offset**, not dequantized floats, or the compact form is lost before the
writer sees it. Do **not** infer the type from values (a float model with integer-valued
outputs would be mis-typed) — use the reported `dtype` or an explicit INSERT/macro option.
(A jsonl Phase-0 side-file is text JSON and can't hold compact types at all; native packing
is inherently a Phase-1 columnar/Parquet concern.)

**Who cracks the API response encoding — `VECTORIZE_BATCH`, always (the transport only).**
Embedding APIs may return `encoding_format:"base64"` (raw dtype bytes, base64'd, to save
bandwidth) or bit-packed integer arrays, with metadata (format/dtype/dim) saying how to
decode. Base64/bit-packing is **transport, not a value type** — nobody downstream should see
it, so `VECTORIZE_BATCH` decodes it, and the model-specific response shape lives in the
endpoint-configured extension, in one place (swap models = swap the decoder). The *representation
it decodes to* is phase-separated, and reconciles composability with raw-bytes efficiency:
- **Phase 0:** decode → a plain SQL++ **numeric array**. Composable (chain more SQL; reuse the
  free array-based `VECTOR_DISTANCE`); jsonl storage. The compact-bytes optimization isn't
  lost, just not yet applicable.
- **Phase 1:** decoding to a float64 array then re-packing for Parquet is a wasteful round-trip
  through the float64-boxed value layer. Instead carry the vector as **raw typed bytes + dtype**
  (a cbq `binaryValue`) end-to-end: `VECTORIZE_BATCH` emits typed bytes → the columnar/Parquet
  writer memcpy-stores them (no re-encode) → the **native `VECTOR_DISTANCE` port** reads them
  directly → a boxed consumer that chains more SQL gets a **lazy decode** to a float64 array on
  demand. So the hot store+search path never boxes; only ad-hoc chaining pays the decode —
  pro-SQL++ composability AND raw-bytes storage coexist. (Constraint: the free `VECTOR_DISTANCE`
  wants an `ARRAY`, so the typed-bytes fast path rides the *same* native `VECTOR_DISTANCE`
  columnar/SIMD port already planned for perf — it now also serves zero-round-trip storage.
  For decode perf, do the HTTP+decode in a Go host helper so goja never crunches big byte arrays.)

## Storage & caching

- **Side-file:** materialize vectors as a columnar/Parquet keyspace (fixed-width
  `float32[dim]`; `dim` in the file's metadata, discovered at write or declared). Reuses
  the Parquet-keyspace work. Vectors are usually far smaller than the source text.
- **Caching mostly already exists.** `INSERT INTO <file>` default mode `"new"` opens with
  `O_CREATE|O_EXCL` and **errors if the target already exists** (`glue/insert.go`). So the
  cache check is free: the macro/CLI **names the destination by a config-address** of its
  args — `vecs/<source>.<model>.<recipe-version>.jsonl` (computable at expand time from
  `source`+`model`+`opts`) — and the INSERT's own error-if-exists *is* the skip: a wrapper
  runs the generated `INSERT` and treats "target file already exists" as a **cache hit**
  (or checks existence first). A macro can't touch the filesystem, but it doesn't need to —
  it just produces the deterministic destination name; the existing INSERT semantics do the
  rest. For a stronger data-level address (catch "the source changed"), expose a
  **`CONTENT_HASH(...)` scalar UDF** (surfaced in SQL, not trapped in a Go inner tier).
  Config-address (source + model + version [+ mtime/size]) is cheap and adequate for dev.

## Progress / observability

Embedding is the slow step (ms/row), so a long `INSERT INTO vecs SELECT
@vectorize_field(...)` wants progress feedback — and n1k1 already has the surface:
**`.stats on`** draws a **live, in-place per-operator counter tree** (stderr) that updates
*during* the run. The engine fires `Session.OnStats(*base.Stats)` at scan checkpoints
(`YieldStats`, ~every 1024 rows, adaptively re-paced off wall-clock to a display-friendly
rate; CLI redraws ≤10 Hz), including live in-flight `GROUP BY` aggregate partials. So the
scan/group/project counters climb in real time; the final result reports the inserted row
count. Custom-display hooks: `Session.OnStats` (paced) + `Session.OnRow` (per row,
jsonlines). See DESIGN-stats.md.

Caveats: (1) it's a **count/throughput readout, not a %-complete bar with ETA** — no total
denominator is wired in. The pieces exist (a plan-time keyspace doc-count estimate; FTS
`DocCount()`; columnar `Count`), so a real %+ETA via the `OnStats` hook is a small nicety,
not built. (2) Progress advances **per checkpoint (between operator yields), not during an
in-flight model call** — a `VECTORIZE_BATCH` HTTP round-trip that blocks stalls the footer
for that batch, so smaller batches give smoother progress (more round-trips): a tuning knob.

## The cgo fork in the road (deferred)

An ANN index at ~10M+ vectors needs one of: (a) an opt-in `CGO_ENABLED=1` FAISS build
variant (breaks the single pure-Go binary), (b) a **pure-Go HNSW** library (cgo-free,
slower index build), or (c) a sidecar index process. **Decide later** — brute-force ships
first and covers dev/debug scale.

## Phased plan

- **Phase 0 (de-risk, cgo-free, no model):** `VECTORIZE_BATCH` with `{"fake":true}` + native
  `http.post`; verify `UNNEST`; `@vectorize_field` macro; end-to-end
  `INSERT INTO vecs SELECT @vectorize_field(ks, …)` then brute-force `VECTOR_DISTANCE` top-K.
  Vectors as plain SQL++ float64 arrays first (correctness), separate from columnar packing.
- **Phase 1:** real model via `http.post` (ollama/nomic-embed-text); columnar float32
  vector column + native-lane distance; caching (config-address naming + skip-if-present);
  `CONTENT_HASH()` UDF.
- **Phase 2:** remote-source ingest (S3/Box/Drive/HF → local vector side-file); then the
  ANN-index cgo decision, only if N demands.

## Future: signal-preserving preprocessing (log templating / dedup)

Raw log lines are dominated by boilerplate (timestamps, hosts, PIDs, constant prefixes) and
are highly *templated* — only a few tokens vary per line. Embed the raw line and the cruft
dominates the vector: everything clusters, the real signal is lost. The standard fixes all
fit n1k1's existing seams:

- **Extract the field first (cheapest, already here).** `*.extract.js` already frames a line
  into typed fields — embed the `msg` field, not the whole raw line, so the timestamp/level
  boilerplate is stripped *before* embedding. This alone is most of the win.
- **Sample → learn → transform (mirrors extract's describe/apply split).** Reuse the index
  advisor's sampling seam to sample the keyspace, then LEARN what's low-signal — token IDF
  (down-weight tokens present in ~every line) and/or **log-template mining** (Drain/Spell-
  style: separate the constant template from the variable params) — and derive a
  normalization transform applied cheaply per row before `VECTORIZE_BATCH`. Sample-once to
  build the spec, apply per-row: the exact shape of the extract phase.
- **Dedup by template (a compute + storage win).** A repetitive log collapses to few
  distinct normalized lines/templates. `SELECT DISTINCT normalize(line)` → embed only the
  distinct set (e.g. 500 templates, not 1M lines) → join each row back to its template's
  vector. Both a quality win (the template *is* the signal) and a large cost win (embed far
  fewer texts), riding n1k1's existing GROUP BY / DISTINCT + the compute-once/cache
  philosophy. Optionally keep a small per-line param delta for lines whose params matter.

Later-phase direction, captured here — not Phase 0.

## Prior art

- **cbq/Couchbase:** provides the vector *index + search* (bleve + FAISS); embedding is
  **BYO** — you store precomputed vectors. n1k1 inherits the functions, not the embedder.
- **DuckDB:** `FLOAT[N]` + `array_cosine_similarity` (brute-force, fast) + `vss` HNSW for
  scale; embedding via extension/UDF callout.
- **pgvector:** `vector` type + ivfflat/hnsw; embedding via BYO / `pgai` callout.
- **LanceDB:** columnar file + an **embedding-function registry** applied at write,
  materialized into the file — the closest model to what we want, doable cgo-free.

## Open questions

1. ~~`UNNEST` support~~ — **VERIFIED** (works over computed `ARRAY_AGG` arrays).
2. ~~Row-ordinal for paging~~ — **VERIFIED** (`ROW_NUMBER()` / `_meta.pos`).
3. ~~`INSERT INTO` skip-if-present~~ — **RESOLVED**: mode `"new"` already errors-if-exists.
4. `VECTORIZE_BATCH` extension: the goja↔native-`http.post` host binding + the `{"fake":true}`
   mode (the one genuinely new piece).
5. Native-lane / columnar float32 port of `vectorDistance`, with element-type variants
   (Phase 1 perf).
6. Parquet fixed-size-list encoding of a vector column, incl. the element-type tag
   (float32/int8/…) (DESIGN-col).
