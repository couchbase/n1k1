# k-means centroids, IVF partitioning & a dataset census — no engine changes

This directory is a working pipeline for the question *"we have a pile of documents with
embedding vectors — what's IN this dataset, and can centroids speed up search?"*:

1. **fit** — k-means centroids over any keyspace's vector field, via SQL++ scans.
2. **census** — a human-readable view of the dataset: per-cluster sizes, the *exemplar*
   docs nearest each centroid, top text tokens per cluster. (The BERTopic /
   SemDeDup-style "map of the corpus", in SQL++.)
3. **partition** — IVF layout: each cluster's rows written to its own Parquet keyspace
   (`part_<k>/data.parquet`, the columnar-`VECTOR_DISTANCE`-ready vec column), plus a
   queryable `centroids/` keyspace.
4. **probe** — ANN-style search: pick the `nprobe` nearest partitions by centroid
   distance, scan only those; report recall vs the brute-force answer.

Everything runs through the stock `n1k1` CLI — **no engine changes**. The driver
(`kmeans.py`, stdlib-only Python) is just a loop + JSON plumbing around SQL++
statements; two optional goja UDFs (`extensions/functions/js/vector_nearest*.js`) make
the argmin terser/faster. Design rationale + measured findings: `DESIGN-vectors.md`
§"Phase 1.5".

## Quickstart (synthetic data)

```sh
make cli                    # or: go build -tags n1ql -o n1k1 ./cmd/n1k1
export N1K1=$PWD/n1k1

python3 examples/kmeans/gen_vectors.py --out /tmp/kmdemo --n 2000 --dim 16 --clusters 5
python3 examples/kmeans/kmeans.py fit       --data /tmp/kmdemo --k 5 --use-udf
python3 examples/kmeans/kmeans.py census    --data /tmp/kmdemo --text-field txt
python3 examples/kmeans/kmeans.py partition --data /tmp/kmdemo --use-udf
python3 examples/kmeans/kmeans.py probe     --data /tmp/kmdemo --nprobe 2 \
    --qvec "$(python3 -c 'import json; print(json.dumps([0.1]*16))')"
```

The synthetic docs carry topic words, so the census visibly rediscovers the topics:

```
cluster 0:  401 docs (20.1%)  terms=['index','ranking','query','recall']   exemplars=['query search recall', ...]
cluster 1:  383 docs (19.1%)  terms=['packet','latency','gateway','tcp']   ...
cluster 2:  378 docs (18.9%)  terms=['oauth','session','login','token']    ...
```

## Using your own dataset

Any keyspace works — point the flags at your data:

```sh
python3 examples/kmeans/kmeans.py fit --data <dataRoot> \
    --keyspace docs --vec-field emb --id-field doc_id --k 16 --limit 10000 --use-udf
```

- **You bring the vectors** (any JSON numeric array field, jsonl or parquet keyspaces
  both fine), or produce them in n1k1 with `VECTORIZE_BATCH` / the `@vectorize_field`
  macro against an embedding endpoint (see `DESIGN-vectors.md` §"Embedding"), e.g.
  `INSERT INTO emb/data.parquet (KEY UUID(), VALUE self) SELECT d.id AS id, ... AS vec ...`.
- **Metric**: distances are euclidean. For cosine semantics, use **unit-normalized
  embeddings** (most embedding models emit these): on unit vectors euclidean and cosine
  produce the same nearest-neighbor ordering, and k-means on them ≈ spherical k-means.
  ⚠ **ollama users: this is an endpoint choice** — `/api/embed` (the batch API n1k1
  targets) returns unit vectors (norm 1.0); the older `/api/embeddings` returns raw
  magnitudes (norm ~23 observed). Nothing errors on the wrong one — the clusters just
  quietly become about magnitude. `fit` samples norms and warns when they aren't ~1.
- **Rows without the vector field are skipped** (`IS VALUED` guard), never guessed.

Embedding-with-`@vectorize_field` traps (field-tested by the n1k1-for-ai team — both
fail silently; also in `.help vectorize`):

- Its rows come back as **`{id, text, vec}`** — the embedded text is always named
  `text`, never the source field's name. `SELECT r.line AS txt` yields MISSING and
  SQL++ **silently drops** the column from the projection, so a materialized parquet
  simply lacks it; the census would then show empty `top_terms` everywhere.
  `census --text-field` now fails loud when the field has no valued rows; after any
  materialize, sanity-check with `SELECT * FROM <target> LIMIT 1`.
- Keep `batch` modest (the macro default is now 64): `batch => 256` of ~1.5KB prompts
  crashed a local ollama (`/tokenize: connection reset by peer`).

## Subcommands & the flags that matter

| step | what runs in SQL++ | notes |
|---|---|---|
| `fit` | farthest-first init scans + per-iteration **assignment** scan (argmin per row) | `--k`, `--iters`, `--tol`; **`--limit N` = sampled fit** (recommended ≥20K rows — standard practice; pgvector trains ivfflat on a sample too); `--init-limit` caps init scans (default 5000); `--pure-sql` runs the *entire* Lloyd iteration as one SQL++ statement (see below); `--use-udf` swaps the argmin to the goja UDF |
| `partition` | one assignment pass materialized to a temp Parquet keyspace, then K filtered `INSERT INTO part_<k>/data.parquet` copies | run over the FULL dataset (fit may be sampled; partition never is) |
| `census` | per-cluster `GROUP BY` sizes; per-centroid exemplar top-K via `VECTOR_DISTANCE` (the centroid is a constant → the engine's fused columnar path on parquet); per-cluster `TOKENS()` term counts | `--text-field`, `--exemplars`, `--show extra,fields` |
| `probe` | per-partition `VECTOR_DISTANCE` top-K over only the `nprobe` nearest partitions | reports recall + timing vs brute force |

`centroids.json` (written by `fit`, read by the rest) is the only state between steps;
`partition` also writes the centroids as a queryable `centroids/` keyspace
(`{id, part, vec, size}` docs) so plain SQL++ can reference them.

## The SQL++ idioms (for use without the driver)

Nearest-centroid **argmin** — pure SQL++, no extensions (`cents` = a `WITH` alias or an
inlined JSON array of arrays):

```sql
ARRAY_SORT(ARRAY [ARRAY_SUM(ARRAY POWER(d.vec[q]-c[q], 2)
                            FOR q IN ARRAY_RANGE(0, ARRAY_LENGTH(d.vec)) END), i]
           FOR i:c IN cents END)[0][1]
```

(arrays compare elementwise, so `ARRAY_SORT` orders by distance; `[0][1]` is the winning
index). With the UDFs loaded (`-ext extensions/functions/js`), the same thing is
`vector_nearest(d.vec, cents)`, and `vector_nearest_dist(d.vec, cents)` returns
`[idx, dist]` — handy for outlier reports or SemDeDup-style within-cluster ordering.

A **full Lloyd iteration in one statement** (assignment + per-cluster element-wise mean
+ sizes) — what `fit --pure-sql` runs:

```sql
WITH cents AS ([[...], [...], ...])
SELECT g.cl, ARRAY x[1] FOR x IN ARRAY_SORT(ARRAY_AGG([g.p, g.m])) END AS centroid, MAX(g.n) AS n
FROM (
  SELECT dd.cl, p, AVG(dd.v[p]) AS m, COUNT(1) AS n
  FROM (SELECT d.v AS v, <argmin> AS cl
        FROM (SELECT d0.vec AS v FROM vecs d0 WHERE d0.vec IS VALUED) AS d) AS dd
  UNNEST ARRAY_RANGE(0, ARRAY_LENGTH(dd.v)) AS p
  GROUP BY dd.cl, p
) AS g
GROUP BY g.cl ORDER BY g.cl
```

**Exemplars near a centroid** (the centroid is a per-query constant, so the engine's
`VECTOR_DISTANCE` applies — and on a Parquet vec column this is the fused columnar
top-K):

```sql
WITH qv AS ([<centroid k>])
SELECT d.id, ROUND(VECTOR_DISTANCE(d.vec, qv, "euclidean"), 4) AS dist
FROM vecs d WHERE d.vec IS VALUED ORDER BY dist LIMIT 5
```

## Measured numbers (M2 Pro; see DESIGN-vectors.md §Phase 1.5 for the full findings)

20K docs × 64-d, k=8, `--limit 5000 --use-udf`:

| step | time |
|---|---|
| fit (12 iterations, sampled) | ~2s / iteration, ~30s total |
| partition (full 20K: 1 assignment pass + 8 copies) | ~10s one-time |
| census (sizes + exemplars + terms) | a few seconds |
| brute-force top-10, jsonl row lane | ~1.9s |
| brute-force top-10, ONE parquet file (columnar) | **~0.08s** |
| probe top-10, nprobe=2 of 8 parquet partitions | ~0.12s wall, **recall 1.00** (nprobe=1: 0.90) |

Two honest conclusions baked into the design:

- **At ≤100K vectors, a single Parquet file + columnar brute force is already the
  answer** (~0.4–0.5s at 100K×384 per DESIGN-vectors.md). The IVF partitioning payoff
  arrives when the full-file decode exceeds your latency budget (millions of vectors):
  probing `nprobe/K` of the partitions decodes only that fraction of the bytes — and it
  composes with the columnar kernel, since partitions ARE parquet vec files.
- **The census is useful at any scale** — it's a dataset map, not an index.

## Gotchas & limits (the sharp edges we hit so you don't have to)

- ⚠ **`VECTOR_DISTANCE` requires a STATIC query vector** (constant / `$param` / `WITH`
  alias — a cbq-inherited validation). Ranging it over a centroid list in a
  comprehension is rejected; that's exactly why the argmin uses plain arithmetic or the
  UDF. Per-centroid *constant* queries (census exemplars, probe) use it freely.
- ⚠ **Derived tables are flattened by the optimizer**: computing the assignment in a
  subselect does NOT materialize it — under an `UNNEST` it re-evaluates per (doc,
  position), dim× the work. `fit --pure-sql` pays this; the default fit sidesteps it
  (assignment scan in SQL++, tiny mean in the driver); `partition` sidesteps it by
  materializing the assignment through a real temp Parquet keyspace.
- ⚠ **goja UDF boundary cost scales with ARGUMENT size**: `vector_nearest(vec, cents)`
  re-converts the whole K×dim centroid array per row (~0.3ms/row at 8×64). Still ~4×
  faster than the pure-SQL++ argmin at that size, but don't expect native speed. (A
  native `VECTOR_AVG` aggregate + native argmin are the proposed Phase 1.5 engine
  follow-ups — see DESIGN-vectors.md.)
- ⚠ **Parquet writer is first-row-defines-schema, strict** — the partition writer emits
  only `{id, vec}` (+`cl` on the temp), on purpose. Join back to the source keyspace by
  id for full docs.
- k-means caveats (standard, not n1k1-specific): k is yours to choose (try the census at
  a few k's — over-split clusters like two "disk" clusters are obvious to the eye);
  empty clusters keep their previous centroid; farthest-first init is deterministic, so
  runs are reproducible.
