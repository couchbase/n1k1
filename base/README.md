# base/ — shared types & runtime primitives

The foundational, dependency-free layer that everything else in n1k1 builds on
(`engine/`, `glue/`, the generated `intermed/`, and future integrations import
it; it imports nothing from n1k1). Pure-Go — only `buger/jsonparser` and
`couchbase/rhmap/store`.

Data model:
- `Val` — one value, JSON-encoded `[]byte` (usually immutable).
- `Vals` — one row, a `[]Val` of positional "registers".
- `Labels` — names the columns of a row (e.g. `.`, `.["address","city"]`,
  `^id`), so field access becomes a positional slice index.
- `Op` — a query-plan node: `Kind` + `Labels` + `Params` + `Children`. The tree
  the engine executes (and the compiler compiles).

Runtime & contracts:
- `Vars` / `Ctx` — the context threaded down through execution (temps, comparer,
  expression catalog, spill allocators).
- Push-based callbacks: `YieldVals` / `YieldErr`, and the `ExprFunc` /
  `ProjectFunc` shapes.
- `ExprCatalog` — the extension hook by which `glue/` plugs in SQL++ expressions.

Helpers:
- `ValComparer` + canonical JSON — N1QL value comparison and key
  canonicalization (for GROUP BY / DISTINCT / set ops).
- `Agg` and window-frame types (COUNT/SUM/MIN/MAX/AVG, window navigation).
- `Stage` — producer/consumer data-staging with optional concurrency.
- Spillable `Heap` (ORDER BY) and chained byte stores, backed by
  `rhmap/store`, so large operators spill to temp files instead of OOMing.
