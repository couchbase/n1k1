# engine/ — the query-plan interpreter (and compiler source)

n1k1's push-based execution engine. `ExecOp` recursively walks a `base.Op` tree
and processes it by pushing rows (`base.Vals`) from child operators up to parent
operators via `yield` callbacks — shorter codepaths than a pull/iterator model,
and fusable by the compiler.

Operators (`op_*.go`) + expression evaluation (`expr*.go`):
- scan, filter, project
- joins: nested-loop & hash, inner & left-outer, ON KEYS; NEST; UNNEST
- group / distinct, aggregates, HAVING
- order-offset-limit, UNION ALL, INTERSECT / EXCEPT
- window functions, sequence, temp (subquery) ops

Two roles, one source:
1. **Interpreter** — run directly (`engine.ExecOp(op, vars, yieldVals, yieldErr,
   ...)`).
2. **Compiler source** — the files are written in a careful "lz" (lazy /
   late-bound) subset of Go with comment directives, so `cmd/intermed_build`
   can translate them into the generated `intermed/` package, which the n1k1
   compiler then uses to emit Go for a specific query (a Futamura projection).
   See `../DESIGN.md`.

Extension points keep the engine decoupled from SQL++ and storage:
- `ExprCatalog` — expression constructors (`glue/` registers SQL++ expressions).
- `ExecOpEx` — extra op kinds (`glue/` plugs in datastore scan/fetch).

Pure-Go; depends only on `base/` (plus `rhmap/store` for spilling). The `glue/`
layer maps couchbase/query SQL++ plans onto these ops.
