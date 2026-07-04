# Example JavaScript extensions (drop-in functions)

Two kinds of file live here, both keyed by file name:

- **`*.js` — a scalar function.** The SQL++ function name is the file's base name
  (minus `.js`), and the file must define a JavaScript function of that **same
  name**. E.g. `slugify.js` defines `slugify(...)`.
- **`*.agg.js` — an aggregate function** (3-callback protocol). For base name
  `NAME`, the file defines `NAME_init()`, `NAME_update(state, value)` and
  `NAME_final(state)`; the accumulator `state` is any JSON-serializable value.
  `NAME(expr)` then works in `GROUP BY` (or as a bare aggregate). E.g.
  `geomean.agg.js` → `SELECT host, geomean(latency) FROM … GROUP BY host`.

The directory *is* the catalog — add a file (or `git pull` a repo of them) and
the function is available; no `CREATE FUNCTION` DDL, no rebuild. (A JS function
that **returns an array** also works directly in a `FROM` clause as a
table-valued source: `SELECT x.* FROM myfunc(…) AS x`.)

## Loading them

From the CLI (kind auto-detected from the file extension). `-ext` (alias
`-extensions`) is **repeatable** and accepts comma-separated lists, so you can
point at several dirs and/or files:

```sh
# a whole directory of extensions...
n1k1 -ext extensions/functions/js  -c "SELECT slugify('Hello, World!')"  examples/shop
# ...several dirs/files (repeat the flag, or comma-separate)...
n1k1 -ext extensions/functions/js -ext ./my_udfs -ext extra.js  -c "..."  examples/shop
```

In the REPL, manage them at any time with the `.extensions` dot-command (alias
`.ext`) and its sub-commands:

```
n1k1> .extensions load extensions/functions/js     # load a dir (or file)
loaded: add_two_numbers, celsius_to_fahrenheit, slugify
n1k1> .extensions list                             # what's loaded
3 loaded extension function(s):
  add_two_numbers       javascript  extensions/functions/js/add_two_numbers.js
  ...
n1k1> SELECT add_two_numbers(o.items, 10) AS bumped, slugify(o.customer) AS slug FROM orders o;
n1k1> .extensions unload slugify                   # disable one (reload to re-enable)
```

Programmatically (embedders):

```go
glue.RegisterExtensionDir("extensions/functions/js")     // a directory
glue.RegisterExtensionFile("path/to/my_fn.js")           // one file (kind by extension)
glue.RegisterJSFunc("triple", "function triple(x){return x*3;}") // inline scalar
glue.RegisterJSAggregate("product",                              // inline aggregate
    "function product_init(){return 1;} function product_update(s,v){return s*v;} function product_final(s){return s;}")
glue.ListExtensions()                                    // []ExtensionInfo{name,kind,source}
glue.UnloadExtension("triple")                           // disable (reload to re-enable)
```

## Runtime model (what you can rely on)

Each **query** gets its own JavaScript runtime (and each concurrent UNION ALL
branch its own), holding all loaded UDFs together. So:

- **UDFs can call each other** — `foobar.js` may call `slugify(...)` if `slugify`
  is also loaded (they share one scope).
- **`console.log(...)`** (and `.warn/.error/.info/.debug`) work for debugging;
  output goes to stderr.
- **Module-scope `var`/globals persist across calls *within a query* and reset on
  the next query.** Great for per-query caches (e.g. a compiled regex hoisted out
  of the function, a memo table). Do **not** use a global as a running total or
  row counter — it's per-runtime and resets each query; use SQL (`COUNT`/`SUM`,
  or `GROUP BY` with an aggregate) for cross-row accumulation.

## Notes

- Loading is **opt-in** — executing user code in-process is a real attack
  surface, so nothing is auto-loaded; you choose when to enable it.
- A UDF whose name would shadow a built-in function or aggregate is refused.
- A runaway UDF is interrupted after a time limit (`glue.JSCallTimeout`).
- These run in the interpreted lane, fine for enrichment — not for tight inner
  loops. See `DESIGN-extensions.md` for the runtime model and the roadmap
  (streaming table-valued UDFs, WASM extensions, a signed registry).
