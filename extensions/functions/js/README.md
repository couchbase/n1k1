# Example JavaScript UDFs (drop-in scalar functions)

Each `*.js` file here is a **scalar user-defined function**. The convention is:

- the SQL++ function name is the file's base name (minus `.js`);
- the file must define a JavaScript function of that **same name**.

The directory *is* the catalog — add a file (or `git pull` a repo of them) and
the function is available; no `CREATE FUNCTION` DDL, no rebuild.

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
glue.RegisterJSFunc("triple", "function triple(x){return x*3;}") // inline source
glue.ListExtensions()                                    // []ExtensionInfo{name,kind,source}
glue.UnloadExtension("triple")                           // disable (reload to re-enable)
```

## Notes

- Loading is **opt-in** — executing user code in-process is a real attack
  surface, so nothing is auto-loaded; you choose when to enable it.
- A UDF whose name would shadow a built-in function or aggregate is refused.
- A runaway UDF is interrupted after a time limit (`glue.JSCallTimeout`).
- These run in the interpreted lane, fine for enrichment — not for tight inner
  loops. See `DESIGN-extensions.md` for the runtime model and the roadmap
  (streaming table-valued UDFs, WASM extensions, a signed registry).
