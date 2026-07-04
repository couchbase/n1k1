# Example JavaScript UDFs (drop-in scalar functions)

Each `*.js` file here is a **scalar user-defined function**. The convention is:

- the SQL++ function name is the file's base name (minus `.js`);
- the file must define a JavaScript function of that **same name**.

The directory *is* the catalog — add a file (or `git pull` a repo of them) and
the function is available; no `CREATE FUNCTION` DDL, no rebuild.

## Loading them

From the CLI (kind auto-detected from the file extension):

```sh
# a whole directory of extensions...
n1k1 -ext extensions/functions/js  -c "SELECT slugify('Hello, World!')"  examples/shop
# ...or a single file...
n1k1 -ext extensions/functions/js/celsius_to_fahrenheit.js  -c "SELECT celsius_to_fahrenheit(37)"  examples/shop
```

In the REPL, load more at any time with the `.ext` dot-command:

```
n1k1> .ext extensions/functions/js
registered extension function(s): add_two_numbers, celsius_to_fahrenheit, slugify
n1k1> SELECT add_two_numbers(o.items, 10) AS bumped, slugify(o.customer) AS slug FROM orders o;
```

Programmatically (embedders):

```go
glue.RegisterExtensionDir("extensions/functions/js")     // a directory
glue.RegisterExtensionFile("path/to/my_fn.js")           // one file (kind by extension)
glue.RegisterJSFunc("triple", "function triple(x){return x*3;}") // inline source
```

## Notes

- Loading is **opt-in** — executing user code in-process is a real attack
  surface, so nothing is auto-loaded; you choose when to enable it.
- A UDF whose name would shadow a built-in function or aggregate is refused.
- A runaway UDF is interrupted after a time limit (`glue.JSCallTimeout`).
- These run in the interpreted lane, fine for enrichment — not for tight inner
  loops. See `DESIGN-extensions.md` for the runtime model and the roadmap
  (streaming table-valued UDFs, WASM extensions, a signed registry).
