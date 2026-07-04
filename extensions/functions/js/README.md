# Example drop-in JS UDFs (goja / Tier 2)

Each `*.js` file here is a **scalar user-defined function**. The convention
(`glue.RegisterJSDir`) is:

- the SQL++ function name is the file's base name (minus `.js`);
- the file must define a JavaScript function of that **same name**.

Load them into a session's parser at startup, then call them in SQL++:

```go
names, err := glue.RegisterJSDir("extensions/functions/js")
// names == ["add_two_numbers", "celsius_to_fahrenheit", "slugify"] (sorted)
```

```sql
SELECT add_two_numbers(o.qty, 10) AS bumped,
       celsius_to_fahrenheit(t.c) AS f,
       slugify(p.title)           AS slug
FROM ...
```

The directory *is* the catalog — add a file (or `git pull` a repo of them) and
the function is available; no `CREATE FUNCTION` DDL, no rebuild. See
`DESIGN-extensions.md` (Tier 2) for the runtime model (pooled goja runtimes,
in-process sandbox caveats, the streaming `emit()` extension for table-valued
UDFs). Registration is **opt-in** because executing user JS in-process is a real
attack surface — an embedder decides when to enable it.

These run in the interpreted/boxed lane (via cbq `Expression.Evaluate`), not the
zero-alloc byte fast path — fine for enrichment, not for tight inner loops.
