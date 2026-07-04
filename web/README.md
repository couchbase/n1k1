# n1k1 in the browser (WebAssembly demo)

A zero-install way to try n1k1: the SQL++/N1QL engine compiled to WebAssembly,
running entirely client-side over pre-canned JSON. No server, no network calls —
the query planner and executor run in the browser tab.

![screenshot placeholder]

## Try it

Build the wasm binary, then serve the `web/` directory over HTTP (wasm can't be
loaded from `file://`):

```sh
sh web/wasm/build.sh                 # produces web/n1k1.wasm + web/wasm_exec.js
cd web && python3 -m http.server 8080
open http://localhost:8080/
```

Pick an example query or write your own, then **Run** (or ⌘/Ctrl+Enter). Joins,
`GROUP BY`, `UNNEST`, subqueries and `EXPLAIN` all work against the sample
`beers` / `breweries` keyspaces.

The engine runs in a **Web Worker**, so the page stays responsive and live
per-operator stats stream under the status line while a query runs (visible on
larger datasets — a tiny query just shows the final line). A running query can be
**cancelled** (the ■ Cancel button): since synchronous wasm can't be interrupted
by a message, this terminates the worker and respawns a fresh one, re-mounting
your current dataset.

The sample ships a `.n1k1/catalog.json` declaring secondary indexes on `beers`.
The browser has no bbolt (it needs mmap), so these are **in-memory** indexes
(`glue/idx_mem.go`) — the **Indexed filter (EXPLAIN)** example shows the planner
choosing an `IndexScan` over them, exactly as the native binary does with its
on-disk bbolt indexes.

### Drag & drop your own files (all browsers)

Drag `.json`, `.jsonl`/`.ndjson`, their `.gz` variants, or a `.tar` / `.tar.gz`
onto the page — or click **📤 Load files…**. They're read in the browser
(nothing is uploaded), decompressed/untarred in JS, and mapped to keyspaces:

- a loose `beers.jsonl` → keyspace `beers`, one document per line;
- a `.tar` folder `breweries/*.json` → keyspace `breweries`, one doc per file;
- document keys come from an `id` / `_id` / `uuid` / `key` field when present.

Then query away. This path works in every browser (no picker API). `.zst` and
`.zip` aren't supported yet; caps: 200k docs / 64 MB.

### Query your own local folder

In a Chromium browser (Chrome / Edge), click **📁 Open folder…** to point n1k1 at
a directory of JSON on your own machine — via the
[File System Access API](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API).
The files are read into memory (nothing is uploaded; nothing leaves the tab) and
mapped to n1k1's keyspace layout:

- each **subdirectory** of the picked folder becomes a keyspace of
  `<key>.json` documents (e.g. `orders/order-1001.json` → keyspace `orders`);
- any **loose `.json` / `.jsonl` files** at the top level become one keyspace
  named after the folder.

The schema sidebar and starter queries are then rebuilt by asking the engine
itself (`COUNT(*)` + a `LIMIT 1` sample per keyspace). Eager-load caps: 5000
files / 64 MB (larger folders are truncated with a note). Firefox / Safari don't
implement the picker, so the button is hidden there; the built-in sample still
works everywhere.

## How it works

n1k1 is pure Go, so it cross-compiles to `GOOS=js GOARCH=wasm`. Two things need
handling because a browser has no OS:

1. **No syscalls.** A few files in the engine and its dependencies use mmap /
   flock / rlimit / rusage, which don't exist under `GOOS=js`:
   - The on-disk index backends (`glue/idx_si.go` bbolt GSI, `glue/idx_fts.go`
     bleve FTS) are excluded via `//go:build !wasm`; `glue/idx_wasm.go` supplies
     the few symbols the core still references. The demo runs every query as a
     primary scan — no secondary indexes — which is fine for in-memory data.
   - A handful of dependency files (`couchbase/query/{logging,util}`,
     `edsrzf/mmap-go`) get js stubs applied to local copies via `go.mod`
     replaces. `build.sh` does this automatically; the edits are host-specific
     and left uncommitted (like the EE stubs in `DESIGN-testing.md`).

2. **No filesystem.** `wasm/fs_mem.js` installs a tiny in-memory `fs` (the read
   path only: `open`/`read`/`stat`/`readdir`), backed by the JSON in
   `samples.js`. The engine reads its `<namespace>/<keyspace>/<key>.json`
   document tree from there exactly as it would from disk.

## Files

| file | role |
|------|------|
| `index.html`        | the demo page (query editor, results table, schema sidebar) |
| `samples.js`        | sample datasets + example queries — edit freely, no rebuild needed |
| `wasm/main_wasm.go` | the wasm entry point; exposes `globalThis.n1k1RunQuery(sql)` |
| `wasm/fs_mem.js`    | in-memory read-only filesystem shim for `GOOS=js` |
| `wasm/ingest.js`    | drag-drop / file ingestion: gunzip + untar + JSON(L) → keyspaces |
| `wasm/opfs.js`      | OPFS cache for built in-memory indexes (preload/persist; optional) |
| `wasm/worker.js`    | Web Worker hosting the engine off the main thread (query RPC + live stats) |
| `wasm/build.sh`     | builds `n1k1.wasm`, ships `wasm_exec.js`, applies dep patches |
| `wasm/overlay/`     | js-tagged stubs copied into the local dependency copies |

`n1k1.wasm` (~67 MB raw, ~12 MB gzipped) and `wasm_exec.js` are build outputs and
are git-ignored; run `build.sh` to (re)generate them.

## Tests

The JavaScript is tested with Node's built-in runner (no dependencies):

```sh
node --test "web/wasm/*.test.mjs"      # or: sh web/wasm/test.sh
```

- `ingest.test.mjs` — gunzip / untar / record parsing / keyspace mapping / caps.
- `fs_mem.test.mjs` — the in-memory fs (readdir, stat, open/read, path norm, mount, read-only).
- `e2e.test.mjs` — loads the built `n1k1.wasm` and runs real queries (sample queries,
  `n1k1OpenDir`, and the drag-drop ingestion path). **Skips** if `n1k1.wasm` isn't built,
  so build first (`sh web/wasm/build.sh`) to include it.

## Customizing the data

Edit `samples.js`: add keys to `DATASETS.default` (each keyspace is
`{ "<docKey>.json": <jsonText> }`) and add entries to `SAMPLE_QUERIES`. Reload
the page — no rebuild required.
