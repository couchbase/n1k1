# n1k1 CLI — design

_Last reviewed: 2026-07-25._

A single-binary CLI + shell for SQL++/N1QL over n1k1's file datastore: download, point at data, get
pretty results. REPL, one-shot queries, pipe-friendly output — inspired by DuckDB/sqlite, adapted to
n1k1 (SQL++, directory-as-database, query-focused). Key enabler: the engine's end-to-end driver is
extracted into a reusable `glue.Session` so `cmd/n1k1` is a pure front-end.

**Status:** ships as a single-binary REPL + one-shot/`-c`/`-f`/pipe front-end over `glue.Session`,
with `box`/`box|pretty`/`jsonlines`/`json`/`csv`/`markdown`/`line`/`list` output modes, a rich
dot-command set (§ below), a `~/.n1k1rc` init file, `peterh/liner` line editing, framing-tagged
keyspace listings, reserved-word / shell-quoting hints, cooperative query cancellation, and
materialization statements (`CREATE TEMP KEYSPACE`, `INSERT INTO … SELECT`) surfaced in `.help`.

**Remaining TODOs:** ⚠ `.schema` with no arg dumps giant box tables (ignores `.maxwidth`) on a real
bundle — make it a compact one-line-per-keyspace summary; file-as-table (`FROM 'foo.csv'` /
`read_json_auto(...)` — scans exist but aren't reachable through N1QL `FROM` via glue); tab
completion; multi-line 2D cursor editing + mouse (deferred, §Line editing); `.import`/`COPY`/writes
(engine is query-only); persistent settings / PRAGMA; the syntax-error caret (parse errors carry a
renderable offset but the CLI doesn't yet draw `^` under the token).

**Out of scope for v1:** persistent DB file format (store is read-only JSON dir), general DDL flows,
HTTP/UI server. (Landed beyond v1: `INSERT INTO` materialize-to-a-new-keyspace-file — `DESIGN-data.md`
§2; extensions/`INSTALL` — `DESIGN-extensions.md`.)

## The core refactor: a reusable session

The end-to-end driver (once hardcoded in `test/suite_test.go: n1k1RunStatement`) now lives in `glue`
as the shared engine for both test and CLI:

```go
// glue/session.go  (//go:build n1ql)
type Session struct {
    Store     *Store
    Namespace string            // "default"; render/REPL state stays in the CLI
}
func OpenSession(datastoreDir string) (*Session, error) // FileStore + InitParser

type Result struct {
    Labels   base.Labels        // column set, from conv.TopOp.Labels
    Rows     []json.RawMessage  // canonical JSON, as the harness produces
    Elapsed  time.Duration
    Plan     *base.Op           // optional, for .explain / debug
}
func (s *Session) Run(stmt string) (*Result, error)
type ErrUnsupported struct{ Reason string }  // nil TopOp, convert failure, panic
```

`Run` is the body of `n1k1RunStatement` minus test plumbing
(`ParseStatement → store.PlanStatement → Conv.Accept → NewConvertVals → MakeVars → ExecOp(DatastoreOp)`),
promoting the harness's unsupported-vs-genuine-error distinction to a typed `ErrUnsupported` so the
CLI can say "not supported yet" vs "your query is wrong." The harness is now a thin caller of `Run`;
engine knowledge stays in `glue/`, `cmd/n1k1` is a pure front-end.

## Binary, build, invocation

`cmd/n1k1/main.go`, behind `//go:build n1ql`. Build: `CGO_ENABLED=0 go build -tags n1ql ./cmd/n1k1`
(cross-compiles).

```
n1k1 [flags] [datastore-dir]
n1k1                         # REPL on cwd (or no store until .open)
n1k1 ./test/suite/json       # REPL with that datastore opened
n1k1 -c "SELECT 1+1"         # one-shot, print, exit
echo "SELECT ..." | n1k1     # stdin pipe (batch, no prompt)
n1k1 -f script.n1ql          # run a file of ;-separated statements

flags:
  -c <stmt>       run one statement and exit
  -f <file>       run statements from a file and exit
  -mode <m>       box|json|jsonlines|csv|markdown|line|list
                  (append |pretty to indent JSON 2 spaces; jsonlines also accepts jsonl/ndjson)
  -timer          show timing
  -echo           echo each input line as read (like .echo on; handy with -f)
  -init <file>    run dot-commands/SQL at startup (default ~/.n1k1rc; "", "-", "none" skips)
  -index <mode>   secondary index build: eager|lazy|off (DESIGN-indexing.md)
  -formats <set>  restrict scanning to a format set (DESIGN-data.md)
  -meta <mode>    per-record _meta injection: on|off|auto
  -verbose / -v   diagnostics level (bare = on; -verbose=on|off|debug|<n>)
  -stats <mode>   per-op counters: on (live) | off | final (DESIGN-stats.md)
  -prepare <lvl>  max compile level: interpreted | data | full (DESIGN-prepare.md)
  -ext <path>     load extension(s) (dir/file, repeatable; .js = JS UDF); -extensions is an alias
  -variant-fidelity    Parquet VARIANT scan carries typed-scalar fidelity (V-carrier) instead of
                  the Phase-0 JSON projection (records.VariantFidelity; DESIGN-variant.md)
  -version        print version + build info and exit
  -profile-cpu / -profile-mem <file>   pprof profiles
```

- Namespace isn't a flag — the file datastore only uses `default` (`defaultNamespace` const); a rare
  multi-namespace tree is still reachable via a `<ns>:<keyspace>` qualifier in SQL.
- **Default mode:** TTY → `box|pretty`; pipe/`-c` → compact `jsonlines` unless `-mode` overrides.
- **Remote sources:** the datastore-dir / `FROM` target may be an object-store URL (`s3://…`,
  `gs://…`, `abfs://…`) — a bare Parquet object, or an Iceberg/Parquet table dir, becomes a FROM-able
  keyspace (DESIGN-data.md §8).
- Flag ownership: `-index`/`.indexes` → DESIGN-indexing.md; `-formats`/`-meta` → DESIGN-data.md;
  `-stats` → DESIGN-stats.md; `-prepare` → DESIGN-prepare.md.

## The REPL

Prompt `n1k1>`; continuation `   ...>` until a statement ends with `;` (buffer accumulates, `;`
flushes to `Session.Run`). Dot-commands are recognized when a line starts with `.` and no SQL is
buffered; execute immediately, no `;`.

**Signals:** at the prompt, Ctrl-C clears the input buffer (a second at an empty prompt exits);
**during a query, Ctrl-C cooperatively HALTS it and keeps the session** (a second force-quits); a
closed output pipe (`… | head`) halts the query the same way (SIGPIPE ignored so the write returns
EPIPE); Ctrl-D / `.quit` / `.exit` exit. Engine side: `Session.Interrupt → Ctx.Halt`, checked at
scan checkpoints (scans stop with `base.ErrHalted`).

**Line editing / history:** `github.com/peterh/liner` (MIT, pure Go) — arrow-key history + emacs
editing; history persists to `~/.n1k1_history`. ⚠ **Add deps via explicit `go get <pkg>@<ver>`,
never `go mod tidy`** (it prunes the n1ql-only `query` dep); verify `CGO_ENABLED=0`.

## Dot-commands

DuckDB names where the concept exists (muscle memory carries). Dispatched in `cmd/n1k1/dot.go`; every
on/off-style setting shows its current value in `.help`.

| Command | Behavior |
|---|---|
| `.help` | List commands + current datastore + a live example query. |
| `.open <dir>` | Open a new file datastore dir (re-`FileStore`+`InitParser`); closes the prior session's TEMP KEYSPACE spills. |
| `.tables` / `.keyspaces` | List keyspaces via the datastore interface (so flattening/synthetic roots show), each tagged with record framing + file count + a copy-paste example. |
| `.schema [<keyspace>]` | Sampled shape from a 50-doc `SELECT x.*` sample (fields + JSON types + distinct values + a WHERE example), rendered as a box. ⚠ No arg → every keyspace (giant on a real bundle — a compact summary is a TODO). |
| `.index [list\|show <name>\|rebuild [<name>]\|suggest [<ks>]\|help]` | Secondary-index family (`.indexes` = `.index list`). DESIGN-indexing.md. |
| `.mode <m>` | Set output mode; `jsonl`/`ndjson` = `jsonlines`. |
| `.meta [on\|off\|auto]` | Get/set `_meta` sub-object; mirrors `-meta` (mutates `glue.ScanWalkOptions.Meta`). |
| `.formats [<set>]` | Get/set which formats scanning considers (persists to catalog.json); mirrors `-formats`. |
| `.timer [on\|off]` | Elapsed-time footer. |
| `.stats [on\|off\|final\|about]` | Per-op counters: live footer / totals-at-end / glossary. DESIGN-stats.md. |
| `.explain [on\|off]` | Also print the converted `base.Op` plan tree (per-expr native vs boxed); shows *why* something is UNSUPPORTED. |
| `.prepare [interpreted\|data\|full \| <stmt>]` | Set the compile-level ceiling, or one-shot emit the generated Go for `<stmt>` then run. DESIGN-prepare.md. |
| `.verbose [off\|on\|debug\|<n>]` | Diagnostics level (routes `base.Logf` through the same knob). |
| `.maxrows <n>` | box: cap rows. `>0` = head+tail with `·` elision; `<0` = last `\|n\|`; `0` = all. |
| `.maxwidth <n\|auto>` | box: cap column width, truncate with `…`. `0` = uncapped; `auto` = fit box to terminal. |
| `.multi [list\|run\|lint\|test\|help]` | Run/lint/test a corpus of tagged `*.sql++` detector recipes over the open bundle (`--queries <dir>`). DESIGN-prepare.md. |
| `.extensions [list\|load <dir>…\|unload <name>…]` (`.ext`) | Manage loaded extensions (`.js` = JS UDF). |
| `.extract [help\|list]` | Authoring reference + inventory for `*.extract.js` framing recipes. |
| `.macro [help\|list\|expand <stmt>]` (`.macros`) | Pre-parse SQL++ macros: `@name(...)` → generated SQL++; `expand` shows the rewrite (`*.macro.js`). |
| `.read <file>` | Execute statements/dot-commands from a file. |
| `.output [<file>]` | Redirect results to a file, or back to stdout. |
| `.bail [on\|off]` | Stop the input loop on the first statement error (scripts). |
| `.echo [on\|off]` | Echo each input line as read (scripts). |
| `.print <text>` | Emit text to stderr (script progress markers). |
| `.version` | Build version, Go toolchain, VCS stamp, dep graph with go.sum hashes (from `runtime/debug.ReadBuildInfo`, honors `replace` pins). `-version` flag prints the same and exits. |
| `.quit` / `.exit` | Leave. |

## Output modes and the box renderer

Formatters live in `cmd/render.go`, taking `[]json.RawMessage` (+ `pretty bool`) from
`Result{Labels, Rows}` — no engine coupling.

- **`box`** (default, TTY): columns = `Result.Labels` (a bare `raw`/`SELECT VALUE` → single `value`
  column); box-drawing borders; right-align numbers, left-align strings; nested objects/arrays as
  compact JSON truncated to `.maxwidth`. `.maxwidth auto` fits the box to terminal width (columns
  widen into spare space, shrink max-min fair-share only on overflow). Footer
  `N rows (showing X) · C columns · elapsed` when `.timer on`. Over `.maxrows`, head+tail split by a
  `·····` elision row (true count in footer; negative keeps the last `|n|`).
- **`jsonlines`** (default, pipes/`-c`) — one canonical JSON row per line. **`json`** — one pretty
  array. **`csv`** — header from labels, nested values as quoted JSON text. **`markdown`** — GitHub
  table. **`line`** — DuckDB vertical `key = value` per field (best for wide/nested docs). **`list`**
  — values joined by a separator (pipe-friendly).
- **`|pretty` modifier:** any mode may carry `|pretty` (or `-pretty`) — indents nested JSON 2 spaces.
  In `box` a pretty cell spans multiple lines (row grows to its tallest cell, others blank-pad);
  `markdown|pretty` folds newlines to `<br>`; `csv|pretty` relies on the csv writer quoting newlines.

## Line editing: shipped keys + deferred multi-line/mouse

**Shipped:** the REPL binds `peterh/liner`'s full single-line emacs set (`Ctrl-A/E`, `Ctrl-B/F` +
arrows, `Ctrl-D`, `Ctrl-K/U/W`, `Ctrl-Y`, `Ctrl-T`, `Ctrl-L`, `Ctrl-R/S` search, `Ctrl-P/N` +
arrows history, `Ctrl-C` abort). Statements are usually one line, so history walking covers most of
the "emacs feel."

**Deferred** — two gaps, both requiring a step from readline-class blocking `Prompt()` to a TUI-class
raw-mode event loop with a 2D buffer (not a drop-in swap): (1) multi-line 2D cursor nav (liner has no
multi-row buffer); (2) mouse click-to-position (needs xterm mouse reporting + `(col,row)`→offset
mapping, and mouse mode breaks native select-to-copy). **Recommendation:** keep `peterh/liner`
(zero-cost, fits the blocking loop). If multi-line matters, `reeflective/readline` (Apache-2.0) adds
true multi-row emacs editing while keeping the blocking model; only if mouse click-to-position is
also wanted, move to `charmbracelet/bubbletea` + `bubbles/textarea` (MIT), paying the Elm-loop /
larger-dep / select-to-copy cost. ⚠ Add any dep via `go get <pkg>@<ver>`, never `go mod tidy`; verify
`CGO_ENABLED=0`.
