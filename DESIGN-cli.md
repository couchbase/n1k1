# n1k1 CLI — design

A single-binary CLI and shell for SQL++/N1QL over n1k1's file datastore:
download, point at data, get pretty results. REPL, one-shot queries,
pipe-friendly output — inspired by DuckDB/sqlite, adapted to n1k1 (SQL++,
directory-as-database, query-focused). Key enabler: extract the engine's
end-to-end driver into a reusable `glue.Session` so `cmd/n1k1` is a pure
front-end.

## Status & remaining TODOs

_Last reviewed: 2026-07-23._

**Done:** The CLI ships as a single-binary REPL + one-shot/`-c`/`-f`/pipe
front-end over `glue.Session`, with `box`/`box|pretty`/`jsonlines`/`json`/`csv`/
`markdown`/`line`/`list` output modes and a rich dot-command set (`.tables`/
`.keyspaces`, `.schema`, `.index`, `.multi`, `.extract`, `.extensions`, `.mode`/
`.meta`/`.formats`/`.timer`/`.stats`/`.explain`/`.prepare`/`.verbose`/`.maxrows`/
`.maxwidth`/`.read`/`.output`/`.bail`/`.echo`/`.print`/`.open`/`.version`), a
`~/.n1k1rc` init file, `peterh/liner` line editing, framing-tagged keyspace
listings, reserved-word / shell-quoting hints, and materialization statements
(`CREATE TEMP KEYSPACE`, `INSERT INTO … SELECT`) surfaced in `.help`.

**Remaining (headline TODOs):**
- [ ] `.schema` with no arg dumps giant box tables (ignores `.maxwidth`) on a real bundle — make it a compact one-line-per-keyspace summary, or redirect to `.tables`.
- [ ] File-as-table (`FROM 'foo.csv'` / `read_json_auto(...)`): scans exist but aren't reachable through N1QL `FROM` via glue.
- [x] Query cancellation mid-run — DONE: Ctrl-C during a query cooperatively halts it
  (Session.Interrupt → Ctx.Halt → scans stop with base.ErrHalted), keeping the session; a
  closed output pipe (`… | head`) halts the same way; Ctrl-D / double-Ctrl-C exit.
- [ ] Tab completion of keywords / keyspaces / dot-commands.
- [ ] Multi-line 2D cursor editing + mouse click-to-position (`reeflective/readline` or `bubbletea`) — deferred (§7).
- [ ] `.import` / `COPY` / writes (engine is query-only); persistent settings / PRAGMA.

## Contents

1. [DuckDB inspiration and n1k1 fit](#1-duckdb-inspiration-and-n1k1-fit)
2. [The core refactor: a reusable session](#2-the-core-refactor-a-reusable-session)
3. [Binary, build, and invocation](#3-binary-build-and-invocation)
4. [The REPL](#4-the-repl)
5. [Dot-commands](#5-dot-commands)
6. [Output modes and the box renderer](#6-output-modes-and-the-box-renderer)
7. [Line editing: emacs keys, multi-line, and mouse](#7-line-editing-emacs-keys-multi-line-and-mouse)
8. [Resolved decisions](#8-resolved-decisions)
9. [Build order (delivered)](#9-build-order-delivered)
10. [Deferred / future work](#10-deferred--future-work)

------------------------------------------------------------------------
## 1. DuckDB inspiration and n1k1 fit

| DuckDB trait | n1k1 fit |
|---|---|
| **Single static binary**, no deps/server | **Perfect** — pure-Go, `CGO_ENABLED=0`, cross-compiles; `cmd/n1k1` is ~free. |
| **REPL** (multiline-until-`;`, history, Ctrl-C/D) | Shipped; on `peterh/liner`. |
| **`-c "<sql>"`** one-shot + **stdin pipe** | Shipped (`-c`, `-f`, stdin batch). |
| **Dot-commands** (`.help`, `.tables`, `.mode`, `.open`, `.read`, `.timer`…) | Shipped; maps onto store/render state. |
| **`box` renderer** (boxed table, footer, truncation) | Shipped; column set is `conv.TopOp.Labels`. |
| **Output modes** (`json`, `jsonlines`, `csv`, `markdown`, `line`, `list`) | Shipped; canonical JSON rows, each mode a formatter. |
| **`.timer on`** | Shipped; wraps the engine call with timing. |
| **File-as-table** (`FROM 'data.csv'`) | **Partial** — CSV/NDJSON scans exist, but N1QL `FROM` resolves keyspaces in a dir. A single-file arg becomes a keyspace; bare `FROM 'file'` is later (§10). |
| **`~/.duckdbrc` init**, `-init` | Shipped (`~/.n1k1rc`, `-init`). |
| **Syntax errors with a caret** | Partial — parse errors carry a renderable offset. |

**Out of scope for v1:** persistent DB file format (store is read-only JSON
dir), general DDL flows, HTTP/UI server. (Since landed beyond v1: `INSERT INTO`
phase-1 materialize-to-a-brand-new-keyspace-file — see `DESIGN-data.md` §2
"`INSERT INTO` — user-driven materialization"; extensions/`INSTALL` — see
`DESIGN-extensions.md`.)

------------------------------------------------------------------------
## 2. The core refactor: a reusable session

The end-to-end driver, once hardcoded in `test/suite_test.go:
n1k1RunStatement`, now lives in `glue` as the shared engine for both test and
CLI:

```go
// glue/session.go  (//go:build n1ql)

type Session struct {
    Store     *Store
    Namespace string            // "default"; render/REPL state stays in the CLI
}

func OpenSession(datastoreDir string) (*Session, error) // FileStore + InitParser

// Rows are canonical JSON (same as today's harness).
type Result struct {
    Labels   base.Labels   // column set, from conv.TopOp.Labels
    Rows     []json.RawMessage
    Elapsed  time.Duration
    Plan     *base.Op      // optional, for .explain / debug
}

// Run promotes the harness's unsupported-vs-genuine-error distinction to a
// typed error so the CLI can say "not supported yet" vs. "your query is wrong".
func (s *Session) Run(stmt string) (*Result, error)

type ErrUnsupported struct{ Reason string }  // nil TopOp, convert failure, panic
```

`Run` is the body of `n1k1RunStatement` minus test plumbing:
`ParseStatement → store.PlanStatement → Conv.Accept → NewConvertVals → MakeVars
→ ExecOp(DatastoreOp)`. The harness is now a thin caller of `Run` (the
extraction held the pass count). Engine knowledge stays in `glue/`; `cmd/n1k1`
is a pure front-end.

------------------------------------------------------------------------
## 3. Binary, build, and invocation

- **Location:** `cmd/n1k1/main.go`, behind `//go:build n1ql` (depends on
  n1ql-gated `glue/`). Built under `make n1ql`; add `make cli` /
  `make install-cli`.
- **Build:** `CGO_ENABLED=0 go build -tags n1ql ./cmd/n1k1`; cross-compiles.

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
                  (append |pretty, e.g. box|pretty, to indent JSON 2 spaces;
                   jsonlines also accepts jsonl / ndjson)
  -timer          show timing
  -echo           echo each input line as read (like .echo on; handy with -f)
  -init <file>    run dot-commands/SQL at startup (default ~/.n1k1rc;
                  "", "-" or "none" skips it)
  -index <mode>   secondary index build: eager|lazy|off (DESIGN-indexing.md)
  -formats <set>  restrict scanning to a format set (DESIGN-data.md)
  -meta <mode>    per-record _meta injection: on|off|auto
  -verbose / -v   diagnostics level (bare = on; -verbose=on|off|debug|<n>)
  -stats <mode>   per-op counters: on (live) | off | final
  -prepare <lvl>  max compile level: interpreted | data | full (DESIGN-prepare.md)
  -ext <path>     load extension(s) (dir/file, repeatable; .js = JS UDF)
  -extensions <path>   alias for -ext
  -variant-fidelity    Parquet VARIANT scan carries typed-scalar fidelity
                  (V-carrier) instead of the Phase-0 JSON projection
                  (records.VariantFidelity; DESIGN-variant.md)
  -version        print version + build info and exit
  -profile-cpu / -profile-mem <file>   pprof profiles
```

Namespace isn't a flag — n1k1's file datastore only uses `default`, so it's the
`defaultNamespace` const (a rare multi-namespace tree is still reachable via a
`<ns>:<keyspace>` qualifier in SQL). `-index`/`.indexes` owned by
**DESIGN-indexing.md "CLI control"**; `-formats`/`-meta` owned by
**DESIGN-data.md**; `-stats` by **DESIGN-stats.md**; `-prepare` by
**DESIGN-prepare.md**.

**Default mode:** TTY → `box|pretty`; pipe/`-c` → `jsonlines` (compact) unless
`-mode` overrides.

**Remote sources:** the datastore-dir / `FROM` target may now be an object-store URL
(`s3://…`, `gs://…`, `abfs://…`) — a bare Parquet object, or an Iceberg/Parquet table dir
becomes a FROM-able keyspace. See DESIGN-data.md §8.

------------------------------------------------------------------------
## 4. The REPL

- **Prompt:** `n1k1>`; continuation `   ...>` until a statement ends with `;`.
  Buffer accumulates lines; `;` flushes to `Session.Run`.
- **Dot-commands:** recognized when a line starts with `.` and no SQL is
  buffered; execute immediately, no `;`.
- **Signals:** at the prompt, Ctrl-C clears the input buffer (a second Ctrl-C at an
  empty prompt exits); during a query, Ctrl-C cooperatively HALTS it and keeps the
  session (a second force-quits); a closed output pipe (`… | head`) halts the query
  the same way (SIGPIPE ignored so the write returns EPIPE); Ctrl-D / `.quit` /
  `.exit` exit. Engine side: Session.Interrupt → Ctx.Halt, checked at scan checkpoints.

**Line editing / history:** the REPL runs on `github.com/peterh/liner` (MIT,
pure Go) — arrow-key history + emacs editing; history persists to
`~/.n1k1_history` (§7, §8). Add deps via explicit `go get <pkg>@<ver>`, never
`go mod tidy` (it prunes the n1ql-only `query` dep); verify `CGO_ENABLED=0`.

------------------------------------------------------------------------
## 5. Dot-commands

DuckDB names where the concept exists, so muscle memory carries. The shipped set
(dispatched in `cmd/n1k1/dot.go`; every on/off-style setting shows its current
value in `.help`):

| Command | Behavior |
|---|---|
| `.help` | List commands, plus the current datastore + a live example query. |
| `.open <dir>` | Open a new file datastore dir (re-`FileStore`+`InitParser`); closes the prior session's TEMP KEYSPACE spills. |
| `.tables` / `.keyspaces` | List keyspaces (via the datastore interface, so flattening/synthetic roots show), each tagged with its record framing + file count and a copy-paste example. |
| `.schema [<keyspace>]` | Sampled shape from a 50-doc `SELECT x.*` sample (fields + JSON types + distinct values + a WHERE example), rendered as a box. No arg → every keyspace (giant on a real bundle — a compact summary is a TODO). |
| `.index [list\|show <name>\|rebuild [<name>]\|suggest [<ks>]\|help]` | Secondary-index family (`.indexes` = `.index list`). Owned by DESIGN-indexing.md. |
| `.mode <m>` | Set output mode (§6); `jsonl`/`ndjson` are synonyms for `jsonlines`. |
| `.meta [on\|off\|auto]` | Get/set `_meta` sub-object (path/name/ext/size/mtime). Mirrors `-meta`; mutates `glue.ScanWalkOptions.Meta`. |
| `.formats [<set>]` | Get/set which formats/modes scanning considers (persists to catalog.json for a dir). Mirrors `-formats`. |
| `.timer [on\|off]` | Elapsed-time footer. |
| `.stats [on\|off\|final\|about]` | Per-operator counters: live footer / totals-at-end / glossary. DESIGN-stats.md. |
| `.explain [on\|off]` | Also print the converted `base.Op` plan tree (per-expr native vs boxed); shows *why* something is UNSUPPORTED. |
| `.prepare [interpreted\|data\|full \| <stmt>]` | Set the compile-level ceiling, or one-shot emit the generated Go for `<stmt>` then run it. DESIGN-prepare.md. |
| `.verbose [off\|on\|debug\|<n>]` | Diagnostics level; routes `base.Logf` through the same knob. |
| `.maxrows <n>` | box: cap rows. `>0` = head+tail with `·` elision; `<0` = last `|n|` rows; `0` = all. |
| `.maxwidth <n\|auto>` | box: cap column width, truncate with `…`. `0` = uncapped; `auto` = fit box to terminal. |
| `.multi [list\|run\|lint\|test\|help]` | Run/lint/test a corpus of tagged `*.sql++` detector recipes over the open bundle (`--queries <dir>`). DESIGN-prepare.md. |
| `.extensions [list\|load <dir>…\|unload <name>…]` (`.ext`) | Manage loaded extensions (`.js` = JavaScript UDF). |
| `.extract [help\|list]` | Authoring reference + inventory for `*.extract.js` framing recipes. |
| `.macro [help\|list\|expand <stmt>]` (`.macros`) | Pre-parse SQL++ macros: `@name(...)` → generated SQL++; `expand` shows the rewrite (`*.macro.js` recipes). |
| `.read <file>` | Execute statements/dot-commands from a file. |
| `.output [<file>]` | Redirect results to a file, or back to stdout. |
| `.bail [on\|off]` | Stop the input loop on the first statement error (scripts). |
| `.echo [on\|off]` | Echo each input line as it's read (scripts). |
| `.print <text>` | Emit text to stderr (script progress markers). |
| `.version` | Build version, Go toolchain, VCS stamp, dep graph with go.sum hashes (from `runtime/debug.ReadBuildInfo`, honors `replace` pins). `-version` flag prints the same and exits. |
| `.quit` / `.exit` | Leave. |

------------------------------------------------------------------------
## 6. Output modes and the box renderer

Formatters live in `cmd/render.go`, taking `[]json.RawMessage` (+ `pretty
bool`) from `Result{Labels, Rows}` — no engine coupling.

- **`box`** (default, TTY):
  - Columns = `Result.Labels`; bare value (`raw`/`SELECT VALUE`) → single
    `value` column.
  - Box-drawing borders (`┌─┬─┐ │ ├─┼─┤ └─┴─┘`); right-align numbers, left-align
    strings; nested objects/arrays as compact JSON truncated to `.maxwidth`.
  - `.maxwidth auto`: fit box to terminal width (tty, else `$COLUMNS`); columns
    widen into spare space, shrink (max-min fair share) only on overflow.
  - Footer: `N rows (showing X)  ·  C columns  ·  elapsed` when `.timer on`.
  - Over `.maxrows`: head+tail split by a `·····` elision row, true count in
    footer; negative keeps the last `|n|` rows.
- **`jsonlines`** (default, pipes/`-c`) — one canonical JSON row per line.
- **`json`** — single pretty JSON array.
- **`csv`** — header from labels; nested values as quoted JSON text.
- **`markdown`** — GitHub table.
- **`line`** — DuckDB vertical mode: `key = value` per field, blank line between
  rows. Best for wide/nested docs.
- **`list`** — values joined by a separator (pipe-friendly).

**`|pretty` modifier:** any mode may carry `|pretty` (or `-pretty`) — e.g.
`box|pretty`, `json|pretty` — indenting nested JSON by 2 spaces. In `box` a
pretty cell spans multiple lines; the row grows to its tallest cell, others
blank-pad. `markdown|pretty` folds newlines to `<br>`; `csv|pretty` relies on
the csv writer quoting newlines.

------------------------------------------------------------------------
## 7. Line editing: emacs keys, multi-line, and mouse

**Shipped:** the REPL binds `peterh/liner`'s full single-line emacs set (`Ctrl-A`/
`Ctrl-E`, `Ctrl-B`/`Ctrl-F` + arrows, `Ctrl-D`, `Ctrl-K`/`Ctrl-U`/`Ctrl-W`,
`Ctrl-Y`, `Ctrl-T`, `Ctrl-L`, `Ctrl-R`/`Ctrl-S` search, `Ctrl-P`/`Ctrl-N` +
arrows history, `Ctrl-C` abort). Statements are usually one line, so history
walking covers most of the "emacs feel".

**Deferred** (two gaps, both requiring a step from readline-class blocking
`Prompt()` to a TUI-class raw-mode event loop with a 2D buffer, not a drop-in
swap): (1) multi-line 2D cursor nav — liner has no multi-row buffer; and
(2) mouse click-to-position — liner has no Unix mouse support (needs xterm
mouse reporting `ESC[?1000h`/SGR `ESC[?1006h` + `(col,row)`→offset mapping, and
mouse mode breaks native select-to-copy so users hold Shift/Option to select).

Library options for that later step (all permissive; none GPL/viral):

| Library | License | Multi-line | Mouse | Model | Notes |
|---|---|---|---|---|---|
| `peterh/liner` *(current)* | MIT | ✗ | ✗ | blocking | single line + emacs + history |
| `chzyer/readline` | MIT | partial | ✗ | blocking | fuller readline (kill-ring, vi); single line |
| `reeflective/readline` | Apache-2.0 | ✓ | ✗ | blocking | true multiline emacs/vi — closes gap #1 |
| `c-bata/go-prompt` (+ `elk-language` fork) | MIT | ✗ | ✗ | callback | rich completion; single line |
| `charmbracelet/bubbletea` + `bubbles/textarea` | MIT | ✓ | ✓ | Elm loop | 2D nav + click + SGR — closes **both** gaps |
| `gdamore/tcell` (+ `rivo/tview`) | Apache-2.0 | ✓ | ✓ | event loop | lowest level; full mouse + 2D |

**Recommendation:** keep `peterh/liner` (zero-cost, fits the blocking loop). If
multi-line editing matters, `reeflective/readline` (Apache-2.0) adds true
multi-row emacs editing while keeping the blocking model. Only if mouse
click-to-position is also wanted, move to `bubbletea` + `bubbles/textarea`
(MIT), paying the Elm-loop / larger-dep / select-to-copy cost. Add any dep via
`go get <pkg>@<ver>`, never `go mod tidy`; verify `CGO_ENABLED=0`.

------------------------------------------------------------------------
## 8. Resolved decisions

- **Binary name:** `n1k1` (`cmd/n1k1/main.go`); matches the module name.
- **Default TTY mode:** `box|pretty`; pipes/`-c` default to compact
  `jsonlines`. (`.mode line` available for wide/nested docs.)
- **Line editor:** `peterh/liner v1.2.0` (MIT, CGO-free) for history + editing;
  history persists to `~/.n1k1_history`. See §7.

------------------------------------------------------------------------
## 9. Build order (delivered)

The staged plan, all shipped: (0) extract `glue.Session` from
`n1k1RunStatement` and re-point the test (pass count held); (1) minimal CLI —
`-c` + stdin + read-until-`;` REPL, `jsonlines`; (2) box renderer + `.mode` +
`.timer`; (3) navigation dot-commands (`.open`, `.tables`/`.keyspaces`,
`.schema`, `.read`, `.output`, `.help`, `.quit`); (4) niceties — history + line
editing, `~/.n1k1rc`, `.explain`, `.maxrows`/`.maxwidth`. The dot-command set
has since grown well past this (§5). Still partial from the original plan: the
syntax-error caret (parse errors carry an offset but no rendered caret yet).

------------------------------------------------------------------------
## 10. Deferred / future work

- **File-as-table** `FROM 'foo.csv'` / `read_json_auto(...)`: scans exist but
  aren't reachable through N1QL `FROM` via glue; its own project.
- ~~Query cancellation mid-run~~ — DONE (cooperative `Ctx.Halt`; Ctrl-C / closed pipe).
- **Tab completion** of keywords / keyspaces / dot-commands.
- **`.import` / `COPY` / writes** — engine is query-only.
- **Live stats:** `.stats`/`-stats` (live footer + final totals) shipped;
  still-designed extras in `DESIGN-stats.md` — a `pruning` view, `.rec`/`.play`
  record-and-replay, and `EXPLAIN PRICE`/`EXPLAIN COST` (`.price`/`.cost`).
- **Persistent settings / PRAGMA**.
- **Syntax-error caret:** parse errors carry a renderable offset, but the CLI
  doesn't yet draw the `^` under the offending token.
