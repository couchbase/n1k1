# n1k1 CLI — design

A single-binary CLI and shell for SQL++/N1QL over n1k1's file datastore:
download, point at data, get pretty results. REPL, one-shot queries,
pipe-friendly output — inspired by DuckDB/sqlite, adapted to n1k1 (SQL++,
directory-as-database, query-focused). Key enabler: extract the engine's
end-to-end driver into a reusable `glue.Session` so `cmd/n1k1` is a pure
front-end.

## Contents

1. [DuckDB inspiration and n1k1 fit](#1-duckdb-inspiration-and-n1k1-fit)
2. [The core refactor: a reusable session](#2-the-core-refactor-a-reusable-session)
3. [Binary, build, and invocation](#3-binary-build-and-invocation)
4. [The REPL](#4-the-repl)
5. [Dot-commands (v1 set)](#5-dot-commands-v1-set)
6. [Output modes and the box renderer](#6-output-modes-and-the-box-renderer)
7. [Line editing: emacs keys, multi-line, and mouse](#7-line-editing-emacs-keys-multi-line-and-mouse)
8. [Resolved decisions](#8-resolved-decisions)
9. [Build order](#9-build-order)
10. [Deferred / future work](#10-deferred--future-work)

------------------------------------------------------------------------
## 1. DuckDB inspiration and n1k1 fit

| DuckDB trait | n1k1 fit |
|---|---|
| **Single static binary**, no deps/server | **Perfect** — pure-Go, `CGO_ENABLED=0`, cross-compiles; `cmd/n1k1` is ~free. |
| **REPL** (multiline-until-`;`, history, Ctrl-C/D) | New, small; engine call exists. |
| **`-c "<sql>"`** one-shot + **stdin pipe** | New, thin. |
| **Dot-commands** (`.help`, `.tables`, `.mode`, `.open`, `.read`, `.timer`…) | New; maps onto store/render state. |
| **`box` renderer** (boxed table, footer, truncation) | New; column set is `conv.TopOp.Labels`. |
| **Output modes** (`json`, `jsonlines`, `csv`, `markdown`, `line`, `list`) | New; canonical JSON rows, each mode a formatter. |
| **`.timer on`** | New; wrap engine call with timing. |
| **File-as-table** (`FROM 'data.csv'`) | **Partial** — CSV/NDJSON scans exist, but N1QL `FROM` resolves keyspaces in a dir. v1 opens a dir; file-tables later (§10). |
| **`~/.duckdbrc` init**, `-init` | New, trivial. |
| **Syntax errors with a caret** | Partial — parse errors carry a renderable offset. |

**Out of scope for v1:** persistent DB file format (store is read-only JSON
dir), general DDL flows, HTTP/UI server. (Since landed beyond v1: `INSERT INTO`
phase-1 materialize-to-a-brand-new-keyspace-file — see `DESIGN-data.md` §2
"`INSERT INTO` — user-driven materialization"; extensions/`INSTALL` — see
`DESIGN-extensions.md`.)

------------------------------------------------------------------------
## 2. The core refactor: a reusable session

Today the only end-to-end driver is `test/suite_test.go: n1k1RunStatement`,
which hardcodes the pipeline. Extract it into `glue` as the shared engine for
both test and CLI:

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
→ ExecOp(DatastoreOp)`. The harness becomes a ~10-line caller of `Run`; if the
pass count holds at **631**, the extraction is correct. Engine knowledge stays
in `glue/`; `cmd/n1k1` is a pure front-end.

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
  -c <stmt>      run one statement and exit
  -f <file>      run statements from a file and exit
  -ns <name>     namespace (default "default")
  -mode <m>      box|json|jsonlines|csv|markdown|line|list
                 (append |pretty, e.g. box|pretty, to indent JSON 2 spaces)
  -timer         show timing
  -init <file>   run dot-commands/SQL at startup (default ~/.n1k1rc)
  -no-init       skip the init file
  -readonly      no-op (store already read-only — reserved)
  -index <mode>  secondary index build: eager|lazy|off (DESIGN-indexing.md)
  -formats <set> restrict scanning to a format set (DESIGN-data.md)
  -meta <mode>   per-record _meta injection: on|off|auto
  -v             verbose (unsupported reasons, plan on error)
```

`-index`/`.indexes` owned by **DESIGN-indexing.md "CLI control"**; `-formats`/
`-meta` owned by **DESIGN-data.md**.

**Default mode:** TTY → `box|pretty`; pipe/`-c` → `jsonlines` (compact) unless
`-mode` overrides.

------------------------------------------------------------------------
## 4. The REPL

- **Prompt:** `n1k1>`; continuation `   ...>` until a statement ends with `;`.
  Buffer accumulates lines; `;` flushes to `Session.Run`.
- **Dot-commands:** recognized when a line starts with `.` and no SQL is
  buffered; execute immediately, no `;`.
- **Signals:** Ctrl-C cancels the input buffer (not the process); Ctrl-D /
  `.quit` / `.exit` exits. (Engine-level cancellation is later, §10.)

**Line editing / history:** REPL runs on `github.com/peterh/liner` (MIT, pure
Go) — arrow-key history + emacs editing from v1; history persists to
`~/.n1k1_history` (§7, §8). Add via explicit `go get <pkg>@<ver>`, never
`go mod tidy` (it prunes the n1ql-only `query` dep); verify `CGO_ENABLED=0`.

------------------------------------------------------------------------
## 5. Dot-commands (v1 set)

Match DuckDB names where the concept exists, so muscle memory carries.

| Command | Behavior |
|---|---|
| `.help` | List commands. |
| `.open <dir>` | Open a new file datastore dir (re-`FileStore`+`InitParser`). |
| `.tables` / `.keyspaces` | List keyspaces under the namespace (subdirs of `<dir>/<ns>/`); accept both names, print "keyspaces". |
| `.schema [<keyspace>]` | Sampled shape from first N docs (top-level keys + JSON types); labeled sampled, since no real schema exists. |
| `.index [list\|show <name>\|rebuild [<name>]\|suggest [<ks>]\|help]` | Secondary-index family (`.indexes` = `.index list`): list/show from `.n1k1/catalog.json`, force-rebuild, suggest from a sample, create defs + build. Owned by DESIGN-indexing.md. |
| `.mode <m>` | Set output mode (§6). |
| `.meta [on\|off\|auto]` | Get/set `_meta` sub-object (path/name/ext/size/mtime/pos). Mirrors `-meta`; mutates `glue.ScanWalkOptions.Meta`. |
| `.formats [<set>]` | Get/set which formats/modes (json,csv,gzip,recurse,…) scanning considers. Mirrors `-formats`; over `glue.ScanWalkOptions`. |
| `.timer on\|off` | Toggle elapsed-time footer. |
| `.maxrows <n>` | box: cap rows. `>0` = head+tail with `·` elision; `<0` = last `|n|` rows, elision at front; `0` = all. |
| `.maxwidth <n\|auto>` | box: cap column width, truncate with `…`. `0` = uncapped; `auto` = fit box to terminal (max-min fair share). |
| `.read <file>` | Execute statements/dot-commands from a file. |
| `.output <file>` / `.output` | Redirect results to file / back to stdout. |
| `.explain` | Toggle: also print the converted `base.Op` plan tree; shows *why* something is UNSUPPORTED. |
| `.version` | Build version (`git describe` via `-ldflags -X main.version`), Go toolchain, VCS stamp, dep graph with go.sum hashes (from `runtime/debug.ReadBuildInfo`, honors `replace` pins). `-version` flag prints same and exits. |
| `.shell <cmd>` / `.system <cmd>` | Run a shell command (gated, off by default). |
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

Some CLIs support emacs keys (`Ctrl-N`/`Ctrl-P` between lines, `Ctrl-A`/
`Ctrl-E`) plus mouse click-to-position. What would n1k1 need, using non-viral
libraries?

### 7.1 What we have for free

The REPL runs on **`github.com/peterh/liner`** (MIT, pure Go), binding the full
single-line emacs set: `Ctrl-A`/`Ctrl-E` (line ends), `Ctrl-B`/`Ctrl-F` +
arrows (move), `Ctrl-D` (delete char / EOF if empty), `Ctrl-K`/`Ctrl-U` (kill
to end/start), `Ctrl-W` (delete word), `Ctrl-Y` (yank), `Ctrl-T` (transpose),
`Ctrl-L` (clear), `Ctrl-R`/`Ctrl-S` (history search), `Ctrl-P`/`Ctrl-N` +
arrows (prev/next history), `Ctrl-C` (abort line).

Statements are usually one line, so `Ctrl-P`/`Ctrl-N` history walking covers
most of the "emacs feel".

### 7.2 What's genuinely missing

1. **Multi-line cursor nav.** liner has no 2D buffer — `Ctrl-P`/`Ctrl-N` move
   through history, not rows of a multi-row buffer. Claude-Code-style
   up/down-a-line needs a 2D cursor over a multi-row buffer.
2. **Mouse click-to-position.** liner has zero Unix mouse support. Needs xterm
   mouse reporting (`ESC[?1000h` + SGR `ESC[?1006h`; terminal sends
   `ESC[<btn;col;row;M/m`), event parsing, `(col,row)` → offset mapping.

Both push from **readline-class** (blocking `Prompt()`) to **TUI-class** (own
raw mode, render input, event loop, 2D buffer) — an architectural step change,
not a swap.

### 7.3 Library landscape (all permissive)

| Library | License | Multi-line | Mouse | Model | Notes |
|---|---|---|---|---|---|
| `peterh/liner` *(current)* | MIT | ✗ | ✗ | blocking | single line + emacs + history |
| `chzyer/readline` | MIT | partial | ✗ | blocking | fuller readline (kill-ring, vi); single line |
| `reeflective/readline` | Apache-2.0 | ✓ | ✗ | blocking | true multiline emacs/vi — closes gap #1 |
| `c-bata/go-prompt` (+ `elk-language` fork) | MIT | ✗ | ✗ | callback | rich completion; single line |
| `charmbracelet/bubbletea` + `bubbles/textarea` | MIT | ✓ | ✓ | Elm loop | 2D nav + click + SGR — closes **both** gaps |
| `gdamore/tcell` (+ `rivo/tview`) | Apache-2.0 | ✓ | ✓ | event loop | lowest level; full mouse + 2D |

No mainstream Go line-editor is GPL/viral — the choice is architecture vs.
feature.

### 7.4 Mouse-mode caveat

xterm mouse reporting takes clicks from the terminal, breaking native
select-to-copy (users then hold **Shift**, or **Option/Alt** on macOS iTerm2).
Mitigation: enable mouse only while the prompt is up, disable around output,
document Shift-to-select.

### 7.5 Recommendation

- **Now:** keep **`peterh/liner`** — emacs single-line bindings incl. history,
  ~zero-cost dep, blocking `Prompt()` fits the loop.
- **If multi-line editing matters:** **`reeflective/readline`** (Apache-2.0) —
  keeps the blocking model, adds true multi-row emacs editing, no TUI/mouse
  tradeoff.
- **If mouse click-to-position too (Claude-Code parity):** **`bubbletea` +
  `bubbles/textarea`** (MIT). Cost: Elm event-loop, larger dep tree, §7.4
  caveat. **Deferred** — revisit if long multi-line statements become common.
- **Dependency hygiene:** add via `go get <pkg>@<ver>`, never `go mod tidy`;
  verify `CGO_ENABLED=0` builds.

------------------------------------------------------------------------
## 8. Resolved decisions

- **Binary name:** `n1k1` (`cmd/n1k1/main.go`); matches the module name.
- **Default TTY mode:** `box|pretty`; pipes/`-c` default to compact
  `jsonlines`. (`.mode line` available for wide/nested docs.)
- **Line editor:** `peterh/liner v1.2.0` (MIT, CGO-free) for history + editing
  from v1; history persists to `~/.n1k1_history`. See §7.

------------------------------------------------------------------------
## 9. Build order

Each step independently shippable.

0. **Extract `glue.Session`** from `n1k1RunStatement`; re-point the test. Gate:
   pass count stays **631**. (No user-visible change; de-risks the rest.)
1. **Minimal CLI:** `cmd/n1k1`, `-c` + stdin + naive REPL (read-until-`;`),
   `jsonlines` only.
2. **box renderer + `.mode`** (box/jsonlines/json/csv/markdown/line), `.timer`.
3. **Navigation dot-commands:** `.open`, `.tables`/`.keyspaces`, `.schema`,
   `.read`, `.output`, `.help`, `.quit`.
4. **Niceties:** history + line editing, `~/.n1k1rc`, `.explain`, syntax-error
   caret, `.maxrows`/`.maxwidth`.

------------------------------------------------------------------------
## 10. Deferred / future work

- **File-as-table** `FROM 'foo.csv'` / `read_json_auto(...)`: scans exist but
  aren't reachable through N1QL `FROM` via glue; its own project.
- **Query cancellation** mid-run (Ctrl-C aborting `ExecOp` via ctx).
- **Tab completion** of keywords / keyspaces / dot-commands.
- **`.import` / `COPY` / writes** — engine is query-only.
- **Progress / live stats** (engine has `YieldStats` seam). Designed in
  `DESIGN-stats.md`: `-progress` flag + `.stats`, a `pruning` view, `.rec`/
  `.play` record-and-replay, `EXPLAIN PRICE`/`EXPLAIN COST` (`.price`/`.cost`).
- **Persistent settings / PRAGMA**.
