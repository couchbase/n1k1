# n1k1 CLI — design

A single-binary, batteries-included CLI and shell for running
SQL++/N1QL using n1k1's engine over a file datastore. We take
inspiration from great CLI's, such as from DuckDB, sqlite, etc.

Just download the binary and run -- point it at some data, get pretty
results — adapted to n1k1's capabilities (SQL++, directory-as-database,
query-focused). REPL welcome.

------------------------------------------------------------------------
## 1. What we're borrowing from DuckDB (and why it fits)

DuckDB's CLI taste, and how each piece maps onto n1k1:

| DuckDB trait | Why it's loved | n1k1 fit |
|---|---|---|
| **Single static binary**, no deps, no server | `brew install`/download and go | **Perfect** — the n1ql engine is already pure-Go, `CGO_ENABLED=0`, cross-compiles to linux/darwin/windows. A `cmd/n1k1` binary is ~free. |
| **REPL** with multiline-until-`;`, history, Ctrl-C/Ctrl-D | Low-friction exploration | New, but small; the engine call already exists. |
| **`-c "<sql>"`** one-shot + **stdin pipe** | Scriptable, composable with unix | New, thin. |
| **Dot-commands** (`.help`, `.tables`, `.mode`, `.open`, `.read`, `.timer`…) | Meta-ops that aren't SQL | New; maps cleanly onto store/render state. |
| **`box` renderer** (boxed unicode table, row-count footer, smart truncation) | The signature "it just looks good" | New; n1k1 already knows the **column set** — it's `conv.TopOp.Labels`. |
| **Multiple output modes** (`json`, `jsonlines`, `csv`, `markdown`, `line`, `list`) | Right format for the job | New; the engine yields canonical JSON rows, so each mode is a formatter. |
| **`.timer on`** | Instant feedback on cost | New; engine call is easy to wrap with timing. |
| **File-as-table** (`FROM 'data.csv'`) | Zero-setup querying | **Partial** — n1k1 has CSV/NDJSON file scans, but the N1QL `FROM` resolves keyspaces in a datastore dir. v1 opens a directory; file-table funcs are a later item (§7). |
| **`~/.duckdbrc` init file**, `-init` | Personalization | New, trivial. |
| **Rich syntax errors with a caret** | Teaching tool | Partial — couchbase/query parse errors carry an offset we can render. |

Things we deliberately **don't** take on yet in v1: persistent DB file format
(n1k1's store is a read-only directory of JSON), `INSERT`/DDL-heavy flows
(the engine is query-focused), extensions/`INSTALL`, the HTTP/UI server.

------------------------------------------------------------------------
## 2. The single most important refactor: a reusable session

Today the *only* end-to-end driver is the test harness `test/suite_test.go:
n1k1RunStatement`. It hardcodes the full pipeline. The CLI must not duplicate
that. Extract it into `glue` as the shared engine both the test and the CLI use:

```go
// glue/session.go  (//go:build n1ql)

type Session struct {
    Store     *Store
    Namespace string            // "default"
    // render/REPL state lives in the CLI, not here.
}

func OpenSession(datastoreDir string) (*Session, error) // wraps FileStore + InitParser

// Result of one statement. Rows are canonical JSON (same as today's harness).
type Result struct {
    Labels   base.Labels   // column set, from conv.TopOp.Labels
    Rows     []json.RawMessage
    Elapsed  time.Duration
    Plan     *base.Op      // optional, for .explain / debug
}

// Run executes one statement. The (unsupported vs. genuine error) distinction
// the harness already makes is promoted to a typed error so the CLI can phrase
// "this query isn't supported yet" differently from "your query is wrong".
func (s *Session) Run(stmt string) (*Result, error)

type ErrUnsupported struct{ Reason string }  // nil TopOp, convert failure, panic
```

`Run` is exactly the body of `n1k1RunStatement`, minus the test plumbing:
`ParseStatement → store.PlanStatement → Conv.Accept → NewConvertVals → MakeVars
→ ExecOp(DatastoreOp)`. The harness then becomes a ~10-line caller of `Run`,
which also de-risks the refactor — if the pass count holds at 631, the
extraction is correct.

This keeps all engine knowledge in `glue/` and makes `cmd/n1k1` a pure
front-end (parse args, read lines, format rows).

------------------------------------------------------------------------
## 3. Binary, build, invocation

- **Location:** `cmd/n1k1/main.go`, behind `//go:build n1ql` (it depends on
  `glue/`, which is n1ql-gated). Built/tested under the existing `make n1ql`
  flow; add a `make cli` / `make install-cli` target.
- **Pure-Go, single binary:** `CGO_ENABLED=0 go build -tags n1ql ./cmd/n1k1`.
  Cross-compiles like the rest of the engine.
- **Invocation surface (DuckDB-parallel):**

  ```
  n1k1 [flags] [datastore-dir]

  n1k1                         # REPL on cwd (or no store until .open)
  n1k1 ./test/suite/json       # REPL with that datastore opened
  n1k1 -c "SELECT 1+1"         # one-shot, print, exit
  echo "SELECT ..." | n1k1     # stdin pipe (batch mode, no prompt)
  n1k1 -f script.n1ql          # run a file of ;-separated statements

  flags:
    -c <stmt>     run one statement and exit
    -f <file>     run statements from a file and exit (DuckDB: -init/-c hybrid)
    -ns <name>    namespace (default "default")
    -mode <m>     output mode: box|json|jsonlines|csv|markdown|line|list
                  (append |pretty, e.g. box|pretty, to indent JSON 2 spaces)
    -timer        show timing
    -init <file>  run dot-commands/SQL from file at startup (default ~/.n1k1rc)
    -no-init      skip the init file
    -readonly     (no-op today; the file store is already read-only — reserved)
    -v            verbose (show unsupported reasons, plan on error)
  ```

  Mode selection: a TTY defaults to `box|pretty`; a pipe/`-c`
  defaults to `jsonlines` (compact, clean for downstream tools) unless `-mode`
  says otherwise.

------------------------------------------------------------------------
## 4. The REPL

- **Prompt:** `n1k1>` ; continuation `   ...>` until a statement terminates
  with `;` (DuckDB behavior). Buffer accumulates lines; `;` flushes to
  `Session.Run`.
- **Dot-commands** are recognized only when a line *starts* with `.` and no
  SQL is buffered — they execute immediately, no `;` needed.
- **Line editing / history:** v1 can ship on a minimal pure-Go reader
  (`golang.org/x/term` raw mode, or `bufio` + no editing for the very first
  cut). For real history/editing, prefer a **small pure-Go** line editor
  (`github.com/peterh/liner` or `github.com/chzyer/readline`) — both are
  CGO-free, preserving the cross-compile story. History persists to
  `~/.n1k1_history`.
  - Dependency note: go.mod pins versions and the repo says *don't* run
    `go mod tidy` (it would prune the n1ql-only `query` dep). Add any line-
    editor dep with an explicit `go get <pkg>@<ver>` and hand-verify it's
    CGO-free; keep v1's hard dependency surface as close to zero as possible.
- **Signals:** Ctrl-C cancels the current input buffer (not the process);
  Ctrl-D / `.quit` / `.exit` exits. (Engine-level query cancellation via the
  `base.Vars` context is a later nicety, §7.)

------------------------------------------------------------------------
## 5. Dot-commands (v1 set)

Chosen to match DuckDB names where the concept exists, so muscle memory carries.

| Command | Behavior |
|---|---|
| `.help` | List commands. |
| `.open <dir>` | Open a new file datastore directory (re-`FileStore`+`InitParser`). |
| `.tables` / `.keyspaces` | List keyspaces under the namespace (the subdirs of `<dir>/<ns>/`). DuckDB calls them tables; we accept both, print "keyspaces". |
| `.schema [<keyspace>]` | Infer a shape from sampling the first N docs of a keyspace (top-level keys + observed JSON types). No real schema exists in a JSON store, so it's a *sampled* shape, clearly labeled. |
| `.mode <m>` | Set output mode (see §6). |
| `.timer on\|off` | Toggle elapsed-time footer. |
| `.maxrows <n>` | box: cap rows shown. `>0` = head+tail with a `·` elision row (DuckDB-style); `<0` = last `|n|` rows with the `·` elision row at the front; `0` = all. |
| `.maxwidth <n\|auto>` | box: cap column width, truncate with `…`. `0` = uncapped; `auto` = fit the box to the detected terminal width, widening columns to use spare space and shrinking (max-min fair share) only when the table overflows. |
| `.read <file>` | Execute statements/dot-commands from a file. |
| `.output <file>` / `.output` | Redirect results to a file / back to stdout. |
| `.explain` | Toggle: also print the converted `base.Op` plan tree for each query. The natural home for showing *why* something is UNSUPPORTED. |
| `.shell <cmd>` / `.system <cmd>` | Run a shell command (DuckDB parity; gated, off by default if we want caution). |
| `.quit` / `.exit` | Leave. |

------------------------------------------------------------------------
## 6. Output modes & the box renderer

The engine hands back `Result{Labels, Rows}`. Rows are JSON objects keyed by
the projection's column labels. Formatters:

- **`box`** (default, TTY) — the signature look:
  - Columns = `Result.Labels` (the projection aliases). When a row is a bare
    value (`raw`/`SELECT VALUE`), use a single `value` column.
  - Box-drawing borders (`┌─┬─┐ │ ├─┼─┤ └─┴─┘`).
  - Right-align numbers, left-align strings; nested objects/arrays rendered as
    compact JSON, truncated to `.maxwidth` with `…`. With `.maxwidth auto` the
    box is fit to the terminal width (detected via the tty, else `$COLUMNS`):
    columns widen into spare space and only shrink — max-min fair share — when
    the natural table would overflow.
  - Footer: `N rows (showing X)  ·  C columns  ·  elapsed` when `.timer on`.
  - When rows exceed `.maxrows`, show head + tail split by a `·····` elision
    row (DuckDB's trick), with the true count in the footer. A negative
    `.maxrows` instead keeps the last `|n|` rows, with the elision row at the
    front.
- **`jsonlines`** (default for pipes/`-c`) — one canonical JSON row per line.
- **`json`** — a single pretty JSON array.
- **`csv`** — header from labels; nested values emitted as JSON text, quoted.
- **`markdown`** — GitHub table (great for pasting into TODO.md/PRs).
- **`line`** — DuckDB's vertical mode: `key = value` per field, blank line
  between rows. Best for wide/nested docs, which n1k1 has a lot of.
- **`list`** — values joined by a separator (pipe-friendly).

**`|pretty` modifier.** Any mode may carry a `|pretty` (or `-pretty`) suffix —
e.g. `box|pretty`, `json|pretty` — which indents nested JSON values by 2 spaces
instead of emitting them compactly. In `box` a pretty JSON cell spans multiple
physical lines; the row grows as tall as its tallest cell, shorter cells
blank-pad below, and column widths use each cell's widest line so the frame stays
rectangular. `markdown|pretty` folds a cell's newlines to `<br>` to keep the
table valid; `csv|pretty` relies on the csv writer quoting the embedded newlines.

All formatters live in `cmd/render.go` and take `[]json.RawMessage` (plus a
`pretty bool`) — no engine coupling.

------------------------------------------------------------------------
## 7. Deferred / future (explicitly out of v1)

- **File-as-table** `FROM 'foo.csv'` / `read_json_auto(...)`: n1k1 *has* CSV +
  NDJSON scan operators, but they aren't reachable through the N1QL `FROM`
  grammar via glue. Wiring a table-function-like source is its own project;
  v1 queries keyspaces in an opened datastore dir.
- **Query cancellation** mid-run (Ctrl-C aborting `ExecOp` via the ctx).
- **Tab completion** of keywords / keyspace names / dot-commands.
- **`.import` / `COPY` / writes** — engine is query-only today.
- **Progress bar** for long/spilling queries (the engine already spills; it
  could emit `YieldStats`).
- **Persistent settings / PRAGMA**.

------------------------------------------------------------------------
## 8. Suggested build order (each step independently shippable)

0. **Extract `glue.Session`** from `n1k1RunStatement`; re-point the test at it.
   Gate: suite pass count stays 631. *(No user-visible change; de-risks
   everything after.)*
1. **Minimal CLI**: `cmd/n1k1`, `-c` + stdin + naive REPL (read-until-`;`),
   `jsonlines` output only. Proves the binary + single end-to-end path.
2. **box renderer + `.mode`** (box/jsonlines/json/csv/markdown/line),
   `.timer`. The "feels like DuckDB" moment.
3. **Navigation dot-commands**: `.open`, `.tables`/`.keyspaces`, `.schema`,
   `.read`, `.output`, `.help`, `.quit`.
4. **Niceties**: history + line editing, `~/.n1k1rc`, `.explain` plan dump,
   syntax-error caret, `.maxrows`/`.maxwidth` elision.

------------------------------------------------------------------------
## 9. Decisions (resolved)

- **Binary name:** `n1k1` (`cmd/n1k1/main.go`). Matches the module name.
- **Default TTY mode:** `box|pretty` (DuckDB-like, with nested JSON indented).
  Pipes/`-c` still default to compact `jsonlines`. (`.mode line` remains
  available for wide/nested docs.)
- **Line editor:** accept **one small pure-Go dep** (`peterh/liner` or
  `chzyer/readline`, both CGO-free) for arrow-key history + editing from v1.
  Add it via an explicit `go get <pkg>@<ver>` (never `go mod tidy`) and verify
  it builds with `CGO_ENABLED=0`; history persists to `~/.n1k1_history`.
