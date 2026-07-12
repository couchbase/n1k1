//go:build n1ql

//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// cli statement execution and result rendering.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/cmd"
	"github.com/couchbase/n1k1/glue"
)

// ---- statement execution --------------------------------------------------

func (c *cli) exec(stmt string) {
	// Nothing to run for whitespace- or comment-only text (e.g. a "/* ... */;"
	// or a stray "-- note;"): the parser would reject an empty statement.
	if cmd.IsBlankOrComment(stmt) {
		return
	}
	stmt = strings.TrimSpace(stmt)
	c.failed = false // reset; set below if this statement errors (drives .bail)
	c.outErr = nil   // reset; set by a streaming-write failure (closed output pipe)

	// -prepare/.prepare ceiling: at 'full', a standalone-compilable EXECUTE compiles
	// + runs a child program (interpreter fallback otherwise). See glue.ExecuteRun.
	c.sess.PrepareLevel = c.prepareLevel

	// .stats / -stats: collect per-operator counters. `on` also draws them live on a
	// TTY (throttled, in place, to stderr); `final` collects but shows only the grand
	// totals at the end -- no live footer, so you can measure a query without the
	// live-render overhead. One statsView spans the whole statement: created before
	// Run so its runtime baseline (for the footer's per-statement mem/GC/goroutine
	// deltas) is captured at the start; its end sample is pinned the moment Run
	// returns (below). Reset the Session hooks afterwards so a later query -- or
	// another user of this Session -- pays nothing.
	var sv *statsView
	if c.statsMode != statsOff {
		c.sess.CollectStats = true
		sv = newStatsView(c.stderr, c.fancyTTY, c.terminalWidth())
		if c.statsMode == statsOn && c.fancyTTY {
			c.sess.OnStats = sv.onStats // live footer (on-mode only)
		}
	}

	// Streaming output: in jsonlines mode, emit each row via Session.OnRow the moment
	// it is produced, instead of accumulating the whole result set into Result.Rows and
	// rendering after. First results reach the consumer ASAP -- what an AI agent or a
	// `... | head` pipeline wants, and what a k-way scan-and-filter (PREPARE++) query
	// benefits from. Only jsonlines (each row an independent newline-delimited value)
	// streams; the aggregating renderers (box/json-array/csv/markdown) need all rows for
	// column widths / framing, so they stay buffered. EXPLAIN has its own render path.
	outMode, pretty, _ := cmd.ParseMode(c.mode)
	streaming := outMode == "jsonlines" && !isExplainStmt(stmt)
	if streaming {
		c.sess.OnRow = func(row []byte) {
			// os.Stdout is unbuffered here, so each row reaches the fd immediately.
			// If the downstream consumer closes the pipe (`... | head`), the common
			// stdout case is handled by the runtime's default SIGPIPE (n1k1 exits once
			// the pipe buffer fills). For a redirected .output (a file/pipe that returns
			// EPIPE rather than raising SIGPIPE), record the first write error and stop
			// attempting further writes. Cooperatively HALTING the running query on that
			// signal is future work (needs an always-on checkpoint; see the design note).
			if c.outErr != nil {
				return
			}
			if werr := cmd.RenderJSONLine(c.out, row, pretty); werr != nil {
				// The downstream consumer closed the pipe (`... | head`): record it and
				// cooperatively HALT the query so we don't keep scanning for output
				// nobody will read. (SIGPIPE is ignored in main so the write returns
				// EPIPE here instead of killing the process.)
				c.outErr = werr
				c.sess.Interrupt()
			}
		}
	}

	atomic.StoreInt32(&c.interruptN, 0) // reset per-query Ctrl-C count (see repl signals)
	res, err := c.sess.Run(stmt)

	if sv != nil {
		sv.sampleEnd() // pin end-of-query mem BEFORE result rendering allocates
	}
	c.sess.CollectStats = false
	c.sess.OnStats = nil
	c.sess.OnRow = nil
	if sv != nil {
		sv.finish()
	}

	if err != nil {
		// A cooperative halt (Ctrl-C, or a closed output pipe) is a user-initiated stop,
		// not a query error: keep the session and don't trip the CI-failure latch. It's
		// reported at the source -- the REPL's Ctrl-C handler already printed "^C", and a
		// closed pipe means the consumer is gone -- so return quietly here.
		if errors.Is(err, base.ErrHalted) {
			return
		}
		c.failed = true // .bail on stops the input loop after this
		var unsup *glue.ErrUnsupported
		if errors.As(err, &unsup) {
			fmt.Fprintf(c.stderr, "%s%s\n", c.icon("🚧 "), c.style.Yellow("Unsupported: "+unsup.Reason))
		} else {
			fmt.Fprintf(c.stderr, "%s%s\n", c.icon("✗ "), c.style.Red("Error: "+tidyMsg(err.Error())))
			// Point a caret at the offending column when the parser gives one.
			fmt.Fprint(c.stderr, errorCaret(stmt, err.Error(), c.style))
			// Add a backtick hint when the offender is a reserved word (e.g. the
			// recipe-emitted `level` field).
			fmt.Fprint(c.stderr, reservedWordHint(err.Error(), c.style))
			// Add a hint when an unquoted DOTTED keyspace (`ns_server.error`, the norm
			// in a bundle) parsed as a field path -- and note the shell-quoting so the
			// backticks survive -c (IDEA-0010).
			if names, nerr := c.keyspaceNames(); nerr == nil {
				fmt.Fprint(c.stderr, dottedKeyspaceHint(err.Error(), names, c.style))
			}
		}
		return
	}

	// A PREPARE statement cached a plan and yields no rows: confirm it (and, at a
	// compile ceiling, show how far it compiles), then stop -- nothing to render.
	if res.Prepared != "" {
		c.reportPrepared(res.Prepared)
		return
	}

	// .explain, verbose >= 1, or an EXPLAIN statement prints n1k1's converted op
	// tree (what n1k1 actually runs), annotated with each expression's eval lane.
	if res.Plan != nil && (c.explain || c.verbose >= 1 || isExplainStmt(stmt)) {
		fmt.Fprintf(c.stderr, "%s plan / op tree:\n", prog)
		planStr := glue.FormatConvPlan(res.Plan)
		fmt.Fprint(c.stderr, planStr)
		// Marker key: only the markers this plan actually uses (empty otherwise).
		fmt.Fprint(c.stderr, glue.ConvPlanLegendFor(planStr))
	}

	if isExplainStmt(stmt) {
		// EXPLAIN's result is the cbq plan JSON. Render it as plain pretty-printed
		// JSON (to stdout, so it stays copy-paste- and pipe-friendly) rather than the
		// box renderer, whose cell dividers can't be pasted. The label goes to stderr
		// so a redirect/pipe of stdout gets only the JSON.
		fmt.Fprintln(c.stderr, "cbq plan:")
		cmd.RenderJSONLines(c.out, res.Rows, true)
	} else if streaming {
		// Rows were already emitted by OnRow during Run (Result.Rows is nil); only the
		// row-count/elapsed footer remains. Report a broken output pipe if one occurred.
		c.renderStreamFooter(res)
	} else {
		c.renderResult(res)
	}

	// .stats footer: the final per-operator counters + runtime totals (this is the
	// whole display for `final` mode and for a non-TTY run, which skipped the live
	// draw). Reuse sv so the runtime line shows deltas over the whole statement.
	if sv != nil && res.Stats != nil {
		native, boxed := 0, 0
		if res.Plan != nil {
			native, boxed = glue.ExprCoverage(res.Plan)
		}
		sv.renderFinal(res.Stats, exprStatsLine(native, boxed, res.BoxedEvals))
	}

	for _, w := range res.Warnings {
		fmt.Fprintf(c.stderr, "%s%s\n", c.icon("⚠️  "), c.style.Yellow("Warning: "+w.Error()))
	}
}

func (c *cli) renderResult(res *glue.Result) {
	c.renderRows(res.Rows, res.Elapsed.String(), c.wantFooter())
}

// wantFooter reports whether to show the row-count/elapsed footer: when .timer is
// on, at verbose >= 2 (debug), or whenever stats are being collected -- elapsed time
// is the denominator for the stats (rows/s, alloc/s), so it belongs with them.
func (c *cli) wantFooter() bool {
	return c.timer || c.verbose >= 2 || c.statsMode != statsOff
}

// renderStreamFooter finishes a streamed (jsonlines) result: the rows already went
// out row-by-row via OnRow, so only the footer (using the streamed Result.Count) and
// a closed-pipe note remain. A broken output pipe goes to stderr (a separate fd, so
// still writable when stdout's consumer -- `... | head` -- has gone away).
func (c *cli) renderStreamFooter(res *glue.Result) {
	if c.wantFooter() && c.outErr == nil {
		fmt.Fprintf(c.stderr, "%s%d row(s) in %s\n", c.icon("⏱ "), res.Count, res.Elapsed)
	}
	if c.outErr != nil {
		fmt.Fprintf(c.stderr, "%s%s\n", c.icon("⚠️  "),
			c.style.Yellow("output write failed (consumer closed?): "+tidyMsg(c.outErr.Error())))
	}
}

// renderRows renders JSON-object rows in the current output mode -- used both for
// query results (renderResult) and for tabular dot-command output like `.index
// list`/`.index show`, so those get the box renderer at a TTY too. elapsed is the
// box footer's timing string; footer is whether to show the row-count/elapsed
// footer at all (dot-commands pass false).
func (c *cli) renderRows(rows []json.RawMessage, elapsed string, footer bool) {
	// A "|pretty" / "-pretty" suffix on the mode 2-space-indents JSON values.
	base, pretty, _ := cmd.ParseMode(c.mode)
	switch base {
	case "jsonlines":
		cmd.RenderJSONLines(c.out, rows, pretty)
	case "json":
		cmd.RenderJSON(c.out, rows, pretty)
	case "csv":
		cmd.RenderCSV(c.out, rows, pretty)
	case "markdown":
		cmd.RenderMarkdown(c.out, rows, pretty)
	case "line":
		cmd.RenderLine(c.out, rows, pretty)
	case "list":
		cmd.RenderList(c.out, rows, c.listSep, pretty)
	default: // "box"
		el := ""
		if footer {
			el = c.icon("⏱ ") + elapsed
		}
		termWidth := 0
		if c.maxWidth < 0 { // auto: fit the box to the terminal's width
			termWidth = c.terminalWidth()
		}
		cmd.RenderBox(c.out, rows, c.maxWidth, c.maxRows, termWidth, el, c.style, pretty)
		return // box prints its own row-count/elapsed footer
	}

	if footer {
		fmt.Fprintf(c.stderr, "%s%d row(s) in %s\n", c.icon("⏱ "), len(rows), elapsed)
	}
}

// orderedJSONRow builds a JSON object from ordered key/value pairs, preserving key
// order in the text (Go maps don't) so the box renderer's columns appear in that
// order. Nil values are omitted (so a column absent from every row disappears).
func orderedJSONRow(pairs ...[2]interface{}) json.RawMessage {
	var b strings.Builder
	b.WriteByte('{')
	first := true
	for _, p := range pairs {
		if p[1] == nil {
			continue
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		k, _ := json.Marshal(p[0].(string))
		v, _ := json.Marshal(p[1])
		b.Write(k)
		b.WriteByte(':')
		b.Write(v)
	}
	b.WriteByte('}')
	return json.RawMessage(b.String())
}

// isExplainStmt reports whether stmt is an EXPLAIN statement (so the CLI shows
// n1k1's converted plan even without the .explain toggle).
func isExplainStmt(stmt string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stmt)), "EXPLAIN")
}

// prepareStmt runs the SQL++ -> Go compiler (glue.Session.Prepare) for stmt and,
// when the query is compilable, prints the generated Go source to stdout (so it's
// copy-paste/pipe-friendly, like EXPLAIN's plan JSON). When the query can't be
// compiled -- a boxed expression that needs cbq, or a non-bakeable datastore op
// -- it prints the reason to stderr and DOES NOT emit; the caller then runs the
// statement through the interpreter as usual, so the query still returns results.
// This is the prepare fallback (see DESIGN-prepare.md).
func (c *cli) prepareStmt(stmt string) {
	if c.sess == nil {
		fmt.Fprintln(c.stderr, "prepare: no datastore open (.open <dir>)")
		return
	}
	src, level, reason, err := c.sess.Prepare(stmt)
	if err != nil {
		// A parse/plan error: the statement is wrong. The normal run below will
		// report it with a caret; here just note prepare couldn't proceed.
		fmt.Fprintf(c.stderr, "%sprepare: %s\n", c.icon("🚧 "), c.style.Yellow(tidyMsg(err.Error())))
		return
	}
	if level != glue.PrepareCompiledFull {
		note := "not compilable, running interpreted" // PrepareInterpreted
		if level == glue.PrepareCompiledData {
			note = "compilable but needs a runtime data provider (not yet supported), running interpreted"
		}
		fmt.Fprintf(c.stderr, "%sprepare: %s -- %s\n",
			c.icon("🚧 "), note, c.style.Yellow(reason))
		return
	}
	fmt.Fprintln(c.stderr, "generated Go:")
	fmt.Fprint(c.out, src)
	if !strings.HasSuffix(src, "\n") {
		fmt.Fprintln(c.out)
	}
}

// reportPrepared confirms a PREPARE (which cached a plan under name) and, when the
// -prepare ceiling opts into codegen, shows how far the prepared statement
// compiles: the generated Go at the full ceiling, else a note of the level it
// reaches and why. At the default interpreted ceiling it just confirms the cache
// -- EXECUTE runs it through the interpreter. See DESIGN-prepare.md.
func (c *cli) reportPrepared(name string) {
	fmt.Fprintf(c.stderr, "%sprepared %q\n", c.icon("🔧 "), name)
	if c.prepareLevel == glue.PrepareInterpreted || c.sess == nil {
		return
	}
	// Compiled EXECUTE (data/full) needs the `go` toolchain to build the emitted
	// program; without it, EXECUTE degrades to the interpreter. Say so, and don't
	// bother analyzing/emitting.
	if glue.PrepareLevelAchievable(c.prepareLevel) == glue.PrepareInterpreted {
		fmt.Fprintf(c.stderr, "  %s\n", c.style.Yellow("no go toolchain -- compiled EXECUTE unavailable, running interpreted"))
		return
	}
	inner, ok := c.sess.PreparedInner(name)
	if !ok {
		return
	}
	src, level, reason, err := c.sess.Prepare(inner)
	if err != nil {
		return
	}
	switch {
	case level == glue.PrepareCompiledFull && c.prepareLevel >= glue.PrepareCompiledFull:
		fmt.Fprintln(c.stderr, "generated Go:")
		fmt.Fprint(c.out, src)
		if !strings.HasSuffix(src, "\n") {
			fmt.Fprintln(c.out)
		}
	case level == glue.PrepareCompiledFull:
		fmt.Fprintf(c.stderr, "  compiles to full (raise -prepare=full to emit the Go)\n")
	default:
		fmt.Fprintf(c.stderr, "  max level %s -- %s\n", level, c.style.Yellow(reason))
	}
}
