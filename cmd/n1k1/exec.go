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

	// .prepare / -prepare on: print the generated Go for this statement first (or
	// the reason it can't be compiled), then fall through to run it as usual.
	// EXPLAIN is handled by the normal path below, not prepare'd.
	if c.prepare && !isExplainStmt(stmt) {
		c.prepareStmt(stmt)
	}

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

	res, err := c.sess.Run(stmt)

	if sv != nil {
		sv.sampleEnd() // pin end-of-query mem BEFORE result rendering allocates
	}
	c.sess.CollectStats = false
	c.sess.OnStats = nil
	if sv != nil {
		sv.finish()
	}

	if err != nil {
		c.failed = true // .bail on stops the input loop after this
		var unsup *glue.ErrUnsupported
		if errors.As(err, &unsup) {
			fmt.Fprintf(c.stderr, "%s%s\n", c.icon("🚧 "), c.style.Yellow("Unsupported: "+unsup.Reason))
		} else {
			fmt.Fprintf(c.stderr, "%s%s\n", c.icon("✗ "), c.style.Red("Error: "+tidyMsg(err.Error())))
			// Point a caret at the offending column when the parser gives one.
			fmt.Fprint(c.stderr, errorCaret(stmt, err.Error(), c.style))
		}
		return
	}

	// .explain, verbose >= 1, or an EXPLAIN statement prints n1k1's converted op
	// tree (what n1k1 actually runs), annotated with each expression's eval lane.
	if res.Plan != nil && (c.explain || c.verbose >= 1 || isExplainStmt(stmt)) {
		fmt.Fprintf(c.stderr, "%s plan / op tree:\n", prog)
		fmt.Fprint(c.stderr, glue.FormatConvPlan(res.Plan))
	}

	if isExplainStmt(stmt) {
		// EXPLAIN's result is the cbq plan JSON. Render it as plain pretty-printed
		// JSON (to stdout, so it stays copy-paste- and pipe-friendly) rather than the
		// box renderer, whose cell dividers can't be pasted. The label goes to stderr
		// so a redirect/pipe of stdout gets only the JSON.
		fmt.Fprintln(c.stderr, "cbq plan:")
		cmd.RenderJSONLines(c.out, res.Rows, true)
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
	// Show the row-count/elapsed footer when .timer is on, at verbose >= 2 (debug),
	// or whenever stats are being collected -- elapsed time is the denominator for
	// the stats (rows/s, alloc/s), so it belongs with them.
	footer := c.timer || c.verbose >= 2 || c.statsMode != statsOff
	c.renderRows(res.Rows, res.Elapsed.String(), footer)
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
// This is the prepare fallback (see DESIGN-extensions-prepare.md).
func (c *cli) prepareStmt(stmt string) {
	if c.sess == nil {
		fmt.Fprintln(c.stderr, "prepare: no datastore open (.open <dir>)")
		return
	}
	src, ok, reason, err := c.sess.Prepare(stmt)
	if err != nil {
		// A parse/plan error: the statement is wrong. The normal run below will
		// report it with a caret; here just note prepare couldn't proceed.
		fmt.Fprintf(c.stderr, "%sprepare: %s\n", c.icon("🚧 "), c.style.Yellow(tidyMsg(err.Error())))
		return
	}
	if !ok {
		fmt.Fprintf(c.stderr, "%sprepare: not compilable, running interpreted -- %s\n",
			c.icon("🚧 "), c.style.Yellow(reason))
		return
	}
	fmt.Fprintln(c.stderr, "generated Go:")
	fmt.Fprint(c.out, src)
	if !strings.HasSuffix(src, "\n") {
		fmt.Fprintln(c.out)
	}
}
