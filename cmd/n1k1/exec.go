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
	"io"
	"strings"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/cmd"
	"github.com/couchbase/n1k1/glue"
)

// ---- statement execution --------------------------------------------------

func (c *cli) exec(stmt string) {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return
	}

	// .stats / -stats: collect per-operator counters, and on a TTY draw them live
	// (throttled, in place, to stderr) while the query runs. Reset afterwards so a
	// later query -- or another user of this Session -- pays nothing.
	var sv *statsView
	if c.stats {
		c.sess.CollectStats = true
		if c.fancyTTY {
			sv = newStatsView(c.stderr, true, c.terminalWidth())
			c.sess.OnStats = sv.onStats
		}
	}

	res, err := c.sess.Run(stmt)

	c.sess.CollectStats = false
	c.sess.OnStats = nil
	if sv != nil {
		sv.finish()
	}

	if err != nil {
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

	// .explain, or verbose >= 1, prints the converted plan per statement.
	if c.explain || c.verbose >= 1 {
		fmt.Fprintln(c.stderr, c.style.Dim("plan:"))
		printPlan(c.stderr, res.Plan, 1)
	}

	c.renderResult(res)

	// .stats footer: the final per-operator counters (also the whole display for a
	// non-TTY run, which skipped the live draw).
	if c.stats && res.Stats != nil {
		newStatsView(c.stderr, c.fancyTTY, c.terminalWidth()).renderFinal(res.Stats)
	}

	for _, w := range res.Warnings {
		fmt.Fprintf(c.stderr, "%s%s\n", c.icon("⚠️  "), c.style.Yellow("Warning: "+w.Error()))
	}
}

func (c *cli) renderResult(res *glue.Result) {
	// verbose >= 2 (debug) shows the row-count/elapsed footer even without .timer.
	c.renderRows(res.Rows, res.Elapsed.String(), c.timer || c.verbose >= 2)
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

// printPlan prints the converted n1k1 op tree, one node per line, indented.
func printPlan(w io.Writer, op *base.Op, depth int) {
	if op == nil {
		return
	}
	fmt.Fprintf(w, "%s%s %v\n", strings.Repeat("  ", depth), op.Kind, op.Labels)
	for _, ch := range op.Children {
		printPlan(w, ch, depth+1)
	}
}
