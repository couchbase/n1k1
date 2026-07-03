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

// cli per-operator stats display (.stats / -stats). See DESIGN-stats.md.
package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/couchbase/n1k1/base"
)

// statsView renders per-operator counters: live during a long query (throttled,
// redrawn in place on an interactive TTY) and once at the end. It reads
// base.Stats, whose Ops slice lists the counter-contributing operators in
// pre-order with their counter offsets -- enough to draw an indented tree without
// a separate plan. All output goes to stderr so stdout results stay clean.
type statsView struct {
	w     io.Writer
	fancy bool
	width int       // wrap width for the footer glossary (0 -> a default)
	last  time.Time // last live render, for throttling
	drawn int       // lines drawn by the last live render (for cursor-up)
}

func newStatsView(w io.Writer, fancy bool, width int) *statsView {
	return &statsView{w: w, fancy: fancy, width: width}
}

// onStats is the live callback wired to Session.OnStats. It runs on the execution
// goroutine at each engine checkpoint (~every 1024 scanned rows), so it stays
// cheap: throttle to ~10 Hz and redraw in place. Non-TTY runs skip live drawing
// (the final render still prints once).
func (v *statsView) onStats(s *base.Stats) {
	if !v.fancy || s == nil || len(s.Ops) == 0 {
		return
	}
	now := time.Now()
	if !v.last.IsZero() && now.Sub(v.last) < 100*time.Millisecond {
		return
	}
	v.last = now

	if v.drawn > 0 {
		fmt.Fprintf(v.w, "\033[%dA", v.drawn) // cursor up over the previous draw
	}
	lines := statsLines(s)
	for _, ln := range lines {
		fmt.Fprintf(v.w, "\r\033[K%s\n", ln) // clear line + content
	}
	v.drawn = len(lines)
}

// finish clears the live area (if any) so the caller can render the final view or
// the query results without leftover in-place rows.
func (v *statsView) finish() {
	if v.fancy && v.drawn > 0 {
		fmt.Fprintf(v.w, "\033[%dA", v.drawn) // back over the live rows
		for i := 0; i < v.drawn; i++ {
			fmt.Fprint(v.w, "\r\033[K\n") // clear each
		}
		fmt.Fprintf(v.w, "\033[%dA", v.drawn) // back to the top so final render reuses it
		v.drawn = 0
	}
}

// renderFinal prints the counters once, after the query completes, followed by a
// compact glossary of the stat names that appeared (so the footer is
// self-explanatory).
func (v *statsView) renderFinal(s *base.Stats) {
	if s == nil || len(s.Ops) == 0 {
		return
	}
	fmt.Fprintln(v.w, "stats:")
	for _, ln := range statsLines(s) {
		fmt.Fprintln(v.w, ln)
	}
	for _, ln := range statsGlossary(statNamesIn(s), v.width) {
		fmt.Fprintln(v.w, ln)
	}
}

// statNamesIn returns the distinct stat names present in s (across all ops).
func statNamesIn(s *base.Stats) []string {
	seen := map[string]bool{}
	var names []string
	for _, op := range s.Ops {
		for _, n := range op.Names {
			if !seen[n] {
				seen[n] = true
				names = append(names, n)
			}
		}
	}
	return names
}

// statsGlossary formats a compact "name: description" glossary for the given stat
// names -- alphabetized and concatenated so it uses few lines (wrapped to width,
// continuation lines aligned under the first entry). Names without a registered
// description (base.StatAbout) are skipped. Returns nil if none have one.
func statsGlossary(names []string, width int) []string {
	uniq := append([]string(nil), names...)
	sort.Strings(uniq)

	var items []string
	seen := map[string]bool{}
	for _, n := range uniq {
		if seen[n] {
			continue
		}
		seen[n] = true
		if d := base.StatAbout[n]; d != "" {
			items = append(items, n+": "+d)
		}
	}
	if len(items) == 0 {
		return nil
	}
	return wrapItems(items, "glossary: ", "; ", width)
}

// wrapItems greedily packs items (separated by sep) onto lines no wider than
// width, prefixing the first line with prefix and indenting continuations to
// align under it. width <= 0 falls back to a sensible default.
func wrapItems(items []string, prefix, sep string, width int) []string {
	if width <= 0 {
		width = 100
	}
	if width < 24 {
		width = 24
	}

	indent := strings.Repeat(" ", len(prefix))

	var lines []string
	line, empty := prefix, true
	for _, it := range items {
		piece := it
		if !empty {
			piece = sep + it
		}
		if !empty && len(line)+len(piece) > width {
			lines = append(lines, line)
			line, empty = indent+it, false
		} else {
			line, empty = line+piece, false
		}
	}
	if !empty {
		lines = append(lines, line)
	}
	return lines
}

// statsAbout formats the full glossary of every registered stat (base.StatAbout),
// one per line with an aligned name column -- the ".stats about" reference.
func statsAbout() []string {
	names := make([]string, 0, len(base.StatAbout))
	for n := range base.StatAbout {
		names = append(names, n)
	}
	sort.Strings(names)

	nameW := 0
	for _, n := range names {
		if len(n) > nameW {
			nameW = len(n)
		}
	}

	lines := make([]string, 0, len(names))
	for _, n := range names {
		lines = append(lines, fmt.Sprintf("  %-*s  %s", nameW, n, base.StatAbout[n]))
	}
	return lines
}

// statsLines formats the counters as an aligned table: a tree-indented "op"
// column, then one right-aligned numeric column per stat name shared by two or
// more operators (so a value repeated down the tree -- RowsOut, Probes -- lines up
// for easy comparison), then a trailing free-form "misc" column carrying each
// op's one-off Key=Val stats. A counter with a known estimate (Totals) shows as
// "cur/total" to preview a progress bar. Example:
//
//	op                       RowsLeft   Probes  RowsOut  misc
//	group                                                RowsIn=262144 GroupsOut=1
//	  joinNL-inner               4096   262144
//	    joinNL-inner               64     4096
//	      datastore-scan-index                     64/64
//	      datastore-scan-index                     64/64
//	    datastore-scan-index                       64/64
func statsLines(s *base.Stats) []string {
	if len(s.Ops) == 0 {
		return nil // No counter-contributing ops -> no table (caller prints nothing).
	}

	// Count how many ops carry each stat name, remembering first-seen order.
	count := map[string]int{}
	var order []string
	for _, op := range s.Ops {
		for _, name := range op.Names {
			if count[name] == 0 {
				order = append(order, name)
			}
			count[name]++
		}
	}

	// A name shared by >=2 ops earns its own column; a one-off goes to "misc".
	var cols []string
	colAt := map[string]int{}
	for _, name := range order {
		if count[name] >= 2 {
			colAt[name] = len(cols)
			cols = append(cols, name)
		}
	}

	// Build each row's cells (op label + per-column values + misc), tracking widths.
	type row struct {
		op    string
		cells []string
		misc  string
	}
	rows := make([]row, 0, len(s.Ops))

	opW := len("op")
	colW := make([]int, len(cols))
	for i, c := range cols {
		colW[i] = len(c)
	}
	anyMisc := false

	for _, op := range s.Ops {
		r := row{
			op:    strings.Repeat("  ", strings.Count(op.Id, "/")) + op.Kind,
			cells: make([]string, len(cols)),
		}
		if len(r.op) > opW {
			opW = len(r.op)
		}

		var misc []string
		for i, name := range op.Names {
			cell := statCell(s, op.Base+i)
			if ci, ok := colAt[name]; ok {
				r.cells[ci] = cell
				if len(cell) > colW[ci] {
					colW[ci] = len(cell)
				}
			} else {
				misc = append(misc, fmt.Sprintf("%s=%s", name, cell))
			}
		}
		if len(misc) > 0 {
			r.misc = strings.Join(misc, " ")
			anyMisc = true
		}
		rows = append(rows, r)
	}

	emit := func(op string, cells []string, misc string) string {
		var b strings.Builder
		fmt.Fprintf(&b, "%-*s", opW, op)
		for i, c := range cells {
			fmt.Fprintf(&b, "  %*s", colW[i], c)
		}
		if misc != "" {
			fmt.Fprintf(&b, "  %s", misc)
		}
		return strings.TrimRight(b.String(), " ")
	}

	out := make([]string, 0, len(rows)+1)
	miscHdr := ""
	if anyMisc {
		miscHdr = "misc"
	}
	out = append(out, emit("op", cols, miscHdr))
	for _, r := range rows {
		out = append(out, emit(r.op, r.cells, r.misc))
	}
	return out
}

// statCell formats one counter: its value, or "cur/total" when an estimate
// (Totals) is known -- a compact progress preview (cur can be < total mid-run,
// and may reset for a re-run op, so it is not necessarily monotonic).
func statCell(s *base.Stats, i int) string {
	v := s.Counters[i]
	if s.Totals != nil && s.Totals[i] > 0 {
		return fmt.Sprintf("%d/%d", v, s.Totals[i])
	}
	return fmt.Sprintf("%d", v)
}
