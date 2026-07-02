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
	last  time.Time // last live render, for throttling
	drawn int       // lines drawn by the last live render (for cursor-up)
}

func newStatsView(w io.Writer, fancy bool) *statsView {
	return &statsView{w: w, fancy: fancy}
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

// renderFinal prints the counters once, after the query completes.
func (v *statsView) renderFinal(s *base.Stats) {
	if s == nil || len(s.Ops) == 0 {
		return
	}
	fmt.Fprintln(v.w, "stats:")
	for _, ln := range statsLines(s) {
		fmt.Fprintln(v.w, ln)
	}
}

// statsLines formats one indented line per counter-contributing operator, e.g.
//
//	  scan  RowsOut=9500
//	filter  RowsIn=9500 RowsOut=42
//
// Indentation follows the op's tree depth (the number of '/'s in its id).
func statsLines(s *base.Stats) []string {
	lines := make([]string, 0, len(s.Ops))
	for _, op := range s.Ops {
		depth := strings.Count(op.Id, "/")

		var b strings.Builder
		b.WriteString(strings.Repeat("  ", depth))
		b.WriteString(op.Kind)
		for i, name := range op.Names {
			fmt.Fprintf(&b, "  %s=%d", name, s.Counters[op.Base+i])
		}
		lines = append(lines, b.String())
	}
	return lines
}
