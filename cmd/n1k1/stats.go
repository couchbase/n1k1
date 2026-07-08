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
	"runtime"
	"runtime/metrics"
	"sort"
	"strings"
	"time"

	"github.com/couchbase/n1k1/base"
)

// Stats display modes for -stats / .stats.
const (
	statsOff   = "off"   // collect nothing, show nothing (zero cost)
	statsOn    = "on"    // collect + live footer during the query + final totals
	statsFinal = "final" // collect + print grand totals once at the end (no live footer)
)

// parseStatsMode normalizes a -stats/.stats value to a mode constant. "final" has
// aliases (end/total/summary). An empty string means off (the flag/default case).
func parseStatsMode(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "on", "true", "live":
		return statsOn, nil
	case "", "off", "false":
		return statsOff, nil
	case "final", "end", "total", "totals", "summary":
		return statsFinal, nil
	}
	return "", fmt.Errorf("want on|off|final")
}

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

	baseRT  runtimeSample // process runtime baseline, captured at construction (query start)
	endRT   runtimeSample // pinned end-of-query sample (see sampleEnd)
	haveEnd bool          // endRT is set -> the footer uses it instead of sampling now
}

func newStatsView(w io.Writer, fancy bool, width int) *statsView {
	// Capture the runtime baseline at query start, so the footer's process line can
	// show per-statement deltas (bytes allocated, GCs, goroutines spawned).
	return &statsView{w: w, fancy: fancy, width: width, baseRT: readRuntimeSample()}
}

// sampleEnd pins the end-of-query runtime sample. The caller invokes it the moment
// Run returns -- before result rendering, which itself allocates -- so the footer's
// "allocated"/"GCs" deltas reflect the statement, not the display. renderFinal (in
// both `on` and `final` modes) then uses this pinned sample for the grand totals.
func (v *statsView) sampleEnd() {
	v.endRT, v.haveEnd = readRuntimeSample(), true
}

// runtimeSample is a cheap, process-wide snapshot of memory/GC/goroutine usage.
// It is NOT per-operator (n1k1 leans on cbq's expression machinery, which heap-
// allocs opaquely, so we can't attribute allocations to a single op) -- it is a
// coarse "how hard is the whole process working" readout.
type runtimeSample struct {
	allocBytes uint64 // cumulative bytes allocated (garbage + live); delta = churn
	allocObjs  uint64 // cumulative allocation count; delta = objects allocated
	heapBytes  uint64 // live heap object bytes right now (a gauge; GC lowers it)
	gcCycles   uint64 // cumulative GC cycles; delta = GCs during the statement
	goroutines int    // current goroutine count
}

// rtMetrics are read via runtime/metrics, which (unlike runtime.ReadMemStats)
// does not stop the world -- so sampling at the render cadence is cheap. We only
// sample when we actually redraw (≤10 Hz, throttled), never per row, so a fast
// query pays almost nothing regardless of its row rate.
var rtMetrics = []string{
	"/gc/heap/allocs:bytes",
	"/gc/heap/allocs:objects",
	"/memory/classes/heap/objects:bytes",
	"/gc/cycles/total:gc-cycles",
}

func readRuntimeSample() runtimeSample {
	s := make([]metrics.Sample, len(rtMetrics))
	for i := range rtMetrics {
		s[i].Name = rtMetrics[i]
	}
	metrics.Read(s)
	u := func(i int) uint64 {
		if s[i].Value.Kind() == metrics.KindUint64 {
			return s[i].Value.Uint64()
		}
		return 0
	}
	return runtimeSample{
		allocBytes: u(0),
		allocObjs:  u(1),
		heapBytes:  u(2),
		gcCycles:   u(3),
		goroutines: runtime.NumGoroutine(),
	}
}

// runtimeLine formats the process footer line as per-statement deltas from the
// baseline, plus current gauges (live heap, goroutines). Live frames sample fresh;
// the final footer uses the pinned end-of-query sample (see sampleEnd).
func (v *statsView) runtimeLine() string {
	now := readRuntimeSample()
	if v.haveEnd {
		now = v.endRT
	}
	return fmt.Sprintf("runtime: %s allocated · %s allocs · heap %s · %d GCs · %d goroutines",
		humanBytes(int64(now.allocBytes-v.baseRT.allocBytes)),
		humanCount(now.allocObjs-v.baseRT.allocObjs),
		humanBytes(int64(now.heapBytes)),
		now.gcCycles-v.baseRT.gcCycles,
		now.goroutines)
}

// bodyLines are the live-updating footer lines: the per-op table, the in-flight
// running-aggregate block (COUNT/SUM/AVG/MIN/MAX climbing toward their finals),
// then the process runtime line (all animate). The glossary (renderFinal only) is
// a static legend appended after.
func (v *statsView) bodyLines(s *base.Stats) []string {
	lines := statsLines(s)
	lines = append(lines, runningAggLines(s)...)
	return append(lines, v.runtimeLine())
}

// runningAggDisplayMax bounds how many running-aggregate group rows the footer
// shows (the snapshot itself is already bounded at base.RunningAggMaxGroups); any
// extra groups collapse into a trailing "… N more" line so the live footer stays
// compact and its in-place redraw doesn't scroll.
const runningAggDisplayMax = 12

// runningAggLines renders the in-flight aggregate partials as a footer block --
// the same bytes that will land in the finalized result, shown climbing toward it.
// When the plan supplied running-aggregate labels (s.RunningAggLabels, filled by
// glue for both the live footer and the final block) each aggregate gets its own
// "alias (expr): value" line with decimal-aligned numbers; otherwise it stays
// compact, one line per group with bare handler names. Returns nil when no op
// published running aggregates (only the cheap fixed-width aggregates do; see
// base.IsRunningAggCapable).
func runningAggLines(s *base.Stats) []string {
	if s == nil || len(s.RunningAggLabels) == 0 {
		return runningAggLinesCompact(s)
	}
	return runningAggLinesLabeled(s, s.RunningAggLabels)
}

// runningAggLinesCompact is the live footer form: one line per group (sorted for a
// stable order across live frames), a group-by key followed by name=partial pairs.
// Values are read under the checkpoint lock via RunningAggsRange and copied into
// strings in the callback, so nothing is retained past the (buffer-reusing) row.
func runningAggLinesCompact(s *base.Stats) []string {
	var all []string
	s.RunningAggsRange(func(r *base.RunningAggRow) {
		var b strings.Builder
		for i, k := range r.Key {
			if i > 0 {
				b.WriteByte(',')
			}
			b.Write(k) // decoded group-by key val (JSON bytes)
		}
		if len(r.Key) > 0 {
			b.WriteString("  ")
		}
		for i, name := range r.Names {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(runningAggDisplayName(name))
			b.WriteByte('=')
			b.Write(r.Aggs[i]) // partial value (JSON bytes)
		}
		all = append(all, b.String())
	})
	if len(all) == 0 {
		return nil
	}
	sort.Strings(all) // stable order so groups don't jump between live frames

	out := []string{"running:"}
	shown := all
	if len(shown) > runningAggDisplayMax {
		shown = shown[:runningAggDisplayMax]
	}
	for _, r := range shown {
		out = append(out, "  "+r)
	}
	if len(all) > len(shown) {
		out = append(out, fmt.Sprintf("  … %d more", len(all)-len(shown)))
	}
	return out
}

// rAgg is one aggregate's decoded partial for the labeled render: the SQL alias
// (may be ""), the aggregate expression, the value bytes as a string, and whether
// the value is numeric (so it right-aligns into the value column).
type rAgg struct {
	alias, expr, val string
	numeric          bool
}

// rGroup is one running group's decoded partials (its group-by key, blank when
// ungrouped, and its aggregates in layout order).
type rGroup struct {
	key  string
	aggs []rAgg
}

// runningAggLinesLabeled is the labeled form (live footer + final block): each
// aggregate on its own "alias (expr): value" line (alias omitted when the
// aggregate is nested in a larger projection term, e.g. ROUND(SUM..)), aliases
// padded to a column, numeric values aligned on their decimal point. labels[i] is
// one group op's per-aggregate labels; a group row is matched to the entry with
// the same aggregate count (the common query has one group op). Falls back to the
// handler name when no label is available.
func runningAggLinesLabeled(s *base.Stats, labels [][]base.RunningAggLabel) []string {
	pick := func(nAggs int) []base.RunningAggLabel {
		for _, e := range labels {
			if len(e) == nAggs {
				return e
			}
		}
		return nil
	}

	var groups []rGroup
	s.RunningAggsRange(func(r *base.RunningAggRow) {
		le := pick(len(r.Aggs))
		var keyB strings.Builder
		for i, k := range r.Key {
			if i > 0 {
				keyB.WriteByte(',')
			}
			keyB.Write(k)
		}
		g := rGroup{key: keyB.String()}
		for i := range r.Aggs {
			var a rAgg
			if le != nil && i < len(le) {
				a.alias, a.expr = le[i].Alias, le[i].Expr
			}
			if a.expr == "" { // no plan label -> fall back to the handler name
				a.expr = runningAggDisplayName(r.Names[i])
			}
			a.val = string(r.Aggs[i])
			a.numeric = looksNumeric(a.val)
			g.aggs = append(g.aggs, a)
		}
		groups = append(groups, g)
	})
	if len(groups) == 0 {
		return nil
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].key < groups[j].key })

	// Column widths. Aliases pad so the exprs line up; the label column pads so the
	// values line up. Numeric values align on the decimal point: split each into
	// integer and fractional (".frac", or "" when none) parts, right-pad the widest
	// integer part and left-pad the widest fractional part so the dots stack.
	aliasW, intW, fracW := 0, 0, 0
	for _, g := range groups {
		for _, a := range g.aggs {
			if a.alias != "" && len(a.alias) > aliasW {
				aliasW = len(a.alias)
			}
			if a.numeric {
				ip, fp := splitNumber(a.val)
				if len(ip) > intW {
					intW = len(ip)
				}
				if len(fp) > fracW {
					fracW = len(fp)
				}
			}
		}
	}
	label := func(a rAgg) string {
		if a.alias == "" {
			return a.expr
		}
		return fmt.Sprintf("%-*s (%s)", aliasW, a.alias, a.expr)
	}
	value := func(a rAgg) string {
		if !a.numeric {
			return a.val
		}
		ip, fp := splitNumber(a.val)
		// Right-align the integer part, left-pad the fraction, then trim the
		// trailing pad so a whole number doesn't drag blanks to end-of-line.
		return strings.TrimRight(fmt.Sprintf("%*s%-*s", intW, ip, fracW, fp), " ")
	}
	labelW := 0
	for _, g := range groups {
		for _, a := range g.aggs {
			if w := len(label(a)) + 1; w > labelW { // +1 for the ':'
				labelW = w
			}
		}
	}

	out := []string{"running:"}
	shown, extra := groups, 0
	if len(shown) > runningAggDisplayMax {
		extra = len(shown) - runningAggDisplayMax
		shown = shown[:runningAggDisplayMax]
	}
	for _, g := range shown {
		indent := "  "
		if g.key != "" {
			out = append(out, "  ["+g.key+"]")
			indent = "    "
		}
		for _, a := range g.aggs {
			out = append(out, strings.TrimRight(
				fmt.Sprintf("%s%-*s %s", indent, labelW, label(a)+":", value(a)), " "))
		}
	}
	if extra > 0 {
		out = append(out, fmt.Sprintf("  … %d more", extra))
	}
	return out
}

// splitNumber splits a numeric string into its integer part and fractional part
// (the "." and everything after, or "" when there is no dot), for decimal-point
// alignment. Scientific notation ("1.5e+07") splits at its first dot, which is
// good enough for the fixed-width partials the running aggregates emit.
func splitNumber(s string) (intPart, fracPart string) {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return s[:i], s[i:]
	}
	return s, ""
}

// looksNumeric reports whether a JSON value's bytes are a number (so the running
// block decimal-aligns it): a leading '-' or digit is enough for the partials the
// running aggregates emit (COUNT/SUM/AVG/MIN/MAX).
func looksNumeric(s string) bool {
	if s == "" {
		return false
	}
	c := s[0]
	return c == '-' || (c >= '0' && c <= '9')
}

// runningAggDisplayName strips the vectorized-representation suffix from an
// aggregate handler name so the footer shows the SQL-level name (count/sum/avg),
// not its columnar encoding variant (count_v, sum_v_float64, ...).
func runningAggDisplayName(name string) string {
	if i := strings.Index(name, "_v"); i > 0 {
		return name[:i]
	}
	return name
}

// humanCount abbreviates a large count as K/M/G (distinct from humanBytes's
// byte units), e.g. 214000 -> "214.0K".
func humanCount(n uint64) string {
	f := float64(n)
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fG", f/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", f/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", f/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
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
	lines := v.bodyLines(s)
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
func (v *statsView) renderFinal(s *base.Stats, exprLine string) {
	if s == nil || len(s.Ops) == 0 {
		return
	}
	fmt.Fprintln(v.w, "stats:")
	for _, ln := range v.bodyLines(s) {
		fmt.Fprintln(v.w, ln)
	}
	if exprLine != "" {
		fmt.Fprintln(v.w, exprLine)
	}
	for _, ln := range statsGlossary(statNamesIn(s), v.width) {
		fmt.Fprintln(v.w, ln)
	}
}

// exprStatsLine summarizes the boxed (GC-heavy cbq-fallback) expression work for
// the stats footer: how many of the plan's project/filter expressions box (a
// static, build-time count) followed by boxedEvals, the per-row evaluations that
// actually took the boxed lane at run time. Native work is not reported -- the
// absence of a boxed mention means it stayed on the native byte path. Returns ""
// when nothing boxed. See glue.ExprCoverage / Result.BoxedEvals.
func exprStatsLine(native, boxed int, boxedEvals int64) string {
	if boxed == 0 && boxedEvals == 0 {
		return ""
	}
	var parts []string
	if total := native + boxed; total > 0 {
		parts = append(parts, fmt.Sprintf("%d/%d exprs boxed", boxed, total))
	}
	parts = append(parts, fmt.Sprintf("%s boxed evals", humanCount(uint64(boxedEvals))))
	return "expr: " + strings.Join(parts, " · ")
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
//
// displayDepths returns each op's indentation depth for the table: the number of
// *shown* ancestors (ops whose tree id is a path-prefix of this one). Nesting thus
// reflects the operators actually in the table -- uncounted intermediate ops
// (project, sequence, ...) don't inflate the indent, which otherwise jumps by
// several levels between, say, a group and the join beneath it. s.Ops is pre-order,
// so ancestors precede descendants and a simple id-prefix stack suffices.
func displayDepths(ops []base.StatsOpInfo) []int {
	depths := make([]int, len(ops))
	var stack []string // ids of the current shown-ancestor chain
	for i, op := range ops {
		for len(stack) > 0 && !strings.HasPrefix(op.Id, stack[len(stack)-1]+"/") {
			stack = stack[:len(stack)-1]
		}
		depths[i] = len(stack)
		stack = append(stack, op.Id)
	}
	return depths
}

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

	depths := displayDepths(s.Ops)

	for i, op := range s.Ops {
		r := row{
			op:    strings.Repeat("  ", depths[i]) + op.Kind,
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
