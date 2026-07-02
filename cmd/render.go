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

package cmd

// Result renderers shared by n1k1 command-line tools. Rows are canonical-JSON
// values ([]json.RawMessage); each renderer turns them into a textual output
// mode. Pure (no engine / n1ql deps) so it is reusable and unit-testable
// without the n1ql build tag.

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// OutputModes are the supported renderers. "box" is the default at a TTY;
// "jsonlines" for pipes / one-shot.
var OutputModes = []string{"box", "jsonlines", "json", "csv", "markdown", "line", "list"}

// ParseMode splits an output-mode string into its base mode and an optional
// "pretty" modifier. The modifier is appended with a '|' or '-' separator (e.g.
// "box|pretty" or "box-pretty") and, when present, indents nested JSON values by
// 2 spaces — so in box mode a JSON cell prints across multiple lines. ok is
// false when the base is unknown or the modifier is anything but "pretty".
func ParseMode(m string) (base string, pretty bool, ok bool) {
	base = m
	if i := strings.IndexAny(m, "|-"); i >= 0 {
		base = m[:i]
		if m[i+1:] != "pretty" {
			return base, false, false
		}
		pretty = true
	}
	return base, pretty, isBaseMode(base)
}

func isBaseMode(m string) bool {
	for _, x := range OutputModes {
		if x == m {
			return true
		}
	}
	return false
}

// ValidMode reports whether m names a known output mode, optionally with a
// "|pretty" / "-pretty" modifier.
func ValidMode(m string) bool {
	_, _, ok := ParseMode(m)
	return ok
}

// scalarCol is the synthetic column name for rows that are a bare JSON value
// (SELECT RAW / SELECT VALUE) rather than an object.
const scalarCol = "value"

// ---------------------------------------------------------------------------
// Style: optional ANSI styling. Zero value (On=false) emits plain text, so it
// is safe for pipes / files; callers turn it On only for an interactive TTY.

type Style struct{ On bool }

const (
	ansiReset  = "\x1b[0m"
	ansiDim    = "\x1b[2m"
	ansiBold   = "\x1b[1m"
	ansiCyan   = "\x1b[36m"
	ansiRed    = "\x1b[31m"
	ansiYellow = "\x1b[33m"
)

func (s Style) wrap(code, x string) string {
	if !s.On || x == "" {
		return x
	}
	return code + x + ansiReset
}

func (s Style) Dim(x string) string    { return s.wrap(ansiDim, x) }
func (s Style) Bold(x string) string   { return s.wrap(ansiBold, x) }
func (s Style) Cyan(x string) string   { return s.wrap(ansiCyan, x) }
func (s Style) Red(x string) string    { return s.wrap(ansiRed, x) }
func (s Style) Yellow(x string) string { return s.wrap(ansiYellow, x) }

func (s Style) header(x string) string { return s.wrap(ansiBold+ansiCyan, x) }

// ---------------------------------------------------------------------------

// tableOf decodes rows into a column set (object keys in first-seen order, plus
// scalarCol if any row is a bare value) and a string cell grid. Cell strings
// are display-ready: JSON strings unquoted, everything else compact JSON (or,
// when pretty, 2-space-indented JSON that may span multiple lines).
func tableOf(rows []json.RawMessage, pretty bool) (cols []string, cells [][]string) {
	seen := map[string]bool{}
	decoded := make([]interface{}, len(rows))

	for i, raw := range rows {
		var v interface{}
		if err := json.Unmarshal(raw, &v); err != nil {
			v = string(raw)
		}
		decoded[i] = v

		if _, ok := v.(map[string]interface{}); ok {
			for _, k := range orderedKeys(raw) {
				if !seen[k] {
					seen[k] = true
					cols = append(cols, k)
				}
			}
		} else if !seen[scalarCol] {
			seen[scalarCol] = true
			cols = append(cols, scalarCol)
		}
	}

	for _, v := range decoded {
		row := make([]string, len(cols))
		m, isObj := v.(map[string]interface{})
		for j, c := range cols {
			switch {
			case isObj:
				if cv, ok := m[c]; ok {
					row[j] = cellString(cv, pretty)
				}
			case c == scalarCol:
				row[j] = cellString(v, pretty)
			}
		}
		cells = append(cells, row)
	}

	return cols, cells
}

// orderedKeys returns an object's keys in their source-text order (Go maps lose
// it), so columns appear in projection order.
func orderedKeys(raw json.RawMessage) []string {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	t, err := dec.Token()
	if err != nil {
		return nil
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return nil
	}
	var keys []string
	depth := 0
	for dec.More() || depth > 0 {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if d, ok := tok.(json.Delim); ok {
			if d == '{' || d == '[' {
				depth++
			} else {
				depth--
			}
			continue
		}
		if depth == 0 {
			if k, ok := tok.(string); ok {
				keys = append(keys, k)
				skipValue(dec)
			}
		}
	}
	return keys
}

// skipValue consumes one full JSON value (the decoder is positioned right after
// a key token) so the next token read is the following key.
func skipValue(dec *json.Decoder) {
	tok, err := dec.Token()
	if err != nil {
		return
	}
	if d, ok := tok.(json.Delim); ok && (d == '{' || d == '[') {
		depth := 1
		for depth > 0 {
			t, err := dec.Token()
			if err != nil {
				return
			}
			if dd, ok := t.(json.Delim); ok {
				if dd == '{' || dd == '[' {
					depth++
				} else {
					depth--
				}
			}
		}
	}
}

// cellString renders a decoded JSON value for display: strings as-is, numbers/
// bools/null as JSON, objects/arrays as compact JSON (or 2-space-indented,
// possibly multi-line, JSON when pretty).
func cellString(v interface{}, pretty bool) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	default:
		var b []byte
		if pretty {
			b, _ = json.MarshalIndent(x, "", "  ")
		} else {
			b, _ = json.Marshal(x)
		}
		return string(b)
	}
}

func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// ---------------------------------------------------------------------------

// RenderJSONLines prints one JSON value per line (compact; clean for pipes).
// When pretty, each value is 2-space-indented and so spans multiple lines.
func RenderJSONLines(w io.Writer, rows []json.RawMessage, pretty bool) {
	for _, r := range rows {
		if pretty {
			fmt.Fprintln(w, indentJSON(r, ""))
		} else {
			fmt.Fprintln(w, compactJSON(r))
		}
	}
}

// RenderJSON prints all rows as one JSON array, one row per element. Rows are
// compact by default; when pretty, each element is fully 2-space-indented.
func RenderJSON(w io.Writer, rows []json.RawMessage, pretty bool) {
	if len(rows) == 0 {
		fmt.Fprintln(w, "[]")
		return
	}
	var b strings.Builder
	b.WriteString("[\n")
	for i, r := range rows {
		b.WriteString("  ")
		if pretty {
			b.WriteString(indentJSON(r, "  "))
		} else {
			b.WriteString(compactJSON(r))
		}
		if i < len(rows)-1 {
			b.WriteByte(',')
		}
		b.WriteByte('\n')
	}
	b.WriteString("]")
	fmt.Fprintln(w, b.String())
}

func compactJSON(r json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Compact(&buf, r); err != nil {
		return string(r)
	}
	return buf.String()
}

// indentJSON 2-space-indents a JSON value. prefix is prepended to every line
// after the first (so a value can be nested under an outer indent).
func indentJSON(r json.RawMessage, prefix string) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, r, prefix, "  "); err != nil {
		return compactJSON(r)
	}
	return buf.String()
}

// RenderCSV prints a header row of columns then one CSV record per row. When
// pretty, JSON cells are indented; the csv writer quotes their embedded newlines
// so the output stays valid CSV.
func RenderCSV(w io.Writer, rows []json.RawMessage, pretty bool) {
	cols, cells := tableOf(rows, pretty)
	cw := csv.NewWriter(w)
	cw.Write(cols)
	for _, row := range cells {
		cw.Write(row)
	}
	cw.Flush()
}

// RenderMarkdown prints a GitHub-flavored Markdown table. When pretty, a JSON
// cell's newlines become <br> so the multi-line value stays inside one table row.
func RenderMarkdown(w io.Writer, rows []json.RawMessage, pretty bool) {
	cols, cells := tableOf(rows, pretty)
	if len(cols) == 0 {
		return
	}
	fmt.Fprintln(w, "| "+strings.Join(escapeMD(cols), " | ")+" |")
	seps := make([]string, len(cols))
	for i := range seps {
		seps[i] = "---"
	}
	fmt.Fprintln(w, "| "+strings.Join(seps, " | ")+" |")
	for _, row := range cells {
		fmt.Fprintln(w, "| "+strings.Join(escapeMD(row), " | ")+" |")
	}
}

func escapeMD(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		s = strings.ReplaceAll(s, "|", "\\|")
		s = strings.ReplaceAll(s, "\n", "<br>") // keep pretty JSON in one table row
		out[i] = s
	}
	return out
}

// RenderLine prints each row vertically as "col = value" lines (DuckDB's line
// mode), best for wide/nested docs. Rows are separated by a blank line.
func RenderLine(w io.Writer, rows []json.RawMessage, pretty bool) {
	cols, cells := tableOf(rows, pretty)
	width := 0
	for _, c := range cols {
		if len(c) > width {
			width = len(c)
		}
	}
	for i, row := range cells {
		if i > 0 {
			fmt.Fprintln(w)
		}
		for j, c := range cols {
			fmt.Fprintf(w, "%*s = %s\n", width, c, row[j])
		}
	}
}

// RenderList prints each row's values joined by sep (pipe-friendly), no header.
func RenderList(w io.Writer, rows []json.RawMessage, sep string, pretty bool) {
	_, cells := tableOf(rows, pretty)
	for _, row := range cells {
		fmt.Fprintln(w, strings.Join(row, sep))
	}
}

// RenderBox prints the signature boxed unicode table.
//
// maxWidth caps a column's width, truncating with an ellipsis: >0 is a fixed
// per-column cap, 0 is uncapped, and <0 ("auto") fits the box to termWidth,
// widening columns to use whatever horizontal space is available (and shrinking
// them, fairly, when the natural table is too wide). termWidth is only consulted
// in auto mode; when it is unknown (<=0) auto falls back to uncapped.
//
// maxRows caps shown rows: >0 keeps a head+tail with a "·" elision row in the
// middle, 0 shows all, and <0 keeps the last |maxRows| rows with the "·"
// elision row at the front. elapsed (if non-empty) joins the footer. style adds
// dim borders/footer and a cyan-bold header when On.
//
// When pretty, JSON cells are 2-space-indented and a cell may span multiple
// lines; such a row is as tall as its tallest cell, with shorter cells blank-
// padded below their content. Column widths use each cell's widest line. Because
// those multi-line rows would otherwise run together, pretty mode also draws a
// horizontal splitter (├─┼─┤) between adjacent body rows.
func RenderBox(w io.Writer, rows []json.RawMessage, maxWidth, maxRows, termWidth int, elapsed string, style Style, pretty bool) {
	cols, cells := tableOf(rows, pretty)
	if len(cols) == 0 {
		fmt.Fprintln(w, style.Dim("(0 rows)"))
		return
	}

	// Per-column: numeric (for right-align) and display width.
	numeric := make([]bool, len(cols))
	for j := range cols {
		numeric[j] = len(cells) > 0
	}
	widths := make([]int, len(cols))
	for j, c := range cols {
		widths[j] = runeLen(c)
	}
	for _, row := range cells {
		for j, cell := range row {
			if !isNumeric(cell) {
				numeric[j] = false
			}
			if l := cellWidth(cell); l > widths[j] {
				widths[j] = l
			}
		}
	}
	if maxWidth > 0 {
		for j := range widths {
			if widths[j] > maxWidth {
				widths[j] = maxWidth
			}
		}
	} else if maxWidth < 0 && termWidth > 0 {
		capColumnsToWidth(widths, termWidth)
	}

	// Which rows to show. dotsFront places the elision row before the shown
	// rows (tail mode); otherwise a middle elision row separates head from tail.
	type span struct{ from, to int } // [from, to)
	var shown []span
	elided := 0
	dotsFront := false
	switch {
	case maxRows > 0 && len(cells) > maxRows:
		head := (maxRows + 1) / 2
		tail := maxRows - head
		shown = []span{{0, head}, {len(cells) - tail, len(cells)}}
		elided = len(cells) - maxRows
	case maxRows < 0 && len(cells) > -maxRows:
		n := -maxRows
		shown = []span{{len(cells) - n, len(cells)}}
		elided = len(cells) - n
		dotsFront = true
	default:
		shown = []span{{0, len(cells)}}
	}

	border := func(l, m, r string) {
		var b strings.Builder
		b.WriteString(l)
		for j := range cols {
			b.WriteString(strings.Repeat("─", widths[j]+2))
			if j < len(cols)-1 {
				b.WriteString(m)
			}
		}
		b.WriteString(r)
		fmt.Fprintln(w, style.Dim(b.String()))
	}
	bar := style.Dim("│")
	printRow := func(vals []string, head bool) {
		// A cell may span multiple lines (pretty JSON); the row is as tall as
		// its tallest cell, and shorter cells pad with blank lines below.
		lines := make([][]string, len(vals))
		height := 1
		for j, v := range vals {
			lines[j] = strings.Split(v, "\n")
			if len(lines[j]) > height {
				height = len(lines[j])
			}
		}
		for k := 0; k < height; k++ {
			var b strings.Builder
			b.WriteString(bar)
			for j := range vals {
				seg := ""
				if k < len(lines[j]) {
					seg = lines[j][k]
				}
				cell := pad(truncate(seg, widths[j]), widths[j], numeric[j])
				if head {
					cell = style.header(cell)
				}
				b.WriteString(" ")
				b.WriteString(cell)
				b.WriteString(" ")
				b.WriteString(bar)
			}
			fmt.Fprintln(w, b.String())
		}
	}

	printDots := func() {
		dots := make([]string, len(cols))
		for j := range dots {
			dots[j] = "·"
		}
		printRow(dots, false)
	}

	// Body rows in display order (any leading/middle elision "·" row included),
	// gathered as thunks so pretty mode can slip a splitter between each pair.
	var body []func()
	if dotsFront {
		body = append(body, printDots)
	}
	for si, sp := range shown {
		for i := sp.from; i < sp.to; i++ {
			i := i
			body = append(body, func() { printRow(cells[i], false) })
		}
		if si == 0 && len(shown) > 1 {
			body = append(body, printDots)
		}
	}

	border("┌", "┬", "┐")
	printRow(cols, true)
	border("├", "┼", "┤")
	for i, emit := range body {
		// Pretty rows may span several lines, so fence them apart for legibility.
		if i > 0 && pretty {
			border("├", "┼", "┤")
		}
		emit()
	}
	border("└", "┴", "┘")

	footer := fmt.Sprintf("%d row(s)", len(cells))
	if elided > 0 {
		footer += fmt.Sprintf(" (showing %d, %d elided)", len(cells)-elided, elided)
	}
	footer += fmt.Sprintf(" · %d column(s)", len(cols))
	if elapsed != "" {
		footer += " · " + elapsed
	}
	fmt.Fprintln(w, style.Dim(footer))
}

// capColumnsToWidth shrinks widths in place so the whole box fits within
// termWidth columns, distributing the budget fairly (max-min): narrow columns
// keep their natural width, and only the columns wide enough to overflow are
// capped, sharing the leftover space equally. When the table already fits,
// widths are left untouched — so wider terminals simply show more.
func capColumnsToWidth(widths []int, termWidth int) {
	if len(widths) == 0 {
		return
	}
	// Non-content overhead per the box frame: each column adds a leading "│ "
	// and trailing " " (3 runes) and the box has one final "│".
	budget := termWidth - (3*len(widths) + 1)
	if budget < len(widths) {
		budget = len(widths) // floor: at least 1 content rune per column
	}

	// Max-min fair share: repeatedly hand each not-yet-fixed column an equal
	// slice of the remaining budget; any column that fits under its slice is
	// fixed at its natural width, returning the slack for the rest.
	fixed := make([]bool, len(widths))
	remaining := budget
	nFree := len(widths)
	for {
		share := remaining / nFree
		if share < 1 {
			share = 1
		}
		grewFixed := false
		for j, wd := range widths {
			if !fixed[j] && wd <= share {
				fixed[j] = true
				remaining -= wd
				nFree--
				grewFixed = true
			}
		}
		if !grewFixed || nFree == 0 {
			// Cap every remaining (over-share) column at the final fair share.
			for j := range widths {
				if !fixed[j] && widths[j] > share {
					widths[j] = share
				}
			}
			return
		}
	}
}

func runeLen(s string) int { return utf8.RuneCountInString(s) }

// cellWidth is a cell's display width — the rune width of its widest line, so
// multi-line (pretty JSON) cells size their column correctly.
func cellWidth(s string) int {
	if !strings.ContainsRune(s, '\n') {
		return runeLen(s)
	}
	w := 0
	for _, line := range strings.Split(s, "\n") {
		if l := runeLen(line); l > w {
			w = l
		}
	}
	return w
}

// truncate shortens s to at most n runes, marking the cut with an ellipsis.
func truncate(s string, n int) string {
	if n <= 0 || runeLen(s) <= n {
		return s
	}
	if n == 1 {
		return "…"
	}
	r := []rune(s)
	return string(r[:n-1]) + "…"
}

// pad right-aligns numeric cells, left-aligns the rest, to width n runes.
func pad(s string, n int, right bool) string {
	gap := n - runeLen(s)
	if gap <= 0 {
		return s
	}
	if right {
		return strings.Repeat(" ", gap) + s
	}
	return s + strings.Repeat(" ", gap)
}

// ---------------------------------------------------------------------------

// SplitStatements splits SQL text on top-level ';' (ignoring ';' inside ' " or
// ` quotes), returning the complete statements and any trailing remainder.
func SplitStatements(s string) (stmts []string, rest string) {
	var start int
	var quote rune
	for i, r := range s {
		if quote != 0 {
			if r == quote {
				quote = 0
			}
			continue
		}
		switch r {
		case '\'', '"', '`':
			quote = r
		case ';':
			stmts = append(stmts, s[start:i])
			start = i + 1
		}
	}
	return stmts, s[start:]
}
