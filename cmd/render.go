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

// ValidMode reports whether m names a known output mode.
func ValidMode(m string) bool {
	for _, x := range OutputModes {
		if x == m {
			return true
		}
	}
	return false
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
// are display-ready: JSON strings unquoted, everything else compact JSON.
func tableOf(rows []json.RawMessage) (cols []string, cells [][]string) {
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
					row[j] = cellString(cv)
				}
			case c == scalarCol:
				row[j] = cellString(v)
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
// bools/null as JSON, objects/arrays as compact JSON.
func cellString(v interface{}) string {
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
		b, _ := json.Marshal(x)
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

// RenderJSONLines prints one compact JSON value per line (clean for pipes).
func RenderJSONLines(w io.Writer, rows []json.RawMessage) {
	for _, r := range rows {
		fmt.Fprintln(w, compactJSON(r))
	}
}

// RenderJSON prints all rows as one pretty JSON array.
func RenderJSON(w io.Writer, rows []json.RawMessage) {
	if len(rows) == 0 {
		fmt.Fprintln(w, "[]")
		return
	}
	var b strings.Builder
	b.WriteString("[\n")
	for i, r := range rows {
		b.WriteString("  ")
		b.WriteString(compactJSON(r))
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

// RenderCSV prints a header row of columns then one CSV record per row.
func RenderCSV(w io.Writer, rows []json.RawMessage) {
	cols, cells := tableOf(rows)
	cw := csv.NewWriter(w)
	cw.Write(cols)
	for _, row := range cells {
		cw.Write(row)
	}
	cw.Flush()
}

// RenderMarkdown prints a GitHub-flavored Markdown table.
func RenderMarkdown(w io.Writer, rows []json.RawMessage) {
	cols, cells := tableOf(rows)
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
		out[i] = strings.ReplaceAll(s, "|", "\\|")
	}
	return out
}

// RenderLine prints each row vertically as "col = value" lines (DuckDB's line
// mode), best for wide/nested docs. Rows are separated by a blank line.
func RenderLine(w io.Writer, rows []json.RawMessage) {
	cols, cells := tableOf(rows)
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
func RenderList(w io.Writer, rows []json.RawMessage, sep string) {
	_, cells := tableOf(rows)
	for _, row := range cells {
		fmt.Fprintln(w, strings.Join(row, sep))
	}
}

// RenderBox prints the signature boxed unicode table. maxWidth caps a column's
// width (0 = uncapped), truncating with an ellipsis; maxRows caps shown rows
// (0 = all) with a head/tail elision row; elapsed (if non-empty) joins the
// footer. style adds dim borders/footer and a cyan-bold header when On.
func RenderBox(w io.Writer, rows []json.RawMessage, maxWidth, maxRows int, elapsed string, style Style) {
	cols, cells := tableOf(rows)
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
			if l := runeLen(cell); l > widths[j] {
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
	}

	// Which rows to show (head+tail elision when over maxRows).
	type span struct{ from, to int } // [from, to)
	var shown []span
	elided := 0
	if maxRows > 0 && len(cells) > maxRows {
		head := (maxRows + 1) / 2
		tail := maxRows - head
		shown = []span{{0, head}, {len(cells) - tail, len(cells)}}
		elided = len(cells) - maxRows
	} else {
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
		var b strings.Builder
		b.WriteString(bar)
		for j, v := range vals {
			cell := pad(truncate(v, widths[j]), widths[j], numeric[j])
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

	border("┌", "┬", "┐")
	printRow(cols, true)
	border("├", "┼", "┤")
	for si, sp := range shown {
		for i := sp.from; i < sp.to; i++ {
			printRow(cells[i], false)
		}
		if si == 0 && len(shown) > 1 {
			dots := make([]string, len(cols))
			for j := range dots {
				dots[j] = "·"
			}
			printRow(dots, false)
		}
	}
	border("└", "┴", "┘")

	footer := fmt.Sprintf("%d row(s)", len(cells))
	if elided > 0 {
		footer += fmt.Sprintf(" (showing %d, %d elided)", len(cells)-elided, elided)
	}
	footer += fmt.Sprintf("  ·  %d column(s)", len(cols))
	if elapsed != "" {
		footer += "  ·  " + elapsed
	}
	fmt.Fprintln(w, style.Dim(footer))
}

func runeLen(s string) int { return utf8.RuneCountInString(s) }

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
