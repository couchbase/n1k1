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

import (
	"encoding/json"
	"strings"
	"testing"
)

func raws(ss ...string) []json.RawMessage {
	out := make([]json.RawMessage, len(ss))
	for i, s := range ss {
		out[i] = json.RawMessage(s)
	}
	return out
}

func TestValidMode(t *testing.T) {
	for _, m := range OutputModes {
		if !ValidMode(m) {
			t.Errorf("ValidMode(%q) = false", m)
		}
		if !ValidMode(m + "|pretty") {
			t.Errorf("ValidMode(%q|pretty) = false", m)
		}
		if !ValidMode(m + "-pretty") {
			t.Errorf("ValidMode(%q-pretty) = false", m)
		}
	}
	// jsonlines answers to the jsonl / ndjson synonyms too.
	for _, m := range []string{"jsonl", "ndjson", "jsonl|pretty", "ndjson-pretty"} {
		if !ValidMode(m) {
			t.Errorf("ValidMode(%q) = false (jsonlines synonym)", m)
		}
	}
	if ValidMode("nope") {
		t.Errorf("ValidMode(nope) = true")
	}
	if ValidMode("box|nope") {
		t.Errorf("ValidMode(box|nope) = true")
	}
	if ValidMode("nope|pretty") {
		t.Errorf("ValidMode(nope|pretty) = true")
	}
}

func TestParseMode(t *testing.T) {
	tests := []struct {
		in         string
		wantBase   string
		wantPretty bool
		wantOK     bool
	}{
		{"box", "box", false, true},
		{"box|pretty", "box", true, true},
		{"box-pretty", "box", true, true},
		{"json|pretty", "json", true, true},
		{"box|nope", "box", false, false},
		{"nope", "nope", false, false},
		{"nope|pretty", "nope", true, false},
		// jsonl / ndjson canonicalize to jsonlines (the output-mode synonyms).
		{"jsonl", "jsonlines", false, true},
		{"ndjson", "jsonlines", false, true},
		{"jsonl|pretty", "jsonlines", true, true},
		{"ndjson-pretty", "jsonlines", true, true},
	}
	for _, tc := range tests {
		base, pretty, ok := ParseMode(tc.in)
		if base != tc.wantBase || pretty != tc.wantPretty || ok != tc.wantOK {
			t.Errorf("ParseMode(%q) = (%q, %v, %v), want (%q, %v, %v)",
				tc.in, base, pretty, ok, tc.wantBase, tc.wantPretty, tc.wantOK)
		}
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		in        string
		wantStmts []string
		wantRest  string
	}{
		{"SELECT 1; SELECT 2;", []string{"SELECT 1", " SELECT 2"}, ""},
		{"SELECT 1", nil, "SELECT 1"},
		{"SELECT 1;  ", []string{"SELECT 1"}, "  "},
		{`SELECT ';' AS x; SELECT 2`, []string{`SELECT ';' AS x`}, " SELECT 2"},
		{`SELECT "a;b"; 1`, []string{`SELECT "a;b"`}, " 1"},
		{"SELECT `c;d` ; 1", []string{"SELECT `c;d` "}, " 1"},
		// ';' inside comments must not split.
		{"SELECT 1 /* a ; b */ AS x; 2", []string{"SELECT 1 /* a ; b */ AS x"}, " 2"},
		{"SELECT 1; -- tail ; note\nSELECT 2;", []string{"SELECT 1", " -- tail ; note\nSELECT 2"}, ""},
		{"/* c */; 1", []string{"/* c */"}, " 1"},
	}
	for _, tc := range tests {
		stmts, rest := SplitStatements(tc.in)
		if rest != tc.wantRest {
			t.Errorf("SplitStatements(%q) rest = %q, want %q", tc.in, rest, tc.wantRest)
		}
		if strings.Join(stmts, "|") != strings.Join(tc.wantStmts, "|") {
			t.Errorf("SplitStatements(%q) stmts = %q, want %q", tc.in, stmts, tc.wantStmts)
		}
	}
}

func TestIsBlankOrComment(t *testing.T) {
	blank := []string{
		"", "   ", "\t\n ", "-- just a note", "  -- note with ; and 'quote'",
		"/* block */", "/* a */  /* b */", "-- one\n/* two */\n   ",
	}
	for _, s := range blank {
		if !IsBlankOrComment(s) {
			t.Errorf("IsBlankOrComment(%q) = false, want true", s)
		}
	}
	real := []string{
		"SELECT 1", "  x", "-- c\nSELECT 1", "/* unterminated", "/* open\nstill open",
		"SELECT 1 -- trailing", ";",
	}
	for _, s := range real {
		if IsBlankOrComment(s) {
			t.Errorf("IsBlankOrComment(%q) = true, want false", s)
		}
	}
}

func TestTableOf(t *testing.T) {
	// Columns are first-seen order across rows; cells map by key; missing -> "".
	cols, cells := tableOf(raws(`{"a":1,"b":2}`, `{"b":3,"a":4}`, `{"c":5}`), false)
	if got := strings.Join(cols, ","); got != "a,b,c" {
		t.Fatalf("cols = %q, want a,b,c", got)
	}
	want := [][]string{{"1", "2", ""}, {"4", "3", ""}, {"", "", "5"}}
	for i := range want {
		if strings.Join(cells[i], "|") != strings.Join(want[i], "|") {
			t.Fatalf("row %d = %q, want %q", i, cells[i], want[i])
		}
	}
}

func TestTableOfScalars(t *testing.T) {
	cols, cells := tableOf(raws(`5`, `"hi"`, `true`), false)
	if len(cols) != 1 || cols[0] != scalarCol {
		t.Fatalf("cols = %v, want [%s]", cols, scalarCol)
	}
	if cells[0][0] != "5" || cells[1][0] != "hi" || cells[2][0] != "true" {
		t.Fatalf("scalar cells = %v", cells)
	}
}

func TestCellString(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{`null`, "null"},
		{`"hi"`, "hi"},         // strings unquoted
		{`12`, "12"},           // integer-valued
		{`1.5`, "1.5"},         // float
		{`true`, "true"},       // bool
		{`{"x":1}`, `{"x":1}`}, // nested -> compact json
		{`[1,2]`, `[1,2]`},
	}
	for _, tc := range tests {
		var v interface{}
		json.Unmarshal([]byte(tc.raw), &v)
		if got := cellString(v, false); got != tc.want {
			t.Errorf("cellString(%s) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestCellStringPretty(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{`null`, "null"},                // scalars unchanged by pretty
		{`"hi"`, "hi"},                  //
		{`12`, "12"},                    //
		{`{"x":1}`, "{\n  \"x\": 1\n}"}, // nested object -> indented, multi-line
		{`[1,2]`, "[\n  1,\n  2\n]"},    // nested array -> indented, multi-line
	}
	for _, tc := range tests {
		var v interface{}
		json.Unmarshal([]byte(tc.raw), &v)
		if got := cellString(v, true); got != tc.want {
			t.Errorf("cellString(%s, pretty) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		s    string
		n    int
		want string
	}{
		{"hello", 5, "hello"},
		{"hello", 10, "hello"},
		{"hello", 3, "he…"},
		{"hello", 1, "…"},
		{"héllo", 3, "hé…"}, // rune-aware
	}
	for _, tc := range tests {
		if got := truncate(tc.s, tc.n); got != tc.want {
			t.Errorf("truncate(%q,%d) = %q, want %q", tc.s, tc.n, got, tc.want)
		}
	}
}

func TestPad(t *testing.T) {
	if got := pad("hi", 5, false); got != "hi   " {
		t.Errorf("pad left = %q", got)
	}
	if got := pad("hi", 5, true); got != "   hi" {
		t.Errorf("pad right = %q", got)
	}
	if got := pad("toolong", 3, false); got != "toolong" {
		t.Errorf("pad no-shrink = %q", got)
	}
}

func TestRenderJSONLines(t *testing.T) {
	var b strings.Builder
	RenderJSONLines(&b, raws(`{"a":1}`, `{"b": 2}`), false)
	if b.String() != "{\"a\":1}\n{\"b\":2}\n" {
		t.Errorf("jsonlines = %q", b.String())
	}
}

// TestRenderJSONLine covers the single-row streaming primitive: byte-identical to
// one RenderJSONLines iteration (compact and pretty), and surfacing the writer error.
func TestRenderJSONLine(t *testing.T) {
	var b strings.Builder
	if err := RenderJSONLine(&b, json.RawMessage(`{"a": 1}`), false); err != nil {
		t.Fatalf("RenderJSONLine err: %v", err)
	}
	if b.String() != "{\"a\":1}\n" {
		t.Errorf("compact = %q", b.String())
	}

	b.Reset()
	if err := RenderJSONLine(&b, json.RawMessage(`{"a":1}`), true); err != nil {
		t.Fatalf("pretty err: %v", err)
	}
	if b.String() != "{\n  \"a\": 1\n}\n" {
		t.Errorf("pretty = %q", b.String())
	}

	// A failing writer's error propagates (so a streaming caller can stop on a
	// closed pipe).
	if err := RenderJSONLine(errWriter{}, json.RawMessage(`{"a":1}`), false); err == nil {
		t.Error("RenderJSONLine should return the writer error")
	}
}

// errWriter fails every write.
type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errTestWrite }

var errTestWrite = &testWriteErr{}

type testWriteErr struct{}

func (*testWriteErr) Error() string { return "write failed" }

func TestRenderJSON(t *testing.T) {
	var b strings.Builder
	RenderJSON(&b, raws(`{"a":1}`, `{"b":2}`), false)
	want := "[\n  {\"a\":1},\n  {\"b\":2}\n]\n"
	if b.String() != want {
		t.Errorf("json = %q, want %q", b.String(), want)
	}
	var e strings.Builder
	RenderJSON(&e, nil, false)
	if e.String() != "[]\n" {
		t.Errorf("json empty = %q", e.String())
	}
}

func TestRenderCSV(t *testing.T) {
	var b strings.Builder
	RenderCSV(&b, raws(`{"a":1,"b":"x"}`, `{"a":2,"b":"y,z"}`), false)
	want := "a,b\n1,x\n2,\"y,z\"\n" // csv quotes the comma
	if b.String() != want {
		t.Errorf("csv = %q, want %q", b.String(), want)
	}
}

func TestRenderMarkdown(t *testing.T) {
	var b strings.Builder
	RenderMarkdown(&b, raws(`{"a":1,"b":2}`), false)
	want := "| a | b |\n| --- | --- |\n| 1 | 2 |\n"
	if b.String() != want {
		t.Errorf("markdown = %q, want %q", b.String(), want)
	}
}

func TestRenderLine(t *testing.T) {
	var b strings.Builder
	RenderLine(&b, raws(`{"name":"dave","type":"contact"}`), false)
	want := "name = dave\ntype = contact\n"
	if b.String() != want {
		t.Errorf("line = %q, want %q", b.String(), want)
	}
}

func TestRenderList(t *testing.T) {
	var b strings.Builder
	RenderList(&b, raws(`{"a":1,"b":2}`, `{"a":3,"b":4}`), "|", false)
	if b.String() != "1|2\n3|4\n" {
		t.Errorf("list = %q", b.String())
	}
}

func TestRenderBoxPlain(t *testing.T) {
	var b strings.Builder
	RenderBox(&b, raws(`{"name":"dave"}`, `{"name":"earl"}`), 0, 0, 0, "", Style{}, false)
	out := b.String()
	if strings.Contains(out, "\x1b[") {
		t.Errorf("plain box must not contain ANSI: %q", out)
	}
	for _, want := range []string{"┌", "│ name │", "│ dave │", "└", "2 row(s)", "1 column(s)"} {
		if !strings.Contains(out, want) {
			t.Errorf("box missing %q in:\n%s", want, out)
		}
	}
}

func TestRenderBoxStyled(t *testing.T) {
	var b strings.Builder
	RenderBox(&b, raws(`{"n":1}`), 0, 0, 0, "", Style{On: true}, false)
	if !strings.Contains(b.String(), "\x1b[") {
		t.Errorf("styled box should contain ANSI escapes")
	}
}

// TestRenderBoxPretty: a "pretty" box expands a nested JSON cell across several
// physical lines, keeping the frame rectangular (every frame line same width).
func TestRenderBoxPretty(t *testing.T) {
	var b strings.Builder
	RenderBox(&b, raws(`{"n":1,"doc":{"x":1,"y":2}}`), 0, 0, 0, "", Style{}, true)
	out := b.String()
	for _, want := range []string{"│ {", `"x": 1`, `"y": 2`, "1 row(s)"} {
		if !strings.Contains(out, want) {
			t.Errorf("pretty box missing %q in:\n%s", want, out)
		}
	}
	// The nested doc's 4 pretty lines ({ , x, y, }) each become a physical row,
	// so the box has more content lines than a compact one-line render would.
	if n := strings.Count(out, "│"); n < 4 {
		t.Errorf("expected multi-line cell (many │) in:\n%s", out)
	}
	// Every frame line (starts with a box-drawing rune) must share one width,
	// or the multi-line padding is wrong.
	width := -1
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if len(line) < 3 || !strings.ContainsAny(line[:3], "┌├└│") {
			continue
		}
		if width < 0 {
			width = runeLen(line)
		} else if runeLen(line) != width {
			t.Errorf("ragged frame line (%d != %d): %q\nfull:\n%s", runeLen(line), width, line, out)
		}
	}
}

// TestRenderBoxPrettySplitters: pretty mode fences body rows apart with a
// horizontal splitter (├…┤) between each pair, since multi-line cells would
// otherwise run together. Plain mode adds no such splitters.
func TestRenderBoxPrettySplitters(t *testing.T) {
	rows := raws(`{"doc":{"x":1}}`, `{"doc":{"x":2}}`, `{"doc":{"x":3}}`)

	var pretty strings.Builder
	RenderBox(&pretty, rows, 0, 0, 0, "", Style{}, true)
	// One splitter under the header + one between each of the 3 body rows = 3
	// mid-borders starting with "├".
	if n := strings.Count(pretty.String(), "├"); n != 3 {
		t.Errorf("expected 3 ├ mid-borders (header + 2 row splitters), got %d:\n%s", n, pretty.String())
	}

	var plain strings.Builder
	RenderBox(&plain, rows, 0, 0, 0, "", Style{}, false)
	// Plain mode: only the header separator uses "├".
	if n := strings.Count(plain.String(), "├"); n != 1 {
		t.Errorf("plain box should have exactly 1 ├ (header separator), got %d:\n%s", n, plain.String())
	}
}

func TestRenderBoxElision(t *testing.T) {
	var b strings.Builder
	rows := raws(`{"n":1}`, `{"n":2}`, `{"n":3}`, `{"n":4}`, `{"n":5}`)
	RenderBox(&b, rows, 0, 2, 0, "", Style{}, false) // maxRows=2 -> head+tail with elision
	out := b.String()
	if !strings.Contains(out, "·") {
		t.Errorf("expected elision row (·) in:\n%s", out)
	}
	if !strings.Contains(out, "5 row(s)") || !strings.Contains(out, "elided") {
		t.Errorf("expected true count + elided note in footer:\n%s", out)
	}
}

// TestRenderBoxTailElision: a negative maxRows keeps the LAST |n| rows and puts
// the "·" elision row at the front of the box.
func TestRenderBoxTailElision(t *testing.T) {
	var b strings.Builder
	rows := raws(`{"n":1}`, `{"n":2}`, `{"n":3}`, `{"n":4}`, `{"n":5}`)
	RenderBox(&b, rows, 0, -2, 0, "", Style{}, false) // maxRows=-2 -> last 2 rows
	out := b.String()
	if !strings.Contains(out, "· ") {
		t.Errorf("expected elision row (·) in:\n%s", out)
	}
	// Last two values must be shown; the earlier ones elided.
	for _, want := range []string{"│ 4 │", "│ 5 │"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected tail row %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "│ 1 │") || strings.Contains(out, "│ 2 │") {
		t.Errorf("head rows should be elided in:\n%s", out)
	}
	// The elision row must precede the first shown data row.
	if i, j := strings.Index(out, "·"), strings.Index(out, "│ 4 │"); i < 0 || i > j {
		t.Errorf("elision row should be at the front in:\n%s", out)
	}
	if !strings.Contains(out, "5 row(s)") || !strings.Contains(out, "elided") {
		t.Errorf("expected true count + elided note in footer:\n%s", out)
	}
}

// TestRenderBoxAutoWidthFits: auto mode (maxWidth<0) shrinks an over-wide table
// to fit termWidth, but leaves a table that already fits untouched.
func TestRenderBoxAutoWidthFits(t *testing.T) {
	long := strings.Repeat("x", 100)
	rows := raws(`{"a":"` + long + `"}`)

	// Narrow terminal: the 100-char value must be truncated to fit.
	var narrow strings.Builder
	RenderBox(&narrow, rows, -1, 0, 40, "", Style{}, false)
	for _, line := range strings.Split(strings.TrimRight(narrow.String(), "\n"), "\n") {
		if runeLen(line) > 40 {
			t.Errorf("auto line exceeds termWidth 40 (%d): %q", runeLen(line), line)
		}
	}
	if !strings.Contains(narrow.String(), "…") {
		t.Errorf("expected truncation ellipsis in narrow auto box:\n%s", narrow.String())
	}

	// Wide terminal: the whole value fits, so no truncation.
	var wide strings.Builder
	RenderBox(&wide, rows, -1, 0, 200, "", Style{}, false)
	if strings.Contains(wide.String(), "…") {
		t.Errorf("wide auto box should not truncate:\n%s", wide.String())
	}
	if !strings.Contains(wide.String(), long) {
		t.Errorf("wide auto box should show full value:\n%s", wide.String())
	}
}

// TestCapColumnsToWidth checks the max-min fair-share width distribution.
func TestCapColumnsToWidth(t *testing.T) {
	// Narrow columns keep their width; the wide one absorbs the shrink.
	w := []int{1, 1, 100}
	capColumnsToWidth(w, 50) // budget = 50 - (3*3+1) = 40
	if w[0] != 1 || w[1] != 1 {
		t.Errorf("narrow columns should be untouched: %v", w)
	}
	if got, want := w[0]+w[1]+w[2], 40; got != want {
		t.Errorf("content total = %d, want %d (%v)", got, want, w)
	}

	// Already fits: untouched.
	w2 := []int{3, 4, 5}
	capColumnsToWidth(w2, 200)
	if w2[0] != 3 || w2[1] != 4 || w2[2] != 5 {
		t.Errorf("fitting table should be untouched: %v", w2)
	}
}

func TestStyleOffIsPlain(t *testing.T) {
	s := Style{On: false}
	if s.Red("x") != "x" || s.Cyan("x") != "x" || s.Dim("x") != "x" || s.Bold("x") != "x" {
		t.Errorf("Style{Off} must not wrap")
	}
	on := Style{On: true}
	if !strings.HasPrefix(on.Red("x"), "\x1b[") {
		t.Errorf("Style{On}.Red should wrap with ANSI")
	}
}

func TestIsByteSize(t *testing.T) {
	for _, s := range []string{"512B", "2.6MB", "603.4KB", "1.0GB", "0B"} {
		if !isByteSize(s) {
			t.Errorf("isByteSize(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"128", "hello", "MB", "", "2.6XB", "MB2"} {
		if isByteSize(s) {
			t.Errorf("isByteSize(%q) = true, want false", s)
		}
	}
	// rightAlignable = numbers OR byte sizes.
	if !rightAlignable("128") || !rightAlignable("2.6MB") || rightAlignable("hello") {
		t.Errorf("rightAlignable wrong")
	}
}

// TestBoxRightAlignsByteSizes: a column of humanBytes-style sizes right-aligns
// (the narrower value is padded on the left), like a numeric column.
func TestBoxRightAlignsByteSizes(t *testing.T) {
	var b strings.Builder
	RenderBox(&b, raws(`{"size":"2.6MB"}`, `{"size":"603.4KB"}`), 0, 0, 0, "", Style{}, false)
	out := b.String()
	// "603.4KB" is the widest (7); "2.6MB" (5) must be right-aligned -> "  2.6MB".
	if !strings.Contains(out, "  2.6MB ") {
		t.Errorf("size column not right-aligned:\n%s", out)
	}
}
