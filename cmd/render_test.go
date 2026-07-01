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
	}
	if ValidMode("nope") {
		t.Errorf("ValidMode(nope) = true")
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

func TestTableOf(t *testing.T) {
	// Columns are first-seen order across rows; cells map by key; missing -> "".
	cols, cells := tableOf(raws(`{"a":1,"b":2}`, `{"b":3,"a":4}`, `{"c":5}`))
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
	cols, cells := tableOf(raws(`5`, `"hi"`, `true`))
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
		if got := cellString(v); got != tc.want {
			t.Errorf("cellString(%s) = %q, want %q", tc.raw, got, tc.want)
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
	RenderJSONLines(&b, raws(`{"a":1}`, `{"b": 2}`))
	if b.String() != "{\"a\":1}\n{\"b\":2}\n" {
		t.Errorf("jsonlines = %q", b.String())
	}
}

func TestRenderJSON(t *testing.T) {
	var b strings.Builder
	RenderJSON(&b, raws(`{"a":1}`, `{"b":2}`))
	want := "[\n  {\"a\":1},\n  {\"b\":2}\n]\n"
	if b.String() != want {
		t.Errorf("json = %q, want %q", b.String(), want)
	}
	var e strings.Builder
	RenderJSON(&e, nil)
	if e.String() != "[]\n" {
		t.Errorf("json empty = %q", e.String())
	}
}

func TestRenderCSV(t *testing.T) {
	var b strings.Builder
	RenderCSV(&b, raws(`{"a":1,"b":"x"}`, `{"a":2,"b":"y,z"}`))
	want := "a,b\n1,x\n2,\"y,z\"\n" // csv quotes the comma
	if b.String() != want {
		t.Errorf("csv = %q, want %q", b.String(), want)
	}
}

func TestRenderMarkdown(t *testing.T) {
	var b strings.Builder
	RenderMarkdown(&b, raws(`{"a":1,"b":2}`))
	want := "| a | b |\n| --- | --- |\n| 1 | 2 |\n"
	if b.String() != want {
		t.Errorf("markdown = %q, want %q", b.String(), want)
	}
}

func TestRenderLine(t *testing.T) {
	var b strings.Builder
	RenderLine(&b, raws(`{"name":"dave","type":"contact"}`))
	want := "name = dave\ntype = contact\n"
	if b.String() != want {
		t.Errorf("line = %q, want %q", b.String(), want)
	}
}

func TestRenderList(t *testing.T) {
	var b strings.Builder
	RenderList(&b, raws(`{"a":1,"b":2}`, `{"a":3,"b":4}`), "|")
	if b.String() != "1|2\n3|4\n" {
		t.Errorf("list = %q", b.String())
	}
}

func TestRenderBoxPlain(t *testing.T) {
	var b strings.Builder
	RenderBox(&b, raws(`{"name":"dave"}`, `{"name":"earl"}`), 0, 0, 0, "", Style{})
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
	RenderBox(&b, raws(`{"n":1}`), 0, 0, 0, "", Style{On: true})
	if !strings.Contains(b.String(), "\x1b[") {
		t.Errorf("styled box should contain ANSI escapes")
	}
}

func TestRenderBoxElision(t *testing.T) {
	var b strings.Builder
	rows := raws(`{"n":1}`, `{"n":2}`, `{"n":3}`, `{"n":4}`, `{"n":5}`)
	RenderBox(&b, rows, 0, 2, 0, "", Style{}) // maxRows=2 -> head+tail with elision
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
	RenderBox(&b, rows, 0, -2, 0, "", Style{}) // maxRows=-2 -> last 2 rows
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
	RenderBox(&narrow, rows, -1, 0, 40, "", Style{})
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
	RenderBox(&wide, rows, -1, 0, 200, "", Style{})
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
