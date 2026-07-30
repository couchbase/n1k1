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

package glue

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestCursorStoreRoundTrip pins the durable cursor store: save/load round-trips
// the water map, unknown names report ErrCursorNotExist, and unsafe names are
// rejected (no dir escape).
func TestCursorStoreRoundTrip(t *testing.T) {
	st := NewCursorStore(t.TempDir())

	if _, err := st.Load("nope"); err != ErrCursorNotExist {
		t.Fatalf("Load(missing): got %v, want ErrCursorNotExist", err)
	}

	cs := &CursorState{Name: "errs", Pack: "pack", Mode: "append",
		Water: map[string]int64{"default/events/events.jsonl": 4096}}
	if err := st.Save(cs); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := st.Load("errs")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Water["default/events/events.jsonl"] != 4096 || got.Mode != "append" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	names, err := st.List()
	if err != nil || len(names) != 1 || names[0] != "errs" {
		t.Fatalf("List: %v names=%v", err, names)
	}

	for _, bad := range []string{"", "..", "../evil", "a/b", ".hidden"} {
		if err := st.Save(&CursorState{Name: bad}); err == nil {
			t.Fatalf("Save(%q): expected rejection", bad)
		}
	}

	if err := st.Remove("errs"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := st.Load("errs"); err != ErrCursorNotExist {
		t.Fatalf("Load after Remove: got %v, want ErrCursorNotExist", err)
	}
}

// TestCursorAppendDelta is the Phase-1 end-to-end: an `append` cursor over a
// growing jsonl keyspace emits each new matching record exactly once across
// re-scans, and the recomputed high-water skips already-seen rows.
func TestCursorAppendDelta(t *testing.T) {
	dir := t.TempDir()
	ksDir := filepath.Join(dir, "default", "events")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(ksDir, "events.jsonl")

	appendLines := func(lines ...string) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		for _, ln := range lines {
			if _, err := f.WriteString(ln + "\n"); err != nil {
				t.Fatal(err)
			}
		}
		f.Close()
	}

	// A fusable query: ERROR-severity events.
	entry, err := ParseMultiQueryEntry("errs.sql++",
		"-- label: errs\n"+`SELECT e.n FROM events e WHERE e.sev = "ERROR"`)
	if err != nil {
		t.Fatalf("ParseMultiQueryEntry: %v", err)
	}
	dets := []MultiQueryEntry{entry}

	runCursor := func(since map[string]int64) *CursorRunResult {
		sess, err := OpenSession(dir, "default")
		if err != nil {
			t.Fatalf("OpenSession: %v", err)
		}
		r, err := sess.RunCursorPack(dets, since)
		if err != nil {
			t.Fatalf("RunCursorPack: %v", err)
		}
		return r
	}

	// Seed: 3 records, 2 of them ERROR.
	appendLines(
		`{"n":1,"sev":"ERROR"}`,
		`{"n":2,"sev":"INFO"}`,
		`{"n":3,"sev":"ERROR"}`,
	)

	// Run 1 (since=nil, from the beginning): both ERRORs are new.
	r1 := runCursor(nil)
	if len(r1.LabelResults) != 2 {
		t.Fatalf("run1: got %d labelResults, want 2 (%v)", len(r1.LabelResults), r1.LabelResults)
	}
	if len(r1.NewWater) != 1 {
		t.Fatalf("run1: water should track 1 container, got %v", r1.NewWater)
	}
	water := r1.NewWater

	// Run 2 (committed to run1's water, no appends): nothing new.
	r2 := runCursor(water)
	if len(r2.LabelResults) != 0 {
		t.Fatalf("run2: got %d labelResults, want 0 (re-emit bug) (%v)", len(r2.LabelResults), r2.LabelResults)
	}
	// Water must not rewind.
	for k, v := range water {
		if r2.NewWater[k] < v {
			t.Fatalf("run2: water rewound for %q: %d < %d", k, r2.NewWater[k], v)
		}
	}

	// Append 2 more (1 ERROR, 1 WARN).
	appendLines(
		`{"n":4,"sev":"ERROR"}`,
		`{"n":5,"sev":"WARN"}`,
	)

	// Run 3 (still at run1's water): only the new ERROR (#4) is delivered — the
	// filter must not re-emit #1/#3, and must skip the non-matching WARN.
	r3 := runCursor(water)
	if len(r3.LabelResults) != 1 {
		t.Fatalf("run3: got %d labelResults, want 1 (the newly appended ERROR) (%v)",
			len(r3.LabelResults), r3.LabelResults)
	}
	// The delivered row is n=4.
	if got := string(r3.LabelResults[0].Result); got != `{"n":4}` {
		t.Fatalf("run3: delivered %q, want {\"n\":4}", got)
	}

	// Committing run3's water then re-scanning yields nothing (idempotent).
	r4 := runCursor(r3.NewWater)
	if len(r4.LabelResults) != 0 {
		t.Fatalf("run4: got %d labelResults, want 0 after committing (%v)", len(r4.LabelResults), r4.LabelResults)
	}

	// PackID is stable for an unchanged pack.
	if PackID("errs", dets) != PackID("errs", dets) {
		t.Fatal("PackID not stable")
	}
}

// TestCursorDiffDelta is the Phase-2 end-to-end: over a MUTABLE current-state
// keyspace (rewritten between runs), diffing the full pack output against a prior
// snapshot yields the Debezium {insert,update,delete} change set.
func TestCursorDiffDelta(t *testing.T) {
	dir := t.TempDir()
	ksDir := filepath.Join(dir, "default", "incidents")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(ksDir, "incidents.jsonl")
	writeState := func(lines ...string) {
		if err := os.WriteFile(path, []byte(joinLines(lines)), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	entry, err := ParseMultiQueryEntry("inc.sql++",
		"-- label: inc\n"+`SELECT e.id, e.status FROM incidents e`)
	if err != nil {
		t.Fatalf("ParseMultiQueryEntry: %v", err)
	}
	dets := []MultiQueryEntry{entry}

	snapshot := func() map[string]SnapshotEntry {
		sess, err := OpenSession(dir, "default")
		if err != nil {
			t.Fatalf("OpenSession: %v", err)
		}
		lrs, err := sess.RunPackFull(dets)
		if err != nil {
			t.Fatalf("RunPackFull: %v", err)
		}
		snap, skipped := SnapshotFromResults(lrs, "id")
		if skipped != 0 {
			t.Fatalf("unexpected %d skipped (no id) rows", skipped)
		}
		return snap
	}

	// Baseline: two open incidents.
	writeState(`{"id":1,"status":"open"}`, `{"id":2,"status":"open"}`)
	base := snapshot()
	if len(base) != 2 {
		t.Fatalf("baseline snapshot: want 2 entries, got %d", len(base))
	}

	// No change → empty diff.
	if evs := DiffSnapshot(base, snapshot(), "id"); len(evs) != 0 {
		t.Fatalf("no-op diff: want 0 events, got %v", evs)
	}

	// Mutate: #1 open→closed (update), #2 gone (delete), #3 new (insert).
	writeState(`{"id":1,"status":"closed"}`, `{"id":3,"status":"open"}`)
	events := DiffSnapshot(base, snapshot(), "id")
	if len(events) != 3 {
		t.Fatalf("mutation diff: want 3 events, got %d: %+v", len(events), events)
	}
	// Sorted by (label,id): update #1, delete #2, insert #3.
	want := []struct{ op, id string }{{"update", "1"}, {"delete", "2"}, {"insert", "3"}}
	for i, w := range want {
		if events[i].Op != w.op || events[i].Id != w.id {
			t.Fatalf("event[%d]: got %s/%s, want %s/%s", i, events[i].Op, events[i].Id, w.op, w.id)
		}
	}
	if string(events[0].Before) != `{"id":1,"status":"open"}` || string(events[0].After) != `{"id":1,"status":"closed"}` {
		t.Fatalf("update before/after wrong: %s → %s", events[0].Before, events[0].After)
	}
	if string(events[1].Before) != `{"id":2,"status":"open"}` || events[1].After != nil {
		t.Fatalf("delete should carry before only: %+v", events[1])
	}
	if events[2].Before != nil || string(events[2].After) != `{"id":3,"status":"open"}` {
		t.Fatalf("insert should carry after only: %+v", events[2])
	}

	// Snapshot store round-trips the entry map.
	st := NewCursorStore(t.TempDir())
	if err := st.SaveSnapshot("c", base); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	got, err := st.LoadSnapshot("c")
	if err != nil || len(got) != 2 {
		t.Fatalf("LoadSnapshot: %v len=%d", err, len(got))
	}
}

func joinLines(lines []string) string {
	out := ""
	for _, ln := range lines {
		out += ln + "\n"
	}
	return out
}

// TestLoadPack pins LoadPack: a single *.sql++ FILE and a DIR of them both load
// into MultiQueryEntry lists (the source a cursor / run compiles).
func TestLoadPack(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "d.sql++")
	if err := os.WriteFile(f, []byte("-- label: d\nSELECT 1 AS n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if es, err := LoadPack(f); err != nil || len(es) != 1 || es[0].Label != "d" {
		t.Fatalf("LoadPack(file): %v %+v", err, es)
	}
	if es, err := LoadPack(dir); err != nil || len(es) != 1 {
		t.Fatalf("LoadPack(dir): %v len=%d", err, len(es))
	}
}

// TestFixtureParsing covers ISSUE-05: a malformed / unterminated @fixture is a hard
// error (not a silent drop that yields an empty golden reporting PASS), a multi-line
// pretty-printed fixture loads, and PackID ignores cosmetic blank-line/whitespace.
func TestFixtureParsing(t *testing.T) {
	// A pretty-printed (multi-line) fixture row loads as one document.
	pretty := "-- label: P\n" +
		"SELECT s.id FROM sessions s\n" +
		"-- @fixture\n" +
		"-- {\n" +
		`--   "id": "s1",` + "\n" +
		`--   "n": {"a": 1}` + "\n" +
		"-- }\n"
	e, err := ParseMultiQueryEntry("p.sql++", pretty)
	if err != nil {
		t.Fatalf("pretty fixture: unexpected error: %v", err)
	}
	if len(e.Fixture.Rows) != 1 {
		t.Fatalf("pretty fixture: want 1 row, got %d: %q", len(e.Fixture.Rows), e.Fixture.Rows)
	}
	if !json.Valid(e.Fixture.Rows[0]) {
		t.Fatalf("pretty fixture row is not valid JSON: %q", e.Fixture.Rows[0])
	}
	// A stored fixture row must be COMPACT (single-line) — an embedded newline breaks
	// the one-doc-per-line JSONL contract in RunFixture, yielding zero rows.
	if bytes.ContainsAny(e.Fixture.Rows[0], "\n") {
		t.Fatalf("pretty fixture row contains a newline (breaks JSONL): %q", e.Fixture.Rows[0])
	}
	if got := string(e.Fixture.Rows[0]); got != `{"id":"s1","n":{"a":1}}` {
		t.Fatalf("pretty fixture row not compacted: got %q", got)
	}

	// A malformed / unterminated fixture row is a HARD ERROR (the ISSUE-05 bug).
	bad := "-- label: B\nSELECT s.id FROM sessions s\n-- @fixture\n-- {\n--   \"id\": \"s1\"\n"
	if _, err := ParseMultiQueryEntry("b.sql++", bad); err == nil {
		t.Fatal("unterminated @fixture: expected a hard error, got nil")
	}

	// Compact one-doc-per-line still works exactly as before.
	compact := "-- label: C\nSELECT s.id FROM sessions s\n-- @fixture\n" +
		`-- {"id":"a"}` + "\n" + `-- {"id":"b"}` + "\n"
	e2, err := ParseMultiQueryEntry("c.sql++", compact)
	if err != nil || len(e2.Fixture.Rows) != 2 {
		t.Fatalf("compact fixture: err=%v rows=%d", err, len(e2.Fixture.Rows))
	}

	// PackID is invariant to interior blank lines + trailing whitespace (ISSUE-05 #4).
	a := MultiQueryEntry{Label: "X", Stmt: "SELECT 1\nFROM t"}
	b := MultiQueryEntry{Label: "X", Stmt: "SELECT 1   \n\n\nFROM t\n"}
	if PackID("x", []MultiQueryEntry{a}) != PackID("x", []MultiQueryEntry{b}) {
		t.Fatal("PackID changed under a cosmetic blank-line/whitespace reformat")
	}
}

// TestFixturePrettyEquivalence is the ISSUE-05 regression guard: a pretty-printed
// (multi-line) fixture and its single-line equivalent must produce BYTE-IDENTICAL
// labelResults through RunFixture (the pretty row used to yield zero rows because it
// carried embedded newlines into the JSONL temp keyspace).
func TestFixturePrettyEquivalence(t *testing.T) {
	compact := "-- label: E\n-- source: t\n" +
		"SELECT s.id, s.n FROM t s\n" +
		"-- @fixture\n" +
		`-- {"id":"a","n":{"x":1}}` + "\n"
	pretty := "-- label: E\n-- source: t\n" +
		"SELECT s.id, s.n FROM t s\n" +
		"-- @fixture\n" +
		"-- {\n" +
		`--   "id": "a",` + "\n" +
		`--   "n": {"x": 1}` + "\n" +
		"-- }\n"

	run := func(text string) []LabelResult {
		e, err := ParseMultiQueryEntry("e.sql++", text)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(e.Fixture.Rows) != 1 {
			t.Fatalf("want 1 fixture row, got %d", len(e.Fixture.Rows))
		}
		actual, err := e.RunFixture()
		if err != nil {
			t.Fatalf("RunFixture: %v", err)
		}
		return actual
	}

	c := run(compact)
	p := run(pretty)
	if len(c) != 1 {
		t.Fatalf("compact fixture: want 1 labelResult, got %d", len(c))
	}
	if len(p) != len(c) {
		t.Fatalf("pretty yielded %d labelResults, compact yielded %d (regression: pretty rows dropped)", len(p), len(c))
	}
	if string(p[0].Result) != string(c[0].Result) || p[0].Label != c[0].Label {
		t.Fatalf("pretty vs compact differ:\n pretty : %s %s\n compact: %s %s", p[0].Label, p[0].Result, c[0].Label, c[0].Result)
	}
}
