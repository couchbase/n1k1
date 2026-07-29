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
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestCensusMonoid is the property the incremental census rides on: censusing a
// corpus in two halves and MERGING equals censusing it whole, on every mergeable
// column (docs=SUM, first=MIN, last=MAX). (first_id is file-scoped provenance, not a
// mergeable aggregate, so it's excluded — as in the prototype's uuid-based check.)
func TestCensusMonoid(t *testing.T) {
	dir := t.TempDir()
	recs := []string{
		`{"type":"a","timestamp":"2026-06-21","x":1}`,
		`{"type":"a","timestamp":"2026-06-22","x":2,"y":"hi"}`,
		`{"type":"a","timestamp":"2026-07-01","x":"str"}`, // x changes type
		`{"type":"b","timestamp":"2026-07-02","z":true}`,
	}
	mk := func(ks string, lines []string) {
		d := filepath.Join(dir, "default", ks)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		body := ""
		for _, l := range lines {
			body += l + "\n"
		}
		if err := os.WriteFile(filepath.Join(d, ks+".jsonl"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mk("all", recs)
	mk("h1", recs[:2])
	mk("h2", recs[2:])

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}
	whole, err := sess.Census("all", CensusOptions{TimeField: "timestamp"})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := sess.Census("h1", CensusOptions{TimeField: "timestamp"})
	b, _ := sess.Census("h2", CensusOptions{TimeField: "timestamp"})
	merged := MergeCensus(a, b)

	key := func(r CensusRow) string {
		return fmt.Sprintf("%s|%s|%s|%d|%s|%s", r.Type, r.Path, r.ValType, r.Docs, r.FirstSeen, r.LastSeen)
	}
	set := map[string]bool{}
	for _, r := range whole.Rows {
		set[key(r)] = true
	}
	if len(merged.Rows) != len(whole.Rows) {
		t.Fatalf("cell count: merged %d, whole %d", len(merged.Rows), len(whole.Rows))
	}
	for _, r := range merged.Rows {
		if !set[key(r)] {
			t.Fatalf("merged cell not in whole census: %s", key(r))
		}
	}
	if merged.Records != whole.Records || merged.TypeTotals["a"] != whole.TypeTotals["a"] {
		t.Fatalf("totals differ: merged rec=%d totals=%v, whole rec=%d totals=%v",
			merged.Records, merged.TypeTotals, whole.Records, whole.TypeTotals)
	}
}

// TestCensusDrift: a window that adds a wholly new path is field_added; a new
// value-type on a known path is type_changed; a cell already in the prior is silent.
func TestCensusDrift(t *testing.T) {
	prior := []CensusRow{
		{Type: "a", Path: "model", ValType: "string", Docs: 5},
		{Type: "a", Path: "type", ValType: "string", Docs: 5},
	}
	window := []CensusRow{
		{Type: "a", Path: "model", ValType: "string", Docs: 1},                           // known cell -> silent
		{Type: "a", Path: "model", ValType: "array", Docs: 1},                            // type_changed
		{Type: "a", Path: "effort", ValType: "string", Docs: 1, FirstSeen: "2026-07-17"}, // field_added
	}
	drift := CensusDrift(prior, window)
	if len(drift) != 2 {
		t.Fatalf("want 2 drift events, got %d: %+v", len(drift), drift)
	}
	byPath := map[string]CensusChange{}
	for _, d := range drift {
		byPath[d.Path] = d
	}
	if byPath["effort"].Op != "field_added" {
		t.Fatalf("effort should be field_added, got %q", byPath["effort"].Op)
	}
	if byPath["model"].Op != "type_changed" || byPath["model"].ValType != "array" {
		t.Fatalf("model->array should be type_changed, got %+v", byPath["model"])
	}
}

// TestCensusSince: a Since-filtered census counts only records past the watermark,
// and reports a NewWater at the head.
func TestCensusSince(t *testing.T) {
	dir := t.TempDir()
	d := filepath.Join(dir, "default", "e")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(d, "e.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"a","x":1}`+"\n"+`{"type":"a","x":2}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, _ := OpenSession(dir, "default")
	full, err := sess.Census("e", CensusOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if full.Records != 2 {
		t.Fatalf("full: want 2 records, got %d", full.Records)
	}
	// Append one; census only the new record via Since = the previous head.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"type":"a","x":3,"born":true}` + "\n")
	f.Close()

	sess2, _ := OpenSession(dir, "default")
	win, err := sess2.Census("e", CensusOptions{Since: full.NewWater})
	if err != nil {
		t.Fatal(err)
	}
	if win.Records != 1 {
		t.Fatalf("windowed: want 1 new record, got %d", win.Records)
	}
	// The new record introduces `born`; nothing else.
	sawBorn := false
	for _, r := range win.Rows {
		if r.Path == "born" {
			sawBorn = true
		}
	}
	if !sawBorn {
		t.Fatalf("windowed census missing the new `born` field: %+v", win.Rows)
	}
}
