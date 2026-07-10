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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// insertTestDir builds a temp file datastore with a `logs` keyspace (4 rows, 2 of
// them sev=ERROR) and returns the dir so a test can both open sessions on it and
// inspect the files INSERT writes.
func insertTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	d := filepath.Join(dir, "default", "logs")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	body := `{"id":"a","sev":"ERROR","code":5}` + "\n" +
		`{"id":"b","sev":"ERROR","code":1}` + "\n" +
		`{"id":"c","sev":"INFO","code":2}` + "\n" +
		`{"id":"d","sev":"WARN","code":9}` + "\n"
	if err := os.WriteFile(filepath.Join(d, "l.jsonl"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestInsertMaterializeRoundTrip is the headline: INSERT INTO a brand-new keyspace
// file materializes a query's results, the file lands on disk, and a fresh session
// reads it back as a keyspace (the directory) -- the "materialize now, slice later"
// flow. Also covers the brand-new-only guard and the datastore-escape guard.
func TestInsertMaterializeRoundTrip(t *testing.T) {
	dir := insertTestDir(t)
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// Materialize the ERROR logs into a brand-new keyspace file.
	res, err := sess.Run("INSERT INTO `analysis/errors.jsonl` (KEY UUID(), VALUE self) " +
		`SELECT l.sev, l.code FROM logs l WHERE l.sev = "ERROR"`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if res.Count != 2 {
		t.Errorf("inserted %d rows, want 2", res.Count)
	}

	// The file exists on disk at <dir>/default/analysis/errors.jsonl with 2 lines.
	file := filepath.Join(dir, "default", "analysis", "errors.jsonl")
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("reading materialized file: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("materialized file has %d lines, want 2:\n%s", len(lines), raw)
	}
	for _, ln := range lines { // each line must be a JSON object with the projected fields
		var m map[string]interface{}
		if e := json.Unmarshal([]byte(ln), &m); e != nil {
			t.Fatalf("line is not JSON: %q (%v)", ln, e)
		}
		if m["sev"] != "ERROR" {
			t.Errorf("materialized row sev=%v, want ERROR", m["sev"])
		}
	}

	// Round-trip: a fresh session reads the new `analysis` keyspace (the directory)
	// and sees exactly the materialized rows.
	sess2, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession 2: %v", err)
	}
	r2, err := sess2.Run("SELECT COUNT(1) AS n, SUM(l.code) AS tot FROM analysis l")
	if err != nil {
		t.Fatalf("round-trip SELECT: %v", err)
	}
	if len(r2.Rows) != 1 {
		t.Fatalf("round-trip returned %d rows, want 1", len(r2.Rows))
	}
	var got struct {
		N   int `json:"n"`
		Tot int `json:"tot"`
	}
	if e := json.Unmarshal(r2.Rows[0], &got); e != nil {
		t.Fatalf("decoding round-trip row: %v", e)
	}
	if got.N != 2 || got.Tot != 6 { // codes 5 + 1
		t.Errorf("round-trip got n=%d tot=%d, want n=2 tot=6", got.N, got.Tot)
	}

	// Brand-new only: re-inserting the same file errors (phase 1).
	if _, err := sess.Run("INSERT INTO `analysis/errors.jsonl` (KEY UUID(), VALUE self) " +
		`SELECT l.sev FROM logs l`); err == nil {
		t.Error("re-INSERT into an existing file should error (brand-new only)")
	}

	// Escape guard: a target that climbs out of the datastore is rejected.
	if _, err := sess.Run("INSERT INTO `../escape.jsonl` (KEY UUID(), VALUE self) " +
		`SELECT l.sev FROM logs l`); err == nil {
		t.Error("INSERT into a path escaping the datastore should error")
	}
}

// TestInsertStreamManyRows materializes many more rows than the writer queue is
// deep, so the source-query producer and the writer goroutine genuinely run
// concurrently (the stage breaker). It guards row integrity end to end: no row is
// dropped, duplicated, or corrupted by the cross-goroutine hand-off. Run under
// -race, it also asserts the producer and writer don't share mutable state.
func TestInsertStreamManyRows(t *testing.T) {
	const nRows = insertWriterQueue * 8 // deeper than the channel -> real overlap
	dir := t.TempDir()
	src := filepath.Join(dir, "default", "seq")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	var body strings.Builder
	wantSum := 0
	for i := 0; i < nRows; i++ {
		fmt.Fprintf(&body, `{"id":"r%d","v":%d}`+"\n", i, i)
		wantSum += i
	}
	if err := os.WriteFile(filepath.Join(src, "s.jsonl"), []byte(body.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	res, err := sess.Run("INSERT INTO `copy/all.jsonl` (KEY UUID(), VALUE self) " +
		`SELECT s.v FROM seq s`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if res.Count != nRows {
		t.Fatalf("inserted %d rows, want %d", res.Count, nRows)
	}

	// Read the materialized keyspace back: same count, same checksum -> every row
	// survived the hand-off intact and in one piece.
	sess2, _ := OpenSession(dir, "default")
	r2, err := sess2.Run("SELECT COUNT(1) AS n, SUM(c.v) AS tot FROM copy c")
	if err != nil {
		t.Fatalf("round-trip SELECT: %v", err)
	}
	var got struct {
		N   int `json:"n"`
		Tot int `json:"tot"`
	}
	if e := json.Unmarshal(r2.Rows[0], &got); e != nil {
		t.Fatalf("decoding: %v", e)
	}
	if got.N != nRows || got.Tot != wantSum {
		t.Errorf("round-trip got n=%d tot=%d, want n=%d tot=%d", got.N, got.Tot, nRows, wantSum)
	}
}

// TestInsertValueConstruct confirms the VALUE expression is evaluated against each
// SELECT OUTPUT row (cbq INSERT-SELECT semantics): a constructed object referencing
// the projected field names materializes correctly.
func TestInsertValueConstruct(t *testing.T) {
	dir := insertTestDir(t)
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	res, err := sess.Run("INSERT INTO `report/high.jsonl` (KEY UUID(), VALUE {\"lbl\": sev, \"n\": code}) " +
		`SELECT l.sev, l.code FROM logs l WHERE l.code >= 5`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if res.Count != 2 { // code 5 (ERROR) and 9 (WARN)
		t.Errorf("inserted %d rows, want 2", res.Count)
	}

	sess2, _ := OpenSession(dir, "default")
	r2, err := sess2.Run(`SELECT r.lbl, r.n FROM report r ORDER BY r.n`)
	if err != nil {
		t.Fatalf("round-trip SELECT: %v", err)
	}
	if len(r2.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(r2.Rows))
	}
	// Rows ordered by n: {level:ERROR,n:5}, {level:WARN,n:9}.
	want := []string{`{"lbl":"ERROR","n":5}`, `{"lbl":"WARN","n":9}`}
	for i, row := range r2.Rows {
		if canonJSON(t, row) != canonJSON(t, []byte(want[i])) {
			t.Errorf("row %d = %s, want %s", i, row, want[i])
		}
	}
}
