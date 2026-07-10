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

// Result-oracle for native window-frame AGGREGATES (SUM/COUNT/AVG/MIN/MAX OVER ...).
// n1k1's window path formerly crashed on the boxed cbq aggregate ("value.objectValue
// is not value.AnnotatedValue"); the frame aggregate is now computed natively by
// OpWindowFrames over each row's frame (base.Agg Init/Update/Result) and exposed
// under "^aggregates|<agg.String()>", read by the projection exactly as GROUP BY
// aggregates are. These hand-checked, ORDER-SENSITIVE assertions are the coverage
// the suite lacked (TestSuiteCases has zero OVER cases), which let the window path
// stay broken unnoticed. Only ROWS frames + all-unbounded frames are wired; RANGE/
// GROUPS value/current-row frames (peer semantics, needs WindowFrame.ValIdx) are a
// follow-up.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

// windowTestSession writes a 5-row nums keyspace (n = 10,20,30,40,50; g alternating
// a/b) and opens a session over it.
func windowTestSession(t *testing.T) *Session {
	t.Helper()
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "nums")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		g := "a"
		if i%2 == 1 {
			g = "b"
		}
		doc := []byte(`{"n":` + strconv.Itoa((i+1)*10) + `,"g":"` + g + `"}`)
		if err := os.WriteFile(filepath.Join(ks, "d"+strconv.Itoa(i)+".json"), doc, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	return sess
}

// winCol runs stmt and returns the named field's value from each result row, in row
// order (the queries carry ORDER BY, so order is deterministic).
func winCol(t *testing.T, sess *Session, stmt, field string) []float64 {
	t.Helper()
	res, err := sess.Run(stmt)
	if err != nil {
		t.Fatalf("Run(%q): %v", stmt, err)
	}
	out := make([]float64, 0, len(res.Rows))
	for _, r := range res.Rows {
		var m map[string]float64
		if err := json.Unmarshal(r, &m); err != nil {
			t.Fatalf("decode %q from %q: %v", r, stmt, err)
		}
		out = append(out, m[field])
	}
	return out
}

// TestWindowFrameAggregates asserts native frame aggregates over the wired frame
// shapes (ROWS + all-unbounded), including PARTITION BY and multiple aggregates in
// one query.
func TestWindowFrameAggregates(t *testing.T) {
	sess := windowTestSession(t)

	cases := []struct {
		name, stmt, field string
		want              []float64
	}{
		{"sum-whole-partition",
			`SELECT SUM(n) OVER () AS s FROM nums ORDER BY n`, "s",
			[]float64{150, 150, 150, 150, 150}},
		{"sum-partition-by-g",
			`SELECT SUM(n) OVER (PARTITION BY g) AS s FROM nums ORDER BY n`, "s",
			[]float64{90, 60, 90, 60, 90}}, // g=a:{10,30,50}=90, g=b:{20,40}=60
		{"running-sum-rows-unbounded-preceding",
			`SELECT SUM(n) OVER (ORDER BY n ROWS UNBOUNDED PRECEDING) AS s FROM nums ORDER BY n`, "s",
			[]float64{10, 30, 60, 100, 150}},
		{"reverse-running-sum",
			`SELECT SUM(n) OVER (ORDER BY n ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s FROM nums ORDER BY n`, "s",
			[]float64{150, 140, 120, 90, 50}},
		{"moving-sum-1p-1f",
			`SELECT SUM(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM nums ORDER BY n`, "s",
			[]float64{30, 60, 90, 120, 90}},
		{"moving-avg-1p-1f",
			`SELECT AVG(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM nums ORDER BY n`, "s",
			[]float64{15, 20, 30, 40, 45}},
		{"moving-count-1p-1f",
			`SELECT COUNT(*) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM nums ORDER BY n`, "s",
			[]float64{2, 3, 3, 3, 2}},
		{"moving-min-1p-1f",
			`SELECT MIN(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM nums ORDER BY n`, "s",
			[]float64{10, 10, 20, 30, 40}},
		{"moving-max-1p-1f",
			`SELECT MAX(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM nums ORDER BY n`, "s",
			[]float64{20, 30, 40, 50, 50}},
	}

	for _, c := range cases {
		got := winCol(t, sess, c.stmt, c.field)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v\n  %s", c.name, got, c.want, c.stmt)
		}
	}
}

// TestWindowMultipleAggregates: several window aggregates in one query each get their
// own "^aggregates|..." column down the chain (the multi-aggregate fix -- a
// re-partitioning chain previously dropped all but the last).
func TestWindowMultipleAggregates(t *testing.T) {
	sess := windowTestSession(t)

	stmt := `SELECT n,
	                SUM(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s,
	                MIN(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mn,
	                MAX(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mx
	         FROM nums ORDER BY n`

	if got := winCol(t, sess, stmt, "s"); !reflect.DeepEqual(got, []float64{30, 60, 90, 120, 90}) {
		t.Errorf("multi-agg SUM: got %v", got)
	}
	if got := winCol(t, sess, stmt, "mn"); !reflect.DeepEqual(got, []float64{10, 10, 20, 30, 40}) {
		t.Errorf("multi-agg MIN: got %v", got)
	}
	if got := winCol(t, sess, stmt, "mx"); !reflect.DeepEqual(got, []float64{20, 30, 40, 50, 50}) {
		t.Errorf("multi-agg MAX: got %v", got)
	}
}
