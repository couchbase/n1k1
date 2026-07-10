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

		// RANGE frames (default OVER (ORDER BY x) running total + value bounds), via
		// the ORDER BY value stored as the "^worderby" column that WindowFrame.ValIdx
		// reads. Distinct keys here, so RANGE running total == row-by-row running total.
		{"running-total-default-range",
			`SELECT SUM(n) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s",
			[]float64{10, 30, 60, 100, 150}},
		{"range-value-15p-15f",
			`SELECT SUM(n) OVER (ORDER BY n RANGE BETWEEN 15 PRECEDING AND 15 FOLLOWING) AS s FROM nums ORDER BY n`, "s",
			[]float64{30, 60, 90, 120, 90}}, // n=10:{10,20}=30; 20:{10,20,30}=60; 30:{20,30,40}=90; 40:{30,40,50}=120; 50:{40,50}=90
	}

	for _, c := range cases {
		got := winCol(t, sess, c.stmt, c.field)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v\n  %s", c.name, got, c.want, c.stmt)
		}
	}
}

// winColAny is like winCol but preserves NULL/absent (as nil) vs numbers (float64),
// so LAG/LEAD boundary NULLs are distinguishable from a real 0.
func winColAny(t *testing.T, sess *Session, stmt, field string) []interface{} {
	t.Helper()
	res, err := sess.Run(stmt)
	if err != nil {
		t.Fatalf("Run(%q): %v", stmt, err)
	}
	out := make([]interface{}, 0, len(res.Rows))
	for _, r := range res.Rows {
		var m map[string]interface{}
		if err := json.Unmarshal(r, &m); err != nil {
			t.Fatalf("decode %q from %q: %v", r, stmt, err)
		}
		out = append(out, m[field])
	}
	return out
}

// TestWindowOffset: FIRST_VALUE / LAST_VALUE / NTH_VALUE (frame-relative) and LAG /
// LEAD (partition-relative; a whole-partition frame is forced). Out-of-range offsets
// yield NULL (nil here).
func TestWindowOffset(t *testing.T) {
	sess := windowTestSession(t) // n = 10,20,30,40,50

	for _, c := range []struct {
		name, stmt, field string
		want              []interface{}
	}{
		{"lag", `SELECT LAG(n) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s",
			[]interface{}{nil, 10.0, 20.0, 30.0, 40.0}},
		{"lag-2", `SELECT LAG(n, 2) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s",
			[]interface{}{nil, nil, 10.0, 20.0, 30.0}},
		{"lead", `SELECT LEAD(n) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s",
			[]interface{}{20.0, 30.0, 40.0, 50.0, nil}},
		{"first_value", `SELECT FIRST_VALUE(n) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s",
			[]interface{}{10.0, 10.0, 10.0, 10.0, 10.0}},
		{"last_value-default-frame", // default frame ends at CURRENT ROW -> current n
			`SELECT LAST_VALUE(n) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s",
			[]interface{}{10.0, 20.0, 30.0, 40.0, 50.0}},
		{"last_value-whole-partition",
			`SELECT LAST_VALUE(n) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM nums ORDER BY n`, "s",
			[]interface{}{50.0, 50.0, 50.0, 50.0, 50.0}},
		{"nth_value-2-whole-partition",
			`SELECT NTH_VALUE(n, 2) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM nums ORDER BY n`, "s",
			[]interface{}{20.0, 20.0, 20.0, 20.0, 20.0}},
	} {
		if got := winColAny(t, sess, c.stmt, c.field); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v\n  %s", c.name, got, c.want, c.stmt)
		}
	}
}

// TestWindowRanking: ROW_NUMBER / RANK / DENSE_RANK. On distinct keys all three are
// 1..N; ROW_NUMBER resets per PARTITION. (ROW_NUMBER over TIED keys is
// non-deterministic -- the tie order is unspecified -- so it's asserted only on
// distinct keys.)
func TestWindowRanking(t *testing.T) {
	sess := windowTestSession(t) // distinct n = 10,20,30,40,50; g = a,b,a,b,a

	for _, c := range []struct {
		name, stmt, field string
		want              []float64
	}{
		{"row_number-distinct",
			`SELECT ROW_NUMBER() OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s",
			[]float64{1, 2, 3, 4, 5}},
		{"rank-distinct",
			`SELECT RANK() OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s",
			[]float64{1, 2, 3, 4, 5}},
		{"dense_rank-distinct",
			`SELECT DENSE_RANK() OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s",
			[]float64{1, 2, 3, 4, 5}},
		{"row_number-partition-by-g",
			// g=a rows (n=10,30,50) -> 1,2,3; g=b rows (n=20,40) -> 1,2. ORDER BY n
			// interleaves them: a1,b1,a2,b2,a3.
			`SELECT ROW_NUMBER() OVER (PARTITION BY g ORDER BY n) AS s FROM nums ORDER BY n`, "s",
			[]float64{1, 1, 2, 2, 3}},
	} {
		if got := winCol(t, sess, c.stmt, c.field); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v\n  %s", c.name, got, c.want, c.stmt)
		}
	}
}

// TestWindowRankingTies: RANK leaves gaps after a tie (1,1,3,...), DENSE_RANK does
// not (1,1,2,...). Uses the tied-key fixture (n=10,10,20,30).
func TestWindowRankingTies(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "t")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	rows := `{"n":10,"x":1}` + "\n" + `{"n":10,"x":2}` + "\n" + `{"n":20,"x":3}` + "\n" + `{"n":30,"x":4}` + "\n"
	if err := os.WriteFile(filepath.Join(ks, "t.jsonl"), []byte(rows), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	if got := winCol(t, sess, `SELECT RANK() OVER (ORDER BY n) AS s FROM t ORDER BY n, x`, "s"); !reflect.DeepEqual(got, []float64{1, 1, 3, 4}) {
		t.Errorf("RANK ties: got %v, want [1 1 3 4]", got)
	}
	if got := winCol(t, sess, `SELECT DENSE_RANK() OVER (ORDER BY n) AS s FROM t ORDER BY n, x`, "s"); !reflect.DeepEqual(got, []float64{1, 1, 2, 3}) {
		t.Errorf("DENSE_RANK ties: got %v, want [1 1 2 3]", got)
	}
}

// TestWindowRangePeers: a RANGE frame groups tied ORDER BY keys into one peer group
// (CURRENT ROW = all peers), so both n=10 rows see the same running SUM(x). A ROWS
// frame would instead give 1 then 3 (and is non-deterministic under ties). This is
// what ValIdx (the "^worderby" column) exists for.
func TestWindowRangePeers(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "t")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	rows := `{"n":10,"x":1}` + "\n" + `{"n":10,"x":2}` + "\n" + `{"n":20,"x":3}` + "\n" + `{"n":30,"x":4}` + "\n"
	if err := os.WriteFile(filepath.Join(ks, "t.jsonl"), []byte(rows), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	got := winCol(t, sess, `SELECT SUM(x) OVER (ORDER BY n) AS s FROM t ORDER BY n, x`, "s")
	want := []float64{3, 3, 6, 10} // n=10 peers both see {1,2}=3; then +3=6; +4=10
	if !reflect.DeepEqual(got, want) {
		t.Errorf("range peers: got %v, want %v", got, want)
	}
}

// TestWindowNamedClause: a named WINDOW clause (`... OVER w ... WINDOW w AS (...)`)
// resolves to its frame/order (via the pre-plan rewrite), so it matches the inline
// form and can be reused across several functions.
func TestWindowNamedClause(t *testing.T) {
	sess := windowTestSession(t)

	// One named ROWS window reused by SUM + MIN.
	stmt := `SELECT n, SUM(n) OVER w AS s, MIN(n) OVER w AS mn
	         FROM nums WINDOW w AS (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
	         ORDER BY n`
	if got := winCol(t, sess, stmt, "s"); !reflect.DeepEqual(got, []float64{30, 60, 90, 120, 90}) {
		t.Errorf("named-window SUM: got %v", got)
	}
	if got := winCol(t, sess, stmt, "mn"); !reflect.DeepEqual(got, []float64{10, 10, 20, 30, 40}) {
		t.Errorf("named-window MIN: got %v", got)
	}

	// A named RANGE window -> running total (matches OVER (ORDER BY n)).
	if got := winCol(t, sess, `SELECT SUM(n) OVER w AS s FROM nums WINDOW w AS (ORDER BY n) ORDER BY n`, "s"); !reflect.DeepEqual(got, []float64{10, 30, 60, 100, 150}) {
		t.Errorf("named-window running total: got %v", got)
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
