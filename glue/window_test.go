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

// TestWindowOffsetDefaultAndFromLast: LAG/LEAD's 3rd arg (the default yielded when the
// offset is out of range, evaluated at the current row), and NTH_VALUE ... FROM LAST
// (counts from the end of the frame).
func TestWindowOffsetDefaultAndFromLast(t *testing.T) {
	sess := windowTestSession(t) // n = 10,20,30,40,50

	for _, c := range []struct {
		name, stmt string
		want       []interface{}
	}{
		// LAG 2 with default -1: first two rows fall off the start -> -1, not NULL.
		{"lag-default", `SELECT LAG(n, 2, -1) OVER (ORDER BY n) AS s FROM nums ORDER BY n`,
			[]interface{}{-1.0, -1.0, 10.0, 20.0, 30.0}},
		// LEAD 2 with default -1: last two rows fall off the end -> -1.
		{"lead-default", `SELECT LEAD(n, 2, -1) OVER (ORDER BY n) AS s FROM nums ORDER BY n`,
			[]interface{}{30.0, 40.0, 50.0, -1.0, -1.0}},
		// NTH_VALUE(n,2) FROM LAST over the whole partition = 2nd from the end = 40
		// (vs 20 counting from the front).
		{"nth-from-last", `SELECT NTH_VALUE(n, 2) FROM LAST OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM nums ORDER BY n`,
			[]interface{}{40.0, 40.0, 40.0, 40.0, 40.0}},
	} {
		if got := winColAny(t, sess, c.stmt, "s"); !reflect.DeepEqual(got, c.want) {
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

// TestWindowPercentRankCumeDist: PERCENT_RANK = (rank-1)/(N-1); CUME_DIST =
// (#rows whose ORDER BY value <= current)/N. Both are partition-level peer functions,
// so ties (peers) share a value. Checked on distinct keys and the tied fixture.
func TestWindowPercentRankCumeDist(t *testing.T) {
	sess := windowTestSession(t) // distinct n = 10,20,30,40,50 (N=5), ranks 1..5

	if got := winCol(t, sess, `SELECT PERCENT_RANK() OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s"); !reflect.DeepEqual(got, []float64{0, 0.25, 0.5, 0.75, 1}) {
		t.Errorf("PERCENT_RANK distinct: got %v", got)
	}
	if got := winCol(t, sess, `SELECT CUME_DIST() OVER (ORDER BY n) AS s FROM nums ORDER BY n`, "s"); !reflect.DeepEqual(got, []float64{0.2, 0.4, 0.6, 0.8, 1}) {
		t.Errorf("CUME_DIST distinct: got %v", got)
	}

	// Tied fixture (n=10,10,20,30, N=4): ranks 1,1,3,4 -> PERCENT_RANK (0,0,2,3)/3;
	// CUME_DIST counts through the whole peer group -> both n=10 rows see 2/4.
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "t")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	rows := `{"n":10,"x":1}` + "\n" + `{"n":10,"x":2}` + "\n" + `{"n":20,"x":3}` + "\n" + `{"n":30,"x":4}` + "\n"
	if err := os.WriteFile(filepath.Join(ks, "t.jsonl"), []byte(rows), 0o644); err != nil {
		t.Fatal(err)
	}
	tsess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	if got := winCol(t, tsess, `SELECT PERCENT_RANK() OVER (ORDER BY n) AS s FROM t ORDER BY n, x`, "s"); !reflect.DeepEqual(got, []float64{0, 0, 2.0 / 3.0, 1}) {
		t.Errorf("PERCENT_RANK ties: got %v", got)
	}
	if got := winCol(t, tsess, `SELECT CUME_DIST() OVER (ORDER BY n) AS s FROM t ORDER BY n, x`, "s"); !reflect.DeepEqual(got, []float64{0.5, 0.5, 0.75, 1}) {
		t.Errorf("CUME_DIST ties: got %v", got)
	}
}

// TestWindowOverGroupedAggregate: a window function whose operand / ORDER BY is
// itself a grouped aggregate (SUM(COUNT(n)) OVER (...)) -- the window runs OVER the
// GROUP BY rows, reading the group's precomputed "^aggregates|..." columns for its
// operand and (via the ORDER-BY-aggregate path) its window ORDER BY.
func TestWindowOverGroupedAggregate(t *testing.T) {
	sess := windowTestSession(t) // nums: n=10,20,30,40,50; g=a,b,a,b,a

	// GROUP BY g -> a (COUNT 3), b (COUNT 2). SUM(COUNT(n)) OVER() sums the per-group
	// counts across both groups = 5, seen by every group row.
	if got := winCol(t, sess, `SELECT SUM(COUNT(n)) OVER() AS w FROM nums GROUP BY g ORDER BY g`, "w"); !reflect.DeepEqual(got, []float64{5, 5}) {
		t.Errorf("SUM(COUNT(n)) OVER(): got %v, want [5 5]", got)
	}
	// Window ORDER BY a grouped aggregate: OVER (ORDER BY MIN(n)) -> a (MIN 10) before
	// b (MIN 20); running SUM(COUNT(n)) = a:3, b:3+2=5. ORDER BY g -> [a, b] -> [3, 5].
	if got := winCol(t, sess, `SELECT SUM(COUNT(n)) OVER(ORDER BY MIN(n)) AS w FROM nums GROUP BY g ORDER BY g`, "w"); !reflect.DeepEqual(got, []float64{3, 5}) {
		t.Errorf("SUM(COUNT(n)) OVER(ORDER BY MIN(n)): got %v, want [3 5]", got)
	}
}

// TestWindowEmptyFrame: a GROUPS frame that resolves to an empty range -- because a
// PRECEDING boundary falls before the first group, or the bounds invert (1 PRECEDING
// AND 2 PRECEDING) -- must yield the aggregate's empty result: NULL for SUM/AVG, 0
// for COUNT. Previously the boundary was clamped into the partition, wrongly folding
// a row (and SUM/AVG over no numeric input returned 0/MISSING instead of NULL).
func TestWindowEmptyFrame(t *testing.T) {
	sess := windowTestSession(t) // n=10,20,30,40,50 distinct -> each its own group

	// GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING: the two groups just before the
	// current. n=10 has none -> empty -> NULL; then {10}; {10,20}; {20,30}; {30,40}.
	if got := winColAny(t, sess, `SELECT SUM(n) OVER (ORDER BY n GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS s FROM nums ORDER BY n`, "s"); !reflect.DeepEqual(got, []interface{}{nil, 10.0, 30.0, 50.0, 70.0}) {
		t.Errorf("GROUPS 2p-1p SUM: got %v, want [<nil> 10 30 50 70]", got)
	}
	// Inverted bounds (1 PRECEDING AND 2 PRECEDING) -> empty for every row: SUM NULL,
	// COUNT 0.
	if got := winColAny(t, sess, `SELECT SUM(n) OVER (ORDER BY n GROUPS BETWEEN 1 PRECEDING AND 2 PRECEDING) AS s FROM nums ORDER BY n`, "s"); !reflect.DeepEqual(got, []interface{}{nil, nil, nil, nil, nil}) {
		t.Errorf("GROUPS 1p-2p SUM (empty): got %v", got)
	}
	if got := winCol(t, sess, `SELECT COUNT(n) OVER (ORDER BY n GROUPS BETWEEN 1 PRECEDING AND 2 PRECEDING) AS s FROM nums ORDER BY n`, "s"); !reflect.DeepEqual(got, []float64{0, 0, 0, 0, 0}) {
		t.Errorf("GROUPS 1p-2p COUNT (empty): got %v", got)
	}
	// AVG over an empty frame is NULL (not 0 / MISSING).
	if got := winColAny(t, sess, `SELECT AVG(n) OVER (ORDER BY n GROUPS BETWEEN 1 PRECEDING AND 2 PRECEDING) AS s FROM nums ORDER BY n`, "s"); !reflect.DeepEqual(got, []interface{}{nil, nil, nil, nil, nil}) {
		t.Errorf("GROUPS 1p-2p AVG (empty): got %v", got)
	}
}

// TestWindowNtile: NTILE(k) splits the ordered partition into k contiguous buckets;
// the first (N mod k) buckets get one extra row. With N=5 rows: NTILE(2)=3+2,
// NTILE(3)=2+2+1; k>=N gives one row per bucket.
func TestWindowNtile(t *testing.T) {
	sess := windowTestSession(t) // n = 10,20,30,40,50 (N=5)

	for _, c := range []struct {
		name, stmt string
		want       []float64
	}{
		{"ntile-1", `SELECT NTILE(1) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, []float64{1, 1, 1, 1, 1}},
		{"ntile-2", `SELECT NTILE(2) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, []float64{1, 1, 1, 2, 2}},
		{"ntile-3", `SELECT NTILE(3) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, []float64{1, 1, 2, 2, 3}},
		{"ntile-5", `SELECT NTILE(5) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, []float64{1, 2, 3, 4, 5}},
		{"ntile-more-than-rows", `SELECT NTILE(10) OVER (ORDER BY n) AS s FROM nums ORDER BY n`, []float64{1, 2, 3, 4, 5}},
		// PARTITION BY g: g=a (n=10,30,50, N=3) NTILE(2)=2+1 -> 1,1,2; g=b (n=20,40,
		// N=2) NTILE(2)=1+1 -> 1,2. ORDER BY n interleaves a,b,a,b,a.
		{"ntile-2-partition-by-g", `SELECT NTILE(2) OVER (PARTITION BY g ORDER BY n) AS s FROM nums ORDER BY n`, []float64{1, 1, 1, 2, 2}},
	} {
		if got := winCol(t, sess, c.stmt, "s"); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v\n  %s", c.name, got, c.want, c.stmt)
		}
	}
}

// TestWindowCompositeOrderBy: multi-column ORDER BY peer detection. Peers are rows
// with an equal ORDER BY *tuple* (a,b), which only the canonical "tuple" ^worderby
// column resolves -- comparing just the first column would merge (1,2) into (1,1)'s
// group. Fixture rows (a,b,x): (1,1,1),(1,2,2),(1,2,3),(2,1,4).
func TestWindowCompositeOrderBy(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "c")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	rows := `{"a":1,"b":1,"x":1}` + "\n" + `{"a":1,"b":2,"x":2}` + "\n" +
		`{"a":1,"b":2,"x":3}` + "\n" + `{"a":2,"b":1,"x":4}` + "\n"
	if err := os.WriteFile(filepath.Join(ks, "c.jsonl"), []byte(rows), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	const ord = " FROM c ORDER BY a, b, x"
	for _, c := range []struct {
		name, fn string
		want     []float64
	}{
		// Groups: {(1,1)}, {(1,2),(1,2)}, {(2,1)}. If only column a were compared, all
		// three (1,*) rows would collapse into one group (rank 1,1,1) -- these expected
		// values only hold with full-tuple peers.
		{"rank", "RANK()", []float64{1, 2, 2, 4}},
		{"dense_rank", "DENSE_RANK()", []float64{1, 2, 2, 3}},
		{"percent_rank", "PERCENT_RANK()", []float64{0, 1.0 / 3.0, 1.0 / 3.0, 1}},
		{"cume_dist", "CUME_DIST()", []float64{0.25, 0.75, 0.75, 1}},
	} {
		stmt := "SELECT " + c.fn + " OVER (ORDER BY a, b) AS s" + ord
		if got := winCol(t, sess, stmt, "s"); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s composite: got %v, want %v\n  %s", c.name, got, c.want, stmt)
		}
	}

	// GROUPS frame aggregate over the composite ORDER BY: the running SUM steps whole
	// peer groups. Comparing only column a would give [6,6,6,10]; the (a,b) tuple keeps
	// (1,1) and (1,2) as separate groups -> [1,6,6,10].
	stmt := `SELECT SUM(x) OVER (ORDER BY a, b GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s` + ord
	if got := winCol(t, sess, stmt, "s"); !reflect.DeepEqual(got, []float64{1, 6, 6, 10}) {
		t.Errorf("GROUPS composite sum: got %v, want [1 6 6 10]\n  %s", got, stmt)
	}

	// Multi-column RANGE (no numeric offset) is pure peer grouping -- executed as
	// GROUPS, so it works over the (a,b) tuple where single-column RANGE arithmetic
	// (ParseFloat64) can't. The default running-total frame and the explicit RANGE
	// CURRENT ROW form both give the peer-group running total, matching the GROUPS
	// result above.
	for _, c := range []struct{ name, frame string }{
		{"range-default", ``},
		{"range-unbounded-current", ` RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`},
	} {
		stmt := `SELECT SUM(x) OVER (ORDER BY a, b` + c.frame + `) AS s` + ord
		if got := winCol(t, sess, stmt, "s"); !reflect.DeepEqual(got, []float64{1, 6, 6, 10}) {
			t.Errorf("multi-col RANGE %s: got %v, want [1 6 6 10]\n  %s", c.name, got, stmt)
		}
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
