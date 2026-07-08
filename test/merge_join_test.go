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

package test

// Track B round 3 (DESIGN-merging.md §2/§3) tests:
//
//   - TestMergeJoinAsofRecognizer -- GOLDEN plan-recognition tests for the
//     argmax-subquery -> ASOF recognizer (glue/optimize_temporal.go): the exact
//     canonical nearest-preceding shape is RECOGNIZED, and a battery of
//     near-misses (LIMIT 2, mismatched ORDER BY direction, an extra correlation,
//     a star projection) are all REJECTED -- the false-positive guards the design
//     calls the correctness heart of the rewrite.
//
//   - TestMergeJoinAsofExec -- an end-to-end interpreter run of the OpMergeJoin
//     ASOF mode (engine.ExecOp over glue's ExprCatalog), proving the engine op the
//     recognizer will lower to produces the correct nearest-preceding result. (The
//     automatic recognizer->merge-join LOWERING is gated on Track A's normalized
//     int64 sort-key wiring, so this drives the op directly rather than via the
//     rewrite -- see optimize_temporal.go.)

import (
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
	"github.com/couchbase/n1k1/glue"
)

// asofRecognizeCount plans+converts stmt with the recognizer ON and returns how
// many argmax subqueries it recognized (the AsofRecognized delta).
func asofRecognizeCount(t *testing.T, store *glue.Store, stmt string) int64 {
	t.Helper()

	s, err := glue.ParseStatement(stmt, "default", true)
	if err != nil {
		t.Fatalf("parse %q: %v", stmt, err)
	}
	p, err := store.PlanStatement(s, "default", nil, nil)
	if err != nil {
		t.Fatalf("plan %q: %v", stmt, err)
	}

	before := glue.AsofRecognized
	glue.EnableASOFRecognize = true
	defer func() { glue.EnableASOFRecognize = false }()

	if _, _, err := glue.ExecConv(p); err != nil {
		t.Fatalf("conv %q: %v", stmt, err)
	}
	return glue.AsofRecognized - before
}

func TestMergeJoinAsofRecognizer(t *testing.T) {
	store, err := glue.FileStore(gsiSuiteRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatal(err)
	}

	// The canonical nearest-preceding argmax subquery: `r.<key> <= e.<key>` +
	// `ORDER BY r.<key> DESC LIMIT 1` over a keyspace. MUST be recognized.
	canonical := `SELECT d.test_id,` +
		` (SELECT r.test_id FROM orders r` +
		`   WHERE r.cntn <= d.cntn` +
		`   ORDER BY r.cntn DESC LIMIT 1) AS y` +
		` FROM orders d`
	if got := asofRecognizeCount(t, store, canonical); got != 1 {
		t.Fatalf("canonical argmax: recognized %d, want 1", got)
	}

	// The canonical shape WITH a look-back guard -> soft ASOF. Still recognized.
	softCanonical := `SELECT d.test_id,` +
		` (SELECT r.test_id FROM orders r` +
		`   WHERE r.cntn <= d.cntn` +
		`     AND r.cntn >= d.cntn - 5` +
		`   ORDER BY r.cntn DESC LIMIT 1) AS y` +
		` FROM orders d`
	if got := asofRecognizeCount(t, store, softCanonical); got != 1 {
		t.Fatalf("soft argmax: recognized %d, want 1", got)
	}

	// The canonical shape WITH an equality partition predicate. Still recognized.
	partCanonical := `SELECT d.test_id,` +
		` (SELECT r.test_id FROM orders r` +
		`   WHERE r.cntn <= d.cntn` +
		`     AND r.test_id = d.test_id` +
		`   ORDER BY r.cntn DESC LIMIT 1) AS y` +
		` FROM orders d`
	if got := asofRecognizeCount(t, store, partCanonical); got != 1 {
		t.Fatalf("partitioned argmax: recognized %d, want 1", got)
	}

	// Near-misses: each deviates from the canonical shape in exactly one way and
	// MUST NOT be recognized (a false positive would silently change semantics).
	nearMisses := map[string]string{
		"LIMIT 2 is not an argmax": `SELECT d.test_id,` +
			` (SELECT r.test_id FROM orders r` +
			`   WHERE r.cntn <= d.cntn` +
			`   ORDER BY r.cntn DESC LIMIT 2) AS y` +
			` FROM orders d`,

		"ORDER BY ASC contradicts <= (not nearest-preceding)": `SELECT d.test_id,` +
			` (SELECT r.test_id FROM orders r` +
			`   WHERE r.cntn <= d.cntn` +
			`   ORDER BY r.cntn ASC LIMIT 1) AS y` +
			` FROM orders d`,

		"ORDER BY a different key than the inequality": `SELECT d.test_id,` +
			` (SELECT r.test_id FROM orders r` +
			`   WHERE r.cntn <= d.cntn` +
			`   ORDER BY r.test_id DESC LIMIT 1) AS y` +
			` FROM orders d`,

		"a second correlated inequality (not a single argmax)": `SELECT d.test_id,` +
			` (SELECT r.test_id FROM orders r` +
			`   WHERE r.cntn <= d.cntn` +
			`     AND r.test_id <= d.state` +
			`   ORDER BY r.cntn DESC LIMIT 1) AS y` +
			` FROM orders d`,

		"a star projection is not a scalar argmax": `SELECT d.test_id,` +
			` (SELECT r.* FROM orders r` +
			`   WHERE r.cntn <= d.cntn` +
			`   ORDER BY r.cntn DESC LIMIT 1) AS y` +
			` FROM orders d`,
	}
	for name, stmt := range nearMisses {
		if got := asofRecognizeCount(t, store, stmt); got != 0 {
			t.Fatalf("near-miss %q: recognized %d, want 0 (false positive!)", name, got)
		}
	}
}

// --- End-to-end OpMergeJoin ASOF execution through the glue engine stack. ---

// mjExecChild builds a key-ordered scan+project child yielding rows with slots
// [".", "ts", "v"] from an inline JSON-lines source (bare int64 key in "ts").
func mjExecChild(rows [][2]interface{}) *base.Op {
	var sb strings.Builder
	for _, r := range rows {
		sb.WriteString(`{"ts":`)
		sb.WriteString(strconv.FormatInt(int64(r[0].(int)), 10))
		sb.WriteString(`,"v":`)
		sb.WriteString(strconv.Quote(r[1].(string)))
		sb.WriteString("}\n")
	}
	return &base.Op{
		Kind:   "project",
		Labels: base.Labels{".", "ts", "v"},
		Params: []interface{}{
			[]interface{}{"labelPath", "."},
			[]interface{}{"labelPath", ".", "ts"},
			[]interface{}{"labelPath", ".", "v"},
		},
		Children: []*base.Op{{
			Kind:   "scan",
			Labels: base.Labels{"."},
			Params: []interface{}{"jsonsData", sb.String()},
		}},
	}
}

// TestMergeJoinAsofExec drives OpMergeJoin (ASOF nearest-preceding, left-outer)
// through glue's interpreter setup, asserting each left row is paired with the
// greatest-keyed right row <= its key (NULL when none precedes).
func TestMergeJoinAsofExec(t *testing.T) {
	left := mjExecChild([][2]interface{}{
		{5, "L5"},   // no preceding right -> NULL
		{15, "L15"}, // -> R10
		{25, "L25"}, // -> R20
		{40, "L40"}, // -> R30 (right exhausted)
	})
	right := mjExecChild([][2]interface{}{
		{10, "R10"},
		{20, "R20"},
		{30, "R30"},
	})

	op := &base.Op{
		Kind: "merge-join",
		Labels: base.Labels{
			".", "ts", "v", ".", "ts", "v",
		},
		// leftKeyIdx=1, rightKeyIdx=1, left-outer, asof nearest-preceding.
		Params:   []interface{}{1, 1, "left", "asof", int64(0), nil, nil},
		Children: []*base.Op{left, right},
	}

	vars := &base.Vars{
		Temps: []interface{}{nil},
		Ctx: &base.Ctx{
			ValComparer: base.NewValComparer(),
			ExprCatalog: engine.ExprCatalog,
		},
	}

	type pair struct {
		lv string
		rv string // "" == right NULL-extended
	}
	var got []pair
	engine.ExecOp(op, vars,
		func(vals base.Vals) {
			if len(vals) != 6 {
				t.Fatalf("row has %d slots, want 6: %v", len(vals), vals)
			}
			p := pair{lv: jsonUnq(string(vals[2]))}
			if len(vals[5]) != 0 {
				p.rv = jsonUnq(string(vals[5]))
			}
			got = append(got, p)
		},
		func(err error) {
			if err != nil {
				t.Fatalf("exec: %v", err)
			}
		}, "", "")

	want := []pair{
		{lv: "L5", rv: ""},
		{lv: "L15", rv: "R10"},
		{lv: "L25", rv: "R20"},
		{lv: "L40", rv: "R30"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows %+v, want %+v", len(got), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("row %d: got %+v want %+v (all=%+v)", i, got[i], want[i], got)
		}
	}
}

func jsonUnq(s string) string {
	if u, err := strconv.Unquote(s); err == nil {
		return u
	}
	return s
}
