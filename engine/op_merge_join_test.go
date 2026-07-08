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

package engine

import (
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// mjRow is one input row: an int64 sort key, a string value payload, and an
// optional partition tag.
type mjRow struct {
	k int64
	v string
	p string
}

// mjChild builds a key-ordered "scan"+"project" child whose row slots are:
//
//	slot 0 = "."  (the whole doc)
//	slot 1 = "ts" (the bare int64 sort key the merge-join compares on)
//	slot 2 = "v"  (a string value payload)
//	slot 3 = "p"  (a partition tag)
//
// The rows are emitted in the order given (the merge-join requires each side to
// already be ascending on its key).
func mjChild(rows []mjRow) *base.Op {
	var sb strings.Builder
	for _, r := range rows {
		sb.WriteString(`{"ts":`)
		sb.WriteString(strconv.FormatInt(r.k, 10))
		sb.WriteString(`,"v":`)
		sb.WriteString(strconv.Quote(r.v))
		sb.WriteString(`,"p":`)
		sb.WriteString(strconv.Quote(r.p))
		sb.WriteString("}\n")
	}
	return &base.Op{
		Kind:   "project",
		Labels: base.Labels{".", "ts", "v", "p"},
		Params: []interface{}{
			[]interface{}{"labelPath", "."},
			[]interface{}{"labelPath", ".", "ts"},
			[]interface{}{"labelPath", ".", "v"},
			[]interface{}{"labelPath", ".", "p"},
		},
		Children: []*base.Op{{
			Kind:   "scan",
			Labels: base.Labels{"."},
			Params: []interface{}{"jsonsData", sb.String()},
		}},
	}
}

// mjOut is one emitted join row, decoded from the 8-slot output (left slots 0-3,
// right slots 4-7). rightNull reports whether the right side was NULL-extended.
type mjOut struct {
	lk        int64
	lv        string
	rk        int64
	rv        string
	rightNull bool
}

// runMergeJoin executes a merge-join op and decodes its output rows. The left
// child contributes 4 slots, so the right key is slot 5 ("ts") and the right
// value is slot 6 ("v"); a missing right key marks a NULL-extended row.
func runMergeJoin(t *testing.T, op *base.Op) ([]mjOut, error) {
	t.Helper()

	vars := &base.Vars{
		Temps: make([]interface{}, 16),
		Ctx: &base.Ctx{
			ExprCatalog: ExprCatalog,
			ValComparer: base.NewValComparer(),
		},
	}

	var got []mjOut
	var gotErr error
	ExecOp(op, vars,
		func(vals base.Vals) {
			if len(vals) != 8 {
				t.Fatalf("row has %d slots, want 8: %v", len(vals), vals)
			}
			o := mjOut{}
			lk, err := strconv.ParseInt(string(vals[1]), 10, 64)
			if err != nil {
				t.Fatalf("left key %q not int64: %v", string(vals[1]), err)
			}
			o.lk = lk
			o.lv = jsonUnquote(string(vals[2]))
			if len(vals[5]) == 0 { // right NULL-extended (ValMissing)
				o.rightNull = true
			} else {
				rk, err := strconv.ParseInt(string(vals[5]), 10, 64)
				if err != nil {
					t.Fatalf("right key %q not int64: %v", string(vals[5]), err)
				}
				o.rk = rk
				o.rv = jsonUnquote(string(vals[6]))
			}
			got = append(got, o)
		},
		func(err error) {
			if err != nil {
				gotErr = err
			}
		}, "", "")

	return got, gotErr
}

func jsonUnquote(s string) string {
	if u, err := strconv.Unquote(s); err == nil {
		return u
	}
	return s
}

// mergeJoinOp assembles a merge-join with left/right children and the given
// scalar params (joinType, asof, tolerance, partition indices). Both sides use
// slot 1 as their int64 key.
func mergeJoinOp(left, right *base.Op, joinType, asof string,
	tolerance int64, leftParts, rightParts []interface{}) *base.Op {
	return &base.Op{
		Kind: "merge-join",
		Labels: base.Labels{
			".", "ts", "v", "p", ".", "ts", "v", "p",
		},
		Params: []interface{}{
			1, 1, joinType, asof, tolerance, leftParts, rightParts,
		},
		Children: []*base.Op{left, right},
	}
}

func assertOut(t *testing.T, got []mjOut, want []mjOut) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d rows %+v, want %d rows %+v", len(got), got, len(want), want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("row %d: got %+v want %+v\nfull got=%+v", i, got[i], want[i], got)
		}
	}
}

// TestMergeJoinEquiInner covers the classic sort-merge equijoin, including a
// duplicate-key group on both sides (cross-product) and unmatched keys on both
// sides (dropped by the inner join).
func TestMergeJoinEquiInner(t *testing.T) {
	left := mjChild([]mjRow{
		{k: 10, v: "L10"},
		{k: 20, v: "L20a"},
		{k: 20, v: "L20b"},
		{k: 30, v: "L30"}, // no right match -> dropped
	})
	right := mjChild([]mjRow{
		{k: 5, v: "R5"}, // no left match -> skipped
		{k: 20, v: "R20a"},
		{k: 20, v: "R20b"},
		{k: 40, v: "R40"},
	})

	got, err := runMergeJoin(t, mergeJoinOp(left, right, "inner", "off", 0, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	want := []mjOut{
		{lk: 20, lv: "L20a", rk: 20, rv: "R20a"},
		{lk: 20, lv: "L20a", rk: 20, rv: "R20b"},
		{lk: 20, lv: "L20b", rk: 20, rv: "R20a"},
		{lk: 20, lv: "L20b", rk: 20, rv: "R20b"},
	}
	assertOut(t, got, want)
}

// TestMergeJoinEquiLeftOuter covers the left-outer equijoin: unmatched left rows
// are NULL-extended rather than dropped.
func TestMergeJoinEquiLeftOuter(t *testing.T) {
	left := mjChild([]mjRow{
		{k: 10, v: "L10"}, // no match
		{k: 20, v: "L20"},
		{k: 30, v: "L30"}, // no match
	})
	right := mjChild([]mjRow{
		{k: 20, v: "R20"},
	})

	got, err := runMergeJoin(t, mergeJoinOp(left, right, "left", "off", 0, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	want := []mjOut{
		{lk: 10, lv: "L10", rightNull: true},
		{lk: 20, lv: "L20", rk: 20, rv: "R20"},
		{lk: 30, lv: "L30", rightNull: true},
	}
	assertOut(t, got, want)
}

// TestMergeJoinAsofNearestPreceding covers plain ASOF: each left row matches the
// right row with the greatest key <= the left key. The first left row (key 5) has
// NO preceding right row: inner drops it, left-outer NULL-extends it.
func TestMergeJoinAsofNearestPreceding(t *testing.T) {
	left := mjChild([]mjRow{
		{k: 5, v: "L5"},   // no preceding right
		{k: 15, v: "L15"}, // preceding = R10
		{k: 25, v: "L25"}, // preceding = R20
		{k: 30, v: "L30"}, // preceding = R30 (equal key counts as preceding)
		{k: 55, v: "L55"}, // preceding = R30 still (right exhausted)
	})
	right := mjChild([]mjRow{
		{k: 10, v: "R10"},
		{k: 20, v: "R20"},
		{k: 30, v: "R30"},
	})

	// Inner: the no-preceding left row (5) is dropped.
	got, err := runMergeJoin(t, mergeJoinOp(left, right, "inner", "asof", 0, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	assertOut(t, got, []mjOut{
		{lk: 15, lv: "L15", rk: 10, rv: "R10"},
		{lk: 25, lv: "L25", rk: 20, rv: "R20"},
		{lk: 30, lv: "L30", rk: 30, rv: "R30"},
		{lk: 55, lv: "L55", rk: 30, rv: "R30"},
	})

	// Left-outer: the no-preceding left row (5) is NULL-extended.
	left2 := mjChild([]mjRow{
		{k: 5, v: "L5"},
		{k: 15, v: "L15"},
		{k: 25, v: "L25"},
		{k: 30, v: "L30"},
		{k: 55, v: "L55"},
	})
	right2 := mjChild([]mjRow{
		{k: 10, v: "R10"},
		{k: 20, v: "R20"},
		{k: 30, v: "R30"},
	})
	got, err = runMergeJoin(t, mergeJoinOp(left2, right2, "left", "asof", 0, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	assertOut(t, got, []mjOut{
		{lk: 5, lv: "L5", rightNull: true},
		{lk: 15, lv: "L15", rk: 10, rv: "R10"},
		{lk: 25, lv: "L25", rk: 20, rv: "R20"},
		{lk: 30, lv: "L30", rk: 30, rv: "R30"},
		{lk: 55, lv: "L55", rk: 30, rv: "R30"},
	})
}

// TestMergeJoinSoftAsof covers soft ASOF: the nearest-preceding match is honored
// only when left.key - held.key <= tolerance; otherwise it expires. Tolerance 10:
//   - L15 -> R10 (gap 5, ok)
//   - L25 -> R20 (gap 5, ok)
//   - L45 -> R30 held, gap 15 > 10 -> expired -> inner drops / left NULL-extends
func TestMergeJoinSoftAsof(t *testing.T) {
	mkLeft := func() *base.Op {
		return mjChild([]mjRow{
			{k: 15, v: "L15"},
			{k: 25, v: "L25"},
			{k: 45, v: "L45"},
		})
	}
	mkRight := func() *base.Op {
		return mjChild([]mjRow{
			{k: 10, v: "R10"},
			{k: 20, v: "R20"},
			{k: 30, v: "R30"},
		})
	}

	// Inner soft ASOF: the expired L45 is dropped.
	got, err := runMergeJoin(t, mergeJoinOp(mkLeft(), mkRight(), "inner", "soft", 10, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	assertOut(t, got, []mjOut{
		{lk: 15, lv: "L15", rk: 10, rv: "R10"},
		{lk: 25, lv: "L25", rk: 20, rv: "R20"},
	})

	// Left-outer soft ASOF: the expired L45 is NULL-extended.
	got, err = runMergeJoin(t, mergeJoinOp(mkLeft(), mkRight(), "left", "soft", 10, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	assertOut(t, got, []mjOut{
		{lk: 15, lv: "L15", rk: 10, rv: "R10"},
		{lk: 25, lv: "L25", rk: 20, rv: "R20"},
		{lk: 45, lv: "L45", rightNull: true},
	})
}

// TestMergeJoinAsofPartitioned covers partitioned ASOF: one held right row per
// partition, so a left row matches the nearest-preceding right row within ITS
// partition. Partitions "a" and "b" arrive interleaved in key order.
func TestMergeJoinAsofPartitioned(t *testing.T) {
	left := mjChild([]mjRow{
		{k: 15, v: "La15", p: "a"}, // preceding a = Ra10
		{k: 16, v: "Lb16", p: "b"}, // preceding b = Rb12
		{k: 25, v: "La25", p: "a"}, // preceding a = Ra20
		{k: 26, v: "Lb26", p: "b"}, // preceding b = Rb12 still
		{k: 30, v: "Lc30", p: "c"}, // partition c has no right -> NULL
	})
	right := mjChild([]mjRow{
		{k: 10, v: "Ra10", p: "a"},
		{k: 12, v: "Rb12", p: "b"},
		{k: 20, v: "Ra20", p: "a"},
	})

	parts := []interface{}{3} // slot 3 = "p" on both sides
	got, err := runMergeJoin(t, mergeJoinOp(left, right, "left", "asof", 0, parts, parts))
	if err != nil {
		t.Fatal(err)
	}
	assertOut(t, got, []mjOut{
		{lk: 15, lv: "La15", rk: 10, rv: "Ra10"},
		{lk: 16, lv: "Lb16", rk: 12, rv: "Rb12"},
		{lk: 25, lv: "La25", rk: 20, rv: "Ra20"},
		{lk: 26, lv: "Lb26", rk: 12, rv: "Rb12"},
		{lk: 30, lv: "Lc30", rightNull: true},
	})
}

// TestMergeJoinEmptyRight checks that an empty build side yields nothing for inner
// and NULL-extends every left row for left-outer, in both equi and ASOF modes.
func TestMergeJoinEmptyRight(t *testing.T) {
	mkLeft := func() *base.Op {
		return mjChild([]mjRow{{k: 10, v: "L10"}, {k: 20, v: "L20"}})
	}

	for _, mode := range []string{"off", "asof"} {
		got, err := runMergeJoin(t, mergeJoinOp(mkLeft(), mjChild(nil), "inner", mode, 0, nil, nil))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("inner %s over empty right: got %+v, want none", mode, got)
		}

		got, err = runMergeJoin(t, mergeJoinOp(mkLeft(), mjChild(nil), "left", mode, 0, nil, nil))
		if err != nil {
			t.Fatal(err)
		}
		assertOut(t, got, []mjOut{
			{lk: 10, lv: "L10", rightNull: true},
			{lk: 20, lv: "L20", rightNull: true},
		})
	}
}

// TestMergeJoinRightOutOfOrder verifies the monotonicity tripwire fires when the
// build side is not actually ascending on its key.
func TestMergeJoinRightOutOfOrder(t *testing.T) {
	left := mjChild([]mjRow{{k: 10, v: "L10"}})
	right := mjChild([]mjRow{{k: 30, v: "R30"}, {k: 20, v: "R20"}}) // descends
	_, err := runMergeJoin(t, mergeJoinOp(left, right, "inner", "asof", 0, nil, nil))
	if err == nil {
		t.Fatal("expected an out-of-order tripwire error, got nil")
	}
	if !strings.Contains(err.Error(), "out-of-order") {
		t.Fatalf("unexpected error: %v", err)
	}
}
