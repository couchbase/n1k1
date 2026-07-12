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
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/value"
)

// selfTestSession writes a small two-keyspace fixture (orders + items) and opens
// a session over it. order id=2's iid has no matching item, so a LEFT JOIN
// leaves the right side MISSING -- exercising ValsSelfObject's omit-empty path.
func selfTestSession(t *testing.T) *Session {
	t.Helper()

	dir := t.TempDir()

	docs := map[string][]string{
		"orders": {
			`{"id":1,"cust":{"name":"a","city":"x"},"tags":[1,2],"iid":10}`,
			`{"id":2,"cust":{"name":"b"},"iid":99}`,
		},
		"items": {
			`{"id":10,"sku":"s10"}`,
		},
	}
	for ks, ds := range docs {
		d := filepath.Join(dir, "default", ks)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		for i, doc := range ds {
			f := filepath.Join(d, "d"+string(rune('0'+i))+".json")
			if err := os.WriteFile(f, []byte(doc), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	return sess
}

// canonRow order-normalizes a JSON result row (sorts object keys recursively, as
// encoding/json does for maps) so the native self path's label-order keys and
// the boxed path's sorted keys compare equal.
func canonRow(s string) string {
	var v interface{}
	if json.Unmarshal([]byte(s), &v) != nil {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func runRowsCanon(t *testing.T, sess *Session, stmt string) []string {
	t.Helper()
	res, err := sess.Run(stmt)
	if err != nil {
		t.Fatalf("Run(%q): %v", stmt, err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, canonRow(string(r)))
	}
	sort.Strings(out)
	return out
}

// TestSelfProjectNativeDifferential asserts that the native self byte path
// (engine.ExprSelf) yields the same results (value-wise) as the boxed exprTree
// fallback across a battery of star shapes -- both the bare whole-row SELECT *
// that goes native and the path/mixed stars that stay boxed.
func TestSelfProjectNativeDifferential(t *testing.T) {
	sess := selfTestSession(t)

	queries := []string{
		`SELECT * FROM orders o`,                                      // bare self -> native
		`SELECT * FROM orders o WHERE o.id = 1`,                       // native, filtered
		`SELECT * FROM orders o ORDER BY o.id`,                        // native, ordered
		`SELECT * FROM orders o JOIN items i ON o.iid = i.id`,         // native, 2-keyspace self
		`SELECT * FROM orders o LEFT JOIN items i ON o.iid = i.id`,    // native, MISSING right side
		`SELECT o.* FROM orders o`,                                    // path star -> stays boxed
		`SELECT o.*, i.id AS iid FROM orders o JOIN items i ON o.iid = i.id`, // mixed star -> boxed
		`SELECT o.cust.* FROM orders o`,                               // nested path star -> boxed
	}

	restore := SelfProjectNative
	defer func() { SelfProjectNative = restore }()

	for _, q := range queries {
		SelfProjectNative = true
		on := runRowsCanon(t, sess, q)
		SelfProjectNative = false
		off := runRowsCanon(t, sess, q)

		if len(on) != len(off) {
			t.Errorf("%q: %d rows native, %d boxed\n native=%v\n boxed=%v", q, len(on), len(off), on, off)
			continue
		}
		for i := range on {
			if on[i] != off[i] {
				t.Errorf("%q: native self changed results\n native=%v\n boxed=%v", q, on, off)
				break
			}
		}
	}
}

// TestConvertBytesParity asserts ConvertVals.ConvertBytes (the boxing-free row
// encoder) is value-equal to the boxed Convert + json.Marshal(v.Actual()) path
// across label shapes, and reports handled=false only for shapes it defers.
func TestConvertBytesParity(t *testing.T) {
	boxed := func(cv *ConvertVals, vals base.Vals) string {
		v, err := cv.Convert(vals)
		if err != nil {
			t.Fatalf("Convert: %v", err)
		}
		var b []byte
		if v != nil {
			b, _ = json.Marshal(v.Actual())
		} else {
			b = []byte("null")
		}
		return canonRow(string(b))
	}

	cases := []struct {
		name        string
		labels      base.Labels
		vals        base.Vals
		wantHandled bool
	}{
		{"fields", base.Labels{`.["a"]`, `.["b"]`}, base.Vals{base.Val(`1`), base.Val(`"x"`)}, true},
		{"field+attachment", base.Labels{`.["o"]`, "^id"}, base.Vals{base.Val(`{"id":1,"c":{"n":"a"}}`), base.Val(`"k1"`)}, true},
		{"missing-omitted", base.Labels{`.["a"]`, `.["b"]`}, base.Vals{base.ValMissing, base.Val(`"x"`)}, true},
		{"null-kept", base.Labels{`.["a"]`, `.["b"]`}, base.Vals{base.Val(`null`), base.Val(`2`)}, true},
		{"whole-row", base.Labels{"."}, base.Vals{base.Val(`{"z":9,"a":1}`)}, true},
		{"whole-raw-scalar", base.Labels{"."}, base.Vals{base.Val(`42`)}, true},
		{"star-object", base.Labels{".*"}, base.Vals{base.Val(`{"o":{"id":1}}`)}, true},
		{"star-nonobject", base.Labels{".*"}, base.Vals{base.Val(`5`)}, true},
		{"multipath-boxed", base.Labels{`.["a","b"]`}, base.Vals{base.Val(`7`)}, false},
	}

	for _, c := range cases {
		cv, err := NewConvertVals(c.labels)
		if err != nil {
			t.Fatalf("%s: NewConvertVals: %v", c.name, err)
		}

		got, handled := cv.ConvertBytes(c.vals, nil)
		if handled != c.wantHandled {
			t.Errorf("%s: handled = %v, want %v", c.name, handled, c.wantHandled)
			continue
		}
		if !handled {
			continue // boxed fallback: parity is by construction (caller boxes).
		}
		if g, w := canonRow(string(got)), boxed(cv, c.vals); g != w {
			t.Errorf("%s: ConvertBytes %s != boxed %s", c.name, g, w)
		}
	}
}

// TestConvertSkipsInternalAttachmentLabels guards the whole class of the star +
// no-operand-window panic: Convert must SKIP internal "^name" byte-lane columns
// (worderby/ekey/rkey/epart/rpart/cseN/resetScope) and box ONLY ^id, ^smeta, and
// ^aggregates|<fn>. Before the fix, attKey's catch-all folded any "^name" onto
// ATT_AGGREGATES, so a plain internal column stored a binary value there and a
// following "^aggregates|..." merge asserted map -> panic.
func TestConvertSkipsInternalAttachmentLabels(t *testing.T) {
	aggOf := func(v value.Value) map[string]value.Value {
		av, ok := v.(value.AnnotatedValue)
		if !ok {
			t.Fatalf("result is not AnnotatedValue: %T", v)
		}
		m, _ := av.GetAttachment(value.ATT_AGGREGATES).(map[string]value.Value)
		return m
	}

	// Each internal label sits BEFORE a real "^aggregates|foo" -- the exact ordering
	// that panicked. Convert must skip the internal column and still box foo.
	for _, internal := range []string{"^worderby", "^ekey", "^rkey", "^epart0", "^rpart1", "^cse0", "^resetScope"} {
		cv, err := NewConvertVals(base.Labels{`.["x"]`, internal, "^aggregates|foo"})
		if err != nil {
			t.Fatalf("%s: NewConvertVals: %v", internal, err)
		}
		v, err := cv.Convert(base.Vals{base.Val(`1`), base.Val(`42`), base.Val(`7`)})
		if err != nil {
			t.Fatalf("%s: Convert: %v", internal, err)
		}
		if xv, _ := v.Field("x"); xv == nil || xv.Actual() != 1.0 {
			t.Errorf("%s: x field = %v, want 1", internal, xv)
		}
		agg := aggOf(v)
		if agg["foo"] == nil || agg["foo"].Actual() != 7.0 {
			t.Errorf("%s: aggregates[foo] = %v, want 7", internal, agg["foo"])
		}
		// The internal column must not have leaked as a field.
		if _, ok := v.Field(internal[1:]); ok {
			t.Errorf("%s: internal label leaked as a field", internal)
		}
	}

	// Two window/aggregate columns merge into one ATT_AGGREGATES map (e.g. SELECT *,
	// ROW_NUMBER() OVER(w), RANK() OVER(w)).
	cv, _ := NewConvertVals(base.Labels{`.["x"]`, "^worderby", "^aggregates|rn", "^aggregates|rk"})
	v, err := cv.Convert(base.Vals{base.Val(`1`), base.Val(`9`), base.Val(`1`), base.Val(`2`)})
	if err != nil {
		t.Fatalf("two-aggregates: Convert: %v", err)
	}
	if agg := aggOf(v); agg["rn"] == nil || agg["rk"] == nil {
		t.Errorf("two-aggregates: want both rn and rk, got %v", agg)
	}
}

// TestSelfProjectNativeEliminatesBox confirms the native path removes the per-row
// box: a bare SELECT * boxes once per row when disabled and zero times when
// enabled, while a path star (SELECT o.*) stays boxed either way.
func TestSelfProjectNativeEliminatesBox(t *testing.T) {
	sess := selfTestSession(t)

	restore := SelfProjectNative
	defer func() { SelfProjectNative = restore }()

	boxedEvals := func(q string) int64 {
		res, err := sess.Run(q)
		if err != nil {
			t.Fatalf("Run(%q): %v", q, err)
		}
		return res.BoxedEvals
	}

	// Bare SELECT *: 2 rows -> 2 boxed evals when off, 0 when on.
	SelfProjectNative = false
	if got := boxedEvals(`SELECT * FROM orders o`); got != 2 {
		t.Errorf("SELECT * boxed-off BoxedEvals = %d, want 2", got)
	}
	SelfProjectNative = true
	if got := boxedEvals(`SELECT * FROM orders o`); got != 0 {
		t.Errorf("SELECT * native BoxedEvals = %d, want 0", got)
	}

	// Path star stays boxed regardless of the flag (native gate rejects it).
	if got := boxedEvals(`SELECT o.* FROM orders o`); got == 0 {
		t.Errorf("SELECT o.* native BoxedEvals = %d, want > 0 (stays boxed)", got)
	}
}
