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
	"time"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/value"

	"testing"
)

// TestSubqueryUncorrelatedMemoized guards the ISSUE-11 fix: an UNCORRELATED subquery
// (no outer references) is evaluated ONCE and memoized, not re-run per outer row. Before
// the fix, `x IN (SELECT ...)` re-executed the whole sub-plan for every outer row --
// O(n*m). Drives EvaluateSubquery directly (as cbq's In.Evaluate would, once per outer
// row) and asserts the sub-plan executed exactly once, returning a stable result.
func TestSubqueryUncorrelatedMemoized(t *testing.T) {
	sess := multiQueryTestSession(t) // logs: 4 rows; a/b are sev "ERROR" (code 5, 1)

	parsed, err := ParseStatement(`SELECT RAW l.code FROM logs l WHERE l.sev = "ERROR"`, sess.Namespace, true)
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := parsed.(*algebra.Select)
	if !ok {
		t.Fatalf("parsed statement is %T, want *algebra.Select", parsed)
	}
	if sel.IsCorrelated() {
		t.Fatal("test subquery must be uncorrelated (no outer refs)")
	}

	gctx := NewGlueContext(time.Now())
	gctx.InitSubqueries(sess.Store, sess.Namespace, nil, nil)

	// Probe as if scanning 5 outer rows: each passes a different parent, but the
	// uncorrelated result must be identical and computed only once.
	var first []interface{}
	for i := 0; i < 5; i++ {
		parent := value.NewValue(map[string]interface{}{"x": i})
		v, err := gctx.EvaluateSubquery(sel, parent)
		if err != nil {
			t.Fatalf("EvaluateSubquery #%d: %v", i, err)
		}
		arr, ok := v.Actual().([]interface{})
		if !ok {
			t.Fatalf("#%d result is %T, want array", i, v.Actual())
		}
		if first == nil {
			first = arr
		} else if len(arr) != len(first) {
			t.Fatalf("#%d result length %d != first %d (memo returned a different value)", i, len(arr), len(first))
		}
	}

	// The whole point of the fix: the sub-plan ran ONCE, not 5x.
	if got := gctx.subq.execs; got != 1 {
		t.Errorf("uncorrelated subquery executed %d times over 5 outer rows, want 1 (memoized)", got)
	}
	// Sanity: it found the two ERROR rows' codes.
	if len(first) != 2 {
		t.Errorf("subquery result = %v, want the 2 ERROR codes", first)
	}
}
