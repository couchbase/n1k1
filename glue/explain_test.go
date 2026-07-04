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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestExplainConvPlan: an EXPLAIN statement populates Result.Plan (n1k1's converted
// op tree) alongside the cbq plan row, and FormatConvPlan tags each expression's
// eval lane -- native for byte-path exprs, boxed for the SELECT * self-projection
// and a non-optimizable scalar function.
func TestExplainConvPlan(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "orders")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "o1.json"), []byte(`{"custId":"c1","orderId":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	cases := []struct {
		q       string
		wantSub []string // substrings the rendered n1k1 plan must contain
	}{
		{
			// arithmetic + field refs stay native; the whole query converts.
			`EXPLAIN SELECT o.custId, o.orderId + 1 AS n FROM orders o WHERE o.orderId > 0`,
			[]string{"project", "filter", "[native]"},
		},
		{
			// SELECT * is a self-projection n1k1 can't reduce -> boxed.
			`EXPLAIN SELECT * FROM orders o`,
			[]string{"self [boxed]"},
		},
		{
			// A non-optimizable scalar fn boxes; the sibling arithmetic stays native.
			`EXPLAIN SELECT SUBSTR(o.custId,0,1) AS s, o.orderId*2 AS d FROM orders o`,
			[]string{"[boxed]", "[native]"},
		},
	}
	for _, c := range cases {
		res, err := sess.Run(c.q)
		if err != nil {
			t.Fatalf("Run(%q): %v", c.q, err)
		}
		if res.Plan == nil {
			t.Errorf("%q: Result.Plan is nil (conv tree not populated for EXPLAIN)", c.q)
			continue
		}
		got := FormatConvPlan(res.Plan)
		for _, sub := range c.wantSub {
			if !strings.Contains(got, sub) {
				t.Errorf("%q: plan missing %q\n%s", c.q, sub, got)
			}
		}
		// EXPLAIN must still return the cbq plan row (compatibility unchanged).
		if len(res.Rows) != 1 || !strings.Contains(string(res.Rows[0]), `"#operator"`) {
			t.Errorf("%q: expected the cbq plan JSON row, got %v", c.q, res.Rows)
		}
	}
}
