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

// TestGroupInPlaceUpdate guards the OpGroup in-place accumulator update: after the
// first row of a group, a same-size (fixed-width) agg must overwrite the stored
// value in place rather than re-Set it, while a variable-width agg (string min/max
// whose length changes, incl. shrinking) must stay correct via the Set fallback.
// (runRows is defined in discard_elision_test.go.)
func TestGroupInPlaceUpdate(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "items")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	// Three groups with >1 row each so the found->in-place branch fires; string s
	// varies in length within a group so min/max cross length boundaries.
	docs := []struct {
		g, s string
		v    int
	}{
		{"a", "zebra", 1}, {"a", "yak", 2}, {"a", "x", 3}, // sum 6, min "x", max "zebra"
		{"b", "bb", 10}, {"b", "a", 20}, // sum 30, min "a", max "bb"
		{"c", "m", 5}, // sum 5, min/max "m"
	}
	for i, d := range docs {
		doc := fmt.Sprintf(`{"g":%q,"s":%q,"v":%d}`, d.g, d.s, d.v)
		if err := os.WriteFile(filepath.Join(ks, fmt.Sprintf("d%d.json", i)), []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// RAW scalars avoid JSON key-order fragility; runRows sorts the rows.
	cases := []struct {
		q    string
		want []string
	}{
		{`SELECT RAW count(*) FROM items`, []string{"6"}},
		{`SELECT RAW count(*) FROM items WHERE g="a"`, []string{"3"}},          // fixed-width in-place, 3 rows
		{`SELECT RAW sum(v) FROM items WHERE g="a"`, []string{"6"}},            // fixed-width in-place
		{`SELECT RAW min(s) FROM items WHERE g="a"`, []string{`"x"`}},          // shrinks 5->3->1: Set fallback
		{`SELECT RAW max(s) FROM items WHERE g="a"`, []string{`"zebra"`}},      // grows: Set fallback
		{`SELECT RAW count(*) FROM items GROUP BY g`, []string{"1", "2", "3"}}, // 3 independent groups
		{`SELECT RAW sum(v) FROM items GROUP BY g`, []string{"30", "5", "6"}},  // sorted-string order
		{`SELECT RAW min(s) FROM items GROUP BY g`, []string{`"a"`, `"m"`, `"x"`}},
		{`SELECT RAW max(s) FROM items GROUP BY g`, []string{`"bb"`, `"m"`, `"zebra"`}},
	}
	for _, c := range cases {
		got := runRows(t, sess, c.q)
		if len(got) != len(c.want) {
			t.Errorf("%q: got %v, want %v", c.q, got, c.want)
			continue
		}
		for i := range c.want {
			if got[i] != c.want[i] {
				t.Errorf("%q: got %v, want %v", c.q, got, c.want)
				break
			}
		}
	}
}
