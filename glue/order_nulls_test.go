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

// Result-oracle for ORDER BY ... NULLS FIRST/LAST. conv previously returned NA
// (unsupported) for any explicit nulls position; the order-offset-limit op now places
// null/missing keys per the request (base.IsNullOrMissing + a per-term nulls-position),
// independent of asc/desc. Natural ORDER BY (no NULLS clause) is unchanged -- it still
// sorts by N1QL collation (missing < null < ...), which the secondary key here relies on.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestOrderNulls: a `v` field present for 3 rows, JSON null for one (k=3), and MISSING
// for one (k=4). NULLS FIRST/LAST positions the null-ish rows (k=3,4); the `, k` tiebreak
// keeps them deterministic among themselves (they compare equal on v).
func TestOrderNulls(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "t")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	rows := `{"k":1,"v":30}` + "\n" + `{"k":2,"v":10}` + "\n" + `{"k":3,"v":null}` + "\n" +
		`{"k":4}` + "\n" + `{"k":5,"v":20}` + "\n"
	if err := os.WriteFile(filepath.Join(ks, "t.jsonl"), []byte(rows), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	col := func(stmt string) []float64 {
		res, err := sess.Run(stmt)
		if err != nil {
			t.Fatalf("Run(%q): %v", stmt, err)
		}
		out := make([]float64, 0, len(res.Rows))
		for _, r := range res.Rows {
			var m map[string]float64
			if err := json.Unmarshal(r, &m); err != nil {
				t.Fatalf("decode %q: %v", r, err)
			}
			out = append(out, m["k"])
		}
		return out
	}

	for _, c := range []struct {
		name, stmt string
		want       []float64
	}{
		// non-null v ascending 10,20,30 -> k 2,5,1; null-ish (k=3,4) placed per clause.
		{"asc-nulls-last", `SELECT k FROM t ORDER BY v ASC NULLS LAST, k`, []float64{2, 5, 1, 3, 4}},
		{"asc-nulls-first", `SELECT k FROM t ORDER BY v ASC NULLS FIRST, k`, []float64{3, 4, 2, 5, 1}},
		{"desc-nulls-first", `SELECT k FROM t ORDER BY v DESC NULLS FIRST, k`, []float64{3, 4, 1, 5, 2}},
		{"desc-nulls-last", `SELECT k FROM t ORDER BY v DESC NULLS LAST, k`, []float64{1, 5, 2, 3, 4}},
		// Natural (no NULLS clause) is unchanged: ASC puts null-ish first, DESC last.
		{"asc-natural", `SELECT k FROM t ORDER BY v ASC, k`, []float64{4, 3, 2, 5, 1}},   // missing(4) < null(3) by collation
		{"desc-natural", `SELECT k FROM t ORDER BY v DESC, k`, []float64{1, 5, 2, 3, 4}}, // reversed: null(3) > missing(4)
	} {
		if got := col(c.stmt); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v\n  %s", c.name, got, c.want, c.stmt)
		}
	}
}
