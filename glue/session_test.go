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

func writePlainBeers(t *testing.T, n int) string {
	t.Helper()
	root := t.TempDir()
	d := filepath.Join(root, "default", "beers")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		doc := fmt.Sprintf(`{"i":%d,"name":"beer-%03d"}`, i, i)
		if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("b%03d.json", i)), []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// Session.OnRow streams each row instead of accumulating: Result.Rows must be
// nil, Result.Count must still be right, and the streamed rows must exactly
// match what a normal (accumulating) run returns.
func TestSessionOnRowStreaming(t *testing.T) {
	root := writePlainBeers(t, 25)
	const query = "SELECT b.name FROM beers b ORDER BY b.name"

	// Baseline: normal accumulating run.
	s1, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	base, err := s1.Run(query)
	if err != nil {
		t.Fatal(err)
	}
	if len(base.Rows) != 25 || base.Count != 25 {
		t.Fatalf("baseline: rows=%d count=%d want 25", len(base.Rows), base.Count)
	}

	// Streaming run.
	s2, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	var streamed []string
	s2.OnRow = func(b []byte) { streamed = append(streamed, string(append([]byte(nil), b...))) }
	res, err := s2.Run(query)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != nil {
		t.Errorf("streaming run must not accumulate Rows; got %d", len(res.Rows))
	}
	if res.Count != 25 {
		t.Errorf("streaming Count = %d, want 25", res.Count)
	}
	if len(streamed) != len(base.Rows) {
		t.Fatalf("streamed %d rows, baseline %d", len(streamed), len(base.Rows))
	}
	for i := range base.Rows {
		if streamed[i] != string(base.Rows[i]) {
			t.Errorf("row %d: streamed %s != baseline %s", i, streamed[i], base.Rows[i])
		}
	}
}
