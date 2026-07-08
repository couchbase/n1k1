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
	"path/filepath"
	"sort"
	"testing"
)

// TestExecuteCompiledFull exercises compiled EXECUTE at the -prepare=full ceiling:
// a standalone-compilable (SELECT *) prepared statement is emitted as cbq-free Go,
// go-built into a child, and run over the scanned records piped in; a boxed query
// (field-access projection) falls back to the interpreter. Both return correct
// rows.
func TestExecuteCompiledFull(t *testing.T) {
	if !GoToolchainDetect().Available {
		t.Skip("no go toolchain: compiled EXECUTE degrades to the interpreter")
	}
	// Building the child needs the n1k1 source; point N1K1_SRC at this checkout.
	repo, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("N1K1_SRC", repo)

	root := writePlainBeers(t, 3) // docs: {"i":0,"name":"beer-000"} .. i=2
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.PrepareLevel = PrepareCompiledFull

	rowsOf := func(stmt string) []string {
		t.Helper()
		res, err := s.Run(stmt)
		if err != nil {
			t.Fatalf("Run(%q): %v", stmt, err)
		}
		got := make([]string, len(res.Rows))
		for i, r := range res.Rows {
			got[i] = string(r)
		}
		sort.Strings(got)
		return got
	}

	// SELECT * is fully native -> compiles + runs as a child.
	if _, err := s.Run(`PREPARE pstar AS SELECT * FROM beers b`); err != nil {
		t.Fatal(err)
	}
	got := rowsOf(`EXECUTE pstar`)
	want := []string{
		`{"b":{"i":0,"name":"beer-000"}}`,
		`{"b":{"i":1,"name":"beer-001"}}`,
		`{"b":{"i":2,"name":"beer-002"}}`,
	}
	if len(got) != len(want) {
		t.Fatalf("EXECUTE pstar rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("EXECUTE pstar row %d = %s, want %s", i, got[i], want[i])
		}
	}
	if s.prepareds["pstar"].compiledBin == "" {
		t.Error("SELECT * should have compiled to a standalone child binary")
	}

	// A field-access projection still boxes (glue.ExprStr) -> not standalone-
	// compilable -> falls back to the interpreter, still correct.
	if _, err := s.Run(`PREPARE pbox AS SELECT b.name FROM beers b WHERE b.i = 1`); err != nil {
		t.Fatal(err)
	}
	gotBox := rowsOf(`EXECUTE pbox`)
	if len(gotBox) != 1 || gotBox[0] != `{"name":"beer-001"}` {
		t.Errorf(`EXECUTE pbox = %v, want [{"name":"beer-001"}]`, gotBox)
	}
	if s.prepareds["pbox"].compiledBin != "" {
		t.Error("a boxed (field-access) query must NOT compile standalone; expected interpreter fallback")
	}
}
