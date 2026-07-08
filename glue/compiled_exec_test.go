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

	// A field-access projection is native (ExprTreesOptimize) -> compiles + runs as
	// a child; the parent assembles the {"name":...} row from the child's positional
	// vals via ConvertVals.
	if _, err := s.Run(`PREPARE pfield AS SELECT b.name FROM beers b WHERE b.i = 1`); err != nil {
		t.Fatal(err)
	}
	gotField := rowsOf(`EXECUTE pfield`)
	if len(gotField) != 1 || gotField[0] != `{"name":"beer-001"}` {
		t.Errorf(`EXECUTE pfield = %v, want [{"name":"beer-001"}]`, gotField)
	}
	if s.prepareds["pfield"].compiledBin == "" {
		t.Error("a native field-access query should compile to a standalone child binary")
	}

	// A multi-column projection: the child yields two positional vals per row; the
	// parent assembles {"i":..,"name":..} -- proving compiled EXECUTE goes beyond
	// single-value SELECT *.
	if _, err := s.Run(`PREPARE pmulti AS SELECT b.i, b.name FROM beers b`); err != nil {
		t.Fatal(err)
	}
	gotMulti := rowsOf(`EXECUTE pmulti`)
	wantMulti := []string{
		`{"i":0,"name":"beer-000"}`,
		`{"i":1,"name":"beer-001"}`,
		`{"i":2,"name":"beer-002"}`,
	}
	if len(gotMulti) != len(wantMulti) {
		t.Fatalf("EXECUTE pmulti rows = %v, want %v", gotMulti, wantMulti)
	}
	for i := range wantMulti {
		if gotMulti[i] != wantMulti[i] {
			t.Errorf("EXECUTE pmulti row %d = %s, want %s", i, gotMulti[i], wantMulti[i])
		}
	}
	if s.prepareds["pmulti"].compiledBin == "" {
		t.Error("a native multi-column query should compile to a standalone child binary")
	}

	// ORDER BY ... LIMIT compiles to a standalone child: its emitted body uses
	// heap.Push/heap.Pop, so the child's main.go must import container/heap. It
	// previously did not -> "undefined: heap" build failure -> silent fallback.
	// This proves compiledMainImports adds the emit.OptionalImports the body needs.
	// (rowsOf sorts, so we assert the SELECTED top-2 set, which also proves the
	// max-heap ORDER BY DESC + LIMIT ran correctly in the child.)
	if _, err := s.Run(`PREPARE porder AS SELECT b.i FROM beers b ORDER BY b.i DESC LIMIT 2`); err != nil {
		t.Fatal(err)
	}
	gotOrder := rowsOf(`EXECUTE porder`)
	wantOrder := []string{`{"i":1}`, `{"i":2}`}
	if len(gotOrder) != len(wantOrder) {
		t.Fatalf("EXECUTE porder rows = %v, want %v", gotOrder, wantOrder)
	}
	for i := range wantOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Errorf("EXECUTE porder row %d = %s, want %s", i, gotOrder[i], wantOrder[i])
		}
	}
	if s.prepareds["porder"].compiledBin == "" {
		t.Error("ORDER BY should compile to a standalone child binary (needs container/heap import)")
	}

	// A genuinely per-row BOXED expr (non-native REPEAT over a field) can't be
	// cbq-free -> must NOT compile standalone -> falls back to the interpreter,
	// still correct.
	if _, err := s.Run(`PREPARE pbox AS SELECT REPEAT(b.name, 2) AS r FROM beers b WHERE b.i = 1`); err != nil {
		t.Fatal(err)
	}
	gotBox := rowsOf(`EXECUTE pbox`)
	if len(gotBox) != 1 || gotBox[0] != `{"r":"beer-001beer-001"}` {
		t.Errorf(`EXECUTE pbox = %v, want [{"r":"beer-001beer-001"}]`, gotBox)
	}
	if s.prepareds["pbox"].compiledBin != "" {
		t.Error("a boxed (per-row cbq) query must NOT compile standalone; expected interpreter fallback")
	}
}
