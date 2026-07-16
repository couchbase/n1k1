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

	// A genuinely per-row BOXED expr (non-native MB_LENGTH over a field) can't be
	// cbq-free -> must NOT compile standalone -> falls back to the interpreter,
	// still correct. (MB_LENGTH("beer-001") == 8.)
	if _, err := s.Run(`PREPARE pbox AS SELECT MB_LENGTH(b.name) AS r FROM beers b WHERE b.i = 1`); err != nil {
		t.Fatal(err)
	}
	gotBox := rowsOf(`EXECUTE pbox`)
	if len(gotBox) != 1 || gotBox[0] != `{"r":8}` {
		t.Errorf(`EXECUTE pbox = %v, want [{"r":8}]`, gotBox)
	}
	if s.prepareds["pbox"].compiledBin != "" {
		t.Error("a boxed (per-row cbq) query must NOT compile standalone; expected interpreter fallback")
	}

	// A NATIVE string op (REPEAT here) re-encodes via base.StrEncode, which needs
	// Ctx.ValComparer -- so it exercises that the standalone-compiled binary's Ctx
	// initializes a ValComparer (regression guard: it previously did not, and any
	// native string/object op crashed standalone with a nil ValComparer).
	if _, err := s.Run(`PREPARE pnative AS SELECT REPEAT(b.name, 2) AS r FROM beers b WHERE b.i = 1`); err != nil {
		t.Fatal(err)
	}
	gotNative := rowsOf(`EXECUTE pnative`)
	if len(gotNative) != 1 || gotNative[0] != `{"r":"beer-001beer-001"}` {
		t.Errorf(`EXECUTE pnative = %v, want [{"r":"beer-001beer-001"}]`, gotNative)
	}
	if s.prepareds["pnative"].compiledBin == "" {
		t.Error("a native string query (REPEAT) should compile to a standalone binary")
	}
}

// TestExecuteCompiledAggAndArith guards two compiled-standalone regressions that
// the intermed differential (make test-compiler) could not catch, because that
// differential runs the emitted BODY under the test harness's own full Ctx, never
// under compiledMain's hand-rolled child Ctx:
//
//   - Aggregates (COUNT/SUM/GROUP BY) nil-panicked in the child: compiledMain built
//     a Ctx without the spill-backed AllocMap/AllocHeap/... pools GROUP BY needs.
//     Fixed by sharing rt.NewSpillCtx between MakeVars and the child. (Same class as
//     the earlier nil-ValComparer crash guarded in TestExecuteCompiledFull.)
//   - A binary op over TWO nested-field operands (`b.x * b.y`) failed to BUILD with
//     "lzValOut redeclared": the ExprLabelPath emit declared a bare (non-varLift'd)
//     lzValOut, so two path-accesses inlined into one block collided. `b.x * 2`
//     (one field + a literal) did not trip it, which is why it slipped through.
//
// Each shape must (a) compile to a standalone child binary and (b) return rows
// identical to the interpreter.
func TestExecuteCompiledAggAndArith(t *testing.T) {
	if !GoToolchainDetect().Available {
		t.Skip("no go toolchain: compiled EXECUTE degrades to the interpreter")
	}
	repo, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("N1K1_SRC", repo)

	root := t.TempDir()
	d := filepath.Join(root, "default", "bkt")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		doc := fmt.Sprintf(`{"i":%d,"g":%d,"amount":%d,"qty":2}`, i, i%2, i*10)
		if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("b%03d.json", i)), []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rowsSorted := func(res *Result) []string {
		out := make([]string, len(res.Rows))
		for i, r := range res.Rows {
			out[i] = string(r)
		}
		sort.Strings(out)
		return out
	}

	queries := []string{
		`SELECT COUNT(*) c FROM bkt b`,                                        // aggregate: AllocMap
		`SELECT b.g AS g, COUNT(*) c, SUM(b.amount) s FROM bkt b GROUP BY b.g`, // GROUP BY: AllocMap
		`SELECT b.amount * b.qty AS p FROM bkt b WHERE b.amount * b.qty > 40`,  // two-field arith: lzValOut
	}
	for i, q := range queries {
		s.PrepareLevel = PrepareInterpreted
		interp, err := s.Run(q)
		if err != nil {
			t.Fatalf("interpreted %q: %v", q, err)
		}

		name := fmt.Sprintf("pc%d", i)
		s.PrepareLevel = PrepareCompiledFull
		if _, err := s.Run(`PREPARE ` + name + ` AS ` + q); err != nil {
			t.Fatalf("prepare %q: %v", q, err)
		}
		comp, err := s.Run(`EXECUTE ` + name)
		if err != nil {
			t.Fatalf("compiled EXECUTE %q: %v", q, err)
		}
		if s.prepareds[name].compiledBin == "" {
			t.Errorf("query did not compile to a standalone child (fell back to interpreter): %q", q)
		}
		if iw, cw := rowsSorted(interp), rowsSorted(comp); fmt.Sprint(iw) != fmt.Sprint(cw) {
			t.Errorf("compiled != interpreted for %q:\n  interp=%v\n  compiled=%v", q, iw, cw)
		}
	}
}
