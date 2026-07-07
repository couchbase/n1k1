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
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// TestPrepareNativeEmitsParseableGo asserts that a fully-native, datastore-free
// query is Preparable and that Session.Prepare emits non-empty Go source which
// the go/parser accepts (so the emit path produces syntactically valid Go, not
// just some string). See DESIGN-extensions-prepare.md.
func TestPrepareNativeEmitsParseableGo(t *testing.T) {
	root := writePlainBeers(t, 3)
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}

	// Constant / arithmetic no-FROM SELECTs: every expression is native, no
	// datastore leaf, so the compiler can emit fully self-contained Go.
	native := []string{
		`SELECT 1+2 AS x`,
		`SELECT (10 * 3) - 4 AS y`,
		`SELECT abs(-5) AS a, 1 < 2 AS b`,
	}
	for _, q := range native {
		src, level, reason, err := s.Prepare(q)
		if err != nil {
			t.Fatalf("Prepare(%q) err: %v", q, err)
		}
		if level != PrepareCompiledFull {
			t.Fatalf("Prepare(%q) level %d, reason: %q (want PrepareCompiledFull)", q, level, reason)
		}
		if strings.TrimSpace(src) == "" {
			t.Fatalf("Prepare(%q) emitted empty source", q)
		}
		// It must be parseable Go.
		if _, perr := parser.ParseFile(token.NewFileSet(), "gen.go", src, parser.AllErrors); perr != nil {
			t.Fatalf("Prepare(%q) emitted unparseable Go: %v\n---\n%s", q, perr, src)
		}
		// Sanity: the emitted file exposes the documented Run entry point and the
		// projection labels.
		if !strings.Contains(src, "func "+PrepareFuncName+"(") {
			t.Errorf("Prepare(%q): emitted source lacks a %s func", q, PrepareFuncName)
		}
		if !strings.Contains(src, "PrepareLabels") {
			t.Errorf("Prepare(%q): emitted source lacks PrepareLabels", q)
		}
	}
}

// TestPrepareBoxedFallsBackToInterp asserts that a query with a boxed expression
// (one using a non-native scalar fn, or LIKE) is NOT Preparable -- reported with
// a sensible reason and WITHOUT emitting -- yet still runs correctly through the
// interpreter (Session.Run). This is the prepare fallback contract: a query that
// needs cbq is executed interpreter-only, never failing.
func TestPrepareBoxedFallsBackToInterp(t *testing.T) {
	root := writePlainBeers(t, 3)
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		stmt   string
		wantIn string // substring the reason should contain
		want   string // the interpreter's expected first row
	}{
		// REPEAT is a non-native scalar fn (delegated to cbq -- see DESIGN-exprs.md).
		{`SELECT REPEAT("x", 3) AS r`, "boxed expression", `{"r":"xxx"}`},
		// LIKE is delegated too.
		{`SELECT ("beer-1" LIKE "beer%") AS m`, "boxed expression", `{"m":true}`},
	}
	for _, c := range cases {
		src, level, reason, err := s.Prepare(c.stmt)
		if err != nil {
			t.Fatalf("Prepare(%q) err: %v", c.stmt, err)
		}
		if level != PrepareInterpreted {
			t.Fatalf("Prepare(%q) level %d (want PrepareInterpreted -- boxed needs cbq)", c.stmt, level)
		}
		if src != "" {
			t.Errorf("Prepare(%q) emitted source despite not being standalone", c.stmt)
		}
		if !strings.Contains(reason, c.wantIn) {
			t.Errorf("Prepare(%q) reason %q, want substring %q", c.stmt, reason, c.wantIn)
		}

		// The fallback: the interpreter still runs the query and returns results.
		res, err := s.Run(c.stmt)
		if err != nil {
			t.Fatalf("Run(%q) err: %v", c.stmt, err)
		}
		if len(res.Rows) != 1 || string(res.Rows[0]) != c.want {
			t.Errorf("Run(%q) rows = %v, want [%s]", c.stmt, res.Rows, c.want)
		}
		if res.BoxedEvals == 0 {
			t.Errorf("Run(%q): expected a boxed eval (BoxedEvals>0), got 0", c.stmt)
		}
	}
}

// TestPreparableNilAndReasons exercises Preparable's reason strings on the two
// gate conditions directly (nil tree, and a non-bakeable datastore op via a
// FROM-scan pushdown), so the gate's human-readable reasons are covered without
// depending on the CLI.
func TestPreparableReasons(t *testing.T) {
	if level, reason := Preparable(nil); level != PrepareInterpreted || !strings.Contains(reason, "nil op") {
		t.Errorf("Preparable(nil) = (%d, %q), want (PrepareInterpreted, ...nil op...)", level, reason)
	}

	root := writePlainBeers(t, 3)
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	// A FROM query bottoms out in a datastore-scan-records op whose params carry a
	// non-bakeable pushdown ([]string project-columns) and a live-plan Temps index
	// -- not compilable this milestone. Prepare must report that and (via the CLI)
	// fall back; here we assert the gate verdict + reason directly.
	_, level, reason, err := s.Prepare(`SELECT b.i FROM beers b`)
	if err != nil {
		t.Fatalf("Prepare err: %v", err)
	}
	// Native exprs (b.i is a field access) but a non-bakeable scan -> the compiled
	// level that needs a runtime data provider, NOT the boxed/interpreter level.
	if level != PrepareCompiledData {
		t.Fatalf("Prepare(FROM scan) level %d, want PrepareCompiledData", level)
	}
	if !strings.Contains(reason, "datastore op not bakeable") {
		t.Errorf("reason = %q, want ...datastore op not bakeable...", reason)
	}
}
