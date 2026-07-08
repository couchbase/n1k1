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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// prepareTestCLI opens a tiny file datastore and returns a cli wired to it, with
// stdout/stderr captured into the returned buffers.
func prepareTestCLI(t *testing.T, out, errb *strings.Builder) *cli {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "d.jsonl"), []byte(`{"a":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	return &cli{sess: sess, mode: "jsonlines", out: out, stderr: errb}
}

// TestDotPrepareBoxedFallsBack: `.prepare <boxed-stmt>` prints the not-compilable
// reason (to stderr) and still runs the statement through the interpreter (its
// result reaches stdout). This is the key fallback: a query needing cbq is
// executed interpreter-only, never failing.
func TestDotPrepareBoxedFallsBack(t *testing.T) {
	var out, errb strings.Builder
	c := prepareTestCLI(t, &out, &errb)

	// REPEAT is a non-native scalar fn (boxed -> needs cbq), so it can't be
	// compiled; the CLI must say so and fall back to interpreting it.
	c.dot(`.prepare SELECT REPEAT("z", 4) AS r`)

	if !strings.Contains(errb.String(), "not compilable") {
		t.Errorf("stderr should note not-compilable; got %q", errb.String())
	}
	if !strings.Contains(errb.String(), "boxed expression") {
		t.Errorf("stderr should give the boxed-expression reason; got %q", errb.String())
	}
	if !strings.Contains(out.String(), `"r":"zzzz"`) {
		t.Errorf("interpreter result must reach stdout; got %q", out.String())
	}
	// It fell back, so it did NOT emit Go source.
	if strings.Contains(out.String(), "func "+glue.PrepareFuncName+"(") {
		t.Errorf("boxed query should not emit Go; stdout=%q", out.String())
	}
}

// TestDotPrepareNativeEmits: `.prepare <native-stmt>` prints generated Go (to
// stdout) AND still runs the statement so the user gets results too.
func TestDotPrepareNativeEmits(t *testing.T) {
	var out, errb strings.Builder
	c := prepareTestCLI(t, &out, &errb)

	c.dot(`.prepare SELECT 6*7 AS x`)

	if !strings.Contains(out.String(), "func "+glue.PrepareFuncName+"(") {
		t.Errorf("native query should emit Go with a %s func; stdout=%q", glue.PrepareFuncName, out.String())
	}
	if !strings.Contains(out.String(), "package n1k1gen") {
		t.Errorf("emitted Go should be a package; stdout=%q", out.String())
	}
	// And the interpreter result is still produced.
	if !strings.Contains(out.String(), `"x":42`) {
		t.Errorf("interpreter result must also reach stdout; got %q", out.String())
	}
}

// TestDotPrepareLevel: `.prepare <level>` sets the compile-ceiling. At the full
// ceiling, PREPARE of a native statement emits its generated Go (and confirms);
// at the interpreted ceiling it just caches. EXECUTE runs the prepared statement
// either way. (PREPARE/EXECUTE are ordinary SQL statements, not a per-query mode.)
func TestDotPrepareLevel(t *testing.T) {
	var out, errb strings.Builder
	c := prepareTestCLI(t, &out, &errb)

	// Setting the ceiling.
	c.dot(".prepare full")
	if c.prepareLevel != glue.PrepareCompiledFull {
		t.Fatalf(".prepare full should set the ceiling to full, got %v", c.prepareLevel)
	}

	// PREPARE at the full ceiling confirms and emits Go for a native statement.
	out.Reset()
	errb.Reset()
	c.exec(`PREPARE p AS SELECT 2+3 AS s`)
	if !strings.Contains(errb.String(), "prepared") {
		t.Errorf("PREPARE should confirm 'prepared'; stderr=%q", errb.String())
	}
	if !strings.Contains(out.String(), "func "+glue.PrepareFuncName+"(") {
		t.Errorf("PREPARE at full should emit Go; stdout=%q", out.String())
	}

	// EXECUTE runs it through the interpreter, producing the result.
	out.Reset()
	errb.Reset()
	c.exec(`EXECUTE p`)
	if !strings.Contains(out.String(), `"s":5`) {
		t.Errorf("EXECUTE p should produce {s:5}; stdout=%q", out.String())
	}

	// At the interpreted ceiling, PREPARE just caches -- no Go emitted.
	c.dot(".prepare interpreted")
	if c.prepareLevel != glue.PrepareInterpreted {
		t.Fatalf(".prepare interpreted should set the ceiling to interpreted, got %v", c.prepareLevel)
	}
	out.Reset()
	errb.Reset()
	c.exec(`PREPARE q AS SELECT 9 AS n`)
	if strings.Contains(out.String(), "func "+glue.PrepareFuncName+"(") {
		t.Errorf("PREPARE at interpreted should NOT emit Go; stdout=%q", out.String())
	}
	out.Reset()
	c.exec(`EXECUTE q`)
	if !strings.Contains(out.String(), `"n":9`) {
		t.Errorf("EXECUTE q should produce {n:9}; stdout=%q", out.String())
	}
}
