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

import "testing"

// TestExtInlineExamples covers the inline-`examples` golden mechanism (ext_examples.go)
// across every JS extension kind: an example is CAPTURED from the file's `examples`
// array at registration and EXECUTED by RunExtensionExamples through that kind's real
// call protocol -- scalar call, aggregate init/update/final, stream emit, macro expand,
// extract describe+frame. It also pins the failure paths: a wrong `out` reports Pass=false
// and a throwing body reports Err.
func TestExtInlineExamples(t *testing.T) {
	// Scalar UDF: two passing examples + one deliberately WRONG one.
	mustReg(t, RegisterJSFunc("exq_add", `
		function exq_add(a, b) { return a + b; }
		exq_add.examples = [
			{ in: [2, 3], out: 5 },
			{ desc: "negatives", in: [-1, 1], out: 0 },
			{ name: "WRONG", in: [1, 1], out: 99 }
		];`))

	// Aggregate: geometric-mean-style fold over a value sequence (global `examples`).
	mustReg(t, RegisterJSAggregate("exq_sum", `
		function exq_sum_init()       { return 0; }
		function exq_sum_update(s, v) { return s + v; }
		function exq_sum_final(s)     { return s; }
		var examples = [ { in: [1, 2, 3, 4], out: 10 } ];`))

	// Stream source: emit one {n} row per value.
	mustReg(t, RegisterJSStream("exq_upto", `
		function exq_upto(emit, n) { for (var i = 1; i <= n; i++) emit({ n: i }); }
		exq_upto.examples = [ { in: [3], out: [ {n:1}, {n:2}, {n:3} ] } ];`))

	// Macro: a call expands to SQL++ text (compared whitespace-insensitively).
	mustReg(t, RegisterJSMacro("exq_wrap", `
		var macro = { name: "exq_wrap", params: [ { name: "src", required: true } ] };
		function expand(args) { return "SELECT * FROM " + args.src; }
		macro.examples = [ { in: "@exq_wrap(logs)", out: "(SELECT * FROM logs)" } ];`))

	// Extract recipe: a sample file frames (section) into {title,text} rows.
	mustReg(t, RegisterJSExtractRecipe("exq_sect", `
		var match = { names: ["exq_sect"] };
		function describe(file) {
			return { format: "exq_sect", framing: { kind: "section", section: "^={3,}$" } };
		}
		var examples = [
			{ in: "===\nfoo\n===\nbar\n", out: [ { title: "foo", text: "bar" } ] }
		];`))

	results := RunExtensionExamples("")
	byLabel := map[string]ExampleResult{} // key: name+"/"+label
	for _, r := range results {
		byLabel[r.Name+"/"+r.Label] = r
	}

	// Every named example must have run, passing except the one seeded WRONG.
	wantPass := []string{
		"exq_add/#0", "exq_add/negatives",
		"exq_sum/#0",
		"exq_upto/#0",
		"exq_wrap/#0",
		"exq_sect/#0",
	}
	for _, k := range wantPass {
		r, ok := byLabel[k]
		if !ok {
			t.Errorf("example %q did not run (not captured?)", k)
			continue
		}
		if r.Err != "" {
			t.Errorf("example %q errored: %s", k, r.Err)
		}
		if !r.Pass {
			t.Errorf("example %q should pass; got %s, want %s", k, r.Got, r.Want)
		}
	}

	// The seeded-wrong scalar example must be detected as a failure (not an error).
	if w, ok := byLabel["exq_add/WRONG"]; !ok {
		t.Error("the WRONG example did not run")
	} else if w.Pass || w.Err != "" {
		t.Errorf("WRONG example should FAIL cleanly; pass=%v err=%q got=%s", w.Pass, w.Err, w.Got)
	}

	// The `only` filter restricts to one extension by name.
	only := RunExtensionExamples("exq_upto")
	if len(only) != 1 || only[0].Name != "exq_upto" {
		t.Errorf("RunExtensionExamples(\"exq_upto\") = %d results, want 1 for exq_upto", len(only))
	}
}

// TestExtInlineExamplesThrow: an example whose body throws is reported as Err, not a
// crash and not a silent pass.
func TestExtInlineExamplesThrow(t *testing.T) {
	mustReg(t, RegisterJSFunc("exq_throw", `
		function exq_throw(x) { throw new Error("boom"); }
		exq_throw.examples = [ { in: [1], out: 1 } ];`))

	for _, r := range RunExtensionExamples("exq_throw") {
		if r.Err == "" {
			t.Errorf("a throwing example must report Err; got pass=%v", r.Pass)
		}
	}
}

func mustReg(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("register: %v", err)
	}
}
