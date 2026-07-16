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
	"bytes"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/cmd"
	"github.com/couchbase/n1k1/glue"
)

// TestBuiltinMacrosShipped: the embedded macros register at startup (no -ext needed),
// are tagged built-in, and .macro show prints a macro's full source.
func TestBuiltinMacrosShipped(t *testing.T) {
	if errs := registerBuiltinMacros(); len(errs) != 0 {
		t.Fatalf("registerBuiltinMacros: %v", errs)
	}
	// @vectorize_field ships built-in.
	found := false
	for _, m := range glue.ListMacros() {
		if m.Name == "vectorize_field" {
			found = true
		}
	}
	if !found {
		t.Fatal("@vectorize_field not registered as a built-in macro")
	}
	if !builtinMacroNames["vectorize_field"] {
		t.Error("@vectorize_field not tagged built-in")
	}

	// .macro show <name>: header (+built-in tag) to stderr, full source to stdout.
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", out: &out, stderr: &errb, style: cmd.Style{}}
	c.cmdMacro("show vectorize_field")
	if !strings.Contains(out.String(), "VECTORIZE_BATCH") || !strings.Contains(out.String(), "function expand") {
		t.Errorf(".macro show did not print the source:\n%s", out.String())
	}
	if !strings.Contains(errb.String(), "[built-in]") {
		t.Errorf(".macro show header missing [built-in] tag:\n%s", errb.String())
	}

	// .macro list shows it with a blurb, not the raw source dump.
	var lerr bytes.Buffer
	c2 := &cli{prog: "n1k1", out: &out, stderr: &lerr, style: cmd.Style{}}
	c2.cmdMacro("list")
	if !strings.Contains(lerr.String(), "@vectorize_field") || strings.Contains(lerr.String(), "function expand") {
		t.Errorf(".macro list should show a blurb, not source:\n%s", lerr.String())
	}
}

// TestBuiltinModulesShipped: the embedded builtin_*.js JS modules register at startup
// (no -ext needed), appear in .extensions list as "(built-in)" javascript-module rows,
// and .extensions show dumps a module's full source (addressed by a function name).
func TestBuiltinModulesShipped(t *testing.T) {
	if errs := registerBuiltinModules(); len(errs) != 0 {
		t.Fatalf("registerBuiltinModules: %v", errs)
	}
	// The DECIMAL_* / EJSON_* functions are now registered without any -ext.
	for _, fn := range []string{"decimal_add", "decimal_sum", "ejson_decode"} {
		if glue.JSModuleOf(fn) == "" {
			t.Errorf("builtin function %q not registered as a module function", fn)
		}
	}

	// .extensions list: builtin_decimal / builtin_ejson as (built-in) module rows.
	var lb bytes.Buffer
	(&cli{prog: "n1k1", out: &lb, stderr: &lb, style: cmd.Style{}}).cmdExtensions("list")
	ls := lb.String()
	for _, want := range []string{"builtin_decimal", "builtin_ejson", "javascript-module", "(built-in)", "decimal_add", "ejson_decode"} {
		if !strings.Contains(ls, want) {
			t.Errorf(".extensions list missing %q:\n%s", want, ls)
		}
	}

	// .extensions show <function>: dumps the (embedded) source of its module file.
	var so, se bytes.Buffer
	(&cli{prog: "n1k1", out: &so, stderr: &se, style: cmd.Style{}}).cmdExtensions("show ejson_decode")
	if !strings.Contains(so.String(), "exports.functions") || !strings.Contains(so.String(), "EJSON_DECODE") {
		t.Errorf(".extensions show ejson_decode did not dump the module source:\n%s", so.String())
	}
}

// TestHelpVectorsTopic: the .help vectors topic renders, has the key functions + a
// runnable example, and does NOT leak internal references (e.g. design-doc names).
func TestHelpVectorsTopic(t *testing.T) {
	var errb bytes.Buffer
	c := &cli{prog: "n1k1", stderr: &errb, style: cmd.Style{}}
	c.cmdHelp("vectors")
	got := errb.String()
	for _, want := range []string{"VECTORIZE_BATCH", "VECTOR_DISTANCE", "@vectorize_field", "ORDER BY", "cosine"} {
		if !strings.Contains(got, want) {
			t.Errorf(".help vectors missing %q", want)
		}
	}
	if strings.Contains(strings.ToLower(got), "design-vectors") || strings.Contains(got, ".md") {
		t.Errorf(".help vectors leaked an internal doc reference:\n%s", got)
	}
}
