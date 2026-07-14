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
