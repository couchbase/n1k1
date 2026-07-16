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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/cmd"
	"github.com/couchbase/n1k1/glue"
)

// TestExtensionsShowUnified: .extensions list is the unified view (built-in macros appear
// as extensions too) and .extensions show prints a macro's source (from the registry) and
// a file-loaded UDF's source (read from its file).
func TestExtensionsShowUnified(t *testing.T) {
	registerBuiltinMacros() // built-in macros visible under .extensions

	dir := t.TempDir()
	udf := filepath.Join(dir, "dbl.js")
	if err := os.WriteFile(udf, []byte("function dbl(x){ return x*2; }\ndbl.examples=[{in:[3],out:6}];\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := glue.RegisterExtensionFile(udf); err != nil {
		t.Fatal(err)
	}

	// list: includes a [built-in] macro AND the file UDF.
	var lb bytes.Buffer
	(&cli{prog: "n1k1", out: &lb, stderr: &lb, style: cmd.Style{}}).cmdExtensions("list")
	ls := lb.String()
	if !strings.Contains(ls, "@vectorize_field") || !strings.Contains(ls, "(built-in)") {
		t.Errorf(".extensions list missing a built-in macro:\n%s", ls)
	}
	if !strings.Contains(ls, "dbl") {
		t.Errorf(".extensions list missing the loaded UDF:\n%s", ls)
	}

	// show a built-in macro: source (stdout) from the registry, header (stderr) tags built-in.
	var mo, me bytes.Buffer
	(&cli{prog: "n1k1", out: &mo, stderr: &me, style: cmd.Style{}}).cmdExtensions("show @vectorize_field")
	if !strings.Contains(mo.String(), "VECTORIZE_BATCH") {
		t.Errorf(".extensions show macro: no source:\n%s", mo.String())
	}
	if !strings.Contains(me.String(), "built-in") {
		t.Errorf(".extensions show macro: header missing built-in tag:\n%s", me.String())
	}

	// show a file-loaded UDF: source read from the file.
	var uo, ue bytes.Buffer
	(&cli{prog: "n1k1", out: &uo, stderr: &ue, style: cmd.Style{}}).cmdExtensions("show dbl")
	if !strings.Contains(uo.String(), "function dbl") {
		t.Errorf(".extensions show udf: no source:\n%s", uo.String())
	}

	// unknown name -> friendly error, no source.
	var xo, xe bytes.Buffer
	(&cli{prog: "n1k1", out: &xo, stderr: &xe, style: cmd.Style{}}).cmdExtensions("show nope")
	if xo.Len() != 0 || !strings.Contains(xe.String(), "no extension") {
		t.Errorf(".extensions show nope: want a 'no extension' note, got out=%q err=%q", xo.String(), xe.String())
	}
}

// TestExtensionsModuleSubcommands: a multi-export JS module (exports.functions) shows
// its FUNCTIONS individually in `.extensions list` (all sharing the file), and
// examples/test/show accept EITHER a function name OR the bundle (file stem), which
// expands to the whole family. Regression for the earlier gap where the bundle was
// listed but its functions/examples were unreachable.
func TestExtensionsModuleSubcommands(t *testing.T) {
	dir := t.TempDir()
	mod := filepath.Join(dir, "mymod.js")
	src := "function madd(a,b){return a+b;}\nfunction mmul(a,b){return a*b;}\n" +
		"exports.functions=[" +
		"{name:\"mymod_add\",fn:madd,examples:[{in:[2,3],out:5}]}," +
		"{name:\"mymod_mul\",fn:mmul,examples:[{in:[2,3],out:6}]}];\n"
	if err := os.WriteFile(mod, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := glue.RegisterExtensionFile(mod); err != nil {
		t.Fatalf("RegisterExtensionFile: %v", err)
	}

	run := func(arg string) string {
		var b bytes.Buffer
		(&cli{prog: "n1k1", out: &b, stderr: &b, style: cmd.Style{}}).cmdExtensions(arg)
		return b.String()
	}

	// list shows the FUNCTIONS (not a bare "mymod" bundle row), each with the file source.
	ls := run("list")
	for _, want := range []string{"mymod_add", "mymod_mul", "mymod.js"} {
		if !strings.Contains(ls, want) {
			t.Errorf(".extensions list missing %q:\n%s", want, ls)
		}
	}

	// examples <bundle> expands to every function in the module.
	ex := run("examples mymod")
	if !strings.Contains(ex, "mymod_add") || !strings.Contains(ex, "mymod_mul") {
		t.Errorf(".extensions examples mymod did not expand to the family:\n%s", ex)
	}

	// examples <function> also works directly.
	if exf := run("examples mymod_add"); !strings.Contains(exf, "mymod_add") {
		t.Errorf(".extensions examples mymod_add missing:\n%s", exf)
	}

	// test <bundle> runs every function's examples; both pass, none fail.
	ts := run("test mymod")
	if strings.Contains(ts, "✗") || !strings.Contains(ts, "passed") {
		t.Errorf(".extensions test mymod: want all-pass, got:\n%s", ts)
	}

	// show <bundle> prints the shared source file.
	sh := run("show mymod")
	if !strings.Contains(sh, "exports.functions") {
		t.Errorf(".extensions show mymod did not print the module source:\n%s", sh)
	}
}
