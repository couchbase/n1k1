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
