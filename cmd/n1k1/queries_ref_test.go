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
)

// TestParseQueriesRef pins the --queries scheme resolver: local paths (incl. Windows
// drive paths) stay refFile; builtin: refs parse name/@version/?params and validate
// against the builtin registry; latest resolves to the concrete newest version.
func TestParseQueriesRef(t *testing.T) {
	// Local paths (default + explicit file:), incl. a Windows drive path.
	for _, p := range []string{"./dir", "dir", "/abs/dir", `C:\data\logs`, "file:./dir"} {
		r, err := parseQueriesRef(p)
		if err != nil || r.kind != refFile {
			t.Fatalf("%q: want refFile, got kind=%q err=%v", p, r.kind, err)
		}
		wantPath := p
		if len(p) > 5 && p[:5] == "file:" {
			wantPath = p[5:]
		}
		if r.path != wantPath {
			t.Errorf("%q: path=%q, want %q", p, r.path, wantPath)
		}
	}

	// builtin: latest (no @version) resolves to the concrete newest.
	r, err := parseQueriesRef("builtin:census")
	if err != nil || r.kind != refBuiltin || r.name != "census" || r.version != "1" {
		t.Fatalf("builtin:census => %+v err=%v; want census@1", r, err)
	}

	// builtin: with @version + ?params.
	r, err = parseQueriesRef("builtin:census@1?keyspace=logs&depth=2")
	if err != nil {
		t.Fatalf("parameterized builtin: %v", err)
	}
	if r.version != "1" || r.params["keyspace"] != "logs" || r.params["depth"] != "2" {
		t.Fatalf("params/version wrong: %+v", r)
	}

	// Loud errors: unknown builtin, unsupported version, empty name.
	if _, err := parseQueriesRef("builtin:nope"); err == nil {
		t.Error("unknown builtin should error")
	}
	if _, err := parseQueriesRef("builtin:census@9"); err == nil {
		t.Error("unsupported version should error")
	}
	if _, err := parseQueriesRef("builtin:"); err == nil {
		t.Error("empty builtin name should error")
	}
}

// TestRunBuiltinCensus: `.multi run --queries builtin:census?keyspace=...` routes to
// the census engine (census dissolved into the queries algebra) and produces the same
// NDJSON census rows as the census verb. Also checks the unknown-builtin error.
func TestRunBuiltinCensus(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "sessions")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "s.jsonl"),
		[]byte(`{"type":"a","model":"opus"}`+"\n"+`{"type":"b"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	c.cmdMulti("run --queries builtin:census?keyspace=sessions")
	if c.failed {
		t.Fatalf("run builtin:census failed: %s", errb.String())
	}
	if !strings.Contains(out.String(), `"path":"model"`) || !strings.Contains(out.String(), `"path":"type"`) {
		t.Fatalf("census rows missing; out=%s", out.String())
	}

	// A keyspace param is required.
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("run --queries builtin:census")
	if !c.failed || !strings.Contains(errb.String(), "keyspace") {
		t.Fatalf("builtin:census without keyspace should error; failed=%v stderr=%q", c.failed, errb.String())
	}

	// An unknown builtin errors, naming the known ones.
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("run --queries builtin:nope")
	if !c.failed || !strings.Contains(errb.String(), "unknown builtin") {
		t.Fatalf("unknown builtin should error; failed=%v stderr=%q", c.failed, errb.String())
	}
}

// TestCensusMigrationsRemoved: the `.multi census` verb and the `cursor --mode census`
// selector are gone (hard cut); each errors naming the builtin:census replacement.
func TestCensusMigrationsRemoved(t *testing.T) {
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}

	c.cmdMulti("census events")
	if !c.failed || !strings.Contains(errb.String(), "builtin:census") {
		t.Fatalf(".multi census should error naming builtin:census; stderr=%q", errb.String())
	}

	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("cursor create x --queries ./nope --mode census")
	if !c.failed || !strings.Contains(out.String()+errb.String(), "builtin:census") {
		t.Fatalf("--mode census should error naming builtin:census; out=%q stderr=%q", out.String(), errb.String())
	}
}
