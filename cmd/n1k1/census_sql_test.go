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
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	builtinq "github.com/couchbase/n1k1/cmd/n1k1/builtins"
	"github.com/couchbase/n1k1/glue"
)

// TestCensusSQLViaCLI: the CLI-level plumbing of the shipped census —
// `.multi run --queries builtin:census.sql++?...` runs, `_meta` never appears
// (ISSUE-20), and first_id stays opt-in (default off). The CORRECTNESS guard —
// cell-for-cell agreement of census.sql++ AND the bundled census_agg against the
// frozen Go oracle — is glue's TestCensusForkableDifferential (the oracle is
// test-only there; it has no CLI surface anymore). Also: the retired
// builtin:census must fail with the migration message, not "unknown builtin".
func TestCensusSQLViaCLI(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "c.jsonl"), []byte(strings.Join([]string{
		`{"type":"a","timestamp":"2026-01-02","message":{"id":"m1","model":"x"},"n":5}`,
		`{"type":"a","message":{"id":"m2"},"n":1,"_meta":{"path":"injected","size":9}}`,
		`{"type":"b","timestamp":"2026-01-05","toolUseResult":{"deep":"skip"},"k":[1,2]}`,
	}, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	runCensus := func(t *testing.T, ref string) (rows []string, stderr string, failed bool) {
		t.Helper()
		var out, errb bytes.Buffer
		c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
		c.cmdMulti(`run --queries "` + ref + `"`)
		if txt := strings.TrimSpace(out.String()); txt != "" {
			rows = strings.Split(txt, "\n")
		}
		return rows, errb.String(), c.failed
	}

	rows, stderr, failed := runCensus(t, "builtin:census.sql++?keyspace=*.jsonl&depth=2")
	if failed {
		t.Fatalf("census.sql++ failed: %s", stderr)
	}
	if len(rows) == 0 {
		t.Fatal("census.sql++ produced no rows")
	}
	sort.Strings(rows)
	for _, r := range rows {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(r), &m); err != nil {
			t.Fatalf("bad row %q: %v", r, err)
		}
		if strings.Contains(r, `"_meta`) {
			t.Errorf("_meta leaked into the census: %s", r)
		}
		if strings.Contains(r, "first_id") {
			t.Errorf("first_id must be opt-in (default off): %s", r)
		}
	}

	withID, stderr, failed := runCensus(t, "builtin:census.sql++?keyspace=*.jsonl&depth=2&first-id=1")
	if failed {
		t.Fatalf("census.sql++ first-id=1 failed: %s", stderr)
	}
	anyID := false
	for _, r := range withID {
		anyID = anyID || strings.Contains(r, "first_id")
	}
	if !anyID {
		t.Error("first-id=1: expected first_id in some rows")
	}

	// The retired native census: loud migration, not "unknown builtin".
	_, stderr, failed = runCensus(t, "builtin:census?keyspace=*.jsonl")
	if !failed {
		t.Fatal("builtin:census should fail (retired)")
	}
	if !strings.Contains(stderr, "retired") || !strings.Contains(stderr, "census.sql++") {
		t.Errorf("retired builtin:census error should carry the migration, got: %s", stderr)
	}
}

// TestCensusSQLTemplateIsStandard: the embedded census templates must be usable AS-IS
// as plain parameterized SQL++ — every $reference in a STANDARD position (value params,
// dot-bracket dynamic fields, expression-datasource FROM). Proof: the RAW template
// parses on the engine, failing only at named-parameter BIND time, never with a syntax
// error. This is what keeps a fork of the shown SQL runnable outside n1k1.
func TestCensusSQLTemplateIsStandard(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "d.jsonl"), []byte(`{"n":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := glue.OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	for _, name := range []string{"census.sql++", "census_totals.sql++"} {
		q, ok := builtinq.Lookup(name)
		if !ok {
			t.Fatalf("%s not embedded", name)
		}
		_, rerr := sess.Run(q.Template)
		if rerr == nil {
			t.Fatalf("%s: raw template ran without bound params?", name)
		}
		if !strings.Contains(rerr.Error(), "named parameter") {
			t.Fatalf("%s: raw template must PARSE (bind-stage error only), got: %v", name, rerr)
		}
	}
}
