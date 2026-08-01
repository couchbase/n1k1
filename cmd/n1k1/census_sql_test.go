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

// TestCensusSQLMatchesOracle is the differential guard for the pure-SQL++ census:
// `builtin:census.sql++` must produce cell-identical rows to the native Go
// `builtin:census` (the ORACLE) over the same corpus + params. v2.0 emits the
// mergeable core only, so the comparison runs sql++ with first-id=1 (full fidelity)
// and strips `coverage` from the oracle (a read-time ratio, deliberately not in the
// core — divide against census_totals.sql++). Rows are compared as CANONICAL JSON
// (parsed, re-marshaled with sorted keys), cell for cell. The fixture is
// deliberately adversarial: multiple record types, nested objects (depth-2), a record
// missing the type field (-> "" bucket), records missing the timestamp (first/last-seen
// + first_id must skip them), an array value, an excludable key, and a literal
// `_meta` field that BOTH engines must exclude (ISSUE-20: engine provenance is never
// corpus schema).
func TestCensusSQLMatchesOracle(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "c.jsonl"), []byte(strings.Join([]string{
		`{"type":"a","timestamp":"2026-01-02","message":{"id":"m1","model":"x"},"n":5}`,
		`{"type":"a","timestamp":"2026-01-01","message":{"id":"m0"},"n":9,"extra":true}`,
		`{"type":"a","message":{"id":"m2"},"n":1,"_meta":{"path":"injected","size":9}}`,
		`{"type":"b","timestamp":"2026-01-05","toolUseResult":{"deep":"skip"},"k":[1,2]}`,
		`{"note":"no-type-field","timestamp":"2026-01-09","deep":{"lvl2":{"lvl3":"stop"}}}`,
		`{"type":"c","solo":true}`, // a group with NO timestamps: first/last_seen + first_id must be OMITTED
	}, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Run one census variant through the cli; return each row as CANONICAL JSON
	// (parsed + re-marshaled: Go maps marshal with sorted keys), with dropKeys
	// removed, sorted.
	runCensus := func(t *testing.T, ref string, dropKeys ...string) []string {
		t.Helper()
		var out, errb bytes.Buffer
		c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
		c.cmdMulti(`run --queries "` + ref + `"`)
		if c.failed {
			t.Fatalf("%s failed: %s", ref, errb.String())
		}
		var rows []string
		for _, ln := range strings.Split(strings.TrimSpace(out.String()), "\n") {
			var m map[string]interface{}
			if err := json.Unmarshal([]byte(ln), &m); err != nil {
				t.Fatalf("%s: bad row %q: %v", ref, ln, err)
			}
			for _, k := range dropKeys {
				delete(m, k)
			}
			b, _ := json.Marshal(m)
			rows = append(rows, string(b))
		}
		sort.Strings(rows)
		return rows
	}

	// Each params variant exercises a different code path (depth, exclude, defaults).
	for _, params := range []string{
		"keyspace=*.jsonl&type-field=type&time-field=timestamp&depth=2&exclude=toolUseResult",
		"keyspace=*.jsonl&type-field=type&time-field=timestamp&depth=2", // no exclude
		"keyspace=*.jsonl&type-field=type&time-field=timestamp&depth=1", // depth-1 only
	} {
		t.Run(params, func(t *testing.T) {
			oracle := runCensus(t, "builtin:census?"+params, "coverage")
			sqlpp := runCensus(t, "builtin:census.sql++?"+params+"&first-id=1")
			if len(oracle) != len(sqlpp) {
				t.Fatalf("row count: oracle=%d sql++=%d\noracle:\n%s\nsql++:\n%s",
					len(oracle), len(sqlpp), strings.Join(oracle, "\n"), strings.Join(sqlpp, "\n"))
			}
			for i := range oracle {
				if oracle[i] != sqlpp[i] {
					t.Errorf("row %d differs:\n  oracle: %s\n  sql++ : %s", i, oracle[i], sqlpp[i])
				}
			}
			// ISSUE-20: the literal _meta field must appear in NEITHER census.
			for _, rows := range [][]string{oracle, sqlpp} {
				for _, r := range rows {
					if strings.Contains(r, `"_meta`) {
						t.Errorf("_meta leaked into the census: %s", r)
					}
				}
			}
			// v2.0 default: first_id (and its per-record composite cost) is OPT-IN.
			for _, r := range runCensus(t, "builtin:census.sql++?"+params) {
				if strings.Contains(r, "first_id") {
					t.Errorf("first_id must be opt-in (default off): %s", r)
				}
			}
		})
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
