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
	"sort"
	"strings"
	"testing"
)

// TestCensusSQLMatchesOracle is the differential guard for the pure-SQL++ census:
// `builtin:census.sql++` must produce byte-identical rows to the native Go
// `builtin:census` (the ORACLE) over the same corpus + params. The fixture is
// deliberately adversarial: multiple record types, nested objects (depth-2), a record
// missing the type field (-> "" bucket), records missing the timestamp (first/last-seen
// + first_id must skip them), an array value, and an excludable key.
func TestCensusSQLMatchesOracle(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "c.jsonl"), []byte(strings.Join([]string{
		`{"type":"a","timestamp":"2026-01-02","message":{"id":"m1","model":"x"},"n":5}`,
		`{"type":"a","timestamp":"2026-01-01","message":{"id":"m0"},"n":9,"extra":true}`,
		`{"type":"a","message":{"id":"m2"},"n":1}`,
		`{"type":"b","timestamp":"2026-01-05","toolUseResult":{"deep":"skip"},"k":[1,2]}`,
		`{"note":"no-type-field","timestamp":"2026-01-09","deep":{"lvl2":{"lvl3":"stop"}}}`,
	}, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Run one census variant through the cli and return its stdout rows, sorted.
	runCensus := func(t *testing.T, ref string) []string {
		t.Helper()
		var out, errb bytes.Buffer
		c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
		c.cmdMulti(`run --queries "` + ref + `"`)
		if c.failed {
			t.Fatalf("%s failed: %s", ref, errb.String())
		}
		lines := strings.Split(strings.TrimSpace(out.String()), "\n")
		sort.Strings(lines)
		return lines
	}

	// Each params variant exercises a different code path (depth, exclude, defaults).
	for _, params := range []string{
		"keyspace=*.jsonl&type-field=type&time-field=timestamp&depth=2&exclude=toolUseResult",
		"keyspace=*.jsonl&type-field=type&time-field=timestamp&depth=2", // no exclude
		"keyspace=*.jsonl&type-field=type&time-field=timestamp&depth=1", // depth-1 only
	} {
		t.Run(params, func(t *testing.T) {
			oracle := runCensus(t, "builtin:census?"+params)
			sqlpp := runCensus(t, "builtin:census.sql++?"+params)
			if len(oracle) != len(sqlpp) {
				t.Fatalf("row count: oracle=%d sql++=%d\noracle:\n%s\nsql++:\n%s",
					len(oracle), len(sqlpp), strings.Join(oracle, "\n"), strings.Join(sqlpp, "\n"))
			}
			for i := range oracle {
				if oracle[i] != sqlpp[i] {
					t.Errorf("row %d differs:\n  oracle: %s\n  sql++ : %s", i, oracle[i], sqlpp[i])
				}
			}
		})
	}
}
