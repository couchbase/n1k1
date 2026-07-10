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

package test

// A RESULT-LEVEL ORACLE differential: it runs the couchbase/query fork's own
// filestore test cases (statement + pre-recorded expected results, produced by
// cbq's engine) against n1k1 and asserts n1k1 reproduces those results. This
// complements the interpreter/compiler differential (suite_test.go), which only
// proves n1k1's two lanes AGREE -- it is BLIND to a conversion-layer bug that
// mis-plans identically in both lanes (e.g. a dropped join predicate). cbq's
// recorded results are the ground truth those lanes are silently allowed to drift
// from, so this test is where that drift gets caught.
//
// The fixtures live under test/suite/filestore/<category>/, vendored from the fork's
// test/filestore/test_cases/<category>/: case_*.json ({statements, results}) plus
// insert.json (the seed data as `INSERT INTO ks (KEY,VALUE) VALUES(...)`). We
// materialize the seed into a dir-of-json-files datastore (preserving each doc's
// KEY -- the cases look rows up by META().id / ON KEYS, which n1k1's positional-id
// INSERT would not preserve), then run each case and compare like cbq does:
// order-sensitive reflect.DeepEqual over JSON-unmarshaled rows.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/couchbase/n1k1/glue"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/value"
)

// filestoreCase is one {statements, results} entry of a cbq case_*.json file.
type filestoreCase struct {
	Statements string          `json:"statements"`
	Results    json.RawMessage `json:"results"`
}

// filestoreSeed is one {statements: "INSERT INTO ..."} entry of insert.json.
type filestoreSeed struct {
	Statements string `json:"statements"`
}

// suiteFilestoreDir returns the vendored test/suite/filestore/<category> dir,
// located relative to THIS source file (so it resolves from test/tmp/ too).
func suiteFilestoreDir(category string) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "suite", "filestore", category)
}

// materializeFilestoreSeed replays a cbq insert.json into a dir-of-json-files
// datastore rooted at root: each `INSERT INTO ks (KEY,VALUE) VALUES(key, val)`
// becomes root/default/ks/<key>.json, PRESERVING the document key (parsed via the
// real n1ql parser, not by running n1k1's positional-id INSERT).
func materializeFilestoreSeed(t *testing.T, insertPath, root string) {
	t.Helper()
	raw, err := os.ReadFile(insertPath)
	if err != nil {
		t.Fatalf("read seed %q: %v", insertPath, err)
	}
	var seeds []filestoreSeed
	if err := json.Unmarshal(raw, &seeds); err != nil {
		t.Fatalf("parse seed %q: %v", insertPath, err)
	}

	ctx := glue.NewExprGlueContext(time.Now())
	for i, sd := range seeds {
		stmt, err := glue.ParseStatement(sd.Statements, "default", true)
		if err != nil {
			t.Fatalf("seed[%d] parse: %v", i, err)
		}
		ins, ok := stmt.(*algebra.Insert)
		if !ok {
			t.Fatalf("seed[%d] is not an INSERT (%T)", i, stmt)
		}
		ks := ins.KeyspaceRef().Keyspace()
		dir := filepath.Join(root, "default", ks)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, pair := range ins.Values() {
			keyV, err := pair.Key().Evaluate(value.NULL_VALUE, ctx)
			if err != nil {
				t.Fatalf("seed[%d] key eval: %v", i, err)
			}
			key, ok := keyV.Actual().(string)
			if !ok || key == "" {
				t.Fatalf("seed[%d] key is not a non-empty string: %v", i, keyV)
			}
			valV, err := pair.Value().Evaluate(value.NULL_VALUE, ctx)
			if err != nil {
				t.Fatalf("seed[%d] value eval: %v", i, err)
			}
			b, err := valV.MarshalJSON()
			if err != nil {
				t.Fatalf("seed[%d] value marshal: %v", i, err)
			}
			if err := os.WriteFile(filepath.Join(dir, key+".json"), b, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// compareFilestoreResults mirrors cbq's doResultsMatch: JSON-unmarshal both sides
// and order-sensitive reflect.DeepEqual (the cases carry ORDER BY, so order is
// deterministic; object field order is irrelevant after unmarshaling to maps).
func compareFilestoreResults(actualRows []json.RawMessage, expected json.RawMessage) (bool, string) {
	var exp []interface{}
	if err := json.Unmarshal(expected, &exp); err != nil {
		return false, "bad expected JSON: " + err.Error()
	}
	act := make([]interface{}, 0, len(actualRows))
	for _, r := range actualRows {
		var v interface{}
		if err := json.Unmarshal(r, &v); err != nil {
			return false, "bad actual JSON: " + err.Error()
		}
		act = append(act, v)
	}
	if len(act) != len(exp) {
		return false, fmt.Sprintf("row count %d != expected %d", len(act), len(exp))
	}
	if !reflect.DeepEqual(act, exp) {
		return false, "rows differ"
	}
	return true, ""
}

// runFilestoreOracle is the reusable core: materialize a category's seed, then run
// every case in its case_*.json files and assert n1k1 reproduces the recorded
// results. skip maps "<caseFileBase>#<index>" -> reason for statements that use a
// feature n1k1 genuinely does not support yet (documented, so the oracle stays green
// without hiding a regression); a NON-skipped error or mismatch is a hard failure --
// the whole point of the oracle.
func runFilestoreOracle(t *testing.T, category string, skip map[string]string) {
	dir := suiteFilestoreDir(category)
	root := t.TempDir()
	materializeFilestoreSeed(t, filepath.Join(dir, "insert.json"), root)

	sess, err := glue.OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, "case_*.json"))
	sort.Strings(files)
	if len(files) == 0 {
		t.Fatalf("no case files under %q", dir)
	}

	var pass, skipped int
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read case %q: %v", f, err)
		}
		var cases []filestoreCase
		if err := json.Unmarshal(raw, &cases); err != nil {
			t.Fatalf("parse case %q: %v", f, err)
		}
		base := filepath.Base(f)
		for i, c := range cases {
			if why, ok := skip[fmt.Sprintf("%s#%d", base, i)]; ok {
				skipped++
				t.Logf("SKIP %s[%d]: %s", base, i, why)
				continue
			}
			res, runErr := sess.Run(c.Statements)
			if runErr != nil {
				t.Errorf("ERR  %s[%d]: %v\n    %s", base, i, runErr, c.Statements)
				continue
			}
			if ok, why := compareFilestoreResults(res.Rows, c.Results); !ok {
				t.Errorf("FAIL %s[%d]: %s\n    %s", base, i, why, c.Statements)
				continue
			}
			pass++
		}
	}
	if pass == 0 {
		t.Fatalf("%s oracle: 0 cases passed (harness broken?)", category)
	}
	t.Logf("%s oracle: %d pass, %d skipped", category, pass, skipped)
}

// TestFilestoreOracleJoins runs the fork's filestore "joins" cases against n1k1;
// all 17 reproduce cbq's recorded results (no skips). These exercise ON KEYS lookup
// joins (USE KEYS / UNNEST / META() / VALIDATE / DISTINCT / ORDER BY / LIMIT). The
// dropped-band-join-predicate class of bug (hash/NL joins with a residual) is pinned
// directly by glue's TestJoinBandPredicateNotDropped; broadening this oracle to more
// categories (filters, subqueries, aggregates) is the systematic net for the wider
// class of conversion drift the interpreter/compiler differential can't see.
func TestFilestoreOracleJoins(t *testing.T) {
	runFilestoreOracle(t, "joins", nil)
}
