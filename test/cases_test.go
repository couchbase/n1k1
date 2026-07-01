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

import (
	"testing"

	"github.com/couchbase/n1k1/base"
)

// TestQueryCases runs every queryCase (defined in cases.go, alongside
// TestCasesSimple) as a subtest through the interpreter. The same cases are also
// run through the compiler by TestQueryCasesWithCompiler (query_compiler_test.go).
func TestQueryCases(t *testing.T) {
	for _, c := range queryCases {
		t.Run(c.name, func(t *testing.T) {
			rows := runQuery(t, c.stmt)
			if c.rows >= 0 && len(rows) != c.rows {
				t.Fatalf("expected %d rows, got %d: %+v", c.rows, len(rows), rows)
			}
			if c.check != nil {
				c.check(t, rows)
			}
		})
	}
}

// runQuery parses, plans, converts and executes stmt against the local data:
// file store, failing the test on any error, and returns the result rows. (It
// lives here rather than in cases.go because it uses the _test.go-only
// testFileStoreSelect / testGlueExec harness.)
func runQuery(t *testing.T, stmt string) []base.Vals {
	t.Helper()

	store, _, conv, err := testFileStoreSelect(t, stmt, false)
	if err != nil {
		t.Fatalf("stmt %q: convert err: %v", stmt, err)
	}
	if conv == nil || conv.TopOp == nil {
		t.Fatalf("stmt %q: nil TopOp (unsupported)", stmt)
	}

	return testGlueExec(t, false, store, conv)
}
