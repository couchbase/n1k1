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

// TestMergeRewriteDifferential proves the Track B grammar-free surfacing pass
// (glue/rewrite_temporal.go, DESIGN-merging.md §3): a UNION ALL of two
// time-ordered sources wrapped by ORDER BY <key> is recognized and lowered to
// the streaming merge-scan op (engine/op_merge_scan.go), and that op produces
// globally time-ordered output.
//
// It is a SEMANTIC differential: the rewrite-ON result (via merge-scan) must
// equal the rewrite-OFF baseline (order(union-all) -- the materialize-then-heap-
// sort path) which is itself run through both the interpreter AND the compiler by
// the standard harness (see the "UnionAllSortedMerge" queryCase in cases.go,
// exercised by TestQueryCases + TestQueryCasesWithCompiler). merge-scan itself is
// an interpreter-only op for now (its `// !lz` shape keeps the compiler package
// COMPILING, but it does not fuse-emit yet -- it is listed in usesUnbridgedOp),
// so the compiler leg validates the ordered-output SEMANTICS via that baseline
// and this test proves the merge-scan rewrite reproduces it exactly.

import (
	"reflect"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

// mergeContainsKind reports whether the op tree contains an op of the given kind.
func mergeContainsKind(op *base.Op, kind string) bool {
	if op == nil {
		return false
	}
	if op.Kind == kind {
		return true
	}
	for _, c := range op.Children {
		if mergeContainsKind(c, kind) {
			return true
		}
	}
	return false
}

func TestMergeRewriteDifferential(t *testing.T) {
	// Two individually time-ordered (ascending-key) branches, overlapping ranges
	// -> the merged global timeline is 1,2,3,5,7,8.
	stmt := `(SELECT x.a FROM [{"a":1},{"a":3},{"a":7}] x) UNION ALL ` +
		`(SELECT y.a FROM [{"a":2},{"a":5},{"a":8}] y) ORDER BY a`
	want := []string{`{"a":1}`, `{"a":2}`, `{"a":3}`, `{"a":5}`, `{"a":7}`, `{"a":8}`}

	store, err := glue.FileStore(gsiSuiteRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatal(err)
	}

	s, err := glue.ParseStatement(stmt, "default", true)
	if err != nil {
		t.Fatal(err)
	}
	p, err := store.PlanStatement(s, "default", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Baseline (rewrite OFF): order(union-all). The differential oracle.
	glue.EnableMergeRewrite = false
	baseOp, _, err := glue.ExecConv(p)
	if err != nil {
		t.Fatal(err)
	}
	if mergeContainsKind(baseOp, "merge-scan") {
		t.Fatal("rewrite must not fire when EnableMergeRewrite is off")
	}
	baseRows, err := n1k1RunStatement(store, stmt)
	if err != nil {
		t.Fatalf("baseline run: %v", err)
	}
	if !reflect.DeepEqual(baseRows, want) {
		t.Fatalf("baseline rows=%v want=%v", baseRows, want)
	}

	// Rewrite ON: order(union-all) -> merge-scan, proven to produce the SAME
	// globally-ordered rows as the baseline.
	before := glue.MergeRewriteApplied
	glue.EnableMergeRewrite = true
	defer func() { glue.EnableMergeRewrite = false }()

	op, _, err := glue.ExecConv(p)
	if err != nil {
		t.Fatal(err)
	}
	if !mergeContainsKind(op, "merge-scan") {
		t.Fatal("expected a merge-scan op after the temporal rewrite")
	}
	if glue.MergeRewriteApplied != before+1 {
		t.Fatalf("MergeRewriteApplied = %d, want %d", glue.MergeRewriteApplied, before+1)
	}

	mergeRows, err := n1k1RunStatement(store, stmt)
	if err != nil {
		t.Fatalf("merge-scan run: %v", err)
	}
	if !reflect.DeepEqual(mergeRows, want) {
		t.Fatalf("merge-scan rows=%v want=%v", mergeRows, want)
	}
	if !reflect.DeepEqual(mergeRows, baseRows) {
		t.Fatalf("merge-scan rows=%v != baseline rows=%v", mergeRows, baseRows)
	}
}
