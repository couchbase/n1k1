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
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/glue"
)

// TestNamedArgs exercises SQL++ named query parameters ($name) end-to-end: a
// Session.Run supplied with NamedArgs resolves a `WHERE x IN $inlist`
// NamedParameter at eval time (GlueContext.NamedArg), and omitting the arg is a
// clean error (never a panic). Uses a self-contained temp datastore so it
// doesn't depend on the corpus.
func TestNamedArgs(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "default", "nums")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// n=1..6, one doc per file (keys k1..k6).
	for _, d := range []string{
		`{"n":1}`, `{"n":2}`, `{"n":3}`, `{"n":4}`, `{"n":5}`, `{"n":6}`,
	} {
		key := "k" + d[5:6] // the single digit in {"n":<d>}
		if err := os.WriteFile(filepath.Join(dir, key+".json"), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	store, err := glue.FileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatal(err)
	}

	// sortedRows returns the result rows as a sorted []string, so the assertions
	// check WHICH rows the named parameter selected, independent of row order.
	sortedRows := func(res *glue.Result) []string {
		out := make([]string, len(res.Rows))
		for i, r := range res.Rows {
			out[i] = string(r)
		}
		sort.Strings(out)
		return out
	}

	// (1) $inlist resolves: only n in {2,3,5} come back.
	sess := &glue.Session{Store: store, Namespace: "default",
		NamedArgs: map[string]value.Value{
			"inlist": value.NewValue([]interface{}{2.0, 3.0, 5.0}),
		}}
	res, err := sess.Run(`SELECT RAW x.n FROM nums x WHERE x.n IN $inlist`)
	if err != nil {
		t.Fatalf("named-arg query: %v", err)
	}
	if got, want := sortedRows(res), []string{"2", "3", "5"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("$inlist result = %v, want %v", got, want)
	}

	// (2) a scalar named arg works in a predicate too.
	sess2 := &glue.Session{Store: store, Namespace: "default",
		NamedArgs: map[string]value.Value{"lo": value.NewValue(4.0)}}
	res, err = sess2.Run(`SELECT RAW x.n FROM nums x WHERE x.n > $lo`)
	if err != nil {
		t.Fatalf("scalar named-arg query: %v", err)
	}
	if got, want := sortedRows(res), []string{"5", "6"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("$lo result = %v, want %v", got, want)
	}

	// (3) omitting the named arg is a clean error, not a panic.
	sess3 := &glue.Session{Store: store, Namespace: "default"}
	if _, err := sess3.Run(`SELECT RAW x.n FROM nums x WHERE x.n IN $inlist`); err == nil {
		t.Fatalf("missing named arg should error")
	}
}
