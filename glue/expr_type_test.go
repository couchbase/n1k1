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

package glue

import (
	"os"
	"path/filepath"
	"testing"
)

// TestExprTypeNameNative exercises the nativized TYPE_NAME end-to-end (optimization
// is on, so the native engine.ExprTypeName runs): every JSON type + missing yields
// its cbq type name. That it lowers to the native lane (not boxed) is the point of
// the nativization — TYPE_NAME no longer boxes a census MAP query (ISSUE-06).
func TestExprTypeNameNative(t *testing.T) {
	dir := t.TempDir()
	ksDir := filepath.Join(dir, "default", "t")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ksDir, "t.jsonl"),
		[]byte(`{"s":"hi","n":5,"b":true,"a":[1,2],"o":{"k":1},"z":null}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}
	got := runRowsCanon(t, sess, `SELECT TYPE_NAME(t.s) s, TYPE_NAME(t.n) n, TYPE_NAME(t.b) b,
		TYPE_NAME(t.a) a, TYPE_NAME(t.o) o, TYPE_NAME(t.z) z, TYPE_NAME(t.absent) miss FROM t`)
	want := `{"a":"array","b":"boolean","miss":"missing","n":"number","o":"object","s":"string","z":"null"}`
	if len(got) != 1 || got[0] != want {
		t.Fatalf("TYPE_NAME row: got %v, want [%s]", got, want)
	}

	// Confirm it converts to the NATIVE lane (the boxing this fix removes). A
	// TYPE_NAME entry over the keyspace must lint as native, not boxed.
	e, perr := ParseMultiQueryEntry("tn.sql++", "-- label: TN\nSELECT TYPE_NAME(t.n) AS k FROM t t")
	if perr != nil {
		t.Fatal(perr)
	}
	rep, _, lerr := sess.MultiQueryLint([]MultiQueryEntry{e})
	if lerr != nil {
		t.Fatalf("lint: %v", lerr)
	}
	if len(rep) != 1 || rep[0].Lane != "native" {
		t.Fatalf("TYPE_NAME should lower to the native lane, got %+v", rep)
	}
}
