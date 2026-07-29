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
	"strings"
	"testing"
)

// TestEntryReferencedFields pins the planner-sourced field extraction doctor relies
// on: doc-relative paths (depth included), rooted at the FROM alias, with META()
// and function args excluded — and NO suffix-match bug (the alias is stripped, not
// text-matched).
func TestEntryReferencedFields(t *testing.T) {
	sess, err := OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	ks, paths, ok, err := sess.EntryReferencedFields(
		`SELECT s.message.model, s.type FROM sessions s WHERE s.isAbortedMidStream = true`)
	if err != nil || !ok {
		t.Fatalf("EntryReferencedFields: ok=%v err=%v", ok, err)
	}
	if ks != "sessions" {
		t.Fatalf("keyspace: got %q, want sessions", ks)
	}
	got := strings.Join(paths, ",")
	if got != "isAbortedMidStream,message.model,type" {
		t.Fatalf("paths: got %q, want isAbortedMidStream,message.model,type", got)
	}

	// META(s).id and function args must NOT appear as field paths (ExprFieldPath bails
	// on non-identifier roots), so doctor won't false-flag them as corpus fields.
	_, paths2, ok2, err := sess.EntryReferencedFields(
		`SELECT META(s).id, LENGTH(s.msg) AS n FROM sessions s WHERE s.type = "assistant"`)
	if err != nil || !ok2 {
		t.Fatalf("EntryReferencedFields(2): ok=%v err=%v", ok2, err)
	}
	// Only msg + type are real doc field paths; id (under META) is excluded.
	if strings.Join(paths2, ",") != "msg,type" {
		t.Fatalf("paths2: got %q, want msg,type", strings.Join(paths2, ","))
	}

	// A non-single-keyspace source is not analyzable (doctor skips it).
	if _, _, ok3, _ := sess.EntryReferencedFields(`SELECT 1 AS n`); ok3 {
		t.Fatal("a FROM-less SELECT should not be analyzable")
	}

	if TopLevelField("message.model") != "message" || TopLevelField("type") != "type" {
		t.Fatal("TopLevelField")
	}
}
