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
	"sort"
	"testing"
)

// TestIsReserved (IDEA-0028): IsReserved asks cbq's live parser, so it flags the
// keywords that bite as identifiers, accepts ordinary field names, is case-
// insensitive, and never builds a bogus probe from non-identifier input.
func TestIsReserved(t *testing.T) {
	reserved := []string{"level", "keys", "groups", "bucket", "prev", "probe",
		"LEVEL", "Keys"} // case-insensitive
	for _, w := range reserved {
		if !IsReserved(w) {
			t.Errorf("IsReserved(%q) = false, want true (a reserved keyword)", w)
		}
	}

	// Ordinary names -- including `type`, which is a keyword token yet is accepted as
	// an identifier here (probing the actual position avoids that false positive).
	usable := []string{"msg", "node", "ts", "user", "action", "type", "foo123"}
	for _, w := range usable {
		if IsReserved(w) {
			t.Errorf("IsReserved(%q) = true, want false (usable as an identifier)", w)
		}
	}

	// Non-identifiers must return false (never build a malformed probe statement).
	for _, w := range []string{"", "a.b", "`level`", "1abc", "a b", "SELECT 1", "x;y"} {
		if IsReserved(w) {
			t.Errorf("IsReserved(%q) = true, want false (not a simple identifier)", w)
		}
	}
}

// TestReservedWords: the enumerated list is non-trivial, sorted, contains real reserved
// keywords, and EXCLUDES the candidate-set tokens that aren't reserved identifiers
// (lexical classes like str/int, punctuation names like lparen, and keywords still legal
// as identifiers like type). Every entry must itself pass IsReserved (self-consistent).
func TestReservedWords(t *testing.T) {
	got := ReservedWords()
	if len(got) < 150 {
		t.Fatalf("ReservedWords returned %d words, want a substantial list (>150)", len(got))
	}
	if !sort.StringsAreSorted(got) {
		t.Errorf("ReservedWords not sorted")
	}
	set := map[string]bool{}
	for _, w := range got {
		set[w] = true
		if !IsReserved(w) {
			t.Errorf("listed word %q is not IsReserved (list/predicate disagree)", w)
		}
	}
	for _, w := range []string{"select", "where", "level", "keys", "groups", "bucket"} {
		if !set[w] {
			t.Errorf("reserved word %q missing from the list", w)
		}
	}
	for _, w := range []string{"type", "str", "int", "lparen", "ident", "msg", "node"} {
		if set[w] {
			t.Errorf("non-reserved candidate %q must not be in the list", w)
		}
	}
}
