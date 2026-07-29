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

// `.multi doctor` support (DESIGN-census.md Phase 2): the pack↔census JOIN — the
// piece a stateless engine structurally can't ship, because it needs both the
// corpus's key space (the census) AND the standing questions asked of it (the
// pack). The first, baseline-free check is "a detector references a field the
// corpus doesn't have" — which catches a birth-in-error (a detector aimed at a
// field that never existed) that a yield alarm never can (no cliff).
//
// The referenced-field set MUST be planner-sourced, never a text heuristic: a text
// match reports false positives because a keyspace alias makes a field path a suffix
// of the token (the n1k1-for-ai team hit exactly this). EntryReferencedFields walks
// the PARSED statement and uses ExprFieldPath (the same static-path extractor conv
// uses) so `s.message.model` yields the doc-relative path `message.model`, rooted at
// the FROM alias.

import (
	"sort"
	"strings"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
)

// EntryReferencedFields parses a single-source entry statement and returns the
// keyspace it scans and the DOC-RELATIVE field paths it references (e.g.
// "message.model", "type"), rooted at the FROM alias. ok=false for a statement
// doctor can't analyze (multi-source, a derived-table/expression source, a non-
// SELECT) — the caller skips it rather than guessing.
func (s *Session) EntryReferencedFields(stmt string) (keyspace string, paths []string, ok bool, err error) {
	parsed, perr := ParseStatement(stmt, s.Namespace, true)
	if perr != nil {
		return "", nil, false, perr
	}
	sel, isSel := parsed.(*algebra.Select)
	if !isSel {
		return "", nil, false, nil
	}
	ss, isSS := sel.Subresult().(*algebra.Subselect)
	if !isSS {
		return "", nil, false, nil
	}
	ks, okKS := fromKeyspaceName(ss.From())
	alias, okAlias := fromKeyspaceAlias(ss.From())
	if !okKS || !okAlias {
		return "", nil, false, nil
	}
	// fromKeyspaceName may qualify as "<ns>:<keyspace>"; Census resolves a bare name.
	if i := strings.LastIndexByte(ks, ':'); i >= 0 {
		ks = ks[i+1:]
	}

	seen := map[string]bool{}
	var walk func(e expression.Expression)
	walk = func(e expression.Expression) {
		if e == nil {
			return
		}
		// A complete static field path rooted at the FROM alias is a reference; record
		// its doc-relative form and don't descend (the whole path is captured).
		if p, okp := ExprFieldPath(e); okp && len(p) >= 2 && p[0] == alias {
			seen[strings.Join(p[1:], ".")] = true
			return
		}
		for _, ch := range e.Children() {
			walk(ch)
		}
	}
	for _, e := range sel.Expressions() {
		walk(e)
	}
	if w := ss.Where(); w != nil { // walk WHERE explicitly, in case Expressions() omits it
		walk(w)
	}

	paths = make([]string, 0, len(seen))
	for p := range seen {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return ks, paths, true, nil
}

// TopLevelField returns a field path's first segment ("message.model" -> "message",
// "type" -> "type"), the granularity of doctor's high-precision first check: a
// missing top-level field is a birth-in-error with no false positives against a
// depth-limited census (a present top-level field with a deep sub-path is never
// wrongly flagged).
func TopLevelField(path string) string {
	if i := strings.IndexByte(path, '.'); i >= 0 {
		return path[:i]
	}
	return path
}
