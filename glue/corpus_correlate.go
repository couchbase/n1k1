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

// Correlation-detector recognition + grouping -- the FOUNDATION of Part B of the shared
// sorted-stream substrate (DESIGN-sorting.md, "correlation consumers on the shared
// substrate"). A temporal cross-keyspace detector ("XYZ in log1, then ABC in log2") is a
// correlated argmax subquery (the ASOF shape, MatchArgmaxAsof); K such detectors over the
// SAME (left keyspace, right keyspace, time key, direction) all sort+scan those two
// keyspaces separately today. This recognizer groups them by that signature so the
// sharing opportunity is visible now and the corpus can, in a later slice, feed the group
// from ONE shared sorted materialization of each keyspace (see the design note's Part B
// execution sub-slices). This slice does NOT change execution -- correlation detectors
// still run standalone; it surfaces the grouping (CompiledCorpus.CorrelationGroups + the
// `.rules run` report).

import (
	"strings"

	"github.com/couchbase/query/algebra"
)

// analyzeCorrelationDetector recognizes a temporal-correlation detector (a projected
// correlated argmax subquery, the ASOF shape) purely from its parsed algebra -- no plan
// or convert needed, and independent of whether it is ASOF-lowerable (that needs sorted-
// source metadata). It returns the sharing SIGNATURE: the outer (left/probe) keyspace, the
// subquery (right/build) keyspace, the correlation key field, and the direction. Detectors
// with the same signature scan+sort the same two keyspaces the same way, so they can share
// that work. ok=false for a non-correlation detector.
func (s *Session) analyzeCorrelationDetector(stmt string) (sig string, ok bool) {
	parsed, err := ParseStatement(stmt, s.Namespace, true)
	if err != nil {
		return "", false
	}
	sel, ok := parsed.(*algebra.Select)
	if !ok {
		return "", false
	}
	ss, ok := sel.Subresult().(*algebra.Subselect)
	if !ok || ss.Projection() == nil {
		return "", false
	}
	outerKS, ok := fromKeyspaceName(ss.From())
	if !ok {
		return "", false
	}

	// A projection term carrying a recognized argmax subquery is the correlation.
	for _, pt := range ss.Projection().Terms() {
		if pt.Expression() == nil {
			continue
		}
		for _, subq := range collectSubqueries(pt.Expression()) {
			m, matched := MatchArgmaxAsof(subq)
			if !matched {
				continue
			}
			subSS, ok := subq.Select().Subresult().(*algebra.Subselect)
			if !ok {
				continue
			}
			subKS, ok := fromKeyspaceName(subSS.From())
			if !ok {
				continue
			}
			return outerKS + "\x00" + subKS + "\x00" + m.KeyField + "\x00" + m.Direction, true
		}
	}
	return "", false
}

// fromKeyspaceName returns the qualified keyspace path of a single plain-keyspace FROM
// term (the correlation grouping key), or ok=false for a join / derived-table / expression
// source (not a plain-keyspace correlation). An unqualified keyspace (e.g. a recipe-framed
// `FROM memcached r`) parses to an *algebra.ExpressionTerm that WRAPS a KeyspaceTerm
// (isKeyspace), not a bare KeyspaceTerm -- so unwrap that too, else the correlation is
// unrecognized and the shared-scan cache never fires (real bundles use unqualified names).
func fromKeyspaceName(from algebra.FromTerm) (string, bool) {
	var kt *algebra.KeyspaceTerm
	switch t := from.(type) {
	case *algebra.KeyspaceTerm:
		kt = t
	case *algebra.ExpressionTerm:
		kt = t.KeyspaceTerm() // non-nil iff the FROM expr is a keyspace reference.
	}
	if kt != nil {
		// PathString backtick-quotes each part (`default`:`errors`); strip the quotes so
		// it matches a keyspace's QualifiedName() ("default:errors"), which the scan
		// cache keys on.
		if p := strings.ReplaceAll(kt.PathString(), "`", ""); p != "" {
			return p, true
		}
	}
	return "", false
}
