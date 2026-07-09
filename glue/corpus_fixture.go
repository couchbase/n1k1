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

// corpus_fixture.go runs a recipe's GOLDEN FIXTURE (DESIGN-prepare.md phase 7,
// "a golden-fixture diff ... is the detector's unit test"). It builds a throwaway
// single-keyspace datastore from the fixture's input rows, runs JUST that one detector
// over it (through the exact CorpusCompile -> Run path .detect run uses, so the produced
// findings' shape matches a real run), and returns the findings for the caller to diff
// against the recipe's @expect golden (DiffFindings) or record as the new golden
// (RewriteExpect + --update).
//
// MVP SCOPE (matches corpus_recipe.go's deferred list): the fixture feeds the
// detector's SINGLE `source` keyspace. A detector that also reads a second keyspace
// (a join / correlated subquery over another keyspace) can't be fixtured yet; RunFixture
// surfaces that as ErrFixtureUnresolved so the runner can SKIP it with a clear note
// rather than counting a spurious failure.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrFixtureUnresolved reports that a fixture run could not resolve a keyspace the
// detector references -- almost always because the detector reads a SECOND keyspace the
// single-source fixture doesn't provide (multi-source fixtures are a deferred MVP gap),
// or because the recipe's `source` doesn't match the statement's FROM. The runner treats
// it as a SKIP (with this message), not a hard failure.
type ErrFixtureUnresolved struct{ Reason string }

func (e *ErrFixtureUnresolved) Error() string {
	return "fixture keyspace unresolved (multi-source fixture unsupported (MVP), or source mismatch): " + e.Reason
}

// RunFixture materializes the recipe's fixture rows as a temporary single-keyspace
// datastore named after Source, runs the detector over it, and returns the tagged
// findings. It requires HasFixture and a non-empty Source (a fixture with no `source`
// front-matter can't be placed into a keyspace). The temp datastore is removed before
// returning. Findings order is not guaranteed (compare as a set -- DiffFindings).
func (r *Recipe) RunFixture() ([]Finding, error) {
	if !r.HasFixture {
		return nil, fmt.Errorf("recipe %q has no @fixture", r.Tag)
	}
	if strings.TrimSpace(r.Source) == "" {
		return nil, fmt.Errorf("recipe %q has a fixture but no `source:` front-matter "+
			"(can't place fixture rows into a keyspace)", r.Tag)
	}

	tmp, err := os.MkdirTemp("", "n1k1fixture")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)

	// Lay the fixture out as <tmp>/default/<source>/rows.jsonl so the file datastore
	// exposes it as the keyspace <source> in the "default" namespace (the same
	// <ns>/<keyspace>/ convention the CLI's bundle dirs use).
	ksDir := filepath.Join(tmp, "default", r.Source)
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		return nil, err
	}
	var jsonl strings.Builder
	for _, row := range r.Fixture.Rows {
		jsonl.Write(row)
		jsonl.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(ksDir, "rows.jsonl"), []byte(jsonl.String()), 0o644); err != nil {
		return nil, err
	}

	sess, err := OpenSession(tmp, "default")
	if err != nil {
		return nil, err
	}

	cc, err := sess.CorpusCompile([]CorpusDetector{r.AsDetector()})
	if err != nil {
		return nil, err
	}
	// A REJECTED detector (parse/plan/convert failed) runs to ZERO findings, which would
	// otherwise masquerade as a clean pass -- exactly the "rejected -> no findings" lie
	// the report card guards against. Surface it as a hard error so the fixture FAILS.
	if len(cc.Rejected) > 0 {
		return nil, fmt.Errorf("detector rejected (never runs): %s", cc.Rejected[0].Reason)
	}
	findings, err := cc.Run()
	if err != nil {
		// A keyspace-resolution failure means the detector reaches for a keyspace the
		// single-source fixture doesn't provide (deferred multi-source) -- reclassify it
		// as a SKIP so the runner doesn't count a false failure.
		if looksLikeMissingKeyspace(err, r.Source) {
			return nil, &ErrFixtureUnresolved{Reason: err.Error()}
		}
		return nil, err
	}
	return findings, nil
}

// looksLikeMissingKeyspace heuristically recognizes a "keyspace not found" plan/run
// error (the fork's phrasing varies), used to reclassify a multi-source fixture run as a
// skip. It errs toward NOT matching (a real detector bug should still FAIL): it requires
// a recognizably keyspace-resolution phrase.
func looksLikeMissingKeyspace(err error, source string) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, phrase := range []string{
		"keyspace not found",
		"no keyspace",
		"bucket not found",
		"datastore : keyspace",
		"not found in cb datastore",
	} {
		if strings.Contains(msg, phrase) {
			return true
		}
	}
	return false
}
