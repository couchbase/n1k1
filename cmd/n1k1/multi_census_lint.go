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

package main

// `.multi lint --census` — DATA-AWARE lint, the queries↔data JOIN (DESIGN-census.md
// Phase 2; was the `.multi doctor` verb). For each entry, cross-reference the field
// paths it references (planner-sourced) against a top-level field inventory of the
// keyspace it scans, and report:
//
//   - references_absent : an entry reads a top-level field the data doesn't have
//     -> a birth-in-error / typo / renamed-or-retired field. Hard-fails (CI signal).
//   - unreferenced      : data fields no query reads -> unexplored surface, a
//     query-generation queue (informational).
//
// The referenced set is planner-sourced (glue.EntryReferencedFields via ExprFieldPath),
// never a text heuristic. The check is at TOP-LEVEL granularity: high precision, and
// it catches the class that cost the most — a query aimed at a field that never existed.
//
// The inventory is computed by a tiny inline SQL++ (UNNEST OBJECT_NAMES), NOT by any
// census implementation — deliberately: the census is now the forkable
// builtin:census.sql++ / census_agg.agg.js, and lint must keep working however a user
// mutates their census fork. Lint asks the DATA what fields exist, not a census
// artifact. (`--census` keeps its flag name; read it as "check against the data".)
// `_meta` is skipped to match the census convention: engine provenance, not schema.

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/couchbase/n1k1/glue"
)

// lintCensus is the `--census` tier of `.multi lint`: the caller (cmdMultiLint) has
// already loaded the entries and built the session (binding resolved, no gap).
func (c *cli) lintCensus(dets []glue.MultiQueryEntry, sess *glue.Session) {
	// Inventory each distinct keyspace once; the top-level field set is what we join
	// against. Plain SQL++ over the data (see the file comment for why not a census).
	censusTop := map[string]map[string]bool{} // keyspace -> set of top-level fields present
	topLevels := func(ks string) (map[string]bool, error) {
		if s, ok := censusTop[ks]; ok {
			return s, nil
		}
		res, cerr := sess.Run("SELECT DISTINCT RAW p FROM `" + ks + "` AS r " +
			`UNNEST OBJECT_NAMES(r) AS p WHERE p != "_meta"`)
		if cerr != nil {
			return nil, cerr
		}
		set := map[string]bool{}
		for _, row := range res.Rows {
			var f string
			if json.Unmarshal(row, &f) == nil && f != "" {
				set[f] = true
			}
		}
		censusTop[ks] = set
		return set, nil
	}

	type check struct {
		Query    string   `json:"query"`
		Keyspace string   `json:"keyspace"`
		Absent   []string `json:"references_absent,omitempty"`
	}
	var checks []check
	var skipped []string
	referencedByKS := map[string]map[string]bool{} // keyspace -> top-level fields any query reads
	anyAbsent := false

	for _, d := range dets {
		ks, paths, ok, ferr := sess.EntryReferencedFields(d.Stmt)
		if ferr != nil || !ok {
			skipped = append(skipped, d.Label) // multi-source / non-analyzable / parse-rejected
			continue
		}
		present, cerr := topLevels(ks)
		if cerr != nil {
			fmt.Fprintf(c.stderr, "%s: .multi lint --census: census %q: %v\n", c.prog, ks, cerr)
			c.failed = true
			return
		}
		if referencedByKS[ks] == nil {
			referencedByKS[ks] = map[string]bool{}
		}
		var absent []string
		for _, p := range paths {
			tl := glue.TopLevelField(p)
			referencedByKS[ks][tl] = true
			if !present[tl] {
				absent = append(absent, tl)
			}
		}
		absent = dedupSorted(absent)
		if len(absent) > 0 {
			anyAbsent = true
		}
		checks = append(checks, check{Query: d.Label, Keyspace: ks, Absent: absent})
	}

	// Unexplored surface: data top-level fields no query references, per keyspace.
	unreferenced := map[string][]string{}
	for ks, present := range censusTop {
		var un []string
		for f := range present {
			if !referencedByKS[ks][f] {
				un = append(un, f)
			}
		}
		sort.Strings(un)
		if len(un) > 0 {
			unreferenced[ks] = un
		}
	}

	c.printJSON(struct {
		Checks       []check             `json:"checks"`
		Unreferenced map[string][]string `json:"unreferenced,omitempty"`
		Skipped      []string            `json:"skipped,omitempty"`
		OK           bool                `json:"ok"`
	}{Checks: checks, Unreferenced: unreferenced, Skipped: skipped, OK: !anyAbsent})

	if anyAbsent {
		fmt.Fprintf(c.stderr, "%s: .multi lint --census: a query references a field absent from the data "+
			"(a birth-in-error / rename) -- see references_absent above\n", c.prog)
		c.failed = true
	}
}

func dedupSorted(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
