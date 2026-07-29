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

// `.multi doctor` — the pack↔census JOIN (DESIGN-census.md Phase 2). For each
// detector, cross-reference the field paths it references (planner-sourced) against
// a census of the keyspace it scans, and report:
//
//   - references_absent : a detector reads a top-level field the corpus doesn't have
//     -> a birth-in-error / typo / renamed-or-retired field. Hard-fails (CI signal).
//   - unreferenced      : corpus fields no detector reads -> unexplored surface, a
//     detector-generation queue (informational).
//
// The referenced set is planner-sourced (glue.EntryReferencedFields via ExprFieldPath),
// never a text heuristic. First check is at TOP-LEVEL granularity: high precision (no
// false positives against a depth-limited census), and it catches the class that cost
// the most — a detector aimed at a field that never existed.

import (
	"fmt"
	"sort"

	"github.com/couchbase/n1k1/glue"
)

func (c *cli) cmdMultiDoctor(arg string) {
	args, err := parseMultiArgs(arg) // --queries <dir>... [--bind <manifest>]
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi doctor: %v\n", c.prog, err)
		c.failed = true
		return
	}
	dets, err := loadMultiQueryEntries(args.queries)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi doctor: %v\n", c.prog, err)
		c.failed = true
		return
	}
	sess, binding, err := c.multiSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi doctor: %v\n", c.prog, err)
		c.failed = true
		return
	}
	if gap := c.reportBindingCoverage(sess, binding); gap {
		fmt.Fprintf(c.stderr, "%s: .multi doctor: aborting -- unresolved logical keyspace(s) above\n", c.prog)
		c.failed = true
		return
	}

	// Census each distinct keyspace once; the top-level field set is what we join
	// against (present iff some census path has that first segment).
	censusTop := map[string]map[string]bool{} // keyspace -> set of top-level fields present
	topLevels := func(ks string) (map[string]bool, error) {
		if s, ok := censusTop[ks]; ok {
			return s, nil
		}
		res, cerr := sess.Census(ks, glue.CensusOptions{})
		if cerr != nil {
			return nil, cerr
		}
		set := map[string]bool{}
		for _, r := range res.Rows {
			set[glue.TopLevelField(r.Path)] = true
		}
		censusTop[ks] = set
		return set, nil
	}

	type check struct {
		Detector string   `json:"detector"`
		Keyspace string   `json:"keyspace"`
		Absent   []string `json:"references_absent,omitempty"`
	}
	var checks []check
	var skipped []string
	referencedByKS := map[string]map[string]bool{} // keyspace -> top-level fields any detector reads
	anyAbsent := false

	for _, d := range dets {
		ks, paths, ok, ferr := sess.EntryReferencedFields(d.Stmt)
		if ferr != nil || !ok {
			skipped = append(skipped, d.Label) // multi-source / non-analyzable / parse-rejected
			continue
		}
		present, cerr := topLevels(ks)
		if cerr != nil {
			fmt.Fprintf(c.stderr, "%s: .multi doctor: census %q: %v\n", c.prog, ks, cerr)
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
		checks = append(checks, check{Detector: d.Label, Keyspace: ks, Absent: absent})
	}

	// Unexplored surface: corpus top-level fields no detector references, per keyspace.
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
		fmt.Fprintf(c.stderr, "%s: .multi doctor: a detector references a field absent from the corpus "+
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
