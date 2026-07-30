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

import (
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// multi_fix.go centralizes the AUTHOR-facing fix snippets used across the .multi
// command family. Every status an author sees -- a rejected / standalone / always-wake
// / boxed entry, an unresolved logical keyspace, a fixture FAIL or a fixture with no
// golden -- carries not just WHAT it means but HOW to fix it, with a one-line example,
// so a tech-support engineer (or an AI agent) never has to reason it out. Keeping the
// text in one keyed helper means run / lint / test all speak with one voice.

// Fix-snippet situations -- the keys into multiFix.
const (
	fixRejected    = "rejected"     // parse/plan/convert failed: it never runs, so it can never fire.
	fixStandalone  = "standalone"   // valid but not fused into the shared scan (own scan).
	fixAlwaysWake  = "always-wake"  // fused but no discriminating literal: wakes on every row.
	fixBoxed       = "boxed"        // an expression falls back to cbq: caps the compile level (and, if it's the gating predicate, forfeits pruning).
	fixUnresolved  = "unresolved"   // a bound logical keyspace matched 0 files (a bundle gap).
	fixFixtureFail = "fixture-fail" // a golden-fixture diff mismatched.
	fixNoGolden    = "no-golden"    // a fixture with no @expect recorded.
)

// multiFix returns the one-line fix snippet for a situation: what it means and how to
// fix it, with a mini example. detail is the situation-specific fill-in (a reject/
// standalone reason, or the logical keyspace name); it is ignored where none applies.
func multiFix(situation, detail string) string {
	switch situation {
	case fixRejected:
		msg := "not a runnable query"
		if detail != "" {
			msg += ": " + detail
		}
		return msg + ". A query is a single SELECT, e.g. `SELECT x.msg FROM logs x WHERE x.sev = \"ERROR\"`."
	case fixStandalone:
		msg := "runs standalone -- its own scan, not fused into the shared scan"
		if detail != "" {
			msg += " (" + detail + ")"
		}
		return msg + ". That's fine; only single-source filter+project queries fuse."
	case fixAlwaysWake:
		return "evaluated on every row -- no discriminating literal. Add one as a top-level AND conjunct, " +
			"e.g. `... AND msg LIKE '%panic%'` (or `regexp_contains(msg,'panic')`)."
	case fixBoxed:
		// detail == "indexed": the entry STILL fuses and index-prunes on its
		// predicate literal -- the boxed expression is in the projection (or a
		// non-gating conjunct), so boxing is a per-row cbq cost on woken rows, NOT a
		// lost scan or lost pruning. Saying otherwise (the old text did) wrongly pushes
		// authors to rearchitect a query that's already scan-optimal.
		if detail == "indexed" {
			return "a boxed expression (e.g. a UDF in the SELECT list) evaluates via cbq per row and caps the " +
				"compile level -- but the scan still fuses and index-prunes on its literal, so this is a per-row " +
				"projection cost on woken rows, not a scan cost. Leave it inline, or move it to a downstream rollup " +
				"only if that per-row cost matters."
		}
		// Not index-pruned: the boxed expression is (or includes) the discriminating
		// predicate, so the query also can't index-prune. Prefer a native predicate.
		return "a boxed expression falls back to cbq -- caps the compile level, and a boxed WHERE also can't be " +
			"index-pruned. Prefer a native form, e.g. replace a multi-wildcard `msg LIKE '%a%b%'` with " +
			"`regexp_contains(msg,'a.*b')` (a plain `msg LIKE '%lit%'` and CONTAINS are already native)."
	case fixUnresolved:
		return "logical keyspace `" + detail + "` matched 0 files in this bundle -- a GAP, not a clean result. " +
			"Check the manifest glob, e.g. `" + detail + " = **/" + detail + "*.log`."
	case fixFixtureFail:
		return "if this change is intended, re-record the golden: `.multi test --update`."
	case fixNoGolden:
		return "fixture has no expected labelResults recorded. Capture them: `.multi test --update`, then review + commit."
	}
	return ""
}

// lintAdvice builds an entry's advice cell for the lint report card from its
// EntryLint verdict, using the centralized fix snippets. A rejected/standalone
// entry gets the shape advice; a fused-but-always-wake one gets the discriminating-
// literal nudge; and a boxed lane (on any converted entry) gets the native-form
// nudge. Multiple nudges join with "; ". Returns "" for a clean fused+native+indexed
// entry (nothing to say -- don't bloat the PASS row).
func lintAdvice(d glue.EntryLint) string {
	var adv []string
	switch d.Class {
	case glue.LintRejected:
		adv = append(adv, multiFix(fixRejected, d.Reason))
	case glue.LintStandalone:
		adv = append(adv, multiFix(fixStandalone, d.Reason))
	case glue.LintFused:
		if !d.Indexed {
			adv = append(adv, multiFix(fixAlwaysWake, ""))
		}
	}
	if d.Lane == "boxed" {
		// An index-pruned boxed entry (a UDF/boxed expr in the projection, not the
		// gating predicate) keeps its fused, pruned scan -- say so, rather than the
		// blanket "can't be index-pruned" that wrongly reads as a scan penalty.
		boxDetail := ""
		if d.Indexed {
			boxDetail = "indexed"
		}
		adv = append(adv, multiFix(fixBoxed, boxDetail))
	}
	return strings.Join(adv, "; ")
}
