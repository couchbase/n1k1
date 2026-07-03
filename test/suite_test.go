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

// Runs the SQL++ conformance suite -- the upstream couchbase/query corpus,
// vendored under test/suite/json/default -- against n1k1. ("suite" because it's a
// data-driven set of cases stored as files, run over glue.FileStore; it isn't
// itself a test of file-store features.) Each case is {statements, results} over
// the file datastore. n1k1 supports a subset of N1QL, so this reports pass / fail
// / unsupported counts rather than 100%.
//
// Two provenances live side by side in default/cases/:
//   - case_*.json  -- the original tuqtng-era black-box corpus (test/filestore).
//   - case_gsi_*.json  -- constant-expression (no-FROM) cases imported from the
//     fork's newer test/gsi/test_cases function suites; see test/suite/
//     import_gsi_cases.py and DESIGN-testing.md. These need no dataset and
//     exercise the scalar/function expression engine. Date determinism relies on
//     the UTC pin in main_test.go.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/glue"
)

const suiteRoot = "suite/json" // corpus root for glue.FileStore; queries use default:<keyspace>.

// n1k1RunStatement runs a single statement via glue.Session and returns the
// result rows as canonical JSON strings. Any parse/plan/convert/exec error (or
// panic) is returned as err, which the harness treats as "unsupported".
func n1k1RunStatement(store *glue.Store, stmt string) (rows []string, err error) {
	rows, _, err = n1k1RunStatementCtx(store, stmt, nil)
	return rows, err
}

// n1k1RunStatementCtx is n1k1RunStatement that also returns the glue.Result, so
// callers can inspect warnings the engine recorded (Result.Warnings, e.g.
// divide-by-zero). res is nil on a parse/plan/convert/panic error.
//
// The actual engine pipeline lives in glue.Session.Run (shared with cmd/n1k1);
// this is just the rows-as-strings adapter the suite comparisons expect.
func n1k1RunStatementCtx(store *glue.Store, stmt string, named map[string]value.Value) (rows []string, res *glue.Result, err error) {
	sess := &glue.Session{Store: store, Namespace: "default", NamedArgs: named}

	res, err = sess.Run(stmt)
	if err != nil {
		return nil, nil, err
	}

	rows = make([]string, len(res.Rows))
	for i, r := range res.Rows {
		rows[i] = string(r)
	}

	return rows, res, nil
}

// caseRunnable reports whether a case is the simple {statements, results}
// shape n1k1 can attempt (not an error/match/resultset/pre-post case).
func caseRunnable(c map[string]interface{}) (stmt string, results []interface{}, ok bool) {
	for k := range c {
		switch k {
		case "statements", "results", "ordered", "description", "pretty", "sortCount", "comment", "namedArgs", "testcase", "explain", "ignore":
		default:
			return "", nil, false // exotic field -> skip
		}
	}
	s, hasStmt := c["statements"].(string)
	r, hasResults := c["results"].([]interface{})
	if !hasStmt || !hasResults {
		return "", nil, false
	}
	return s, r, true
}

// caseNamedArgs extracts a case's "namedArgs" object as query named parameters
// ($name), converting each JSON value to a value.Value. Returns nil when the
// case has none (the common path). The runner threads these into the session so
// a `WHERE x IN $inlist` NamedParameter resolves at eval time.
func caseNamedArgs(c map[string]interface{}) map[string]value.Value {
	raw, ok := c["namedArgs"].(map[string]interface{})
	if !ok || len(raw) == 0 {
		return nil
	}
	args := make(map[string]value.Value, len(raw))
	for k, v := range raw {
		args[k] = value.NewValue(v)
	}
	return args
}

// NOTE on "resultset": a few cases carried a "resultset" block instead of
// "results". No couchbase/query test harness reads that key (it compares only
// against "results"), so upstream runs the statement but never validates its
// output -- the resultset data is inert/non-authoritative. Where n1k1
// independently reproduces the resultset exactly, we promoted it to "results"
// (case_array[11], case_innerjoin[1,9]) to make it a validated regression test.
// case_array[10] is left as "resultset" (skipped): its resultset is in fact
// wrong (its array_agg values don't match the GROUP BY key, which n1k1 gets
// right), so we must NOT assert against it.

// NOTE on "sortCount": a cbq plan-optimization assertion (the number of sort
// operators the planner emits after pruning) -- inert metadata for n1k1, which
// asserts on results, not plan shape. It's whitelisted (like "description") so a
// {statements, results, sortCount} case still runs and validates its results
// (e.g. order_functions[33], an ORDER BY ... OFFSET ... LIMIT sort-prune case).
// The order_functions sortCount cases that carry NO "results" (or carry
// "explain"/"ignore") are pure plan-shape checks with nothing result-comparable,
// so they stay skipped.

// NOTE on "comment": a human note the fork attaches to some cases (an alias for
// "description"), so it's whitelisted too. This unlocks the array_functions
// `[]`/`[*]` navigation cases (which n1k1 passes); the select_functions cases it
// also unlocks stay non-pass for real feature gaps recorded in gsiExpectedNonPass
// (SELECT * EXCLUDE, UNION, comma-join, case-insensitive `name`i identifiers).

// caseError reports whether a case is a simple error-expectation case: a
// {statements, error} pair (no results) carrying only allowed metadata. n1k1
// reuses query's parser/planner, so it should reject an invalid query with the
// same error -- this turns such cases from "exotic / skipped" into runnable
// negative tests. Cases with extra fields (e.g. prepared-statement preStatements
// / positionalArgs) stay exotic.
func caseError(c map[string]interface{}) (stmt, expErr string, ok bool) {
	for k := range c {
		switch k {
		case "statements", "error", "ordered", "description", "pretty":
		default:
			return "", "", false
		}
	}
	s, hasStmt := c["statements"].(string)
	e, hasErr := c["error"].(string)
	if !hasStmt || !hasErr {
		return "", "", false
	}
	if _, hasResults := c["results"]; hasResults {
		return "", "", false
	}
	return s, e, true
}

// errMatches reports whether n1k1's error text equals the corpus's expected
// error (trimmed). Exact match is meaningful here because n1k1 surfaces query's
// own parse/plan error strings verbatim; a divergence is real signal.
func errMatches(got, want string) bool {
	return strings.TrimSpace(got) == strings.TrimSpace(want)
}

// caseErrorCode reports whether a case is a {statements, errorCode} negative
// test -- it expects the statement to be rejected. Unlike the {error}-text
// cases, we do NOT require the code to match the corpus's: n1k1 reuses query's
// parser/planner (so parse/plan codes do line up), but it has its own execution
// engine, so a runtime error needn't carry query's exact code. The test's
// intent ("this is rejected") is met by any clean (non-panic) error.
func caseErrorCode(c map[string]interface{}) (stmt string, ok bool) {
	for k := range c {
		switch k {
		case "statements", "errorCode", "ordered", "description", "pretty":
		default:
			return "", false
		}
	}
	s, hasStmt := c["statements"].(string)
	_, hasCode := c["errorCode"].(float64)
	if !hasStmt || !hasCode {
		return "", false
	}
	if _, hasResults := c["results"]; hasResults {
		return "", false
	}
	return s, true
}

// caseWarning reports whether a case is a {statements, results, warningCode}
// case: the statement succeeds AND emits a warning (e.g. divide-by-zero ->
// null + warning). We verify the rows match AND that n1k1 recorded a warning
// (n1k1 evaluates these via query's expression eval, whose warnings land in the
// GlueContext). Lenient on the code, like the errorCode cases: any warning
// counts -- it's a runtime advisory, and n1k1's engine is its own.
func caseWarning(c map[string]interface{}) (stmt string, results []interface{}, ok bool) {
	for k := range c {
		switch k {
		case "statements", "results", "warningCode", "ordered", "description", "pretty":
		default:
			return "", nil, false
		}
	}
	s, hasStmt := c["statements"].(string)
	r, hasResults := c["results"].([]interface{})
	_, hasCode := c["warningCode"].(float64)
	if !hasStmt || !hasResults || !hasCode {
		return "", nil, false
	}
	return s, r, true
}

// caseMatch reports whether a case is a {statements, matchStatements} pair: the
// two statements must yield equal results (a cross-statement equivalence check,
// e.g. "SELECT 1+1 AS result" must match "SELECT 2 AS result"). Both are run
// through n1k1 and their result multisets compared.
func caseMatch(c map[string]interface{}) (stmt, matchStmt string, ok bool) {
	for k := range c {
		switch k {
		case "statements", "matchStatements", "ordered", "description", "pretty":
		default:
			return "", "", false
		}
	}
	s, hasStmt := c["statements"].(string)
	m, hasMatch := c["matchStatements"].(string)
	if !hasStmt || !hasMatch {
		return "", "", false
	}
	return s, m, true
}

// rowsEqualStrings compares two sets of n1k1 result rows (each a JSON string) as
// multisets, canonicalizing key/element order like rowsMatch does.
func rowsEqualStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	ca := make([]string, len(a))
	for i, s := range a {
		ca[i] = canonJSON(s)
	}
	cb := make([]string, len(b))
	for i, s := range b {
		cb[i] = canonJSON(s)
	}
	sort.Strings(ca)
	sort.Strings(cb)
	for i := range ca {
		if ca[i] != cb[i] {
			return false
		}
	}
	return true
}

// TestSuiteCases runs the original tuqtng-era + imported no-FROM gsi corpus.
func TestSuiteCases(t *testing.T) {
	runSuiteCases(t, suiteRoot, expectedNonPass, groupWhy, 1041)
}

// TestGsiSuiteCases runs the data-backed gsi corpus (isolated root so its shared
// keyspaces -- customer/orders/product/purchase/review -- don't collide with the
// default corpus's own `orders`). See DESIGN-testing.md + import_gsi_data_cases.py.
func TestGsiSuiteCases(t *testing.T) {
	runSuiteCases(t, gsiSuiteRoot, gsiExpectedNonPass, gsiGroupWhy, gsiPassFloor)
}

func runSuiteCases(t *testing.T, suiteRoot string, expectedNonPass, groupWhy map[string]string, passFloor int) {
	if _, err := os.Stat(suiteRoot + "/default/cases"); err != nil {
		t.Skipf("suite corpus not present: %v", err)
	}

	store, err := glue.FileStore(suiteRoot)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}
	store.InitParser()

	files, _ := filepath.Glob(suiteRoot + "/default/cases/case_*.json")
	sort.Strings(files)

	var pass, errPass, skipped int
	var nonPass []caseOutcome // every fail/unsupported case, for classification
	var exotic []exoticCase   // every skipped (non-{statements,results}) case

	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Errorf("read %s: %v", f, err)
			continue
		}
		var cases []map[string]interface{}
		if json.Unmarshal(b, &cases) != nil {
			continue
		}

		for ci, c := range cases {
			loc := fmt.Sprintf("%s[%d]", filepath.Base(f), ci)

			// Result case: {statements, results} -- run and compare rows.
			if stmt, results, ok := caseRunnable(c); ok {
				got, _, err := n1k1RunStatementCtx(store, stmt, caseNamedArgs(c))
				switch {
				case err != nil:
					nonPass = append(nonPass, errOutcome(loc, stmt, err))
				case rowsMatch(got, results):
					pass++
				default:
					nonPass = append(nonPass, caseOutcome{loc, stmt, "FAIL", "results differ"})
				}
				continue
			}

			// Match case: {statements, matchStatements} -- run both and require
			// equal result multisets (e.g. "SELECT 1+1" must match "SELECT 2").
			if stmt, matchStmt, ok := caseMatch(c); ok {
				got, err := n1k1RunStatement(store, stmt)
				want, err2 := n1k1RunStatement(store, matchStmt)
				switch {
				case err != nil:
					nonPass = append(nonPass, errOutcome(loc, stmt, err))
				case err2 != nil:
					nonPass = append(nonPass, errOutcome(loc, matchStmt, err2))
				case rowsEqualStrings(got, want):
					pass++
				default:
					nonPass = append(nonPass, caseOutcome{loc, stmt, "FAIL", "matchStatements differ"})
				}
				continue
			}

			// Error-expectation case: {statements, error} -- n1k1 reuses query's
			// parser/planner, so it should reject invalid queries with the same
			// message. PASS iff it errors with the expected text; a mismatching
			// error (or none) is a non-pass.
			if stmt, expErr, ok := caseError(c); ok {
				_, err := n1k1RunStatement(store, stmt)
				switch {
				case err == nil:
					nonPass = append(nonPass, caseOutcome{loc, stmt, "FAIL", "expected error, got rows"})
				case errMatches(err.Error(), expErr):
					errPass++
				default:
					nonPass = append(nonPass, errOutcome(loc, stmt, err))
				}
				continue
			}

			// Error-code negative test: {statements, errorCode} -- expects a
			// failure. PASS on any clean (non-panic) error; the code needn't
			// match query's (see caseErrorCode). A panic is still a PANIC bug,
			// and returning rows is a FAIL (it should have been rejected).
			if stmt, ok := caseErrorCode(c); ok {
				_, err := n1k1RunStatement(store, stmt)
				switch {
				case err == nil:
					nonPass = append(nonPass, caseOutcome{loc, stmt, "FAIL", "expected error, got rows"})
				case isPanicErr(err):
					nonPass = append(nonPass, errOutcome(loc, stmt, err))
				default:
					errPass++
				}
				continue
			}

			// Warning case: {statements, results, warningCode} -- succeeds with a
			// warning. PASS when rows match AND n1k1 recorded a warning (code not
			// required to match -- a runtime advisory; see caseWarning).
			if stmt, results, ok := caseWarning(c); ok {
				got, res, err := n1k1RunStatementCtx(store, stmt, caseNamedArgs(c))
				switch {
				case err != nil:
					nonPass = append(nonPass, errOutcome(loc, stmt, err))
				case !rowsMatch(got, results):
					nonPass = append(nonPass, caseOutcome{loc, stmt, "FAIL", "results differ"})
				case res == nil || len(res.Warnings) == 0:
					nonPass = append(nonPass, caseOutcome{loc, stmt, "FAIL", "expected a warning, got none"})
				default:
					pass++ // rows matched and a warning was emitted
				}
				continue
			}

			// Exotic: anything else (resultset/prepared/pre-post/etc.).
			skipped++
			reason, content := exoticInfo(c)
			exotic = append(exotic, exoticCase{loc, reason, content})
		}
	}

	reportSuite(t, len(files), pass, errPass, skipped, nonPass, exotic, expectedNonPass, groupWhy, passFloor)
}

// caseOutcome records one non-passing corpus case.
type caseOutcome struct {
	loc, stmt, status, detail string
}

// errOutcome builds the non-pass outcome for a case that returned an error,
// distinguishing a PANIC (the engine should never panic -- it's a bug) from a
// plain UNSUPPORTED (a plan/feature n1k1 doesn't handle). The panic text comes
// through as an error because n1k1RunStatement (and query's parser) recover.
func errOutcome(loc, stmt string, err error) caseOutcome {
	status := "UNSUPPORTED"
	if isPanicErr(err) {
		status = "PANIC"
	}
	return caseOutcome{loc, stmt, status, oneLine(err.Error())}
}

// isPanicErr reports whether an error came from a recovered panic (rather than a
// deliberate error return). Panics surface as: our own "panic: ..." wrapper,
// query's "Error while parsing: runtime error: ...", or the raw runtime-panic
// strings ("interface conversion:", "invalid memory address", "nil pointer").
func isPanicErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, marker := range []string{
		"panic:", "runtime error:", "interface conversion:",
		"invalid memory address", "nil pointer",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

// exoticCase records one corpus case skipped by caseRunnable -- i.e. not the
// plain {statements, results} shape n1k1 attempts (it carries error/match/
// resultset/pre-post or a non-string statement). reason says why it was
// skipped; content is its full statement text (or whole-case JSON if there's
// no string statement).
type exoticCase struct {
	loc, reason, content string
}

// reportSuite prints a readable summary + a grouped table of the expected
// non-pass cases, then enforces two guards: any UNEXPECTED non-pass (one not in
// expectedNonPass) is a regression and fails the test; a stale table entry (a
// listed case that now passes) is warned about so it can be removed.
func reportSuite(t *testing.T, nFiles, pass, errPass, skipped int, nonPass []caseOutcome, exotic []exoticCase,
	expectedNonPass, groupWhy map[string]string, passFloor int) {
	groupCount := map[string]int{}
	seen := map[string]bool{}
	var unexpected []caseOutcome
	var panics []caseOutcome
	var fail, unsupported, panicked int

	for _, o := range nonPass {
		switch o.status {
		case "FAIL":
			fail++
		case "PANIC":
			panicked++
			panics = append(panics, o)
		default:
			unsupported++
		}
		g, ok := expectedNonPass[o.loc]
		if !ok {
			unexpected = append(unexpected, o)
			continue
		}
		groupCount[g]++
		seen[o.loc] = true
	}

	var stale []string
	for loc := range expectedNonPass {
		if !seen[loc] {
			stale = append(stale, loc)
		}
	}
	sort.Strings(stale)

	total := pass + errPass + fail + unsupported + panicked

	// valW is the width of the widest count, so the value column right-aligns
	// (tabwriter left-aligns each cell, so we right-justify the digits ourselves
	// to a shared width rather than use whole-table AlignRight, which would also
	// right-align the label/text columns).
	valW := 1
	for _, n := range []int{nFiles, total, pass, errPass, unsupported, fail, panicked, skipped} {
		if w := len(strconv.Itoa(n)); w > valW {
			valW = w
		}
	}

	var b strings.Builder

	// Per-case listing of every non-pass case (in corpus order): status
	// (UNSUPPORTED/FAIL) and category first, then loc and the SQL++. The SQL is
	// shown in full (it's the last column, so tabwriter leaves it unpadded) so
	// it's clear exactly what n1k1 can't yet handle -- except EXPLAIN cases, which
	// only differ by the wrapped query (n1k1 doesn't convert the plan-text output
	// at all), so those get just a snippet.
	fmt.Fprintf(&b, "\nsuite non-pass cases (%d):\n", len(nonPass))
	tw := tabwriter.NewWriter(&b, 0, 2, 2, ' ', 0)
	for _, o := range nonPass {
		g, ok := expectedNonPass[o.loc]
		if !ok {
			g = "UNEXPECTED"
		}
		sql := fullLine(o.stmt)
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(o.stmt)), "EXPLAIN") {
			sql = oneLine(o.stmt)
		}
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", o.status, g, o.loc, sql)
	}
	tw.Flush()

	// PANICS: the engine should never panic -- these are bugs, not merely
	// unsupported features, so surface them prominently with the panic message
	// (which the table above omits). They're still tracked in expectedNonPass so
	// the build stays green, but they must not hide inside the UNSUPPORTED count.
	if len(panics) > 0 {
		fmt.Fprintf(&b, "\n!! PANICS (%d) -- the engine should never panic; these are bugs to fix:\n", len(panics))
		tw = tabwriter.NewWriter(&b, 0, 2, 2, ' ', 0)
		for _, o := range panics {
			fmt.Fprintf(tw, "  %s\t%s\n", o.loc, o.detail)
			fmt.Fprintf(tw, "  \t%s\n", fullLine(o.stmt))
		}
		tw.Flush()
	}

	// Exotic (skipped) cases: not a {statements, results/error/matchStatements}
	// shape, so n1k1 doesn't attempt them. Show why + the full statement/content.
	fmt.Fprintf(&b, "\nskipped cases (%d):\n", len(exotic))
	tw = tabwriter.NewWriter(&b, 0, 2, 2, ' ', 0)
	for _, e := range exotic {
		fmt.Fprintf(tw, "  %s\t%s\t%s\n", e.loc, e.reason, e.content)
	}
	tw.Flush()

	b.WriteString("\nsuite conformance\n=================\n")
	tw = tabwriter.NewWriter(&b, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "  suite # files         \t%*d\n", valW, nFiles)
	fmt.Fprintf(tw, "  suite # cases         \t%*d\n", valW, total)
	// A conformance PASS is "n1k1 behaved as the corpus expects": either it
	// produced the expected rows (pass), or it correctly errored on an
	// error-expecting case (errPass). Both belong in the pass-rate numerator.
	passing := pass + errPass
	fmt.Fprintf(tw, "    PASS (results)      \t%*d\n", valW, pass)
	fmt.Fprintf(tw, "    PASS (err expected) \t%*d\n", valW, errPass)
	fmt.Fprintf(tw, "    PASS (total)        \t%*d\t(%.1f%%)\n", valW, passing, 100*float64(passing)/float64(total))
	fmt.Fprintf(tw, "    PANIC               \t%*d\n", valW, panicked)
	fmt.Fprintf(tw, "    non-pass UNSUPPORTED\t%*d\n", valW, unsupported)
	fmt.Fprintf(tw, "    non-pass FAIL       \t%*d\n", valW, fail)
	fmt.Fprintf(tw, "    skipped             \t%*d\n", valW, skipped)
	tw.Flush()

	// Grouped breakdown of the expected non-pass cases, most-common first.
	type gc struct {
		group string
		count int
	}
	var rows []gc
	for g, c := range groupCount {
		rows = append(rows, gc{g, c})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].count != rows[j].count {
			return rows[i].count > rows[j].count
		}
		return rows[i].group < rows[j].group
	})

	// cw right-justifies the COUNT column (header, rule, and counts) to a shared
	// width so the numbers right-align under the header.
	cw := len("COUNT")
	for _, r := range rows {
		if w := len(strconv.Itoa(r.count)); w > cw {
			cw = w
		}
	}

	fmt.Fprintf(&b, "\nsuite non-pass by group (%d cases; shrink as coverage grows):\n",
		len(expectedNonPass))
	tw = tabwriter.NewWriter(&b, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "  %*s\tGROUP\tWHY\n", cw, "COUNT")
	fmt.Fprintf(tw, "  %s\t-----\t---\n", strings.Repeat("-", cw))
	for _, r := range rows {
		fmt.Fprintf(tw, "  %*d\t%s\t%s\n", cw, r.count, r.group, groupWhy[r.group])
	}
	tw.Flush()

	t.Log(b.String())

	// Panics are engine bugs. Warn loudly (even when expected/green) so they
	// stay on the radar rather than blending into the UNSUPPORTED count.
	if panicked > 0 {
		t.Logf("WARNING: %d case(s) PANIC the engine (see the PANICS section above) -- these are bugs to fix",
			panicked)
	}

	// Hygiene: a listed case that no longer fails was likely fixed -- nudge to
	// remove it. Kept a warning (not a failure) so a fix never breaks the build.
	if len(stale) > 0 {
		t.Logf("expectedNonPass has %d stale entr(y/ies) -- now passing or absent, please remove:\n  %s",
			len(stale), strings.Join(stale, "\n  "))
	}

	// Regression: a non-pass case not in the expected table is a new break.
	if len(unexpected) > 0 {
		lines := make([]string, 0, len(unexpected))
		for _, o := range unexpected {
			lines = append(lines, fmt.Sprintf("%s [%s] %s -- %s",
				o.loc, o.status, oneLine(o.stmt), o.detail))
		}
		sort.Strings(lines)
		t.Errorf("%d UNEXPECTED non-pass case(s) (regression -- fix it, or add to expectedNonPass):\n  %s",
			len(unexpected), strings.Join(lines, "\n  "))
	}

	// Backstop on the raw pass count, in case a pass silently turns into a
	// different already-listed failure (no unexpected case, but pass drops).
	if pass < passFloor {
		t.Errorf("suite conformance regressed: PASS=%d < baseline %d", pass, passFloor)
	}
}

// oneLine collapses internal whitespace and truncates, for compact log rows.
func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 100 {
		s = s[:97] + "..."
	}
	return s
}

// fullLine collapses internal whitespace but does NOT truncate -- for showing a
// query's complete text on one tidy log line.
func fullLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// exoticInfo explains why a case was skipped by caseRunnable and returns its
// full content. The reason lists any non-{statements,results,ordered,
// description} fields present (sorted, for stable output), and/or notes a
// missing/typed statements/results. The content prefers the (full) statements
// text, falling back to the whole-case JSON.
func exoticInfo(c map[string]interface{}) (reason, content string) {
	var extra []string
	for k := range c {
		switch k {
		// Fields the harness handles (so they're not what makes a case exotic).
		// "resultset" is deliberately omitted -- it's non-authoritative (see the
		// NOTE by caseRunnable), so it should surface as the exotic reason.
		case "statements", "results", "error", "errorCode", "warningCode",
			"matchStatements", "ordered", "description", "pretty", "sortCount", "comment":
		default:
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)

	var reasons []string
	if len(extra) > 0 {
		reasons = append(reasons, "fields: "+strings.Join(extra, ","))
	}
	if _, ok := c["statements"].(string); !ok {
		if _, present := c["statements"]; present {
			reasons = append(reasons, "non-string statements")
		} else {
			reasons = append(reasons, "no statements")
		}
	} else if _, ok := c["results"].([]interface{}); !ok {
		reasons = append(reasons, "no results array")
	}
	reason = strings.Join(reasons, "; ")

	if s, ok := c["statements"].(string); ok {
		content = fullLine(s)
	} else if v, ok := c["statements"]; ok {
		bb, _ := json.Marshal(v)
		content = fullLine(string(bb))
	} else {
		bb, _ := json.Marshal(c)
		content = fullLine(string(bb))
	}
	return reason, content
}

// expectedNonPass enumerates every corpus case n1k1 does not currently pass:
// the UNSUPPORTED plans/features it can't yet convert, the lone FAIL whose
// result ordering N1QL leaves undefined, and any error-expectation case whose
// error text n1k1 doesn't reproduce. Each maps to a group key explained by
// groupWhy below. This is the accepted baseline -- the test fails if a case NOT
// listed here stops passing (a regression) and warns if a listed case starts
// passing (stale -- remove it). Shrink this table as coverage grows.
var expectedNonPass = map[string]string{
	// Huge generator builtins (engine refuses; upstream errors too).
	"case_func_array.json[58]": "resource-guard",
	"case_func_array.json[59]": "resource-guard",
	"case_func_date.json[84]":  "resource-guard",
	"case_func_str.json[48]":   "resource-guard",

	// system: namespace (needs a systemstore, intentionally nil).
	"case_system_completed.json[0]": "system-namespace",
	"case_system_completed.json[1]": "system-namespace",
	"case_system_completed.json[2]": "system-namespace",
	"case_system_prepareds.json[1]": "system-namespace",
	"case_system_prepareds.json[3]": "system-namespace",
	"case_system_prepareds.json[4]": "system-namespace",

	// Prepared-statement EXECUTE.
	"case_system_prepareds.json[0]": "prepared", // PREPARE ... (rejected in PlanStatementQP)
	"case_system_prepareds.json[2]": "prepared",
	"case_prepare.json[4]":          "prepared",

	// The one FAIL: ARRAY_AGG element order is undefined in N1QL.
	"case_func_array.json[34]": "arrayagg-order",
}

// groupWhy gives a one-line reason for each expectedNonPass group.
var groupWhy = map[string]string{
	"resource-guard":   "engine refuses huge generator builtins (ARRAY_RANGE/REPEAT ~1e10)",
	"system-namespace": "system: namespace needs a systemstore (intentionally nil; see FileStore)",
	"prepared":         "prepared statements not supported (no prepared-statement store): PREPARE is rejected in PlanStatementQP -- planner.Build nil-derefs on it; EXECUTE (plan.Discard) also unsupported",
	"arrayagg-order":   "ARRAY_AGG element order is undefined in N1QL; ordering differs (not fixable)",
}
