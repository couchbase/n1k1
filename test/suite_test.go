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

// Runs the SQL++ conformance suite -- the upstream couchbase/query corpus (from
// their test/filestore tests), vendored under test/suite/json/default -- against
// n1k1. ("suite" because it's a data-driven set of cases stored as files, run
// over glue.FileStore; it isn't itself a test of file-store features.) Each case
// is {statements, results} over the file datastore. n1k1 supports a subset of
// N1QL, so this reports pass / fail / unsupported counts rather than 100%.

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
	"time"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
	"github.com/couchbase/n1k1/glue"
)

const suiteRoot = "suite/json" // corpus root for glue.FileStore; queries use default:<keyspace>.

// n1k1RunStatement parses, plans, converts and executes a single statement
// through n1k1's own operators, returning the result rows as canonical JSON
// strings. Any parse/plan/convert/exec error (or panic) is returned as err,
// which the harness treats as "unsupported".
func n1k1RunStatement(store *glue.Store, stmt string) (rows []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			rows, err = nil, fmt.Errorf("panic: %v", r)
		}
	}()

	s, err := glue.ParseStatement(stmt, "default", true)
	if err != nil {
		return nil, err
	}

	p, err := store.PlanStatement(s, "default", nil, nil)
	if err != nil {
		return nil, err
	}

	conv := &glue.Conv{Temps: []interface{}{nil}}
	if _, err = p.Accept(conv); err != nil {
		return nil, err
	}
	if conv.TopOp == nil {
		return nil, fmt.Errorf("nil TopOp (unsupported plan)")
	}

	cv, err := glue.NewConvertVals(conv.TopOp.Labels)
	if err != nil {
		return nil, err
	}

	if engine.ExprCatalog["exprStr"] == nil {
		engine.ExprCatalog["exprStr"] = glue.ExprStr
	}
	if engine.ExprCatalog["exprTree"] == nil {
		engine.ExprCatalog["exprTree"] = glue.ExprTree
	}

	tmpDir, vars := glue.MakeVars("", "n1k1fs")
	defer os.RemoveAll(tmpDir)

	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, glue.NewGlueContext(time.Now()))
	vars.Temps = append(vars.Temps, conv.Temps[1:]...)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	origExecOpEx := engine.ExecOpEx
	defer func() { engine.ExecOpEx = origExecOpEx }()
	engine.ExecOpEx = glue.DatastoreOp

	var execErr error
	yieldVals := func(vals base.Vals) {
		v, e := cv.Convert(vals)
		if e != nil {
			if execErr == nil {
				execErr = e
			}
			return
		}
		var b []byte
		if v != nil {
			b, _ = json.Marshal(v.Actual())
		} else {
			b = []byte("null")
		}
		rows = append(rows, string(b))
	}
	yieldErr := func(e error) {
		if e != nil && execErr == nil {
			execErr = e
		}
	}

	engine.ExecOp(conv.TopOp, vars, yieldVals, yieldErr, "", "")

	return rows, execErr
}

// caseRunnable reports whether a case is the simple {statements, results}
// shape n1k1 can attempt (not an error/match/resultset/pre-post case).
func caseRunnable(c map[string]interface{}) (stmt string, results []interface{}, ok bool) {
	for k := range c {
		switch k {
		case "statements", "results", "ordered", "description", "pretty":
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

func TestSuiteCases(t *testing.T) {
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
				got, err := n1k1RunStatement(store, stmt)
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

			// Exotic: anything else (resultset/prepared/pre-post/etc.).
			skipped++
			reason, content := exoticInfo(c)
			exotic = append(exotic, exoticCase{loc, reason, content})
		}
	}

	reportSuite(t, len(files), pass, errPass, skipped, nonPass, exotic)
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
func reportSuite(t *testing.T, nFiles, pass, errPass, skipped int, nonPass []caseOutcome, exotic []exoticCase) {
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
	fmt.Fprintf(&b, "\nexotic / skipped cases (%d):\n", len(exotic))
	tw = tabwriter.NewWriter(&b, 0, 2, 2, ' ', 0)
	for _, e := range exotic {
		fmt.Fprintf(tw, "  %s\t%s\t%s\n", e.loc, e.reason, e.content)
	}
	tw.Flush()

	b.WriteString("\nsuite conformance\n=================\n")
	tw = tabwriter.NewWriter(&b, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "  files scanned\t%*d\n", valW, nFiles)
	fmt.Fprintf(tw, "  runnable cases\t%*d\n", valW, total)
	fmt.Fprintf(tw, "  PASS (rows)\t%*d\t(%.1f%%)\n", valW, pass, 100*float64(pass)/float64(total))
	fmt.Fprintf(tw, "  PASS (error rejected)\t%*d\n", valW, errPass)
	fmt.Fprintf(tw, "  UNSUPPORTED\t%*d\n", valW, unsupported)
	fmt.Fprintf(tw, "  PANIC (engine bug)\t%*d\n", valW, panicked)
	fmt.Fprintf(tw, "  FAIL\t%*d\n", valW, fail)
	fmt.Fprintf(tw, "  skipped (exotic)\t%*d\n", valW, skipped)
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

	fmt.Fprintf(&b, "\nexpected non-pass by group (%d cases; shrink as coverage grows):\n",
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
	const passFloor = 643
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
		case "statements", "results", "ordered", "description", "pretty":
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
	// EXPLAIN / plan-text output.
	"case_by_id.json[1]":         "explain",
	"case_by_id.json[3]":         "explain",
	"case_by_id.json[4]":         "explain",
	"case_by_id.json[6]":         "explain",
	"case_by_id.json[7]":         "explain",
	"case_by_id.json[8]":         "explain",
	"case_by_id.json[9]":         "explain",
	"case_func_date.json[6]":     "explain",
	"case_orderby_limit.json[4]": "explain",
	"case_orderby_limit.json[5]": "explain",

	// Secondary index / union scan (n1k1 does primary scans).
	"case_by_id.json[2]": "index-scan",

	// META() over fetch metadata subpaths.
	"case_func_meta.json[0]": "meta-fetch",
	"case_func_meta.json[1]": "meta-fetch",
	"case_func_meta.json[6]": "meta-fetch",

	// GROUP BY on a computed / array-index key.
	"case_func_date.json[4]":       "groupby-key",
	"case_group_by_having.json[5]": "groupby-key",

	// ON KEYS join projection arity mismatch.
	"case_innerjoin.json[10]": "onkeys-proj",
	"case_leftjoin.json[4]":   "onkeys-proj",

	// LET / WITH bindings.
	"case_select.json[23]": "let-with",
	"case_select.json[24]": "let-with",
	"case_select.json[25]": "let-with",

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
	"case_system_prepareds.json[2]": "prepared",

	// The one FAIL: ARRAY_AGG element order is undefined in N1QL.
	"case_func_array.json[34]": "arrayagg-order",
}

// groupWhy gives a one-line reason for each expectedNonPass group.
var groupWhy = map[string]string{
	"explain":          "EXPLAIN / plan-text output not converted to n1k1 ops",
	"index-scan":       "secondary index / union scan not converted (n1k1 does primary scans)",
	"meta-fetch":       "META() over fetch metadata subpaths ($document.exptime) not wired",
	"groupby-key":      "GROUP BY on a computed / array-index key unresolved in VisitFinalGroup",
	"onkeys-proj":      "ON KEYS join projection: label/vals arity mismatch",
	"let-with":         "LET / WITH bindings (plan.Let / plan.With) not converted",
	"resource-guard":   "engine refuses huge generator builtins (ARRAY_RANGE/REPEAT ~1e10)",
	"system-namespace": "system: namespace needs a systemstore (intentionally nil; see FileStore)",
	"prepared":         "prepared-statement EXECUTE (plan.Discard) not supported",
	"arrayagg-order":   "ARRAY_AGG element order is undefined in N1QL; ordering differs (not fixable)",
}
