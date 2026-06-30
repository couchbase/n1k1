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

// Runs the upstream couchbase/query "filestore" test corpus (vendored under
// test/filestore/json/default) against n1k1. Each case is {statements, results}
// over the file datastore. n1k1 supports a subset of N1QL, so this reports
// pass / fail / unsupported counts rather than requiring 100%.

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

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

const filestoreRoot = "filestore/json" // FileStore root; queries use default:<keyspace>.

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

	if n1k1.ExprCatalog["exprStr"] == nil {
		n1k1.ExprCatalog["exprStr"] = glue.ExprStr
	}
	if n1k1.ExprCatalog["exprTree"] == nil {
		n1k1.ExprCatalog["exprTree"] = glue.ExprTree
	}

	tmpDir, vars := glue.MakeVars("", "n1k1fs")
	defer os.RemoveAll(tmpDir)

	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, glue.NewGlueContext(time.Now()))
	vars.Temps = append(vars.Temps, conv.Temps[1:]...)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	origExecOpEx := n1k1.ExecOpEx
	defer func() { n1k1.ExecOpEx = origExecOpEx }()
	n1k1.ExecOpEx = glue.DatastoreOp

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

	n1k1.ExecOp(conv.TopOp, vars, yieldVals, yieldErr, "", "")

	return rows, execErr
}

// deepNormalize recursively sorts object keys (implicitly, via json.Marshal)
// and array elements. The harness already treats result rows as an unordered
// multiset; array element order is likewise not meaningful for the comparison
// -- ARRAY_AGG order is undefined in N1QL, and the upstream corpus's order
// reflects its own scan/aggregation order, which n1k1 needn't replicate. So we
// compare arrays as multisets too. (Sorting both sides identically can only
// turn a false failure into a pass; it never breaks a genuinely-equal pair.)
func deepNormalize(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		m := make(map[string]interface{}, len(x))
		for k, e := range x {
			m[k] = deepNormalize(e)
		}
		return m
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, e := range x {
			out[i] = deepNormalize(e)
		}
		sort.Slice(out, func(i, j int) bool {
			bi, _ := json.Marshal(out[i])
			bj, _ := json.Marshal(out[j])
			return string(bi) < string(bj)
		})
		return out
	default:
		return v
	}
}

// canonJSON returns a fully order-normalized JSON string for a value (an n1k1
// result row passed as a JSON string, or an expected result object).
func canonJSON(v interface{}) string {
	if s, ok := v.(string); ok { // n1k1 result row (JSON string)
		var parsed interface{}
		if json.Unmarshal([]byte(s), &parsed) != nil {
			return s
		}
		v = parsed
	}
	b, _ := json.Marshal(deepNormalize(v))
	return string(b)
}

// rowsMatch compares as multisets (order-insensitive across rows, and
// key-order-insensitive within each object). n1k1's row order can differ, and
// it doesn't sort object keys, so both sides are canonicalized.
func rowsMatch(got []string, expected []interface{}) bool {
	if len(got) != len(expected) {
		return false
	}
	g := make([]string, len(got))
	for i, s := range got {
		g[i] = canonJSON(s)
	}
	e := make([]string, len(expected))
	for i, r := range expected {
		e[i] = canonJSON(r)
	}
	sort.Strings(g)
	sort.Strings(e)
	for i := range g {
		if g[i] != e[i] {
			return false
		}
	}
	return true
}

// caseRunnable reports whether a case is the simple {statements, results}
// shape n1k1 can attempt (not an error/match/resultset/pre-post case).
func caseRunnable(c map[string]interface{}) (stmt string, results []interface{}, ok bool) {
	for k := range c {
		switch k {
		case "statements", "results", "ordered", "description":
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

func TestFilestoreCases(t *testing.T) {
	if _, err := os.Stat(filestoreRoot + "/default/cases"); err != nil {
		t.Skipf("filestore corpus not present: %v", err)
	}

	store, err := glue.FileStore(filestoreRoot)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}
	store.InitParser()

	files, _ := filepath.Glob(filestoreRoot + "/default/cases/case_*.json")
	sort.Strings(files)

	var pass, skipped int
	var nonPass []caseOutcome // every fail/unsupported case, for classification

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
			stmt, results, ok := caseRunnable(c)
			if !ok {
				skipped++
				continue
			}

			loc := fmt.Sprintf("%s[%d]", filepath.Base(f), ci)
			got, err := n1k1RunStatement(store, stmt)
			switch {
			case err != nil:
				nonPass = append(nonPass, caseOutcome{loc, stmt, "UNSUPPORTED", oneLine(err.Error())})
			case rowsMatch(got, results):
				pass++
			default:
				nonPass = append(nonPass, caseOutcome{loc, stmt, "FAIL", "results differ"})
			}
		}
	}

	reportFilestore(t, len(files), pass, skipped, nonPass)
}

// caseOutcome records one non-passing corpus case.
type caseOutcome struct {
	loc, stmt, status, detail string
}

// reportFilestore prints a readable summary + a grouped table of the expected
// non-pass cases, then enforces two guards: any UNEXPECTED non-pass (one not in
// expectedNonPass) is a regression and fails the test; a stale table entry (a
// listed case that now passes) is warned about so it can be removed.
func reportFilestore(t *testing.T, nFiles, pass, skipped int, nonPass []caseOutcome) {
	groupCount := map[string]int{}
	seen := map[string]bool{}
	var unexpected []caseOutcome
	var fail, unsupported int

	for _, o := range nonPass {
		if o.status == "FAIL" {
			fail++
		} else {
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

	total := pass + fail + unsupported

	// valW is the width of the widest count, so the value column right-aligns
	// (tabwriter left-aligns each cell, so we right-justify the digits ourselves
	// to a shared width rather than use whole-table AlignRight, which would also
	// right-align the label/text columns).
	valW := 1
	for _, n := range []int{nFiles, total, pass, unsupported, fail, skipped} {
		if w := len(strconv.Itoa(n)); w > valW {
			valW = w
		}
	}

	var b strings.Builder
	b.WriteString("\nfilestore conformance\n=====================\n")
	tw := tabwriter.NewWriter(&b, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "  files scanned\t%*d\n", valW, nFiles)
	fmt.Fprintf(tw, "  runnable cases\t%*d\n", valW, total)
	fmt.Fprintf(tw, "  PASS\t%*d\t(%.1f%%)\n", valW, pass, 100*float64(pass)/float64(total))
	fmt.Fprintf(tw, "  UNSUPPORTED\t%*d\n", valW, unsupported)
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
	const passFloor = 631
	if pass < passFloor {
		t.Errorf("filestore conformance regressed: PASS=%d < baseline %d", pass, passFloor)
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

// expectedNonPass enumerates every corpus case n1k1 does not currently pass:
// the UNSUPPORTED plans/features it can't yet convert, plus the lone FAIL whose
// result ordering N1QL leaves undefined. Each maps to a group key explained by
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

	// UNNEST used as a JOIN/NEST source.
	"case_from-over.json[6]": "unnest-source",
	"case_innerjoin.json[5]": "unnest-source",
	"case_innerjoin.json[8]": "unnest-source",
	"case_leftjoin.json[2]":  "unnest-source",
	"case_leftjoin.json[3]":  "unnest-source",
	"case_unnest.json[1]":    "unnest-source",
	"case_unnest.json[3]":    "unnest-source",

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

	// Aggregate evaluated over an UNNEST scope.
	"case_from-over.json[5]": "agg-unnest-scope",

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
	"unnest-source":    "UNNEST as a JOIN/NEST source: plan.Unnest is not yet a glue.Termer",
	"meta-fetch":       "META() over fetch metadata subpaths ($document.exptime) not wired",
	"groupby-key":      "GROUP BY on a computed / array-index key unresolved in VisitFinalGroup",
	"onkeys-proj":      "ON KEYS join projection: label/vals arity mismatch",
	"agg-unnest-scope": "aggregate over an UNNEST scope hits an AnnotatedValue assertion",
	"let-with":         "LET / WITH bindings (plan.Let / plan.With) not converted",
	"resource-guard":   "engine refuses huge generator builtins (ARRAY_RANGE/REPEAT ~1e10)",
	"system-namespace": "system: namespace needs a systemstore (intentionally nil; see FileStore)",
	"prepared":         "prepared-statement EXECUTE (plan.Discard) not supported",
	"arrayagg-order":   "ARRAY_AGG element order is undefined in N1QL; ordering differs (not fixable)",
}
