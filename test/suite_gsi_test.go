//go:build n1ql

package test

// Config for the data-backed gsi suite (TestGsiSuiteCases / TestGsiSuiteWithCompiler),
// imported from the couchbase/query fork's test/gsi/test_cases into an isolated
// corpus root. See DESIGN-testing.md and test/suite/import_gsi_data_cases.py.

const gsiSuiteRoot = "suite/json-gsi"

// gsiPassFloor is the results-pass backstop for the gsi corpus (bump as coverage
// grows), mirroring the default suite's floor.
const gsiPassFloor = 830

// gsiExpectedNonPass lists gsi cases n1k1 doesn't yet pass, keyed by loc
// (case_gsi_<cat>.json[i]) -> group. Any non-pass NOT listed is a regression.
// These are feature gaps or output/order differences to chip away at (none
// panic).
var gsiExpectedNonPass = map[string]string{
	"case_gsi_array_functions.json[62]":     "nondeterministic",
	"case_gsi_aggregate_functions.json[1]":  "order-agg",
	"case_gsi_aggregate_functions.json[2]":  "order-agg",
	"case_gsi_aggregate_functions.json[41]": "results-differ",
	"case_gsi_subqexp.json[36]":             "fork-data-missing",
	"case_gsi_subqexp.json[40]":             "fork-data-missing",
	"case_gsi_subqexp.json[43]":             "fork-data-missing",
	"case_gsi_subqexp.json[46]":             "fork-data-missing",
	"case_gsi_inlist.json[11]":              "prepared",
	"case_gsi_inlist.json[12]":              "prepared",
	"case_gsi_inlist.json[14]":              "prepared",
	"case_gsi_inlist.json[15]":              "prepared",
	"case_gsi_inlist.json[17]":              "prepared",
	"case_gsi_inlist.json[18]":              "prepared",
	"case_gsi_inlist.json[20]":              "prepared",
	"case_gsi_inlist.json[21]":              "prepared",
	"case_gsi_unnest.json[0]":               "mega-order-limit",
	"case_gsi_unnest.json[1]":               "mega-order-limit",
	"case_gsi_unnest.json[2]":               "mega-order-limit",
	"case_gsi_unnest.json[5]":               "mega-order-limit",
	"case_gsi_unnest.json[6]":               "mega-order-limit",
	"case_gsi_unnest.json[7]":               "mega-order-limit",

	// The window category (imported from the fork's test/gsi/test_cases/window) records
	// cbq's own results for the FULL window surface. 16 of the 31 cases now pass: the 5
	// non-OVER aggregate cases (0,1,2,3,5), 4 multi-column RANGE cases (7,8,24,25),
	// window[4] (ORDER BY a non-projected aggregate), 4 window-over-a-grouped-aggregate
	// cases (9,10,26,27), and window[6,23] (SUM over a non-numeric column now yields
	// NULL, not 0). The 15 OVER cases below are grouped by proximate blocker. The core
	// window machinery (ROWS/RANGE/GROUPS aggregates + ranking + offset + NULLS ordering
	// + multi-column peer frames + ORDER-BY/PARTITION-BY/OVER an aggregate) works -- see
	// glue/window_test.go + glue/order_nulls_test.go; these gaps are unsupported window
	// funcs / frames, output differences, or nondeterministic tie order. None panic (conv
	// rejects an un-computable window function gracefully).
	"case_gsi_window.json[13]": "window-nondeterministic",

	"case_gsi_window.json[12]": "window-unsupported",
	"case_gsi_window.json[14]": "window-unsupported",
	"case_gsi_window.json[15]": "window-unsupported",
	"case_gsi_window.json[16]": "window-unsupported",
	"case_gsi_window.json[17]": "window-unsupported",
	"case_gsi_window.json[18]": "window-unsupported",
	"case_gsi_window.json[20]": "window-unsupported",
	"case_gsi_window.json[30]": "window-unsupported",

	"case_gsi_window.json[11]": "window-results-differ",
	"case_gsi_window.json[19]": "window-results-differ",
	"case_gsi_window.json[21]": "window-results-differ",
	"case_gsi_window.json[22]": "window-results-differ",

	"case_gsi_window.json[28]": "window-named-frame",
	"case_gsi_window.json[29]": "window-named-frame",
}

// gsiSkipRun names gsi cases n1k1 can parse+plan but must NOT execute. subqexp[1]
// tests cbq's correlated-subquery resource guard ("keyspace cannot have more than
// 1000 documents without appropriate secondary index"): its subquery has no
// predicate on the INNER keyspace (`WHERE d.a = 1` references only the outer), so
// the planner doesn't classify it as an in-correlated-subquery primary scan and
// the plan-time guard (build_scan.go) never fires -- it would run a full ~5000-doc
// `customer` scan per outer row (O(N^2), a hang). subqexp[0] (whose subquery DOES
// correlate on the inner, `d.a = d1.a`) now errors at plan time via patch-04 (a
// live optDocCount feeding that guard), so it's no longer skipped.
var gsiSkipRun = map[string]string{
	"case_gsi_subqexp.json[1]": "correlated-subquery doc-limit guard not plan-detectable (no inner predicate); O(N^2) over customer",
}

var gsiGroupWhy = map[string]string{
	"nondeterministic":        "array_position over ARRAY_AGG's unspecified element order -- n1k1 aggregates in scan order, cbq in its own, so the position differs; no fixed corpus can match it",
	"order-agg":               "ORDER BY an aggregate nested in a larger expr (e.g. MAX(x)[1].unitPrice) with a `.*`-spread projection: no projected column to bind to, so it would re-evaluate the aggregate above the group -- glue rejects it (NA) rather than panic. TODO: evaluate such order keys at the group level",
	"results-differ":          "aggregate[41]: STDDEV(DISTINCT x) over a single distinct value -- cbq's stored expected is 0 but its algebra computes NULL for a 1-element sample; n1k1 follows the documented algorithm",
	"fork-data-missing":       "queries reference docs the fork's shared/global setup provides but its per-category insert.json doesn't (so our merged corpus lacks them): subqexp[36,40,43,46] USE KEYS ['1235'...] (subqexp inserts keys \"subqexp_1235\"...)",
	"mega-order-limit":        "unnest[0,1,2,5,6,7]: UNNEST p.lineItems over the `purchase` MEGA keyspace with ORDER BY <unnested-elem> LIMIT n. The fork loads ~10,000 purchase docs; our corpus keeps a light sample (see MEGA_KEYSPACES), so the top-N after sorting the full unnested set can't be reproduced. UNNEST itself is correct (the specific-`product` unnest cases pass); only the full-set ordered LIMIT differs",
	"prepared":                "inlist[11,12,14,15,17,18,20,21]: EXECUTE now runs (PREPARE/EXECUTE are supported), but these bind a mixed-type / parameterized IN-list ([1,2,3,$1,$2,$3,\"a\",...]) over a GSI index whose scan yields a different row SET than the corpus (verified: the same param binding gives correct rows on a plain scan -- see glue TestPrepareExecute -- so this is a GSI index-scan inlist limitation, not a prepared-statement one)",
	"window-unsupported":      "window[12,14,15,16,17,18,20,30]: a window function/frame n1k1 doesn't compute natively, so conv rejects the statement (NA) rather than emit a boxed cbq window Evaluate (which panics on n1k1's plain rows). Covers RATIO_TO_REPORT, COUNTN, NTH_VALUE ... FROM LAST, a numeric-offset RANGE with multi-column ORDER BY, and DISTINCT-in-window over a non-catalog aggregate. Each bundles several window funcs across w1..w7, so one unsupported func gates the whole SELECT",
	"window-results-differ":   "window[11,19,21,22]: the query runs (all its window funcs are wired) but the output differs from cbq's recorded results -- GROUPS-frame peer semantics, EXCLUDE GROUP/TIES, STDDEV/VARIANCE over an explicit frame, or DISTINCT-in-window, none of which n1k1 fully honors yet",
	"window-nondeterministic": "window[13]: ROWS frames over an ORDER BY with ties (e.g. `ORDER BY d.c3, LOWER(d.c1)`) -- positional (ROWS) frame contents depend on the tie-break order, which n1k1 (scan order) and cbq resolve differently, so it passes intermittently. Listed non-pass so a lucky pass is a stale-entry note, never a flaky failure",
	"window-named-frame":      "window[28,29]: a named WINDOW reference that ADDS a frame (e.g. `OVER (wn2 ROWS CURRENT ROW)`) must inherit wn2's ORDER BY, but n1k1's REWRITE_PHASE1 pass doesn't propagate it before the semantic check, so cbq's own guard fires (\"window frame is not allowed without ORDER BY\" / \"NTILE ... ORDER BY clause is required\") where the fork succeeds",
}
