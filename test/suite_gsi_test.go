//go:build n1ql

package test

// Config for the data-backed gsi suite (TestGsiSuiteCases / TestGsiSuiteWithCompiler),
// imported from the couchbase/query fork's test/gsi/test_cases into an isolated
// corpus root. See DESIGN-testing.md and test/suite/import_gsi_data_cases.py.

const gsiSuiteRoot = "suite/json-gsi"

// gsiPassFloor is the results-pass backstop for the gsi corpus (bump as coverage
// grows), mirroring the default suite's floor.
const gsiPassFloor = 819

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

	// The window category (imported from the fork's test/gsi/test_cases/window)
	// records cbq's own results for the FULL window surface -- far beyond what n1k1
	// supports. Its 5 non-OVER aggregate cases (0,1,2,3,5: LETTING / GROUP BY HAVING
	// / plain SUM / ORDER BY sc1) pass and lifted the floor; the 26 OVER cases below
	// are genuine feature gaps, grouped by their proximate blocker. NOTE: these are
	// NOT gated by the core window machinery (ROWS/RANGE/GROUPS aggregates + ranking +
	// offset all work -- see glue/window_test.go); they're gated by ORDER BY features
	// (NULLS positioning, order-by-aggregate) and by advanced window funcs the corpus
	// bundles (multi-column RANGE, DISTINCT-in-window, RATIO_TO_REPORT, COUNTN, MEAN,
	// EXCLUDE, FROM LAST). None panic.
	"case_gsi_window.json[7]":  "window-nulls-order",
	"case_gsi_window.json[8]":  "window-nulls-order",
	"case_gsi_window.json[11]": "window-nulls-order",
	"case_gsi_window.json[12]": "window-nulls-order",
	"case_gsi_window.json[13]": "window-nulls-order",
	"case_gsi_window.json[15]": "window-nulls-order",
	"case_gsi_window.json[16]": "window-nulls-order",
	"case_gsi_window.json[17]": "window-nulls-order",
	"case_gsi_window.json[19]": "window-nulls-order",
	"case_gsi_window.json[20]": "window-nulls-order",
	"case_gsi_window.json[21]": "window-nulls-order",
	"case_gsi_window.json[24]": "window-nulls-order",
	"case_gsi_window.json[25]": "window-nulls-order",
	"case_gsi_window.json[30]": "window-nulls-order",

	"case_gsi_window.json[4]":  "window-order-by-agg",
	"case_gsi_window.json[6]":  "window-order-by-agg",
	"case_gsi_window.json[9]":  "window-order-by-agg",
	"case_gsi_window.json[10]": "window-order-by-agg",
	"case_gsi_window.json[23]": "window-order-by-agg",
	"case_gsi_window.json[26]": "window-order-by-agg",
	"case_gsi_window.json[27]": "window-order-by-agg",

	"case_gsi_window.json[14]": "window-advanced-funcs",
	"case_gsi_window.json[18]": "window-advanced-funcs",
	"case_gsi_window.json[22]": "window-advanced-funcs",

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
	"nondeterministic":      "array_position over ARRAY_AGG's unspecified element order -- n1k1 aggregates in scan order, cbq in its own, so the position differs; no fixed corpus can match it",
	"order-agg":             "ORDER BY an aggregate nested in a larger expr (e.g. MAX(x)[1].unitPrice) with a `.*`-spread projection: no projected column to bind to, so it would re-evaluate the aggregate above the group -- glue rejects it (NA) rather than panic. TODO: evaluate such order keys at the group level",
	"results-differ":        "aggregate[41]: STDDEV(DISTINCT x) over a single distinct value -- cbq's stored expected is 0 but its algebra computes NULL for a 1-element sample; n1k1 follows the documented algorithm",
	"fork-data-missing":     "queries reference docs the fork's shared/global setup provides but its per-category insert.json doesn't (so our merged corpus lacks them): subqexp[36,40,43,46] USE KEYS ['1235'...] (subqexp inserts keys \"subqexp_1235\"...)",
	"mega-order-limit":      "unnest[0,1,2,5,6,7]: UNNEST p.lineItems over the `purchase` MEGA keyspace with ORDER BY <unnested-elem> LIMIT n. The fork loads ~10,000 purchase docs; our corpus keeps a light sample (see MEGA_KEYSPACES), so the top-N after sorting the full unnested set can't be reproduced. UNNEST itself is correct (the specific-`product` unnest cases pass); only the full-set ordered LIMIT differs",
	"prepared":              "inlist[11,12,14,15,17,18,20,21]: EXECUTE now runs (PREPARE/EXECUTE are supported), but these bind a mixed-type / parameterized IN-list ([1,2,3,$1,$2,$3,\"a\",...]) over a GSI index whose scan yields a different row SET than the corpus (verified: the same param binding gives correct rows on a plain scan -- see glue TestPrepareExecute -- so this is a GSI index-scan inlist limitation, not a prepared-statement one)",
	"window-nulls-order":    "window[7,8,11,12,13,15,16,17,19,20,21,24,25,30]: the window's ORDER BY uses explicit NULLS FIRST/LAST, which cbq's planner emits as a plan.Order (to pre-sort for windowing) carrying a NullsPosExpr; conv's VisitOrder returns NA for any non-natural nulls position (the order-offset-limit op sorts by N1QL natural collation only), blocking the whole query before window execution. Most of these ALSO use a multi-column RANGE default frame / DISTINCT-in-window / RATIO_TO_REPORT / COUNTN / MEAN / EXCLUDE, so they'd stay non-pass even once nulls ordering lands -- nulls ordering is the first blocker, not the only one",
	"window-order-by-agg":   "window[4,6,9,10,23,26,27]: ORDER BY an aggregate/window function not bound to a projected column -- a top-level `ORDER BY COUNT(x)` or `ORDER BY COUNT(x) OVER() DESC`, or a window OVER a grouped aggregate (`SUM(COUNT(c)) OVER (... ORDER BY MAX(c))`). conv rejects the plan.Order (orderReEvalsAggregate) rather than re-evaluate the aggregate above the group -- same family as the default suite's order-agg group",
	"window-advanced-funcs": "window[14,18,22]: navigation/stats window funcs n1k1 doesn't fully implement -- NTH_VALUE ... FROM LAST, LAG/LEAD with a string default ('None'), EXCLUDE GROUP/TIES, RATIO_TO_REPORT/COUNTN. 14 and 18 error in the value path (a bare-word 'None' default reaches the JSON scanner); 22 runs but its STDDEV-over-GROUPS + FROM LAST + LAG results differ from cbq's recorded output",
	"window-named-frame":    "window[28,29]: a named WINDOW reference that ADDS a frame (e.g. `OVER (wn2 ROWS CURRENT ROW)`) must inherit wn2's ORDER BY, but n1k1's REWRITE_PHASE1 pass doesn't propagate it before the semantic check, so cbq's own guard fires (\"window frame is not allowed without ORDER BY\" / \"NTILE ... ORDER BY clause is required\") where the fork succeeds",
}
