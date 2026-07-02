//go:build n1ql

package test

// Config for the data-backed gsi suite (TestGsiSuiteCases / TestGsiSuiteWithCompiler),
// imported from the couchbase/query fork's test/gsi/test_cases into an isolated
// corpus root. See DESIGN-testing.md and test/suite/import_gsi_data_cases.py.

const gsiSuiteRoot = "suite/json-gsi"

// gsiPassFloor is the results-pass backstop for the gsi corpus (bump as coverage
// grows), mirroring the default suite's floor.
const gsiPassFloor = 781

// gsiExpectedNonPass lists gsi cases n1k1 doesn't yet pass, keyed by loc
// (case_gsi_<cat>.json[i]) -> group. Any non-pass NOT listed is a regression.
// These are feature gaps or output/order differences to chip away at (none
// panic).
var gsiExpectedNonPass = map[string]string{
	"case_gsi_array_functions.json[62]":     "nondeterministic",
	"case_gsi_aggregate_functions.json[1]":  "order-agg",
	"case_gsi_aggregate_functions.json[2]":  "order-agg",
	"case_gsi_aggregate_functions.json[41]": "results-differ",
	"case_gsi_aggregate_functions.json[54]": "fork-data-missing",
	"case_gsi_select_functions.json[20]":    "dynamic-field",
	"case_gsi_subqexp.json[2]":              "subquery",
	"case_gsi_subqexp.json[5]":              "subquery",
	"case_gsi_subqexp.json[6]":              "subquery",
	"case_gsi_subqexp.json[7]":              "subquery",
	"case_gsi_subqexp.json[8]":              "subquery",
	"case_gsi_subqexp.json[25]":             "subquery",
	"case_gsi_subqexp.json[32]":             "subquery",
	"case_gsi_subqexp.json[34]":             "subquery",
	"case_gsi_subqexp.json[36]":             "fork-data-missing",
	"case_gsi_subqexp.json[40]":             "fork-data-missing",
	"case_gsi_subqexp.json[43]":             "fork-data-missing",
	"case_gsi_subqexp.json[46]":             "fork-data-missing",
	"case_gsi_subqexp.json[47]":             "subquery",
	"case_gsi_typeconv_functions.json[14]":  "unscoped-orders",
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
	"case_gsi_withs.json[2]":                "set-op",
	"case_gsi_withs.json[3]":                "with-subquery",
	"case_gsi_withs.json[4]":                "with-subquery",
	"case_gsi_withs.json[8]":                "subquery",
	"case_gsi_withs.json[9]":                "subquery",
	"case_gsi_withs.json[10]":               "with-subquery",
	"case_gsi_withs.json[11]":               "with-subquery",
	"case_gsi_withs.json[12]":               "with-subquery",
	"case_gsi_withs.json[13]":               "with-subquery",
	"case_gsi_withs.json[14]":               "with-subquery",
	"case_gsi_withs.json[15]":               "with-subquery",
	"case_gsi_withs.json[19]":               "with-subquery",
	"case_gsi_withs.json[27]":               "set-op",
	"case_gsi_withs.json[28]":               "set-op",
	"case_gsi_withs.json[30]":               "set-op",
	"case_gsi_withs.json[31]":               "set-op",
	"case_gsi_withs.json[32]":               "set-op",
	"case_gsi_withs.json[33]":               "set-op",
	"case_gsi_withs.json[34]":               "set-op",
	"case_gsi_withs.json[35]":               "set-op",
	"case_gsi_withs.json[36]":               "set-op",
}

var gsiGroupWhy = map[string]string{
	"nondeterministic":  "array_position over ARRAY_AGG's unspecified element order -- n1k1 aggregates in scan order, cbq in its own, so the position differs; no fixed corpus can match it",
	"order-agg":         "ORDER BY an aggregate nested in a larger expr (e.g. MAX(x)[1].unitPrice) with a `.*`-spread projection: no projected column to bind to, so it would re-evaluate the aggregate above the group -- glue rejects it (NA) rather than panic. TODO: evaluate such order keys at the group level",
	"results-differ":    "aggregate[41]: STDDEV(DISTINCT x) over a single distinct value -- cbq's stored expected is 0 but its algebra computes NULL for a 1-element sample; n1k1 follows the documented algorithm",
	"fork-data-missing": "queries reference docs the fork's shared/global setup provides but its per-category insert.json doesn't (so our merged corpus lacks them): aggregate[54] test_id=\"median_agg_func\"; subqexp[36,40,43,46] USE KEYS ['1235'...] (subqexp inserts keys \"subqexp_1235\"...)",
	"subquery":          "correlated / nested / derived-table subquery gaps: an aggregate inside a correlated subquery (SUM(...) over an outer field -- 'nil item'); a correlated subquery whose FROM is a subquery+WITH; a correlated subquery in the projection (SELECT (SELECT ... WHERE = outer) ...); a subquery USE KEYS (SELECT RAW ...); a derived-table FROM (SELECT ...) UNNEST ... under UNION; and a no-FROM correlated subquery nested in another subquery's RAW projection (SELECT RAW (SELECT RAW a)) -- its empty row can't resolve the outer id, see TODO(correlated-nil-row) in glue/expr.go. (Plain correlated SELECT / EXISTS / IN subqueries do work.)",
	"mega-order-limit":  "unnest[0,1,2,5,6,7]: UNNEST p.lineItems over the `purchase` MEGA keyspace with ORDER BY <unnested-elem> LIMIT n. The fork loads ~10,000 purchase docs; our corpus keeps a light sample (see MEGA_KEYSPACES), so the top-N after sorting the full unnested set can't be reproduced. UNNEST itself is correct (the specific-`product` unnest cases pass); only the full-set ordered LIMIT differs",
	"dynamic-field":     "select_functions[20]: dynamic bracket field navigation t.[<expr>] / t.[$param] -- n1k1 doesn't evaluate the bracket expression as a field name (t.[t.lookup] should read t[t.lookup], not yield the literal). Not yet supported",
	"prepared":          "inlist[17,18,20,21]: EXECUTE of a PREPAREd statement -- n1k1 has no prepared-statement store, so EXECUTE can't resolve the plan (the PREPARE cases themselves carry no results and are skipped)",
	"unscoped-orders":   "typeconv_functions[14]: queries the shared `orders` keyspace with only `type=\"order\"` and no test_id predicate, so our merged corpus (every category's orders docs) over-matches where the fork's per-category bucket held just two. Same class as the shellTest auto-scope, but for orders",
	"set-op":            "withs[2,27,28,30,31,32,33,34,35,36]: UNION ALL / INTERSECT[ ALL] / EXCEPT[ ALL] set operations (often combined with WITH or a comma-join branch). Only UNION (distinct) is supported today",
	"with-subquery":     "withs[3,4,10,11,12,13,14,15,19]: WITH (CTE) combined with an unsupported shape -- a CTE used as a comma-join/JOIN term, a CTE-as-subquery selected directly (SELECT w2 where w2 is a subquery CTE), FIRST over a CTE, or dynamic-field navigation into a CTE. Plain WITH works; these combinations don't yet",
}
