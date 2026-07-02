//go:build n1ql

package test

// Config for the data-backed gsi suite (TestGsiSuiteCases / TestGsiSuiteWithCompiler),
// imported from the couchbase/query fork's test/gsi/test_cases into an isolated
// corpus root. See DESIGN-testing.md and test/suite/import_gsi_data_cases.py.

const gsiSuiteRoot = "suite/json-gsi"

// gsiPassFloor is the results-pass backstop for the gsi corpus (bump as coverage
// grows), mirroring the default suite's floor.
const gsiPassFloor = 716

// gsiExpectedNonPass lists gsi cases n1k1 doesn't yet pass, keyed by loc
// (case_gsi_<cat>.json[i]) -> group. Any non-pass NOT listed is a regression.
// None panic; these are feature gaps or output/order differences to chip away at.
var gsiExpectedNonPass = map[string]string{
	"case_gsi_array_functions.json[62]":     "nondeterministic",
	"case_gsi_aggregate_functions.json[1]":  "order-agg",
	"case_gsi_aggregate_functions.json[2]":  "order-agg",
	"case_gsi_aggregate_functions.json[41]": "results-differ",
	"case_gsi_aggregate_functions.json[54]": "fork-data-missing",
	"case_gsi_select_functions.json[24]":    "comma-join",
	"case_gsi_subqexp.json[24]":             "from-missing",
	"case_gsi_subqexp.json[25]":             "subquery",
	"case_gsi_subqexp.json[32]":             "subquery",
	"case_gsi_subqexp.json[34]":             "subquery",
	"case_gsi_subqexp.json[36]":             "fork-data-missing",
	"case_gsi_subqexp.json[40]":             "fork-data-missing",
	"case_gsi_subqexp.json[43]":             "fork-data-missing",
	"case_gsi_subqexp.json[46]":             "fork-data-missing",
	"case_gsi_subqexp.json[47]":             "subquery",
}

var gsiGroupWhy = map[string]string{
	"nondeterministic":  "array_position over ARRAY_AGG's unspecified element order -- n1k1 aggregates in scan order, cbq in its own, so the position differs; no fixed corpus can match it",
	"order-agg":         "ORDER BY an aggregate nested in a larger expr (e.g. MAX(x)[1].unitPrice) with a `.*`-spread projection: no projected column to bind to, so it would re-evaluate the aggregate above the group -- glue rejects it (NA) rather than panic. TODO: evaluate such order keys at the group level",
	"results-differ":    "aggregate[41]: STDDEV(DISTINCT x) over a single distinct value -- cbq's stored expected is 0 but its algebra computes NULL for a 1-element sample; n1k1 follows the documented algorithm",
	"fork-data-missing": "queries reference docs the fork's shared/global setup provides but its per-category insert.json doesn't (so our merged corpus lacks them): aggregate[54] test_id=\"median_agg_func\"; subqexp[36,40,43,46] USE KEYS ['1235'...] (subqexp inserts keys \"subqexp_1235\"...)",
	"comma-join":        "comma/cross join (FROM a, b) -- no ON clause; glue rejects it (NA) rather than panic. Not yet supported",
	"subquery":          "remaining correlated-subquery gaps: an aggregate inside a correlated subquery (SUM(...) over FROM outer.field -- hits 'nil item'), and a correlated subquery whose FROM is itself a subquery+WITH. (Plain correlated SELECT / EXISTS / IN subqueries now work.)",
	"from-missing":      "subqexp[24]: FROM <expr> where the expr is MISSING (a field path absent on a constant object) -- n1k1 yields one {..:null} row; cbq yields no rows",
}
