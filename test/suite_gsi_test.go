//go:build n1ql

package test

// Config for the data-backed gsi suite (TestGsiSuiteCases / TestGsiSuiteWithCompiler),
// imported from the couchbase/query fork's test/gsi/test_cases into an isolated
// corpus root. See DESIGN-testing.md and test/suite/import_gsi_data_cases.py.

const gsiSuiteRoot = "suite/json-gsi"

// gsiPassFloor is the results-pass backstop for the gsi corpus (bump as coverage
// grows), mirroring the default suite's floor.
const gsiPassFloor = 678

// gsiExpectedNonPass lists gsi cases n1k1 doesn't yet pass, keyed by loc
// (case_gsi_<cat>.json[i]) -> group. Any non-pass NOT listed is a regression.
// None panic; these are feature gaps or output/order differences to chip away at.
var gsiExpectedNonPass = map[string]string{
	"case_gsi_array_functions.json[62]":     "nondeterministic",
	"case_gsi_aggregate_functions.json[1]":  "join",
	"case_gsi_aggregate_functions.json[14]": "group-as",
	"case_gsi_aggregate_functions.json[15]": "group-as",
	"case_gsi_aggregate_functions.json[16]": "join",
	"case_gsi_aggregate_functions.json[17]": "group-as",
	"case_gsi_aggregate_functions.json[18]": "group-as",
	"case_gsi_aggregate_functions.json[19]": "group-as",
	"case_gsi_aggregate_functions.json[24]": "group-by-expr",
	"case_gsi_aggregate_functions.json[39]": "results-differ",
	"case_gsi_aggregate_functions.json[52]": "fork-data-missing",
}

var gsiGroupWhy = map[string]string{
	"nondeterministic":  "array_position over ARRAY_AGG's unspecified element order -- n1k1 aggregates in scan order, cbq in its own, so the position differs; no fixed corpus can match it",
	"group-as":          "GROUP AS <var> group-collection feature (len(g), g[0]..., correlated subquery over the group) -- not yet supported",
	"join":              "join/subquery-join forms (ANSI INNER JOIN with GROUP AS, UNNEST-of-subquery JOIN) the glue layer reports unsupported",
	"group-by-expr":     "GROUP BY <expr> AS alias (e.g. DATE_PART_STR(...)) with HAVING on the alias -- not yet supported",
	"results-differ":    "aggregate[41]: STDDEV(DISTINCT x) over a single distinct value -- cbq's stored expected is 0 but its algebra computes NULL for a 1-element sample; n1k1 follows the documented algorithm",
	"fork-data-missing": "aggregate[54]: queries test_id=\"median_agg_func\" docs that the fork's insert.json never inserts (only agg_func/cntn_agg_func), so the keyspace has no matching rows",
}
