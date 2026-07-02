//go:build n1ql

package test

// Config for the data-backed gsi suite (TestGsiSuiteCases / TestGsiSuiteWithCompiler),
// imported from the couchbase/query fork's test/gsi/test_cases into an isolated
// corpus root. See DESIGN-testing.md and test/suite/import_gsi_data_cases.py.

const gsiSuiteRoot = "suite/json-gsi"

// gsiPassFloor is the results-pass backstop for the gsi corpus (bump as coverage
// grows), mirroring the default suite's floor.
const gsiPassFloor = 693

// gsiExpectedNonPass lists gsi cases n1k1 doesn't yet pass, keyed by loc
// (case_gsi_<cat>.json[i]) -> group. Any non-pass NOT listed is a regression.
// None panic; these are feature gaps or output/order differences to chip away at.
var gsiExpectedNonPass = map[string]string{
	"case_gsi_array_functions.json[62]":     "nondeterministic",
	"case_gsi_aggregate_functions.json[1]":  "order-agg",
	"case_gsi_aggregate_functions.json[2]":  "order-agg",
	"case_gsi_aggregate_functions.json[26]": "group-by-expr",
	"case_gsi_aggregate_functions.json[41]": "results-differ",
	"case_gsi_aggregate_functions.json[54]": "fork-data-missing",
	"case_gsi_select_functions.json[21]":    "select-exclude",
	"case_gsi_select_functions.json[22]":    "select-exclude",
	"case_gsi_select_functions.json[23]":    "select-exclude",
	"case_gsi_select_functions.json[24]":    "comma-join",
	"case_gsi_select_functions.json[25]":    "select-exclude",
	"case_gsi_select_functions.json[26]":    "select-exclude",
	"case_gsi_select_functions.json[27]":    "union",
	"case_gsi_select_functions.json[28]":    "ci-identifier",
	"case_gsi_select_functions.json[29]":    "ci-identifier",
	"case_gsi_select_functions.json[30]":    "ci-identifier",
}

var gsiGroupWhy = map[string]string{
	"nondeterministic":  "array_position over ARRAY_AGG's unspecified element order -- n1k1 aggregates in scan order, cbq in its own, so the position differs; no fixed corpus can match it",
	"order-agg":         "ORDER BY an aggregate nested in a larger expr (e.g. MAX(x)[1].unitPrice) with a `.*`-spread projection: no projected column to bind to, so it would re-evaluate the aggregate above the group -- glue rejects it (NA) rather than panic. TODO: evaluate such order keys at the group level",
	"group-by-expr":     "GROUP BY <expr> AS alias (e.g. DATE_PART_STR(...)) with HAVING on the alias -- not yet supported",
	"results-differ":    "aggregate[41]: STDDEV(DISTINCT x) over a single distinct value -- cbq's stored expected is 0 but its algebra computes NULL for a 1-element sample; n1k1 follows the documented algorithm",
	"fork-data-missing": "aggregate[54]: queries test_id=\"median_agg_func\" docs that the fork's insert.json never inserts (only agg_func/cntn_agg_func), so the keyspace has no matching rows",
	"select-exclude":    "SELECT * EXCLUDE <path> (and o.* EXCLUDE ...) -- the star-projection EXCLUDE clause isn't applied, so excluded fields still appear; not yet supported",
	"comma-join":        "comma/cross join (FROM a, b) -- no ON clause; glue rejects it (NA) rather than panic. Not yet supported",
	"union":             "UNION / UNION ALL set operators (plan.UnionAll) -- not yet supported",
	"ci-identifier":     "case-insensitive field identifiers (`name`i) -- t.`title`i matching a TITLE field; not yet supported",
}
