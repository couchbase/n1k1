//go:build n1ql

package test

// Config for the data-backed gsi suite (TestGsiSuiteCases / TestGsiSuiteWithCompiler),
// imported from the couchbase/query fork's test/gsi/test_cases into an isolated
// corpus root. See DESIGN-testing.md and test/suite/import_gsi_data_cases.py.

const gsiSuiteRoot = "suite/json-gsi"

// gsiPassFloor is the results-pass backstop for the gsi corpus (bump as coverage
// grows), mirroring the default suite's floor.
const gsiPassFloor = 636

// gsiExpectedNonPass lists gsi cases n1k1 doesn't yet pass, keyed by loc
// (case_gsi_<cat>.json[i]) -> group. Any non-pass NOT listed is a regression.
// None panic; these are feature gaps or output/order differences to chip away at.
var gsiExpectedNonPass = map[string]string{
	"case_gsi_array_functions.json[62]": "nondeterministic",
}

var gsiGroupWhy = map[string]string{
	"nondeterministic": "array_position over ARRAY_AGG's unspecified element order -- n1k1 aggregates in scan order, cbq in its own, so the position differs; no fixed corpus can match it",
}
