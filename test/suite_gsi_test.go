//go:build n1ql

package test

// Config for the data-backed gsi suite (TestGsiSuiteCases / TestGsiSuiteWithCompiler),
// imported from the couchbase/query fork's test/gsi/test_cases into an isolated
// corpus root. See DESIGN-testing.md and test/suite/import_gsi_data_cases.py.

const gsiSuiteRoot = "suite/json-gsi"

// gsiPassFloor is the results-pass backstop for the gsi corpus (bump as coverage
// grows), mirroring the default suite's floor.
const gsiPassFloor = 467

// gsiExpectedNonPass lists gsi cases n1k1 doesn't yet pass, keyed by loc
// (case_gsi_<cat>.json[i]) -> group. Any non-pass NOT listed is a regression.
// None panic; these are feature gaps or output/order differences to chip away at.
var gsiExpectedNonPass = map[string]string{
	"case_gsi_any_functions.json[0]":    "any-every",
	"case_gsi_any_functions.json[1]":    "any-every",
	"case_gsi_any_functions.json[2]":    "any-every",
	"case_gsi_any_functions.json[3]":    "any-every",
	"case_gsi_any_functions.json[4]":    "any-every",
	"case_gsi_any_functions.json[5]":    "any-every",
	"case_gsi_any_functions.json[6]":    "any-every",
	"case_gsi_any_functions.json[7]":    "any-every",
	"case_gsi_any_functions.json[10]":   "any-every",
	"case_gsi_any_functions.json[12]":   "any-every",
	"case_gsi_any_functions.json[13]":   "any-every",
	"case_gsi_array_functions.json[62]": "results-differ",
	"case_gsi_comp_functions.json[0]":   "results-differ",
	"case_gsi_comp_functions.json[1]":   "results-differ",
	"case_gsi_comp_functions.json[2]":   "results-differ",
	"case_gsi_comp_functions.json[3]":   "results-differ",
	"case_gsi_comp_functions.json[4]":   "results-differ",
	"case_gsi_comp_functions.json[5]":   "results-differ",
	"case_gsi_json_functions.json[4]":   "json-funcs",
	"case_gsi_json_functions.json[5]":   "json-funcs",
	"case_gsi_json_functions.json[10]":  "json-funcs",
	"case_gsi_json_functions.json[13]":  "json-funcs",
	"case_gsi_obj_functions.json[2]":    "obj-funcs",
	"case_gsi_obj_functions.json[3]":    "obj-funcs",
	"case_gsi_obj_functions.json[8]":    "obj-funcs",
}

var gsiGroupWhy = map[string]string{
	"any-every":      "ANY/EVERY ... SATISFIES collection predicates (or tie-broken ORDER BY+LIMIT)",
	"json-funcs":     "JSON_ENCODE/ENCODED_SIZE/POLY_LENGTH/TOKENS -- output/order differs or unsupported",
	"obj-funcs":      "OBJECT_PAIRS/OBJECT_VALUES/OBJECT_PAIRS_NESTED -- key-order/output differs",
	"results-differ": "results differ from cbq -- tie-broken ORDER BY+LIMIT for comp GREATEST/LEAST, and array_position over the undefined order of ARRAY_AGG (non-determinism)",
}
