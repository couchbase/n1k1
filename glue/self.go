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

package glue

import (
	"encoding/json"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/plan"

	"github.com/couchbase/n1k1/base"
)

// SelfProjectNative toggles the native byte-path for whole-row `self` /
// SELECT * projections (see engine.ExprSelf). When false, self projections keep
// the boxed exprTree fallback. Default true; flip in tests to assert on/off
// parity.
var SelfProjectNative = true

// selfNativeSpecFor decides whether the star term of an InitialProject can use
// the native self byte path. It resolves against the project's input (child)
// labels -- c.TopOp at this point, before the project op is TopPush'd -- which
// are exactly the labels engine.ExprSelf sees at run time.
func selfNativeSpecFor(c *Conv, o *plan.InitialProject, starExpr expression.Expression) (param []interface{}, ok bool) {
	if !SelfProjectNative || c.TopOp == nil {
		return nil, false
	}

	// Only a bare whole-row self (SELECT *) is a native candidate: its value is
	// the row's own labels wrapped as an object. A path star (SELECT p.*,
	// orderlines[0].*) spreads a SUB-object's fields -- a different value that
	// selfNativeSpec's label walk would not reproduce -- so it stays boxed.
	if _, isSelf := starExpr.(*expression.Self); !isSelf {
		return nil, false
	}

	return selfNativeSpec(c.TopOp.Labels, o.BindingNames())
}

// selfNativeSpec returns the native-self expression param -- an
// ["self", key0, label0, key1, label1, ...] list for engine.ExprSelf -- that
// reproduces a whole-row `self` projection over inputLabels as raw bytes, or
// ok=false to keep the boxed exprTree path.
//
// It is native only when the byte assembly is byte-value-equivalent to cbq's
// boxed Convert->Self->WriteJSON:
//   - a single-element .["name"] field label emits key "name" from that val;
//   - a "^..." attachment label is skipped (metadata rides annotations, not the
//     object body -- see ConvertVals.Convert);
//   - a binding-name (LET var) field is excluded, matching stripBindingNames
//     (WITH/correlated scopes are not row labels, so they are excluded
//     automatically -- reproducing the star's exprResetScope);
//   - emitted keys must be distinct.
//
// A whole-row "." label, a multi-element path (.["a","b"]), a ".*" star-spread,
// or a duplicate key forces the boxed fallback (ok=false).
func selfNativeSpec(inputLabels base.Labels, bindingNames map[string]bool) (param []interface{}, ok bool) {
	param = []interface{}{"self"}

	seen := map[string]bool{}

	for _, label := range inputLabels {
		if len(label) == 0 {
			return nil, false
		}

		if label[0] == '^' {
			continue // Attachment: metadata, not part of the object body.
		}

		name, ok := singleFieldLabelName(label)
		if !ok {
			return nil, false // ".", multi-element path, ".*", or malformed.
		}

		if bindingNames[name] {
			continue // LET binding var: stripped from the star.
		}

		if seen[name] {
			return nil, false // Duplicate key: the boxed map would dedup.
		}
		seen[name] = true

		param = append(param, name, label)
	}

	return param, true
}

// singleFieldLabelName extracts the field name from a single-element field
// label -- e.g. `.["sales"]` -> "sales". It returns ok=false for the whole-row
// "." label, the ".*" star-spread, multi-element paths (`.["a","b"]`),
// attachments, or any label that does not parse as a one-element `.[...]` path
// (mirroring NewConvertVals's label -> path analysis).
func singleFieldLabelName(label string) (string, bool) {
	if len(label) < 2 || label[0] != '.' || label == ".*" {
		return "", false
	}

	var path []string
	if json.Unmarshal([]byte(label[1:]), &path) != nil {
		return "", false
	}
	if len(path) != 1 {
		return "", false
	}

	return path[0], true
}
