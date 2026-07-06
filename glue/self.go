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

// byteMode selects how ConvertVals.ConvertBytes renders a row to JSON without
// the value.Value boxing that Convert + json.Marshal(v.Actual()) incurs.
type byteMode uint8

const (
	byteBoxed  byteMode = iota // Shape not natively encodable -> caller must box.
	byteWhole                  // A lone "." label: the val IS the row JSON.
	byteStar                   // A lone ".*" label: the val IS the (object) row JSON.
	byteFields                 // All single-element .["name"] fields (+ "^" skipped).
)

// planBytes precomputes the ConvertBytes plan from the labels. The label set is
// fixed for a result, so the native-vs-boxed decision (and the field key tokens)
// is made once here rather than per row.
func (s *ConvertVals) planBytes() {
	if len(s.Labels) == 1 {
		switch s.Labels[0] {
		case ".":
			s.byteMode = byteWhole
			return
		case ".*":
			s.byteMode = byteStar
			return
		}
	}

	// Every label must be a single-element .["name"] field, or a "^..."
	// attachment (skipped -- metadata rides annotations, dropped by v.Actual(),
	// so it never appears in the encoded body). Distinct keys. Anything else
	// (whole-row ".", ".*", multi-element paths) needs the boxed path.
	var keyTokens [][]byte
	var indices []int
	seen := map[string]bool{}

	for i, label := range s.Labels {
		if len(label) > 0 && label[0] == '^' {
			continue
		}

		path := s.LabelPaths[i]
		if len(path) != 1 || seen[path[0]] {
			s.byteMode = byteBoxed
			return
		}
		seen[path[0]] = true

		keyTokens = append(keyTokens, base.SelfKeyToken(path[0]))
		indices = append(indices, i)
	}

	s.byteKeyTokens = keyTokens
	s.byteIndices = indices
	s.byteMode = byteFields
}

// ConvertBytes renders vals to the row's JSON, appended to out, WITHOUT the
// value.Value boxing of Convert + json.Marshal(v.Actual()) -- for the row
// shapes planBytes recognized. It returns (out, true) when it encoded the row
// natively (zero garbage beyond out's growth), or (out, false) when the shape
// needs the boxed path, in which case the caller should fall back to Convert.
//
// Keys are emitted in label order and nested values pass through as their
// original bytes (no recursive key-sort or number re-canonicalization), unlike
// the boxed path -- a byte-level, not value-level, difference. A MISSING (empty)
// field is omitted, matching Convert's UnsetField.
func (s *ConvertVals) ConvertBytes(vals base.Vals, out []byte) (rv []byte, handled bool) {
	if len(s.Labels) != len(vals) {
		return out, false
	}

	switch s.byteMode {
	case byteWhole:
		if len(vals[0]) == 0 {
			return append(out, "null"...), true
		}
		return append(out, vals[0]...), true

	case byteStar:
		// A ".*" star over a non-object (or MISSING) contributes {} (SELECT
		// path.* over a scalar); an object passes through as the row body.
		if len(vals[0]) > 0 && vals[0][0] == '{' {
			return append(out, vals[0]...), true
		}
		return append(out, "{}"...), true

	case byteFields:
		return base.ValsSelfObject(s.byteKeyTokens, s.byteIndices, vals, out), true

	default:
		return out, false
	}
}

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
