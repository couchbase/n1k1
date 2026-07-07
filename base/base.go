//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// The base package holds basic, common types and definitions shared
// by n1k1's interpreter and compiler.
package base

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/buger/jsonparser"
)

type Val []byte // JSON encoded, usually treated as immutable.

func (a Val) String() string {
	return fmt.Sprintf("%q", []byte(a))
}

var ValMissing = Val(nil)

var ValNull = Val([]byte("null"))

var ValTrue = Val([]byte("true"))

var ValFalse = Val([]byte("false"))

var ValArrayEmpty = Val([]byte("[]"))

// -----------------------------------------------------

func ValEqualMissing(val Val) bool {
	return len(val) == 0
}

func ValEqualNull(val Val) bool {
	return len(val) != 0 && val[0] == 'n' // Ex: null.
}

func ValEqualTrue(val Val) bool {
	return len(val) != 0 && val[0] == 't' // Ex: true.
}

// ValTruthy implements N1QL's truth-value semantics (value.Value.Truth())
// for boolean condition contexts (WHERE, ON, WHEN, HAVING). A value is truthy
// unless it is MISSING, NULL, false, the number 0 (or NaN), or an empty
// string / array / object. E.g. a non-empty string such as "Euros Lyn" is
// truthy, which is why a FIRST/ANY comprehension yielding a string passes a
// WHERE clause.
func ValTruthy(val Val) bool {
	if len(val) == 0 { // MISSING.
		return false
	}

	switch val[0] {
	case 't': // true.
		return true
	case 'f', 'n': // false or null.
		return false
	case '"': // String: truthy if non-empty (i.e. more than just "").
		return len(val) > 2
	case '[', '{': // Array or object: truthy if it has any element/field.
		for i := 1; i < len(val); i++ {
			c := val[i]
			if c == ']' || c == '}' {
				return false // Only whitespace before the close => empty.
			}
			if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
				return true
			}
		}
		return false
	default: // Number: truthy if non-zero and not NaN.
		f, err := strconv.ParseFloat(string(val), 64)
		return err == nil && f != 0 && f == f
	}
}

// ValEqual is based on N1QL's rules for missing & null's.
func ValEqual(valA, valB Val, valComparer *ValComparer) Val {
	if ValEqualMissing(valA) {
		return ValMissing
	} else if ValEqualMissing(valB) {
		return ValMissing
	} else if valA[0] == 'n' { // Avoid ValEqualNull's len() check.
		return ValNull
	} else if valB[0] == 'n' {
		return ValNull
	} else if valComparer.Compare(valA, valB) == 0 {
		return ValTrue
	}

	return ValFalse
}

// ValHasValue returns true if val is not missing and is not null.
func ValHasValue(val Val) bool {
	return len(val) != 0 && val[0] != 'n'
}

// ValIsNumber reports whether v parses as a JSON number.
func ValIsNumber(v Val) bool {
	_, pt := Parse(v)
	return ParseTypeToValType[pt] == ValTypeNumber
}

// -----------------------------------------------------

// ValPathGet navigates through the JSON val using the given path and
// returns the JSON found there, or missing. Example path: [] or
// ["addresses", "work", "city"]. The valOut is an optional
// preallocated output val.
func ValPathGet(val Val, path []string, valOut Val) (Val, Val) {
	v, vType, _, err := jsonparser.Get(val, path...)
	if err != nil {
		return ValMissing, valOut
	}

	if vType == jsonparser.String {
		valOut = append(append(append(valOut[:0], '"'), v...), '"')
		return valOut, valOut
	}

	return v, valOut
}

// -----------------------------------------------------

// Vals is a slice of Val's. Vals are often passed between data
// processing operators. The positional entries in a Vals are often
// associated with matching Labels.
type Vals []Val

// -----------------------------------------------------

// ValsDeepCopy copies vals into the optional, preallocated slices.
func ValsDeepCopy(vals Vals, preallocVals Vals, preallocVal Val) (
	Vals, Vals, Val) {
	var bytesNeeded int
	for _, val := range vals {
		bytesNeeded += len(val)
	}

	if len(preallocVal) < bytesNeeded {
		preallocVal = make(Val, bytesNeeded)
	}

	copyVal := preallocVal[:0]
	preallocVal = preallocVal[bytesNeeded:]

	if len(preallocVals) < len(vals) {
		preallocVals = make(Vals, len(vals))
	}

	copyVals := preallocVals[:0]
	preallocVals = preallocVals[len(vals):]

	for _, val := range vals {
		copyVal = append(copyVal, val...)
		copyVals = append(copyVals, copyVal)
		copyVal = copyVal[len(val):]
	}

	return copyVals, preallocVals, preallocVal
}

// -----------------------------------------------------

// ValsEncode appends the encoded vals to the out slice.
func ValsEncode(vals Vals, out []byte) []byte {
	out = BinaryAppendUint64(out, uint64(len(vals)))

	for _, v := range vals {
		out = append(BinaryAppendUint64(out, uint64(len(v))), v...)
	}

	return out
}

// ValsEncodeCanonical appends the canonical encoded vals to the out
// slice, so its usable as a map key.
func ValsEncodeCanonical(vals Vals, out []byte,
	valComparer *ValComparer) (rv []byte, err error) {
	out = BinaryAppendUint64(out, uint64(len(vals)))

	var buf8 [8]byte

	for _, v := range vals {
		beg := len(out)
		out = append(out, buf8[:]...) // Prepare space for val len.

		if len(v) > 0 {
			out, err = valComparer.CanonicalJSON(v, out)
			if err != nil {
				return out, err
			}
		}

		// Write the canonical val len into the earlier prepared space.
		binary.LittleEndian.PutUint64(out[beg:beg+8], uint64(len(out)-8-beg))
	}

	return out, nil
}

// ValsDecode appends each decoded val to the valsOut slice.
func ValsDecode(b []byte, valsOut Vals) Vals {
	n := binary.LittleEndian.Uint64(b[:8])
	b = b[8:]

	for i := uint64(0); i < n; i++ {
		vLen := binary.LittleEndian.Uint64(b[:8])
		b = b[8:]

		valsOut = append(valsOut, Val(b[0:vLen]))
		b = b[vLen:]
	}

	return valsOut
}

// -----------------------------------------------------

// BinaryAppendUint64 appends a binary encoded uint64 to out.
func BinaryAppendUint64(out []byte, v uint64) []byte {
	var buf8 [8]byte
	binary.LittleEndian.PutUint64(buf8[:], v)
	return append(out, buf8[:]...)
}

// -----------------------------------------------------

// YieldVals memory ownership: the receiver implementation should
// generally copy any inputs that it wants to keep if it's in a
// different "pipeline", because the provided slices might be reused
// by future invocations.
type YieldVals func(Vals)

// YieldErr is the callback invoked when there's an error, and is
// invoked with a final nil error when data processing is complete.
type YieldErr func(error)

// -----------------------------------------------------

// Labels represent names for a related instance of Vals. Usually, the
// related Vals has the same size: len(vals) == len(labels), which
// enables optimizations through slice positional lookups.
//
// A label names what its Vals[i] slot holds. The engine treats labels as
// opaque strings (positional lookup via IndexOf -- see ExprLabelPath); their
// SEMANTICS are assigned by the glue conversion (conv.go) and interpreted when
// a row is materialized (glue ConvertVals.Convert / ConvertBytes). The first
// character selects the namespace:
//
//	"."  -- the row/value body (what a projection serializes to JSON):
//	  "."              the whole current value/row -- the slot IS the value.
//	                   Used by RAW/ELEMENT projections and whole-doc scans.
//	  `.["city"]`      a field: the value at top-level key "city".
//	  `.["a","b"]`     a nested field path a.b (a JSON-encoded []string, one
//	                   element per path step). Built via LabelSuffix (conv.go).
//	  ".*"             star-spread (SELECT *, SELECT path.*): the slot holds an
//	                   object whose fields are MERGED into the row, not nested
//	                   under a key. Multiple ".*" (and ".*" mixed with fields)
//	                   combine into one object.
//	  (Synthetic field names may appear inside the path, e.g. `.["$group0"]`
//	   for a computed GROUP BY key column -- still ordinary "." fields.)
//
//	"^"  -- an attachment: out-of-band metadata that rides ALONGSIDE the row
//	        but is NOT emitted in the serialized body (it becomes a value.Value
//	        annotation, dropped by WriteJSON / v.Actual()). Attached to the
//	        preceding "."-set doc so META(alias)/SEARCH_META(alias) resolve.
//	  "^id"            the document id            -> AnnotatedValue.SetId (META().id).
//	  "^smeta"         FTS search-meta {score,id} -> ATT_SMETA (SEARCH_META/SCORE).
//	  `^name`          a generic named attachment.
//	  `^name|key`      an attachment that is a map, with sub-key "key". E.g.
//	                   `^aggregates|count(*)` -- the group op stores each
//	                   finalized aggregate's JSON value here, keyed by the
//	                   aggregate's expression text (read natively by labelPath;
//	                   see glue/expr_optimize.go).
type Labels []string // Ex: [`.`, `.["address","city"]`, `^id`, `^aggregates|count(*)`].

func (a Labels) IndexOf(s string) int {
	for i, v := range a {
		if v == s {
			return i
		}
	}

	return -1
}

// -----------------------------------------------------

// An Op represents a node or operator in a query-plan tree.
type Op struct {
	// Ex: "scan", "filter", "project", etc.
	Kind string `json:"Kind,omitempty"`

	// Labels for the vals yielded by this operator.
	Labels Labels `json:"Labels,omitempty"`

	// Params are the input to the operator based on the kind.
	Params []interface{} `json:"Params,omitempty"`

	// Children are the data source operators that feed or yield data
	// to their immediate parent operator.
	Children []*Op `json:"Children,omitempty"`

	// StatsBase is the offset of this op's first counter within the flat
	// Ctx.Stats.Counters array, assigned once by LayoutStats at request setup
	// (-1 if the op contributes no counters). Not serialized -- it is recomputed
	// from StatsDescs each run. See stats.go and DESIGN-stats.md.
	StatsBase int `json:"-"`

	// PreviewSlot is this op's fixed index into Ctx.Stats.Previews (the per-op
	// live-aggregate snapshot buffers), assigned once by LayoutStats at request
	// setup (-1 if the op never previews). Like StatsBase it gives each op a
	// private, single-writer slot so parallel actors (e.g. a GROUP BY inside each
	// UNION ALL branch) never collide -- see stats.go / vars.go RefreshPreviews and
	// DESIGN-stats.md "Live aggregates". Not serialized.
	PreviewSlot int `json:"-"`
}

// -----------------------------------------------------

// An ExprFunc evaluates an expression against the given vals.
type ExprFunc func(vals Vals, yieldErr YieldErr) Val

// A BiExprFunc represents a two-parameter expression.
type BiExprFunc func(a, b ExprFunc, vals Vals, yieldErr YieldErr) Val

// A TriExprFunc represents a three-parameter ("ternary") expression.
type TriExprFunc func(a, b, c ExprFunc, vals Vals, yieldErr YieldErr) Val

// A NaryExprFunc represents a variadic expression: it is handed the already-built
// child ExprFuncs and reduces them to a result. The child slice is built once at
// setup; the reduce runs per row.
type NaryExprFunc func(children []ExprFunc, vals Vals, yieldErr YieldErr) Val

// A ProjectFunc projects (in relational parlance) the given vals into
// resulting vals, and can reuse the optional, pre-allocated valsPre.
type ProjectFunc func(vals, valsPre Vals, yieldErr YieldErr) Vals

// -----------------------------------------------------

// ExprCatalogFunc is the signature of expression constructors
// that are registered in an expression catalog.
type ExprCatalogFunc func(vars *Vars, labels Labels,
	params []interface{}, path string) ExprFunc

// -----------------------------------------------------

// An Op can occasionally yield stats and progress information,
// and can return an error to abort further processing.
//
// The YieldStats implementor must copy any incoming stats that it
// wants to keep and should be implemented as concurrent safe. The argument
// is the request's shared *Stats (see stats.go), or nil when stats are off.
type YieldStats func(*Stats) error

// -----------------------------------------------------

// ArrayYield invokes the yieldVals callback on every item in the val
// when the val is an array. It reuses the optional valsPre, if needed.
func ArrayYield(val Val, yieldVals YieldVals, valsPre Vals) (Vals, bool) {
	parseVal, parseType := Parse(val)
	if parseType == int(jsonparser.Array) {
		jsonparser.ArrayEach(parseVal, func(v []byte,
			vType jsonparser.ValueType, vOffset int, vErr error) {
			if vErr != nil {
				return
			}

			// jsonparser.ArrayEach strips the surrounding quotes from a string
			// element, so a string like "Humor" arrives as raw bytes (Humor)
			// that downstream reads as BINARY rather than a JSON string. Re-wrap
			// it so the yielded element is valid JSON. (Other element types --
			// objects, numbers, bools, null -- are already valid JSON as-is.)
			//
			// TODO: this allocates per string element; reuse a scratch buffer if
			// it shows up hot (it can't reuse valsPre, whose bytes outlive the
			// callback when the consumer retains references).
			elem := v
			if vType == jsonparser.String {
				q := make([]byte, 0, len(v)+2)
				q = append(q, '"')
				q = append(q, v...)
				q = append(q, '"')
				elem = q
			}

			valsPre = valsPre[:0]
			valsPre = append(valsPre, elem)

			yieldVals(valsPre)
		})

		return valsPre, true
	}

	return valsPre, false
}
