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

// The base package holds types and definitions shared by n1k1's
// interpreter and compiler.
package base

import (
	"encoding/binary"
	"fmt"

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

// -----------------------------------------------------

// ValPathGet navigates through the JSON val using the given path and
// returns the JSON found there, or missing. Example path: [] or
// ["addresses", "work", "city"].
func ValPathGet(val Val, path []string) Val {
	valOut, _, _, err := jsonparser.Get(val, path...)
	if err != nil {
		return ValMissing
	}

	return valOut
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

// ValsDecode appending each decoded val to the valsOut slice.
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

// YieldErr is the callback invoked when there's an error, or with a
// final nil error when data processing is complete.
type YieldErr func(error)

// -----------------------------------------------------

// Labels represent names for a related instance of Vals.  Usually,
// the related Vals has the same size: len(vals) == len(labels), which
// enables optimizations through slice positional lookups.
type Labels []string // Ex: [`.`, `.["address","city"]`].

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

	// Params based on the kind.
	Params []interface{} `json:"Params,omitempty"`

	// Children are the data source operators that feed or yield data
	// to their immediate parent operator.
	Children []*Op `json:"Children,omitempty"`
}

// -----------------------------------------------------

// An ExprFunc evaluates an expression against the given vals.
type ExprFunc func(vals Vals, yieldErr YieldErr) Val

// A BiExprFunc represents a two-parameter expression.
type BiExprFunc func(a, b ExprFunc, vals Vals, yieldErr YieldErr) Val

// A ProjectFunc projects (in relational parlance) the given vals into
// resulting vals, reusing the pre-allocated valsPre if neeeded.
type ProjectFunc func(vals, valsPre Vals, yieldErr YieldErr) Vals

// -----------------------------------------------------

// ExprCatalogFunc is the signature of expression constructors
// that are registered in an expression catalog.
type ExprCatalogFunc func(vars *Vars, labels Labels,
	params []interface{}, path string) ExprFunc

// -----------------------------------------------------

// An Op can occasionally yield stats and progress information,
// where the call can return an error to abort further processing.
//
// The YieldStats implementor must copy any incoming stats that it
// wants to keep and should be implemented as concurrent safe.
type YieldStats func(*Stats) error

type Stats struct{} // TODO.

// -----------------------------------------------------

func ArrayYield(val Val, yieldVals YieldVals, valsPre Vals) (Vals, bool) {
	parseVal, parseType := Parse(val)
	if parseType == int(jsonparser.Array) {
		jsonparser.ArrayEach(parseVal, func(v []byte,
			vType jsonparser.ValueType, vOffset int, vErr error) {
			if vErr != nil {
				return
			}

			valsPre = valsPre[:0]
			valsPre = append(valsPre, v)

			yieldVals(valsPre)
		})

		return valsPre, true
	}

	return valsPre, false
}
