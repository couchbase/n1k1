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

package base

import (
	"math"
	"strconv"

	"github.com/buger/jsonparser"
)

// VectorDistanceVals is the byte-lane core of VECTOR_DISTANCE (DESIGN-vectors.md): it
// computes the distance between two numeric-array Vals WITHOUT boxing them into cbq
// value.Value arrays -- the measured jsonl bottleneck (~1000 allocs/row). vals is
// [vecA, vecB, metric]; out is the reused result buffer. (Two small []float64 scratch
// slices are allocated per call -- ~2 allocs/row vs the boxed path's ~1000; a lifted
// float scratch is a further optimization the gen-compiler's varLift can't carry today.)
//
// Semantics match cbq's expression/func_vector.go vectorDistance() bit-for-bit so the
// native path is a drop-in for the boxed one: MISSING vec -> MISSING; a non-array or a
// length mismatch or a non-number element or an out-of-float32-range value -> NULL;
// metrics l2/euclidean (sqrt), l2_squared/euclidean_squared, dot (negated), cosine
// (1 - cos_sim; NULL if either norm is 0). Result formatted as a JSON number with the
// same 'f' formatting the boxed path uses.
func VectorDistanceVals(vals Vals, out []byte) (Val, []byte) {
	if len(vals) < 3 {
		return ValNull, out
	}
	a, b := vals[0], vals[1]
	if len(a) == 0 || len(b) == 0 { // MISSING operand -> MISSING (cbq: vec.Type()==MISSING)
		return ValMissing, out
	}

	metric := unquoteMetric(vals[2])

	bufA, okA, arrA := parseFloatArray(a)
	bufB, okB, arrB := parseFloatArray(b)
	if !arrA || !arrB || !okA || !okB || len(bufA) != len(bufB) {
		return ValNull, out // non-array / non-number element / length mismatch
	}

	const maxF32 = math.MaxFloat32
	var dist, sdf, sqf float64
	for i := range bufA {
		df, qf := bufA[i], bufB[i]
		if df < -maxF32 || df > maxF32 || qf < -maxF32 || qf > maxF32 {
			return ValNull, out
		}
		switch metric {
		case "l2", "euclidean", "l2_squared", "euclidean_squared":
			d := df - qf
			dist += d * d
		case "dot":
			dist += df * qf
		case "cosine":
			dist += df * qf
			sdf += df * df
			sqf += qf * qf
		}
	}

	var res float64
	switch metric {
	case "l2", "euclidean":
		res = math.Sqrt(dist)
	case "l2_squared", "euclidean_squared":
		res = dist
	case "dot":
		res = -dist
	case "cosine":
		if sdf == 0 || sqf == 0 {
			return ValNull, out
		}
		res = 1.0 - (dist / (math.Sqrt(sdf) * math.Sqrt(sqf)))
	default:
		return ValNull, out
	}

	if res == 0 {
		res = 0 // normalize IEEE -0.0 (e.g. dot of orthogonal vectors) to cbq's "0"
	}
	out = strconv.AppendFloat(out[:0], res, 'f', -1, 64)
	return Val(out), out
}

// parseFloatArray parses a numeric JSON-array Val into a fresh []float64. ok=false if any
// element is not a number; isArray=false if v is not an array at all.
func parseFloatArray(v Val) (out []float64, ok bool, isArray bool) {
	pv, pt := Parse(v)
	if ParseTypeToValType[pt] != ValTypeArray {
		return nil, false, false
	}
	allNum := true
	jsonparser.ArrayEach(pv, func(e []byte, dt jsonparser.ValueType, _ int, _ error) {
		if dt != jsonparser.Number {
			allNum = false
			return
		}
		f, err := ParseFloat64(e)
		if err != nil {
			allNum = false
			return
		}
		out = append(out, f)
	})
	return out, allNum, true
}

// unquoteMetric reads the metric operand Val (a JSON string like `"cosine"`) as a bare
// lowercase-ish string. jsonparser/Parse leaves the quotes on a raw string Val, so strip
// a single surrounding pair; a non-string yields "" (-> the default/NULL branch).
func unquoteMetric(v Val) string {
	s := v
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	return string(s)
}
