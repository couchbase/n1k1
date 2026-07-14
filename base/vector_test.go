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
	"testing"
)

func vdist(t *testing.T, a, b, metric string) Val {
	t.Helper()
	vals := Vals{Val(a), Val(b), Val(`"` + metric + `"`)}
	res, _ := VectorDistanceVals(vals, nil)
	return res
}

func TestVectorDistanceVals(t *testing.T) {
	num := func(v Val) float64 {
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			t.Fatalf("result %q not a number: %v", v, err)
		}
		return f
	}

	// cosine (= 1 - cos_sim): orthogonal -> 1, identical -> 0, opposite -> 2.
	if r := vdist(t, `[1,0,0]`, `[0,1,0]`, "cosine"); math.Abs(num(r)-1) > 1e-12 {
		t.Errorf("cosine orthogonal = %s, want 1", r)
	}
	if r := vdist(t, `[1,2,3]`, `[1,2,3]`, "cosine"); math.Abs(num(r)) > 1e-12 {
		t.Errorf("cosine identical = %s, want ~0", r)
	}
	if r := vdist(t, `[1,0]`, `[-1,0]`, "cosine"); math.Abs(num(r)-2) > 1e-12 {
		t.Errorf("cosine opposite = %s, want 2", r)
	}
	// l2 / l2_squared: [1,0] vs [1,1] -> sqrt(1)=1, squared=1.
	if r := vdist(t, `[1,0]`, `[1,1]`, "l2"); math.Abs(num(r)-1) > 1e-12 {
		t.Errorf("l2 = %s, want 1", r)
	}
	if r := vdist(t, `[3,4]`, `[0,0]`, "l2"); math.Abs(num(r)-5) > 1e-12 {
		t.Errorf("l2 3-4-5 = %s, want 5", r)
	}
	if r := vdist(t, `[1,0]`, `[1,1]`, "l2_squared"); math.Abs(num(r)-1) > 1e-12 {
		t.Errorf("l2_squared = %s, want 1", r)
	}
	// dot (negated): [1,2] . [3,4] = 11 -> -11.
	if r := vdist(t, `[1,2]`, `[3,4]`, "dot"); math.Abs(num(r)+11) > 1e-12 {
		t.Errorf("dot = %s, want -11", r)
	}

	// Edge cases matching cbq: MISSING -> MISSING, non-array -> NULL, length mismatch ->
	// NULL, non-number element -> NULL, cosine over a zero vector -> NULL.
	edge := func(a, b, metric string) Val {
		return vdist(t, a, b, metric)
	}
	if r, _ := VectorDistanceVals(Vals{ValMissing, Val(`[1,2]`), Val(`"l2"`)}, nil); r != nil {
		t.Errorf("MISSING operand -> %q, want MISSING(nil)", r)
	}
	if r := edge(`42`, `[1,2]`, "l2"); string(r) != "null" {
		t.Errorf("non-array -> %q, want null", r)
	}
	if r := edge(`[1,2,3]`, `[1,2]`, "l2"); string(r) != "null" {
		t.Errorf("length mismatch -> %q, want null", r)
	}
	if r := edge(`[1,"x"]`, `[1,2]`, "l2"); string(r) != "null" {
		t.Errorf("non-number element -> %q, want null", r)
	}
	if r := edge(`[0,0]`, `[1,1]`, "cosine"); string(r) != "null" {
		t.Errorf("cosine zero-norm -> %q, want null", r)
	}
	if r := edge(`[1,2]`, `[3,4]`, "bogus"); string(r) != "null" {
		t.Errorf("unknown metric -> %q, want null", r)
	}
}

