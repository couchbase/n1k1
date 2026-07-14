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

// TestVectorDistanceVFloat32MatchesVals is the correctness anchor for the columnar
// float32 kernel: on the SAME float32-rounded values, VectorDistanceVFloat32 must
// produce bit-identical distances to the scalar/native float64 core
// (VectorDistanceVals) -- same float64 promotion, same op order. So the columnar path
// is a drop-in whose only difference from the row path is float32 STORAGE (the win),
// not the math. We round every input to float32 first so both sides compute over
// identical values (the storage rounding is not a divergence, it's the point).
func TestVectorDistanceVFloat32MatchesVals(t *testing.T) {
	const dim, rows = 7, 40
	// A small deterministic LCG -> reproducible pseudo-random float32s in [-1,1).
	seed := uint64(0x9e3779b97f4a7c15)
	nextF32 := func() float32 {
		seed = seed*6364136223846793005 + 1442695040888963407
		return float32(int64(seed>>11))/float32(1<<52)*2 - 1
	}

	col := make([]float32, rows*dim)
	for i := range col {
		col[i] = nextF32()
	}
	q := make([]float64, dim)
	for i := range q {
		q[i] = float64(nextF32()) // already a float32 value, so both paths agree
	}

	// f32arr renders a []float32 slice as a JSON number array for VectorDistanceVals;
	// float64(float32) is exactly representable so it round-trips through strconv.
	f32arr := func(v []float32) Val {
		out := []byte{'['}
		for i, x := range v {
			if i > 0 {
				out = append(out, ',')
			}
			out = strconv.AppendFloat(out, float64(x), 'g', -1, 64)
		}
		return Val(append(out, ']'))
	}
	qf32 := make([]float32, dim)
	for i := range q {
		qf32[i] = float32(q[i])
	}
	qArr := f32arr(qf32)

	out := make([]float64, rows)
	for _, metric := range []string{"l2", "euclidean", "l2_squared", "dot", "cosine"} {
		VectorDistanceVFloat32(out, col, q, rows, dim, metric)
		for r := 0; r < rows; r++ {
			want := vdist(t, string(f32arr(col[r*dim:r*dim+dim])), string(qArr), metric)
			var got Val
			if math.IsNaN(out[r]) {
				got = ValNull // NaN sentinel maps to N1QL NULL
			} else {
				got = Val(strconv.AppendFloat(nil, out[r], 'f', -1, 64))
			}
			if string(got) != string(want) {
				t.Fatalf("%s row %d: columnar %q != scalar %q", metric, r, got, want)
			}
		}
	}
}

// TestVecFloat32Borrow: the unsafe reinterpret is a zero-copy view -- it aliases the
// backing bytes (mutating the bytes shows through, and cap tracks the byte length).
func TestVecFloat32Borrow(t *testing.T) {
	b := make([]byte, 3*4)
	src := []float32{1.5, -2.25, 3.75}
	for i, f := range src {
		bits := math.Float32bits(f)
		b[i*4], b[i*4+1], b[i*4+2], b[i*4+3] = byte(bits), byte(bits>>8), byte(bits>>16), byte(bits>>24)
	}
	v := VecFloat32(b)
	if len(v) != 3 {
		t.Fatalf("len = %d, want 3", len(v))
	}
	for i, f := range src {
		if v[i] != f {
			t.Errorf("v[%d] = %v, want %v", i, v[i], f)
		}
	}
	// Prove the borrow: rewrite the first element's bytes, the view reflects it.
	bits := math.Float32bits(9.0)
	b[0], b[1], b[2], b[3] = byte(bits), byte(bits>>8), byte(bits>>16), byte(bits>>24)
	if v[0] != 9.0 {
		t.Errorf("borrow broken: v[0] = %v after byte rewrite, want 9", v[0])
	}
}

