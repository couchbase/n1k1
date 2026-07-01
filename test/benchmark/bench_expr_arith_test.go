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

package benchmark

// Characterize the memory profile of a native n1k1 arithmetic primitive
// (base.Num, byte-in / byte-out into a reused buffer) versus cbq's expression
// Evaluate() -- the exact path n1k1 falls back to today (glue ExprTree:
// box base.Vals -> value.Value, Evaluate, then WriteJSON unbox). Run e.g.:
//
//   go test -tags n1ql ./test/benchmark -run xxx -bench BenchmarkArith -benchmem
//
// Expectation: the native path holds ~0 allocs/op (buffer reuse, no boxing),
// while the cbq Evaluate path allocates value.Value garbage per row.

import (
	"bytes"
	"testing"
	"time"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// sink prevents the compiler from optimizing the native loop away.
var sink []byte

// --- native byte primitives -------------------------------------------------

func BenchmarkArithAddNative(b *testing.B) {
	// Operand bytes as a child ExprFunc (e.g. labelPath) would deliver them.
	aBytes, bBytes := []byte("12345"), []byte("678")
	var buf []byte

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		na, _ := base.ParseNum(aBytes)
		nb, _ := base.ParseNum(bBytes)
		buf = base.AppendNum(buf[:0], na.Add(nb))
	}
	b.StopTimer()
	sink = buf
}

func BenchmarkArithDivNative(b *testing.B) {
	aBytes, bBytes := []byte("12345"), []byte("678")
	var buf []byte

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		na, _ := base.ParseNum(aBytes)
		nb, _ := base.ParseNum(bBytes)
		r, _ := na.Div(nb)
		buf = base.AppendNum(buf[:0], r)
	}
	b.StopTimer()
	sink = buf
}

// --- cbq expression Evaluate (the fallback path) ----------------------------

// evalCBQ models glue/expr.go:ExprTree's per-row fallback for a binary arith
// expression over two fields: (1) box the row into a value.Value doc, (2)
// Evaluate (which boxes the result), (3) WriteJSON to unbox back to bytes.
func evalCBQ(b *testing.B, expr expression.Expression) {
	ctx := glue.NewExprGlueContext(time.Now())
	var buf bytes.Buffer

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := value.NewValue(map[string]interface{}{"a": 12345, "b": 678}) // box (Convert)
		r, err := expr.Evaluate(item, ctx)                                   // evaluate (boxes result)
		if err != nil {
			b.Fatal(err)
		}
		buf.Reset()
		_ = r.WriteJSON(nil, &buf, "", "", true) // unbox to bytes
	}
	b.StopTimer()
	sink = buf.Bytes()
}

func BenchmarkArithAddCBQ(b *testing.B) {
	evalCBQ(b, expression.NewAdd(
		expression.NewIdentifier("a"), expression.NewIdentifier("b")))
}

func BenchmarkArithDivCBQ(b *testing.B) {
	evalCBQ(b, expression.NewDiv(
		expression.NewIdentifier("a"), expression.NewIdentifier("b")))
}
