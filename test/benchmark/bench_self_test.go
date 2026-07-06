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

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

// The per-row cost of a whole-row `self` / SELECT * projection over a
// two-keyspace star row (SELECT * FROM orders o JOIN items i). The native path
// (engine.ExprSelf -> base.ValsSelfObject) assembles the {"o":..,"i":..} object
// straight from the label bytes into a reused buffer -- zero steady-state
// garbage. The boxed fallback runs ConvertVals.Convert -> value.WriteJSON per
// row, allocating one value.Value plus its JSON.
//
//   go test -tags n1ql -run=xxx -bench=Self -benchmem ./test/benchmark

var benchSelfLabels = base.Labels{`.["o"]`, "^id", `.["i"]`, "^id"}

var benchSelfVals = base.Vals{
	base.Val(`{"id":1,"cust":{"name":"a","city":"x"},"tags":[1,2,3]}`),
	base.Val(`"o0001"`),
	base.Val(`{"id":10,"sku":"s10","qty":5}`),
	base.Val(`"i0010"`),
}

func BenchmarkSelfNative(b *testing.B) {
	keyTokens := [][]byte{base.SelfKeyToken("o"), base.SelfKeyToken("i")}
	indices := []int{0, 2}

	var buf []byte

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = base.ValsSelfObject(keyTokens, indices, benchSelfVals, buf[:0])
	}
	_ = buf
}

func BenchmarkSelfBoxed(b *testing.B) {
	cv, err := glue.NewConvertVals(benchSelfLabels)
	if err != nil {
		b.Fatal(err)
	}

	var buf bytes.Buffer

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, err := cv.Convert(benchSelfVals)
		if err != nil {
			b.Fatal(err)
		}
		buf.Reset()
		if err := v.WriteJSON(nil, &buf, "", "", true); err != nil {
			b.Fatal(err)
		}
	}
}

// The Session.OnRow / Result.Rows encoding path: ConvertVals.ConvertBytes
// (boxing-free, reused buffer) vs the former Convert -> json.Marshal(v.Actual())
// -- a per-projected-column row (SELECT a, b, c), the common result shape.
var benchRowLabels = base.Labels{`.["id"]`, `.["name"]`, `.["tags"]`}

var benchRowVals = base.Vals{
	base.Val(`123`),
	base.Val(`"widget"`),
	base.Val(`["a","b","c"]`),
}

func BenchmarkConvertBytesNative(b *testing.B) {
	cv, err := glue.NewConvertVals(benchRowLabels)
	if err != nil {
		b.Fatal(err)
	}

	var buf []byte

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ = cv.ConvertBytes(benchRowVals, buf[:0])
	}
	_ = buf
}

func BenchmarkConvertBytesBoxed(b *testing.B) {
	cv, err := glue.NewConvertVals(benchRowLabels)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, err := cv.Convert(benchRowVals)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = json.Marshal(v.Actual())
	}
}
