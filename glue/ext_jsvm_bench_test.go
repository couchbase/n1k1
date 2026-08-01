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

// Per-call overhead of the decimal.js UDFs (DESIGN-extensions.md "JS modules"). Each
// benchmark measures one UDF invocation through the goja boundary: marshal the args in
// (toGoja), run the JS function, marshal the result out (fromGoja) — the same path the
// engine drives per row. BenchmarkJSUDFIdentity is the baseline (a trivial JS UDF), so
// (DecimalAdd − Identity) isolates the exact-BigInt-math + EJSON-result cost from the
// fixed goja-boundary cost.

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/dop251/goja"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/value"
)

func benchJSCall(b *testing.B, fnName string, args ...value.Value) {
	b.Helper()
	s := newJSSharedRuntime()
	fn := s.callable(fnName)
	if fn == nil {
		b.Fatalf("%q not callable", fnName)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ja := make([]goja.Value, len(args))
		for k, a := range args {
			ja[k] = toGoja(s.rt, a)
		}
		res, err := fn(goja.Undefined(), ja...)
		if err != nil {
			b.Fatalf("%s: %v", fnName, err)
		}
		if _, err := fromGoja(res); err != nil {
			b.Fatalf("%s fromGoja: %v", fnName, err)
		}
	}
}

func loadDecimalModuleForBench(b *testing.B) {
	b.Helper()
	src, err := os.ReadFile("../extensions/functions/js/builtin_decimal.js")
	if err != nil {
		b.Fatal(err)
	}
	if err := RegisterJSModule("decimal", string(src)); err != nil {
		b.Fatalf("RegisterJSModule: %v", err)
	}
}

// BenchmarkJSUDFIdentity is the goja-boundary baseline: a trivial UDF that returns its
// argument, so it measures only marshal-in + call + marshal-out.
func BenchmarkJSUDFIdentity(b *testing.B) {
	if err := RegisterJSFunc("bench_id", `function bench_id(x){return x;}`); err != nil {
		b.Fatal(err)
	}
	benchJSCall(b, "bench_id", value.NewValue("0.1"))
}

// BenchmarkJSDecimalAdd: DECIMAL_ADD("0.1","0.2") — boundary + parse + BigInt add +
// format + EJSON-object result.
func BenchmarkJSDecimalAdd(b *testing.B) {
	loadDecimalModuleForBench(b)
	benchJSCall(b, "decimal_add", value.NewValue("0.1"), value.NewValue("0.2"))
}

// BenchmarkJSDecimalMul: bigger BigInt coefficients (18-digit) to show the math scaling.
func BenchmarkJSDecimalMul(b *testing.B) {
	loadDecimalModuleForBench(b)
	benchJSCall(b, "decimal_mul", value.NewValue("123456789.012345678"), value.NewValue("9.87654321"))
}

// BenchmarkJSDecimalCmp: returns a plain -1/0/1 (no EJSON object built), isolating the
// parse+compare cost from EJSON-result construction.
func BenchmarkJSDecimalCmp(b *testing.B) {
	loadDecimalModuleForBench(b)
	benchJSCall(b, "decimal_cmp", value.NewValue("0.10"), value.NewValue("0.1"))
}

// --- JS aggregate state round-trip -------------------------------------------------
//
// The JS aggregate bridge threads the accumulator as JSON bytes, so every Update pays
// decode(state) + call + encode(state). These benchmarks isolate that cost at a FIXED
// state size, because the shape matters: a scalar-state aggregate (sum) pays O(1) per
// row, while a LIST-accumulating one (e.g. folding rows into a chart spec) pays
// O(state) per row -- i.e. O(n^2) over the group. Compare against the native base.Agg
// lane (sparkline/histogram), which is zero-garbage.

// benchAggUpdate measures ONE Update against a pre-built state blob of the given size.
func benchAggUpdate(b *testing.B, aggName, stateJSON, rowJSON string) {
	b.Helper()
	sr := newJSSharedRuntime()
	if sr.callable(aggName+"_update") == nil {
		b.Fatalf("%s_update not callable", aggName)
	}
	blob := []byte(stateJSON)
	row := base.Val(rowJSON)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := sr.aggStateFromJSON(blob) // decode the accumulator
		val := valBytesToGoja(sr.rt, row)  // decode the row value
		next := jsAggCall(sr, aggName+"_update", state, val)
		if out := marshalGoja(next); len(out) == 0 { // re-encode the accumulator
			b.Fatal("empty state")
		}
	}
}

func mustRegBenchAgg(b *testing.B, name, src string) {
	b.Helper()
	if err := RegisterJSAggregate(name, src); err != nil {
		b.Fatal(err)
	}
}

// Scalar state: {n,sum} -- O(1) per Update regardless of group size.
func BenchmarkJSAggScalarState(b *testing.B) {
	mustRegBenchAgg(b, "bench_ssum", `
		function bench_ssum_init(){ return {n:0,sum:0}; }
		function bench_ssum_update(s,v){ if (typeof v === "number") { s.n++; s.sum+=v; } return s; }
		function bench_ssum_final(s){ return s.n ? s.sum : null; }`)
	benchAggUpdate(b, "bench_ssum", `{"n":10,"sum":55}`, `7`)
}

// List state: {rows:[...]} -- the chart/collect shape. Parameterized by how many rows
// the accumulator ALREADY holds, to expose the O(state)-per-Update behaviour.
func BenchmarkJSAggListState(b *testing.B) {
	mustRegBenchAgg(b, "bench_lrows", `
		function bench_lrows_init(){ return {rows:[]}; }
		function bench_lrows_update(s,v){ s.rows = s.rows.concat([v]); return s; }
		function bench_lrows_final(s){ return s.rows.length; }`)
	for _, n := range []int{0, 100, 1000} {
		rows := make([]string, n)
		for i := range rows {
			rows[i] = `{"x":"2026-01-15","y":129.5}`
		}
		state := `{"rows":[` + strings.Join(rows, ",") + `]}`
		b.Run(fmt.Sprintf("held=%d", n), func(b *testing.B) {
			benchAggUpdate(b, "bench_lrows", state, `{"x":"2026-02-03","y":49.99}`)
		})
	}
}
