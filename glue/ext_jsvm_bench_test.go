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
	"os"
	"testing"

	"github.com/dop251/goja"

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
