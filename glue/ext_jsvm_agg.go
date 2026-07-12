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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dop251/goja"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
)

// A JS aggregate follows the same three-callback protocol as a native base.Agg
// (Init/Update/Result), written in JavaScript as NAME_init()/NAME_update(state,
// value)/NAME_final(state). The accumulator "state" is any JSON-serializable JS
// value; between rows n1k1 threads it as JSON bytes in the group's byte buffer
// (so it spills like every other aggregate -- DESIGN.md). This is the aggregate
// analogue of the scalar JS UDF (ext_jsvm.go): the trio runs on the same
// per-query/per-actor shared runtime, so it can use console.log and call other
// loaded UDFs.
//
// Trade-off vs a native base.Agg (e.g. sparkline/histogram): state round-trips
// through JSON on every Update (not zero-garbage) and the callbacks can't report
// an error (base.Agg has no error channel -- a throwing/NaN step is contained and
// treated as a no-op / null). It's the interpreted lane: convenient, not for the
// hottest inner loops.

var jsAggNames = map[string]bool{} // JS aggregate names we've registered (reload-idempotent)

// RegisterJSAggregate registers a JS aggregate NAME from source, which must
// define NAME_init(), NAME_update(state, value) and NAME_final(state). NAME(expr)
// then parses and runs as an aggregate (GROUP BY or bare).
func RegisterJSAggregate(name, source string) error {
	name = strings.ToLower(name)

	if !jsAggNames[name] {
		if _, ok := expression.GetFunction(name); ok {
			return fmt.Errorf("JS aggregate %q collides with a builtin function name", name)
		}
		if _, ok := algebra.GetAggregate(name, false, false, false); ok {
			return fmt.Errorf("JS aggregate %q collides with an aggregate function name", name)
		}
	}

	prog, err := goja.Compile(name+".agg.js", source, true)
	if err != nil {
		return fmt.Errorf("goja compile of %q: %w", name, err)
	}
	// Verify the three callbacks exist.
	check := goja.New()
	if _, err := check.RunProgram(prog); err != nil {
		return fmt.Errorf("JS aggregate %q: %w", name, err)
	}
	for _, suffix := range []string{"_init", "_update", "_final"} {
		if _, ok := goja.AssertFunction(check.Get(name + suffix)); !ok {
			return fmt.Errorf("JS aggregate %q: source must define a function %q", name, name+suffix)
		}
	}
	recordExtExamples("javascript-aggregate", name, readJSExamples(check, name+"_final")) // inline goldens.

	// Make the callbacks available in every per-query runtime (keyed distinctly so
	// it can't clash with a same-named scalar UDF's program).
	key := "aggregate:" + name
	if _, exists := jsPrograms[key]; !exists {
		jsProgramOrder = append(jsProgramOrder, key)
	}
	jsPrograms[key] = prog

	// Register the engine-side base.Agg handler (bridging to the JS callbacks) and
	// the cbq parse/plan shim, exactly like the native sparkline/histogram aggs.
	agg := makeJSAgg(name)
	if idx, ok := base.AggCatalog[name]; ok && jsAggNames[name] {
		base.Aggs[idx] = agg // reload in place
	} else {
		base.AggCatalog[name] = len(base.Aggs)
		base.Aggs = append(base.Aggs, agg)
	}
	registerExtAggregate(name, algebra.AGGREGATE_ALLOWS_REGULAR)

	jsAggNames[name] = true
	extLoaded[name] = ExtensionInfo{Name: name, Kind: "javascript-aggregate", Source: "(inline)"}
	return nil
}

// makeJSAgg builds a base.Agg whose Init/Update/Result drive the JS callbacks,
// carrying the accumulator state as a length-prefixed JSON blob.
func makeJSAgg(name string) *base.Agg {
	initFn, updateFn, finalFn := name+"_init", name+"_update", name+"_final"

	return &base.Agg{
		Init: func(vars *base.Vars, agg []byte) []byte {
			sr := jsSharedFromVars(vars)
			state := jsAggCall(sr, initFn)
			return appendJSONBlob(agg, marshalGoja(state))
		},

		Update: func(vars *base.Vars, v base.Val, aggNew, agg []byte, vc *base.ValComparer) ([]byte, []byte, bool) {
			sr := jsSharedFromVars(vars)
			blob, rest := readJSONBlob(agg)
			state := sr.rt.ToValue(unmarshalJSON(blob))
			val := valBytesToGoja(sr.rt, v)
			next := jsAggCall(sr, updateFn, state, val)
			return appendJSONBlob(aggNew, marshalGoja(next)), rest, true
		},

		Result: func(vars *base.Vars, agg, buf []byte) (base.Val, []byte, []byte) {
			sr := jsSharedFromVars(vars)
			blob, rest := readJSONBlob(agg)
			state := sr.rt.ToValue(unmarshalJSON(blob))
			result := jsAggCall(sr, finalFn, state)
			out := marshalGoja(result)
			vBuf := append(buf[:0], out...)
			return base.Val(vBuf), rest, base.BufUnused(buf, len(vBuf))
		},
	}
}

// jsSharedFromVars resolves the per-query/per-actor JS runtime the aggregate
// should run on (the same one the scalar UDFs use), or a throwaway if the eval
// context isn't a *GlueContext (rare).
func jsSharedFromVars(vars *base.Vars) *jsSharedRuntime {
	if gc, ok := vars.Temps[0].(*GlueContext); ok {
		return gc.jsShared()
	}
	return newJSSharedRuntime()
}

// jsAggCall invokes a named callback on the shared runtime, containing any
// panic/throw (base.Agg has no error channel) by returning undefined -- the step
// becomes a no-op and marshals to null.
func jsAggCall(sr *jsSharedRuntime, fnName string, args ...goja.Value) (res goja.Value) {
	defer func() {
		if recover() != nil {
			res = goja.Undefined()
		}
	}()
	fn := sr.callable(fnName)
	if fn == nil {
		return goja.Undefined()
	}
	out, err := fn(goja.Undefined(), args...)
	if err != nil {
		return goja.Undefined()
	}
	return out
}

// --- JSON-blob accumulator helpers: [8-byte len][JSON bytes] ---

func appendJSONBlob(dst, blob []byte) []byte {
	dst = base.BinaryAppendUint64(dst, uint64(len(blob)))
	return append(dst, blob...)
}

func readJSONBlob(agg []byte) (blob, rest []byte) {
	n := binary.LittleEndian.Uint64(agg[:8])
	return agg[8 : 8+n], agg[8+n:]
}

// marshalGoja serializes a goja value to JSON bytes; undefined/null/unmarshalable
// (e.g. NaN) become "null" so the accumulator always holds valid JSON.
func marshalGoja(v goja.Value) []byte {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return []byte("null")
	}
	b, err := json.Marshal(v.Export())
	if err != nil {
		return []byte("null")
	}
	return b
}

func unmarshalJSON(b []byte) interface{} {
	var x interface{}
	if len(b) > 0 {
		_ = json.Unmarshal(b, &x)
	}
	return x
}

// valBytesToGoja converts a base.Val (a value's JSON bytes; empty = MISSING) into
// a goja value for passing to NAME_update.
func valBytesToGoja(rt *goja.Runtime, v base.Val) goja.Value {
	if len(v) == 0 {
		return goja.Undefined()
	}
	return rt.ToValue(unmarshalJSON(v))
}
