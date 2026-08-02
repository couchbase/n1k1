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
// value)/NAME_final(state). This is the aggregate analogue of the scalar JS UDF
// (ext_jsvm.go): the trio runs on the same per-query/per-actor shared runtime,
// so it can use console.log and call other loaded UDFs.
//
// State is JSVM-RESIDENT (DESIGN-extensions.md "JS aggregate state v2"): the
// accumulator lives as a live goja value on the actor's pinned runtime, and the
// group map holds only a fixed 9-byte HANDLE ('H' + uint64 index into the
// runtime's side table). Update passes the LIVE state object -- no decode, no
// re-encode, O(1) at the border however big the state -- so any JS value works
// (Map/Set/closures included, not just JSON shapes), and a big accumulator
// (charts, collected rows) no longer costs O(state) per row. goja values CANNOT
// travel between runtimes, which is exactly why the handle pins to the
// per-query/per-actor runtime the scalar UDFs already use; anything that must
// cross a runtime crosses as JSON bytes instead:
//
//   - an eval context with no runtime to pin to (rare non-GlueContext paths,
//     e.g. the examples runner) threads state as a JSON blob per call, the v1
//     behavior -- correct, just slower;
//   - a JSON-encoded partial arriving at Update/Result/Merge (from such a path,
//     or a future spill/shard boundary) is ADOPTED: decoded once, resident
//     thereafter. The two encodings are unambiguous ('H' never starts JSON).
//
// The handle's side table lives and dies with the runtime (= the query), so
// there is no per-group free bookkeeping; Result deliberately does NOT free its
// handle -- NAME_final must be PURE (repeatable), because it doubles as the
// live "aggregate so far" snapshot (RunningAggsGroup) and may be called many
// times mid-flight before the terminal call. An optional NAME_snapshot(state)
// substitutes a cheaper progressive view for that live channel (Agg.ResultLive).
//
// Remaining trade-off vs a native base.Agg (e.g. sparkline/histogram): each
// callback is still a ~µs JS boundary crossing and the callbacks can't report
// an error (base.Agg has no error channel -- a throwing/NaN step is contained
// and treated as a no-op / null). It's the interpreted lane: convenient, not
// for the hottest inner loops.

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
	// NAME_merge(stateA, stateB) is OPTIONAL: defining it makes the aggregate
	// mergeable (a commutative monoid — foldable across windows/shards). Absent, the
	// aggregate is fold-only, exactly as before.
	_, hasMerge := goja.AssertFunction(check.Get(name + "_merge"))
	jsAggHasMerge[name] = hasMerge
	// NAME_snapshot(state) is OPTIONAL: a cheaper mid-flight view for the live
	// running-aggregates channel; absent, the (pure) NAME_final is used live too.
	_, hasSnapshot := goja.AssertFunction(check.Get(name + "_snapshot"))
	jsAggHasSnapshot[name] = hasSnapshot
	recordExtExamples("javascript-aggregate", name, readJSExamples(check, name+"_final")) // inline goldens.

	// Make the callbacks available in every per-query runtime (keyed distinctly so
	// it can't clash with a same-named scalar UDF's program).
	key := "aggregate:" + name
	if _, exists := jsPrograms[key]; !exists {
		jsProgramOrder = append(jsProgramOrder, key)
	}
	jsPrograms[key] = prog
	jsFuncProgramKey[name] = key // so the example runner finds this program

	installJSAggregate(name)
	extLoaded[name] = ExtensionInfo{Name: name, Kind: "javascript-aggregate", Source: "(inline)"}
	return nil
}

// installJSAggregate registers the engine-side base.Agg handler (bridging to the JS
// NAME_init/_update/_final callbacks, which must already be defined in the shared
// runtime) and the cbq parse/plan shim, exactly like the native sparkline/histogram
// aggs. It does NOT compile/install a program — a single-file *.agg.js and a multi-export
// module both call it after their program is installed and callbacks hoisted.
func installJSAggregate(name string) {
	agg := makeJSAgg(name)
	if idx, ok := base.AggCatalog[name]; ok && jsAggNames[name] {
		base.Aggs[idx] = agg // reload in place
	} else {
		base.AggCatalog[name] = len(base.Aggs)
		base.Aggs = append(base.Aggs, agg)
	}
	registerExtAggregate(name, algebra.AGGREGATE_ALLOWS_REGULAR)
	jsAggNames[name] = true

	// Live running-aggregates: a JS aggregate's partial is a fixed 9-byte handle
	// and its live view (NAME_snapshot, else the contractually-pure NAME_final)
	// renders on demand, so it qualifies for the mid-flight "aggregate so far"
	// channel -- the AI-client early-access / cancel / course-correct story.
	base.RunningAggsCapable[name] = true
}

// makeJSAgg builds a base.Agg whose Init/Update/Result drive the JS callbacks.
// On a stable (GlueContext-pinned) runtime the accumulator is JSVM-resident and
// the blob is a fixed 9-byte handle; on a throwaway runtime it falls back to the
// v1 length-prefixed JSON threading (see the file comment). If the source
// defines NAME_merge(a, b), Merge is wired too (the aggregate is mergeable);
// NAME_snapshot(state) wires ResultLive (the mid-flight view).
func makeJSAgg(name string) *base.Agg {
	initFn, updateFn, finalFn := name+"_init", name+"_update", name+"_final"
	mergeFn, snapshotFn := name+"_merge", name+"_snapshot"

	// render is the shared Result shape: decode (handle or JSON), call fn, emit.
	// It must not disturb the accumulator -- fn is contractually pure -- so the
	// terminal Result and the live ResultLive both reuse it safely.
	render := func(fnName string) func(vars *base.Vars, agg, buf []byte) (base.Val, []byte, []byte) {
		return func(vars *base.Vars, agg, buf []byte) (base.Val, []byte, []byte) {
			sr, _ := jsSharedFromVars(vars)
			payload, rest := readJSONBlob(agg)
			state, _, _ := sr.jsAggStateDecode(payload)
			result := jsAggCall(sr, fnName, state)
			out := marshalGoja(result)
			vBuf := append(buf[:0], out...)
			return base.Val(vBuf), rest, base.BufUnused(buf, len(vBuf))
		}
	}

	agg := &base.Agg{
		Init: func(vars *base.Vars, agg []byte) []byte {
			sr, stable := jsSharedFromVars(vars)
			state := jsAggCall(sr, initFn)
			if stable {
				return appendHandleBlob(agg, sr.aggStateAlloc(state))
			}
			return appendJSONBlob(agg, marshalGoja(state))
		},

		Update: func(vars *base.Vars, v base.Val, aggNew, agg []byte, vc *base.ValComparer) ([]byte, []byte, bool) {
			sr, stable := jsSharedFromVars(vars)
			payload, rest := readJSONBlob(agg)
			state, h, isHandle := sr.jsAggStateDecode(payload)
			val := valBytesToGoja(sr.rt, v)
			next := jsAggCall(sr, updateFn, state, val)
			if stable {
				if isHandle { // steady state: re-target the same slot, same 9 bytes out.
					sr.aggStateSet(h, next)
					return appendHandleBlob(aggNew, h), rest, true
				}
				// A JSON partial arriving on a stable runtime: adopt it as resident.
				return appendHandleBlob(aggNew, sr.aggStateAlloc(next)), rest, true
			}
			return appendJSONBlob(aggNew, marshalGoja(next)), rest, true
		},

		Result: render(finalFn), // pure NAME_final: repeatable, never frees the handle.
	}

	// Optional NAME_merge(stateA, stateB): combine two partial accumulators into one.
	// Wired only when the source defines it, so a fold-only aggregate stays Merge==nil.
	// Either side may be a handle (resident) or JSON (a partial that crossed a
	// runtime boundary); the merged state goes resident when the runtime is stable
	// (re-targeting A's slot when A was resident -- the accumulator side of a fold).
	if jsAggHasMerge[name] {
		agg.Merge = func(vars *base.Vars, aggA, aggB, aggOut []byte) []byte {
			sr, stable := jsSharedFromVars(vars)
			payloadA, _ := readJSONBlob(aggA)
			payloadB, _ := readJSONBlob(aggB)
			stateA, hA, aIsHandle := sr.jsAggStateDecode(payloadA)
			stateB, _, _ := sr.jsAggStateDecode(payloadB)
			merged := jsAggCall(sr, mergeFn, stateA, stateB)
			if stable {
				if aIsHandle {
					sr.aggStateSet(hA, merged)
					return appendHandleBlob(aggOut, hA)
				}
				return appendHandleBlob(aggOut, sr.aggStateAlloc(merged))
			}
			return appendJSONBlob(aggOut, marshalGoja(merged))
		}
	}

	if jsAggHasSnapshot[name] {
		agg.ResultLive = render(snapshotFn)
	}
	return agg
}

// jsAggHasMerge records which JS aggregates defined an optional NAME_merge callback,
// so makeJSAgg wires base.Agg.Merge only for those (and reloads pick it up).
var jsAggHasMerge = map[string]bool{}

// jsAggHasSnapshot records which JS aggregates defined an optional NAME_snapshot
// callback, so makeJSAgg wires base.Agg.ResultLive only for those.
var jsAggHasSnapshot = map[string]bool{}

// jsSharedFromVars resolves the per-query/per-actor JS runtime the aggregate
// should run on (the same one the scalar UDFs use). stable=true means the
// runtime is pinned to the eval context and outlives this call -- the
// precondition for JSVM-resident state (a handle written now resolves on the
// next callback). A throwaway runtime (rare: eval outside a *GlueContext, e.g.
// the examples runner) is stable=false: state must thread as JSON bytes, since
// nothing retains the runtime -- or its handle table -- between calls.
func jsSharedFromVars(vars *base.Vars) (sr *jsSharedRuntime, stable bool) {
	if vars != nil && len(vars.Temps) > 0 {
		if gc, ok := vars.Temps[0].(*GlueContext); ok {
			return gc.jsShared(), true
		}
	}
	return newJSSharedRuntime(), false
}

// --- JSVM-resident accumulator handles ---
//
// The side table lives on the jsSharedRuntime (per query, per actor,
// single-threaded), so handles are meaningful exactly as long as the runtime
// is, and everything frees together at query end -- no per-group bookkeeping.
// Handles are never reused within a query; a group's slot is re-targeted in
// place by Update, so the table's length is bounded by the number of groups
// (plus merge results), not the number of rows.

const jsAggHandleTag = 'H' // never the first byte of JSON, so the encodings can't collide.

// aggStateAlloc retains state and returns its handle.
func (sr *jsSharedRuntime) aggStateAlloc(state goja.Value) uint64 {
	sr.aggState = append(sr.aggState, state)
	return uint64(len(sr.aggState) - 1)
}

// aggStateSet re-targets an existing handle (Update/Merge folding in place).
func (sr *jsSharedRuntime) aggStateSet(h uint64, state goja.Value) {
	if h < uint64(len(sr.aggState)) {
		sr.aggState[h] = state
	}
}

// jsAggStateDecode turns one accumulator payload into the live state value:
// a 9-byte 'H'+uint64 handle resolves through the side table; anything else is
// a JSON partial (v1 threading, or one that crossed a runtime boundary) decoded
// via aggStateFromJSON. An unresolvable handle (wrong runtime -- can't happen
// within one query's stream, but be contained, not corrupt) yields undefined,
// which the JS callback sees exactly like a throwing step's no-op.
func (sr *jsSharedRuntime) jsAggStateDecode(payload []byte) (state goja.Value, h uint64, isHandle bool) {
	if len(payload) == 9 && payload[0] == jsAggHandleTag {
		h = binary.LittleEndian.Uint64(payload[1:])
		if h < uint64(len(sr.aggState)) {
			return sr.aggState[h], h, true
		}
		return goja.Undefined(), h, true
	}
	return sr.aggStateFromJSON(payload), 0, false
}

// appendHandleBlob writes a length-prefixed resident-state handle payload.
func appendHandleBlob(dst []byte, h uint64) []byte {
	dst = base.BinaryAppendUint64(dst, 9)
	dst = append(dst, jsAggHandleTag)
	return base.BinaryAppendUint64(dst, h)
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

// aggStateFromJSON decodes an accumulator blob into the JS value handed to
// NAME_update/_final/_merge as `state`.
//
// The growableSlices pass is load-bearing, for CORRECTNESS not speed: rt.ToValue over
// a plain Go []interface{} yields a FIXED-LENGTH slice view, so `state.rows.push(x)`
// inside a JS aggregate silently did nothing and the row was lost. (Object property
// writes DID persist -- map wrappers are writable -- which is exactly what made the
// bug so easy to miss: partial state survived.) A *[]interface{} wraps as a growable
// array, so push/pop/splice behave as any JS author expects.
//
// Why not the runtime's own JSON.parse, which also yields real JS arrays? Measured
// ~2.5x slower with ~3x the allocations (BenchmarkJSAggListState): goja's parse
// eagerly materializes the whole object graph, while ToValue wraps the decoded Go
// graph lazily. This keeps the lazy path and pays only one cheap rewrite pass.
func (sr *jsSharedRuntime) aggStateFromJSON(blob []byte) goja.Value {
	return sr.rt.ToValue(growableSlices(unmarshalJSON(blob)))
}

// growableSlices rewrites every []interface{} in a decoded JSON graph into a
// *[]interface{} (see aggStateFromJSON). Maps are mutated in place; the returned
// value replaces the input.
func growableSlices(x interface{}) interface{} {
	switch t := x.(type) {
	case []interface{}:
		for i, e := range t {
			t[i] = growableSlices(e)
		}
		return &t
	case map[string]interface{}:
		for k, e := range t {
			t[k] = growableSlices(e)
		}
		return t
	}
	return x
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
