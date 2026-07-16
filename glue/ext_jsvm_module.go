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

// Multi-export JS MODULE loader (DESIGN-extensions.md "JS modules", DESIGN-variant.md
// §5.2). A single `.js` file can export a whole family of functions instead of the
// one-function-per-file convention: it sets
//
//	exports.functions = [
//	  { name: "DECIMAL_ADD", marshal: "variant", fn: (a, b) => ... },
//	  { name: "DECIMAL_CMP", marshal: "variant", fn: (a, b) => ... },
//	];
//
// so e.g. all DECIMAL_* live in one decimal.js namespace. Each entry self-describes its
// SQL `name` and (optional) `marshal` mode, so the FILENAME carries no per-function
// metadata (no `.variant.js`/`.raw.js` suffix chaining — those are values of one axis,
// declared per entry). A `.js` file that does NOT set exports.functions is still loaded
// as a legacy single-function UDF (registerJSFunc) keyed off its filename stem.
//
// The whole module shares one goja program (so its functions can call each other), and a
// hoist shim binds each entry's fn to a global under its lowercased SQL name — so the
// existing per-query call path (jsSharedRuntime.callable -> rt.Get(name)) resolves it
// unchanged.

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dop251/goja"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
)

// jsFuncMarshal records each JS UDF's declared marshal mode — how values cross the goja
// boundary: "json" (default), "variant" (VARIANT-typed values as EJSON-tagged JSON,
// e.g. {"$numberDecimal":"9.99"}), or "raw" (raw n1k1 Val bytes; a perf/opaque lane).
// Consulted by the call boundary (jsMarshalArgs/jsMarshalResult).
//
// Phase-1 status: a true base.Val `V`-carrier does not yet flow through the cbq
// value.Value layer that JS UDFs live in, so a "variant" fn exchanges typed values as
// EJSON-tagged JSON objects — exact and queryable, but riding as ordinary JSON. Wiring
// EJSON<->`V` (so results become real VARIANT bytes) is the write-back-bridge follow-up.
var jsFuncMarshal = map[string]string{}

// jsFuncProgramKey maps a module function's SQL name to its shared jsPrograms key
// ("module:<bundle>"), since a module's functions share ONE program rather than each
// having jsPrograms[name]. Only set for module functions; a single-file UDF has
// jsPrograms[name] directly. Used by the example runner to build a runtime with the
// whole module loaded. See jsProgramForFunc.
var jsFuncProgramKey = map[string]string{}

// jsProgramForFunc returns the compiled program that defines function name: its own
// jsPrograms entry for a single-file UDF, or its module's shared program otherwise.
func jsProgramForFunc(name string) *goja.Program {
	if p := jsPrograms[name]; p != nil {
		return p
	}
	if key := jsFuncProgramKey[name]; key != "" {
		return jsPrograms[key]
	}
	return nil
}

const jsModulePreamble = "var exports={},module={exports:exports};\n"

// jsModuleHoist binds each exported entry to the global(s) the per-kind call path
// resolves: a scalar/stream entry's `fn` as NAME, and an aggregate entry's
// `init`/`update`/`final` as NAME_init/NAME_update/NAME_final (the 3-callback protocol
// the base.Agg bridge looks up).
const jsModuleHoist = "\n;(function(){var fs=(exports&&exports.functions)||[];" +
	"for(var i=0;i<fs.length;i++){var f=fs[i],n=String(f.name).toLowerCase()," +
	"k=String(f.kind||'').toLowerCase();" +
	"if(k==='aggregate'||k==='agg'){globalThis[n+'_init']=f.init;globalThis[n+'_update']=f.update;globalThis[n+'_final']=f.final;}" +
	"else{globalThis[n]=f.fn;}}})();\n"

// looksLikeJSModule reports whether source is a multi-export module (it sets
// exports.functions to a non-empty array) rather than a legacy single-function file. A
// source that fails to run is treated as "not a module" so the single-function loader
// reports the real compile error.
func looksLikeJSModule(source string) bool {
	rt := goja.New()
	installJSConsole(rt)
	if _, err := rt.RunString(jsModulePreamble + source); err != nil {
		return false
	}
	return len(jsModuleEntries(rt)) > 0
}

// jsModuleEntries reads exports.functions off a runtime that has run the module source.
func jsModuleEntries(rt *goja.Runtime) []map[string]interface{} {
	exp := rt.Get("exports")
	if exp == nil {
		return nil
	}
	obj := exp.ToObject(rt)
	if obj == nil {
		return nil
	}
	fv := obj.Get("functions")
	if fv == nil {
		return nil
	}
	arr, ok := fv.Export().([]interface{})
	if !ok {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(arr))
	for _, e := range arr {
		if m, ok := e.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
}

// jsModuleFuncs maps a module bundle name (its filename stem) to the SQL function names
// it registered — so `.extensions` sub-commands can expand a bundle (e.g. "decimal")
// into its functions (DECIMAL_ADD, DECIMAL_CMP, …). See JSModuleFunctions.
var jsModuleFuncs = map[string][]string{}

// JSModuleFunctions returns the SQL function names a loaded module bundle registered
// (nil if the name is not a loaded module). The functions are the things actually
// callable / listable / testable; the bundle is just the file they share.
func JSModuleFunctions(bundle string) []string {
	fns := jsModuleFuncs[strings.ToLower(bundle)]
	if len(fns) == 0 {
		return nil
	}
	return append([]string(nil), fns...)
}

// JSModuleOf returns the module bundle a function belongs to (its filename stem), or ""
// if name is not a module function. The inverse of JSModuleFunctions; lets `.extensions
// list` collapse a module's functions into one bundle row.
func JSModuleOf(name string) string {
	if key := jsFuncProgramKey[strings.ToLower(name)]; strings.HasPrefix(key, "module:") {
		return strings.TrimPrefix(key, "module:")
	}
	return ""
}

// RegisterJSModule loads a multi-export module named bundle from inline source (Source
// recorded as "(inline)"). The file-backed loader (RegisterExtensionFile) calls
// registerJSModule with the real path instead.
func RegisterJSModule(bundle, source string) error {
	_, err := registerJSModule(bundle, source, "(inline)")
	return err
}

// moduleExtKind maps an entry's declared kind to its ExtensionInfo/examples kind and the
// normalized internal kind. Scalar (the default), aggregate, and stream(ing source) are
// supported; anything else is an error.
func moduleExtKind(kind string) (norm, extKind string, ok bool) {
	switch kind {
	case "", "scalar", "function":
		return "scalar", "javascript", true
	case "aggregate", "agg":
		return "aggregate", "javascript-aggregate", true
	case "stream", "source", "table":
		return "stream", "javascript-stream", true
	default:
		return "", "", false
	}
}

// registerJSModule registers each `exports.functions` entry of a multi-export module —
// a scalar UDF, an aggregate (3-callback), or a streaming source — all sharing ONE
// program, records each in the loaded-extension set (Source sourcePath), and returns the
// registered SQL function names.
func registerJSModule(bundle, source, sourcePath string) (names []string, err error) {
	bundle = strings.ToLower(bundle)

	prog, err := goja.Compile(bundle+".js", jsModulePreamble+source+jsModuleHoist, true)
	if err != nil {
		return nil, fmt.Errorf("goja compile of JS module %q: %w", bundle, err)
	}

	check := goja.New()
	installJSConsole(check)
	if _, err := check.RunProgram(prog); err != nil {
		return nil, fmt.Errorf("JS module %q: %w", bundle, err)
	}
	entries := jsModuleEntries(check)
	if len(entries) == 0 {
		return nil, fmt.Errorf("JS module %q: must set exports.functions to a non-empty array of {name, fn}", bundle)
	}

	type reg struct {
		name, kind, extKind, marshal string
		examples                     []ExtExample
	}
	regs := make([]reg, 0, len(entries))
	seen := map[string]bool{}
	for _, e := range entries {
		name := strings.ToLower(strings.TrimSpace(asString(e["name"])))
		if name == "" {
			return nil, fmt.Errorf("JS module %q: a function entry is missing its %q", bundle, "name")
		}
		if seen[name] {
			return nil, fmt.Errorf("JS module %q: duplicate function name %q", bundle, name)
		}
		seen[name] = true

		kind, extKind, ok := moduleExtKind(strings.ToLower(strings.TrimSpace(asString(e["kind"]))))
		if !ok {
			return nil, fmt.Errorf("JS module %q: function %q has unknown kind %q (want scalar|aggregate|stream)",
				bundle, name, asString(e["kind"]))
		}

		marshal := "json"
		if m := strings.ToLower(strings.TrimSpace(asString(e["marshal"]))); m != "" {
			marshal = m
		}
		switch marshal {
		case "json", "variant", "raw":
		default:
			return nil, fmt.Errorf("JS module %q: function %q has unknown marshal %q (want json|variant|raw)",
				bundle, name, marshal)
		}

		// The hoist shim must have bound the callable(s) the kind's call path resolves.
		if kind == "aggregate" {
			for _, suf := range []string{"_init", "_update", "_final"} {
				if _, ok := goja.AssertFunction(check.Get(name + suf)); !ok {
					return nil, fmt.Errorf("JS module %q: aggregate %q must define callable %q", bundle, name, "init/update/final")
				}
			}
		} else if _, ok := goja.AssertFunction(check.Get(name)); !ok {
			return nil, fmt.Errorf("JS module %q: function %q has no callable \"fn\"", bundle, name)
		}

		// Collision guard (mirrors registerJSFunc): don't shadow a stock builtin/aggregate
		// on first registration; a reload of a name we own is fine.
		if !extOurs[name] {
			if _, ok := expression.GetFunction(name); ok {
				return nil, fmt.Errorf("JS module %q: function %q collides with a builtin function name", bundle, name)
			}
			if _, ok := algebra.GetAggregate(name, false, false, false); ok {
				return nil, fmt.Errorf("JS module %q: function %q collides with an aggregate function name", bundle, name)
			}
		}
		regs = append(regs, reg{name, kind, extKind, marshal, moduleEntryExamples(e["examples"])})
	}

	// Install the module program ONCE (shared by all its functions), then register each
	// SQL function per its kind against it.
	key := "module:" + bundle
	if _, exists := jsPrograms[key]; !exists {
		jsProgramOrder = append(jsProgramOrder, key)
	}
	jsPrograms[key] = prog
	names = make([]string, 0, len(regs))
	for _, r := range regs {
		// Per-function golden examples, recorded per SQL name + kind so `.extensions test`
		// runs each through the right protocol (scalar call / agg init-update-final / stream emit).
		recordExtExamples(r.extKind, r.name, r.examples)
		switch r.kind {
		case "aggregate":
			installJSAggregate(r.name)
		case "stream":
			expression.RegisterFunction(r.name, newJSStreamFunc(r.name))
			jsStreamNames[r.name] = true
		default: // scalar
			expression.RegisterFunction(r.name, newJSFunc(r.name))
			jsFuncMarshal[r.name] = r.marshal
		}
		extOurs[r.name] = true
		jsFuncProgramKey[r.name] = key
		extLoaded[r.name] = ExtensionInfo{Name: r.name, Kind: r.extKind, Source: sourcePath}
		names = append(names, r.name)
	}
	jsModuleFuncs[bundle] = names
	return names, nil
}

func asString(v interface{}) string {
	s, _ := v.(string)
	return s
}

// moduleEntryExamples converts a module function entry's `examples` value (a JS array of
// {in, out} objects, Export()ed to Go) into []ExtExample by round-tripping through JSON
// (ExtExample's In/Out are json.RawMessage). Malformed examples are dropped — they are
// advisory golden data, not a load-blocking error (matching readJSExamples).
func moduleEntryExamples(v interface{}) []ExtExample {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var exs []ExtExample
	if json.Unmarshal(b, &exs) != nil {
		return nil
	}
	return exs
}
