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

const jsModulePreamble = "var exports={},module={exports:exports};\n"

// jsModuleHoist binds each exported function to a global under its lowercased SQL name.
const jsModuleHoist = "\n;(function(){var fs=(exports&&exports.functions)||[];" +
	"for(var i=0;i<fs.length;i++){globalThis[String(fs[i].name).toLowerCase()]=fs[i].fn;}})();\n"

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

// RegisterJSModule loads a multi-export module named bundle from source: it registers
// each `exports.functions` entry as a scalar JS UDF sharing one program.
func RegisterJSModule(bundle, source string) error {
	bundle = strings.ToLower(bundle)

	prog, err := goja.Compile(bundle+".js", jsModulePreamble+source+jsModuleHoist, true)
	if err != nil {
		return fmt.Errorf("goja compile of JS module %q: %w", bundle, err)
	}

	check := goja.New()
	installJSConsole(check)
	if _, err := check.RunProgram(prog); err != nil {
		return fmt.Errorf("JS module %q: %w", bundle, err)
	}
	entries := jsModuleEntries(check)
	if len(entries) == 0 {
		return fmt.Errorf("JS module %q: must set exports.functions to a non-empty array of {name, fn}", bundle)
	}

	type reg struct{ name, marshal string }
	regs := make([]reg, 0, len(entries))
	seen := map[string]bool{}
	for _, e := range entries {
		name := strings.ToLower(strings.TrimSpace(asString(e["name"])))
		if name == "" {
			return fmt.Errorf("JS module %q: a function entry is missing its %q", bundle, "name")
		}
		if seen[name] {
			return fmt.Errorf("JS module %q: duplicate function name %q", bundle, name)
		}
		seen[name] = true

		marshal := "json"
		if m := strings.ToLower(strings.TrimSpace(asString(e["marshal"]))); m != "" {
			marshal = m
		}
		switch marshal {
		case "json", "variant", "raw":
		default:
			return fmt.Errorf("JS module %q: function %q has unknown marshal %q (want json|variant|raw)",
				bundle, name, marshal)
		}

		// The hoist shim must have bound a callable global under this name.
		if _, ok := goja.AssertFunction(check.Get(name)); !ok {
			return fmt.Errorf("JS module %q: function %q has no callable \"fn\"", bundle, name)
		}

		// Collision guard (mirrors registerJSFunc): don't shadow a stock builtin/aggregate
		// on first registration; a reload of a name we own is fine.
		if !extOurs[name] {
			if _, ok := expression.GetFunction(name); ok {
				return fmt.Errorf("JS module %q: function %q collides with a builtin function name", bundle, name)
			}
			if _, ok := algebra.GetAggregate(name, false, false, false); ok {
				return fmt.Errorf("JS module %q: function %q collides with an aggregate function name", bundle, name)
			}
		}
		regs = append(regs, reg{name, marshal})
	}

	// Install the module program ONCE (shared by all its functions), then register each
	// SQL function name against it.
	key := "module:" + bundle
	if _, exists := jsPrograms[key]; !exists {
		jsProgramOrder = append(jsProgramOrder, key)
	}
	jsPrograms[key] = prog
	for _, r := range regs {
		expression.RegisterFunction(r.name, newJSFunc(r.name))
		extOurs[r.name] = true
		jsFuncMarshal[r.name] = r.marshal
	}
	return nil
}

func asString(v interface{}) string {
	s, _ := v.(string)
	return s
}
