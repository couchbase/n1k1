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

// Inline golden EXAMPLES for JavaScript extensions -- self-documenting, verifiable
// test data that lives in the *.js file itself, so an author (or an AI, or a reader
// browsing the corpus) can see what a UDF / aggregate / stream source / macro DOES
// without running it, and CI can prove the examples still hold.
//
// An author declares an `examples` array in the extension file -- executable JS, so
// the file stays 100% valid JavaScript (the SQL++ recipe analog lives in comments;
// see corpus_recipe.go). Both a top-level `var examples = [...]` and a property on
// the primary export (`celsius_to_fahrenheit.examples = [...]`) are read. Each entry
// is `{ in: <input>, out: <expected> [, name, desc] }`, where `in`/`out` mean:
//
//	scalar UDF    in = arg array      -> out = return value       foo(2,3) => 5
//	aggregate     in = value sequence -> out = final value        geomean([1,10,100]) => 10
//	stream source in = arg array      -> out = emitted-row array   series(1,3) => [{n:1},{n:2},{n:3}]
//	macro         in = call text      -> out = expanded SQL++      "@grep_context(...)" => "SELECT ..."
//	extract       in = sample text    -> out = framed-row array    "l1\nl2" => [{...},{...}]
//
// Examples are CAPTURED at registration (each loader already evaluates the program
// in a throwaway runtime -- the free hook) into extExampleRegistry, EXECUTED by
// RunExtensionExamples (the `.extensions test` runner), and DISPLAYED by
// `.extensions examples` / listed with a count.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/dop251/goja"
)

// ExtExample is one inline golden example: an invocation input and its expected
// output. `In`/`Out` are raw JSON so they carry any shape (array, scalar, string,
// object array) and are interpreted per extension kind (see the file header).
type ExtExample struct {
	Name string          `json:"name,omitempty"` // optional short label
	Desc string          `json:"desc,omitempty"` // optional one-line description
	In   json.RawMessage `json:"in"`             // input (args / sequence / call text / sample)
	Out  json.RawMessage `json:"out"`            // expected output
}

// extExampleSet is the captured examples for one extension, tagged with its kind so
// RunExtensionExamples can dispatch execution.
type extExampleSet struct {
	kind     string
	name     string
	examples []ExtExample
}

// extExampleRegistry holds every loaded extension's examples, keyed by kind+name.
// Mutated only at load/unload (never concurrently with a run), like the sibling
// extension registries -- so no lock.
var extExampleRegistry = map[string]extExampleSet{}

// extractExamplers lets an extract recipe supply its own executor closure (kind
// "extract" runs describe()+SpecApply, which is records-package logic that lives in
// ext_extract_jsvm.go rather than here). Keyed by recipe name.
var extractExamplers = map[string]func(sample string) (json.RawMessage, error){}

func exKey(kind, name string) string { return kind + "\x00" + name }

// recordExtExamples stores a loaded extension's examples (a no-op if it has none).
func recordExtExamples(kind, name string, exs []ExtExample) {
	if len(exs) == 0 {
		return
	}
	extExampleRegistry[exKey(kind, name)] = extExampleSet{kind: kind, name: name, examples: exs}
}

// forgetExtExamples drops an extension's examples (called from UnloadExtension).
func forgetExtExamples(kind, name string) {
	delete(extExampleRegistry, exKey(kind, name))
	delete(extractExamplers, name)
}

// ExtExamplesFor returns the examples declared for a loaded extension (nil if none).
func ExtExamplesFor(kind, name string) []ExtExample {
	return extExampleRegistry[exKey(kind, name)].examples
}

// ExtExampleSet is one extension's captured examples (for display: `.extensions
// examples`). Kind is the extension kind ("javascript", "macro", "extract", ...).
type ExtExampleSet struct {
	Kind     string
	Name     string
	Examples []ExtExample
}

// ListExtExampleSets returns every loaded extension that declares inline examples,
// sorted by (kind, name) -- across all kinds (UDF / aggregate / stream / macro /
// extract), so one command surfaces the whole self-documenting corpus.
func ListExtExampleSets() []ExtExampleSet {
	out := make([]ExtExampleSet, 0, len(extExampleRegistry))
	for _, s := range extExampleRegistry {
		out = append(out, ExtExampleSet{Kind: s.kind, Name: s.name, Examples: s.examples})
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Kind != out[b].Kind {
			return out[a].Kind < out[b].Kind
		}
		return out[a].Name < out[b].Name
	})
	return out
}

// readJSExamples collects inline examples from a just-evaluated goja runtime: the
// top-level `examples` global, plus an `.examples` property on each named export
// (so both `var examples=[...]` and `foo.examples=[...]` are honored). Malformed
// entries are skipped rather than failing the load -- examples are advisory data.
func readJSExamples(rt *goja.Runtime, names ...string) []ExtExample {
	var out []ExtExample
	add := func(v goja.Value) {
		if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
			return
		}
		var got []ExtExample
		if err := jsExportInto(v, &got); err == nil {
			out = append(out, got...)
		}
	}
	add(rt.Get("examples"))
	for _, n := range names {
		if ev := rt.Get(n); ev != nil && !goja.IsUndefined(ev) && !goja.IsNull(ev) {
			if obj := ev.ToObject(rt); obj != nil {
				add(obj.Get("examples"))
			}
		}
	}
	return out
}

// ExampleResult is one executed example's outcome (for `.extensions test` reporting).
type ExampleResult struct {
	Kind  string
	Name  string
	Index int    // 0-based position within the extension's examples
	Label string // the example's Name/Desc, or a synthesized "#<i>"
	In    string // the input, canonicalized for display
	Want  string // expected output, canonicalized
	Got   string // actual output, canonicalized ("" when Err set)
	Pass  bool
	Err   string // execution error (bad example, throw, unknown kind, ...)
}

// RunExtensionExamples executes every loaded extension's inline examples and returns
// one ExampleResult per example, sorted by (kind, name, index) for stable output. A
// non-empty `only` filters to that single extension name (any kind).
func RunExtensionExamples(only string) []ExampleResult {
	keys := make([]string, 0, len(extExampleRegistry))
	for k := range extExampleRegistry {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var results []ExampleResult
	for _, k := range keys {
		set := extExampleRegistry[k]
		if only != "" && !strings.EqualFold(only, set.name) {
			continue
		}
		for i, ex := range set.examples {
			results = append(results, runOneExample(set.kind, set.name, i, ex))
		}
	}
	return results
}

func runOneExample(kind, name string, i int, ex ExtExample) ExampleResult {
	r := ExampleResult{
		Kind: kind, Name: name, Index: i,
		Label: exampleLabel(ex, i),
		In:    displayJSON(ex.In),
		Want:  displayJSON(ex.Out),
	}
	got, err := execExample(kind, name, ex.In)
	if err != nil {
		r.Err = err.Error()
		return r
	}
	r.Got = displayJSON(got)
	r.Pass = exampleMatches(kind, got, ex.Out)
	return r
}

func exampleLabel(ex ExtExample, i int) string {
	if ex.Name != "" {
		return ex.Name
	}
	if ex.Desc != "" {
		return ex.Desc
	}
	return fmt.Sprintf("#%d", i)
}

// execExample dispatches one example's input to the right executor for its kind,
// returning the produced output as raw JSON.
func execExample(kind, name string, in json.RawMessage) (json.RawMessage, error) {
	switch kind {
	case "javascript": // scalar UDF
		return runJSScalarExample(name, in)
	case "javascript-aggregate":
		return runJSAggExample(name, in)
	case "javascript-stream":
		return runJSStreamExample(name, in)
	case "macro":
		return runMacroExample(in)
	case "extract":
		exec := extractExamplers[name]
		if exec == nil {
			return nil, fmt.Errorf("extract recipe %q: no example executor", name)
		}
		sample, err := jsonString(in)
		if err != nil {
			return nil, fmt.Errorf("extract example `in` must be a sample-text string: %w", err)
		}
		return exec(sample)
	default:
		return nil, fmt.Errorf("examples not supported for extension kind %q", kind)
	}
}

// exampleMatches compares produced vs expected. Row/value outputs compare structurally
// with float tolerance (so `212` matches `212.0` and a geomean of `10` matches
// `10.000000000000002` -- JS numbers are floats); a macro's SQL++ text compares with
// runs of whitespace collapsed (formatting-insensitive, so only a real change to the
// generated shape fails).
func exampleMatches(kind string, got, want json.RawMessage) bool {
	if kind == "macro" {
		return collapseWS(jsonStringOr(got)) == collapseWS(jsonStringOr(want))
	}
	return jsonEqualApprox(got, want)
}

// jsonEqualApprox reports whether two JSON values are structurally equal, treating
// numbers as equal within a small relative+absolute tolerance (JS produces float64).
func jsonEqualApprox(a, b json.RawMessage) bool {
	var av, bv interface{}
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return canonicalJSON(a) == canonicalJSON(b) // fall back to exact on unparseable input.
	}
	return deepApproxEqual(av, bv)
}

func deepApproxEqual(a, b interface{}) bool {
	switch at := a.(type) {
	case float64:
		bt, ok := b.(float64)
		return ok && math.Abs(at-bt) <= 1e-9*(1+math.Abs(at)+math.Abs(bt))
	case []interface{}:
		bt, ok := b.([]interface{})
		if !ok || len(at) != len(bt) {
			return false
		}
		for i := range at {
			if !deepApproxEqual(at[i], bt[i]) {
				return false
			}
		}
		return true
	case map[string]interface{}:
		bt, ok := b.(map[string]interface{})
		if !ok || len(at) != len(bt) {
			return false
		}
		for k, v := range at {
			bw, ok := bt[k]
			if !ok || !deepApproxEqual(v, bw) {
				return false
			}
		}
		return true
	default: // string, bool, nil
		return reflect.DeepEqual(a, b)
	}
}

// ---- executors ------------------------------------------------------------

func runJSScalarExample(name string, in json.RawMessage) (json.RawMessage, error) {
	rt, err := freshJSProgramRuntime(jsProgramForFunc(name), name)
	if err != nil {
		return nil, err
	}
	fn, ok := goja.AssertFunction(rt.Get(name))
	if !ok {
		return nil, fmt.Errorf("scalar UDF %q is not callable", name)
	}
	args, err := gojaArgsFromJSON(rt, in)
	if err != nil {
		return nil, err
	}
	res, err := fn(goja.Undefined(), args...)
	if err != nil {
		return nil, err
	}
	return marshalGoja(res), nil
}

func runJSAggExample(name string, in json.RawMessage) (json.RawMessage, error) {
	rt, err := freshJSProgramRuntime(jsProgramForFunc(name), name)
	if err != nil {
		return nil, err
	}
	initFn, ok1 := goja.AssertFunction(rt.Get(name + "_init"))
	updFn, ok2 := goja.AssertFunction(rt.Get(name + "_update"))
	finFn, ok3 := goja.AssertFunction(rt.Get(name + "_final"))
	if !ok1 || !ok2 || !ok3 {
		return nil, fmt.Errorf("aggregate %q must define %[1]s_init/_update/_final", name)
	}
	var seq []interface{}
	if err := json.Unmarshal(in, &seq); err != nil {
		return nil, fmt.Errorf("aggregate example `in` must be a value array: %w", err)
	}
	state, err := initFn(goja.Undefined())
	if err != nil {
		return nil, err
	}
	for _, v := range seq {
		state, err = updFn(goja.Undefined(), state, rt.ToValue(v))
		if err != nil {
			return nil, err
		}
	}
	res, err := finFn(goja.Undefined(), state)
	if err != nil {
		return nil, err
	}
	return marshalGoja(res), nil
}

func runJSStreamExample(name string, in json.RawMessage) (json.RawMessage, error) {
	rt, err := freshJSProgramRuntime(jsProgramForFunc(name), name)
	if err != nil {
		return nil, err
	}
	fn, ok := goja.AssertFunction(rt.Get(name))
	if !ok {
		return nil, fmt.Errorf("stream source %q is not callable", name)
	}
	var rows [][]byte
	emit := func(call goja.FunctionCall) goja.Value {
		for _, a := range call.Arguments {
			rows = append(rows, marshalGoja(a))
		}
		return rt.ToValue(true)
	}
	args, err := gojaArgsFromJSON(rt, in)
	if err != nil {
		return nil, err
	}
	callArgs := append([]goja.Value{rt.ToValue(emit)}, args...)
	if _, err := fn(goja.Undefined(), callArgs...); err != nil {
		return nil, err
	}
	return jsonArray(rows), nil
}

func runMacroExample(in json.RawMessage) (json.RawMessage, error) {
	call, err := jsonString(in)
	if err != nil {
		return nil, fmt.Errorf("macro example `in` must be a call-text string: %w", err)
	}
	expanded, err := ExpandMacros(call)
	if err != nil {
		return nil, err
	}
	out, _ := json.Marshal(expanded)
	return out, nil
}

// ---- helpers --------------------------------------------------------------

// freshJSProgramRuntime evaluates a compiled program in a new goja runtime (goja
// runtimes aren't goroutine-safe, so examples run isolated like the per-query path).
func freshJSProgramRuntime(prog *goja.Program, name string) (*goja.Runtime, error) {
	if prog == nil {
		return nil, fmt.Errorf("no compiled program for %q (not loaded?)", name)
	}
	rt := goja.New()
	installJSConsole(rt)
	installJSEjson(rt)
	if _, err := rt.RunProgram(prog); err != nil {
		return nil, err
	}
	return rt, nil
}

// gojaArgsFromJSON reads a JSON array of positional arguments into goja values.
func gojaArgsFromJSON(rt *goja.Runtime, in json.RawMessage) ([]goja.Value, error) {
	var raw []interface{}
	if err := json.Unmarshal(in, &raw); err != nil {
		return nil, fmt.Errorf("example `in` must be an argument array (e.g. [2,3]): %w", err)
	}
	args := make([]goja.Value, len(raw))
	for i, a := range raw {
		args[i] = rt.ToValue(a)
	}
	return args, nil
}

func jsonString(in json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(in, &s); err != nil {
		return "", err
	}
	return s, nil
}

func jsonStringOr(in json.RawMessage) string {
	if s, err := jsonString(in); err == nil {
		return s
	}
	return string(in)
}

// jsonArray assembles pre-marshaled JSON elements into one JSON array value.
func jsonArray(elems [][]byte) json.RawMessage {
	if len(elems) == 0 {
		return json.RawMessage("[]")
	}
	var b bytes.Buffer
	b.WriteByte('[')
	b.Write(bytes.Join(elems, []byte(",")))
	b.WriteByte(']')
	return b.Bytes()
}

// collapseWS trims and collapses every run of whitespace to a single space, so a
// macro's generated SQL++ compares on shape, not indentation/line breaks.
func collapseWS(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// displayJSON renders raw JSON compactly for human display, WITHOUT Go's default HTML
// escaping (so `>`/`<`/`&` in a macro's SQL++ stay literal, not `>`).
func displayJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "null"
	}
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return string(raw)
	}
	return strings.TrimRight(b.String(), "\n")
}
