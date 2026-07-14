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

// ext_macro_jsvm.go loads a "*.macro.js" JS-authored SQL++ MACRO into the pure-Go
// macro registry (macro.go). A macro supplies expand(args, ctx) -> SQL++ string;
// it runs ONCE per invocation at PARSE TIME (a cold path -- garbage is fine),
// never per row. It reuses ext_jsvm.go / ext_extract_jsvm.go's goja
// lifetime/timeout/console patterns: compile once at registration, and build a
// fresh goja.Runtime per expand() call (goja runtimes aren't goroutine-safe, and
// parses can run concurrently -- isolation beats sharing on this cold path).
//
// The JS module contract (see extensions/macros/*.macro.js):
//
//	// Optional module-scope `macro` metadata: its `params` drive positional->named
//	// mapping, defaults, arity/keyword checks, and `.macro help`.
//	var macro = {
//	  name: "grep_context",
//	  params: [ { name: "src", required: true }, { name: "when", required: true },
//	            { name: "before", default: 2 }, { name: "after", default: 2 } ],
//	};
//
//	// expand(args, ctx) -> SQL++ string. args.<name> / args[<i>] are the raw source
//	// text of each argument; args.$lit.<name> is a best-effort literal coercion;
//	// args.$pos is the positional array. ctx.gensym(prefix) mints a collision-free
//	// identifier (hygiene); ctx.error(msg) aborts with a mapped parse-time error.
//	function expand(args, ctx) {
//	  var sub = ctx.gensym("ctx"), near = ctx.gensym("near");
//	  return `(SELECT * FROM (
//	            SELECT _meta.\`path\` AS p, _meta.pos AS pos, line,
//	                   MAX(CASE WHEN (${args.when}) THEN 1 ELSE 0 END)
//	                     OVER (PARTITION BY _meta.\`path\` ORDER BY _meta.pos
//	                           ROWS BETWEEN ${args.before} PRECEDING
//	                                    AND ${args.after}  FOLLOWING) AS ${near}
//	            FROM ${args.src}) ${sub}
//	          WHERE ${sub}.${near} = 1)`;
//	}

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"

	"github.com/couchbase/n1k1/base"
)

// macroCtxVersion is exposed to JS as ctx.version, so a macro can adapt if the
// ctx surface grows. Bump on a breaking ctx change.
const macroCtxVersion = 1

// RegisterJSMacro compiles a *.macro.js source and registers it under name (the
// invocation name, i.e. the file stem). The source must define an expand(args,
// ctx) function; a module-scope `macro` object (with `params`) is optional.
// expand is NOT run here; it is compiled once and invoked per-expansion from the
// registered closure. Safe at startup; not safe concurrently with query parsing.
func RegisterJSMacro(name, source string) error {
	prog, err := goja.Compile(name+".macro.js", source, true)
	if err != nil {
		return fmt.Errorf("goja compile of macro %q: %w", name, err)
	}

	// Run once at registration to surface top-level JS errors early and read the
	// optional module-scope `macro` metadata. A throwaway runtime: each expand()
	// builds its own.
	rt := goja.New()
	installJSConsole(rt)
	if _, err := rt.RunProgram(prog); err != nil {
		return fmt.Errorf("macro %q: %w", name, err)
	}
	if _, ok := goja.AssertFunction(rt.Get("expand")); !ok {
		return fmt.Errorf("macro %q: source defines no expand(args, ctx) function", name)
	}
	var meta struct {
		Name   string       `json:"name"`
		Params []MacroParam `json:"params"`
	}
	if mv := rt.Get("macro"); mv != nil && !goja.IsUndefined(mv) && !goja.IsNull(mv) {
		if err := jsExportInto(mv, &meta); err != nil {
			return fmt.Errorf("macro %q: bad `macro` metadata: %w", name, err)
		}
	}
	recordExtExamples("macro", name, readJSExamples(rt, "macro")) // inline goldens (ext_examples.go).

	params := meta.Params
	// Keep the full JS source in the registry so `.macro show` can print it (and callers
	// can inspect it); `.macro list`/`help` render a short blurb from it, not the raw text.
	RegisterMacro(name, params, func(a *MacroArgs, c *MacroCtx) (string, error) {
		return runJSMacroExpand(prog, name, params, a, c)
	}, source, jsSourceHash(source))
	base.Logf(1, "glue/macro", "loaded JS macro, name: %s, params: %d", name, len(params))
	return nil
}

// runJSMacroExpand invokes a macro's expand(args, ctx) in a fresh goja runtime
// and returns its SQL++ string. Builds the args object (raw text by name +
// index, $lit coercions, $pos array) from the parsed call + the macro's params,
// and wires ctx.gensym/ctx.error/ctx.version to the pass context. A panic/timeout
// is contained so a bad macro can't crash the parser.
func runJSMacroExpand(prog *goja.Program, name string, params []MacroParam,
	a *MacroArgs, c *MacroCtx) (out string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("expand() panicked: %v", r)
		}
	}()

	resolved, err := a.Resolve(params)
	if err != nil {
		return "", err
	}

	rt := goja.New()
	installJSConsole(rt)
	if _, e := rt.RunProgram(prog); e != nil {
		return "", e
	}
	fn, ok := goja.AssertFunction(rt.Get("expand"))
	if !ok {
		return "", fmt.Errorf("expand not callable")
	}

	argsObj := rt.NewObject()
	pos := make([]interface{}, len(a.Positional))
	for i, v := range a.Positional {
		_ = argsObj.Set(strconv.Itoa(i), v) // args[0], args[1], ...
		pos[i] = v
	}
	_ = argsObj.Set("$pos", rt.ToValue(pos))
	lit := rt.NewObject()
	for k, v := range resolved {
		_ = argsObj.Set(k, v)
		_ = lit.Set(k, rt.ToValue(coerceLit(v)))
	}
	_ = argsObj.Set("$lit", lit)

	ctxObj := rt.NewObject()
	_ = ctxObj.Set("gensym", func(prefix string) string { return c.Gensym(prefix) })
	_ = ctxObj.Set("error", func(msg string) { panic(rt.ToValue(msg)) })
	_ = ctxObj.Set("version", macroCtxVersion)

	// Bound a runaway expand (still cold, but a pathological script shouldn't hang
	// the parser). Reuses ext_jsvm.go's JSCallTimeout/interrupt pattern.
	var timedOut int32
	if JSCallTimeout > 0 {
		timer := time.AfterFunc(JSCallTimeout, func() {
			atomic.StoreInt32(&timedOut, 1)
			rt.Interrupt("n1k1: JS macro time limit exceeded")
		})
		defer func() {
			timer.Stop()
			rt.ClearInterrupt()
		}()
	}

	res, callErr := fn(goja.Undefined(), argsObj, ctxObj)
	if atomic.LoadInt32(&timedOut) == 1 {
		return "", fmt.Errorf("expand exceeded the %s time limit", JSCallTimeout)
	}
	if callErr != nil {
		return "", callErr
	}
	if res == nil || goja.IsUndefined(res) || goja.IsNull(res) {
		return "", fmt.Errorf("expand() returned no SQL++ text")
	}
	s, ok := res.Export().(string)
	if !ok {
		return "", fmt.Errorf("expand() must return a string, got %T", res.Export())
	}
	return s, nil
}
