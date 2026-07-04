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
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// This file bridges Tier-2 "drop-in JS" UDFs (DESIGN-extensions.md) into cbq's
// scalar-function seam. A JS source defines a function; registerJSFunc compiles
// it once and registers a jsFunc under that name so the parser resolves
// NAME(args) to it. At Evaluate time the args are marshaled to JS values, the JS
// runs, and its return value is marshaled back -- flowing through the interpreted
// lane n1k1 uses for un-ported cbq expressions (ExprTree -> Expression.Evaluate),
// not the zero-alloc byte fast path.
//
// Runtime model (see also DESIGN-extensions.md "UDF runtime & state"):
//   - Programs are compiled ONCE at registration (immutable, shareable).
//   - The live goja.Runtime is created PER QUERY, PER ACTOR: it hangs off the
//     GlueContext (a fresh one per Session.Run), and base.Vars.ChainExtend's
//     per-actor context clone gives each concurrent UNION ALL branch its own.
//     One goja.Runtime is NOT goroutine-safe; this scoping keeps each single-
//     threaded without any pool or lock (DESIGN.md: avoid sync.Pool + locking).
//   - Consequences the model buys us on purpose: module-scope JS globals live for
//     the query and RESET on the next query (no cross-query leakage); a UDF can
//     call another UDF (all loaded UDFs share the runtime's global scope); and
//     console.log works (installed per runtime).
//
// goja (github.com/dop251/goja) is pure-Go / MIT / cgo-free, so it preserves
// n1k1's CGO_ENABLED=0 static-binary property.

// JSCallTimeout bounds a single UDF invocation: a runaway script (infinite loop,
// pathologically slow) is interrupted after this long and the call fails cleanly
// rather than hanging the actor goroutine forever. 0 disables the guard. An
// embedder may tune it before enabling UDFs. (goja is in-process and cannot be
// memory-capped like a Wasm guest -- see the DESIGN-extensions.md Caveats.)
var JSCallTimeout = 5 * time.Second

// JSConsoleWriter is where console.log/warn/error/info/debug from a UDF are
// written (for debugging). Defaults to os.Stderr; an embedder can redirect it.
var JSConsoleWriter io.Writer = os.Stderr

// The process-global registry of compiled UDF programs. Every per-query runtime
// runs all of these to define their functions in one shared scope (so UDFs can
// call each other). jsProgramOrder preserves load order for stable definition.
// extOurs records every name we've installed (persists across unload) so a
// reload isn't mistaken for a builtin-shadow collision. All mutated only by
// register/unregister, which the contract says run at startup / between queries,
// never concurrently with parsing or execution -- so no lock (matches how cbq's
// own _FUNCTIONS map is populated).
var (
	jsPrograms     = map[string]*goja.Program{}
	jsProgramOrder []string
	extOurs        = map[string]bool{}
)

// jsSharedRuntime is one query/actor-private goja runtime with every loaded UDF
// defined and a console installed. It caches resolved callables by name. Used
// single-threaded (its owning GlueContext/actor is single-threaded), so no lock.
type jsSharedRuntime struct {
	rt  *goja.Runtime
	fns map[string]goja.Callable
}

// newJSSharedRuntime builds a runtime with console + all currently-registered
// UDFs defined. A UDF whose top-level program errors simply won't resolve later
// (callable returns nil -> a clean per-call error), so one bad UDF can't stop
// the rest from loading.
func newJSSharedRuntime() *jsSharedRuntime {
	rt := goja.New()
	installJSConsole(rt)
	for _, name := range jsProgramOrder {
		_, _ = rt.RunProgram(jsPrograms[name])
	}
	return &jsSharedRuntime{rt: rt, fns: map[string]goja.Callable{}}
}

func (s *jsSharedRuntime) callable(name string) goja.Callable {
	if fn, ok := s.fns[name]; ok {
		return fn
	}
	fn, _ := goja.AssertFunction(s.rt.Get(name))
	s.fns[name] = fn
	return fn
}

// call runs UDF name with the given operand expressions. It returns poisoned=true
// when the runtime's state may be inconsistent afterward (a panic, or a timeout
// interrupt), signaling the caller to discard the runtime so a fresh one is built
// next time.
func (s *jsSharedRuntime) call(name string, operands expression.Expressions,
	item value.Value, context expression.Context) (result value.Value, err error, poisoned bool) {
	fn := s.callable(name)
	if fn == nil {
		return value.NULL_VALUE, fmt.Errorf("JS UDF %q: not callable", name), false
	}

	// A misbehaving script (stack overflow, interrupt) surfaces as a panic in
	// goja; contain it so a UDF can never crash the engine, and poison the runtime.
	defer func() {
		if r := recover(); r != nil {
			result, err, poisoned = value.NULL_VALUE, fmt.Errorf("JS UDF %q panicked: %v", name, r), true
		}
	}()

	// Evaluate operands then marshal. Fresh slices per call (not reused across
	// calls): a UDF operand can itself be a UDF call (e.g. foo(bar(x))), which
	// re-enters on the same runtime -- reusing one scratch buffer would corrupt
	// the outer call's args. The interpreted lane already boxes, so this is noise.
	args := make([]value.Value, len(operands))
	for i, op := range operands {
		v, e := op.Evaluate(item, context)
		if e != nil {
			return value.NULL_VALUE, e, false
		}
		args[i] = v
	}
	jsArgs := make([]goja.Value, len(args))
	for i, a := range args {
		jsArgs[i] = toGoja(s.rt, a)
	}

	var timedOut int32
	if JSCallTimeout > 0 {
		timer := time.AfterFunc(JSCallTimeout, func() {
			atomic.StoreInt32(&timedOut, 1)
			s.rt.Interrupt("n1k1: JS UDF time limit exceeded")
		})
		defer func() {
			timer.Stop()
			s.rt.ClearInterrupt()
		}()
	}

	res, callErr := fn(goja.Undefined(), jsArgs...)
	if atomic.LoadInt32(&timedOut) == 1 {
		return value.NULL_VALUE, fmt.Errorf("JS UDF %q exceeded the %s time limit", name, JSCallTimeout), true
	}
	if callErr != nil {
		return value.NULL_VALUE, fmt.Errorf("JS UDF %q: %w", name, callErr), false
	}
	return fromGoja(res), nil, false
}

// installJSConsole defines a minimal console object (log/error/warn/info/debug)
// that writes space-joined arguments to JSConsoleWriter -- a debugging aid for
// UDF authors. goja provides no console by default.
func installJSConsole(rt *goja.Runtime) {
	logFn := func(call goja.FunctionCall) goja.Value {
		var b strings.Builder
		for i, a := range call.Arguments {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(a.String())
		}
		fmt.Fprintln(JSConsoleWriter, b.String())
		return goja.Undefined()
	}
	console := rt.NewObject()
	for _, m := range []string{"log", "error", "warn", "info", "debug"} {
		_ = console.Set(m, logFn)
	}
	_ = rt.Set("console", console)
}

// runJSUDF resolves the per-query/per-actor shared runtime from the eval context
// (creating it lazily) and runs the named UDF on it. A poisoned runtime is
// dropped from the context so the next call rebuilds a clean one.
func runJSUDF(name string, operands expression.Expressions,
	item value.Value, context expression.Context) (value.Value, error) {
	host, ok := context.(jsRuntimeHost)
	if !ok {
		// No context to hang a runtime off (rare: eval outside a GlueContext, e.g.
		// some test paths). Use a throwaway runtime -- correct, just not reused.
		result, err, _ := newJSSharedRuntime().call(name, operands, item, context)
		return result, err
	}
	result, err, poisoned := host.jsShared().call(name, operands, item, context)
	if poisoned {
		host.dropJSShared()
	}
	return result, err
}

// jsRuntimeHost is implemented by the eval contexts that can cache a per-query
// shared JS runtime (GlueContext and ExprGlueContext).
type jsRuntimeHost interface {
	jsShared() *jsSharedRuntime
	dropJSShared()
}

// toGoja marshals a cbq value into the given runtime. MISSING -> undefined,
// NULL -> null; scalars pass by value; OBJECT/ARRAY are DEEP-COPIED first
// (CopyForUpdate) because goja wraps a Go map/slice by reference, so without the
// copy a UDF that mutates an object/array argument would write through into the
// shared source document -- silent corruption of the query's input.
func toGoja(rt *goja.Runtime, v value.Value) goja.Value {
	if v == nil {
		return goja.Undefined()
	}
	switch v.Type() {
	case value.MISSING:
		return goja.Undefined()
	case value.NULL:
		return goja.Null()
	case value.OBJECT, value.ARRAY:
		return rt.ToValue(v.CopyForUpdate().Actual())
	default:
		return rt.ToValue(v.Actual())
	}
}

// fromGoja marshals a goja return value back to a cbq value. undefined -> MISSING.
func fromGoja(res goja.Value) value.Value {
	if res == nil || goja.IsUndefined(res) {
		return value.MISSING_VALUE
	}
	if goja.IsNull(res) {
		return value.NULL_VALUE
	}
	return value.NewValue(res.Export())
}

// --------------------------------------------------------
// jsFunc: the expression.Function the parser instantiates for a JS UDF call.
// It carries only the name; the executable runtime is resolved per-query from
// the eval context (see runJSUDF).

type jsFunc struct {
	expression.FunctionBase
}

func newJSFunc(name string, operands ...expression.Expression) expression.Function {
	rv := &jsFunc{}
	rv.Init(strings.ToLower(name), operands...)
	rv.SetExpr(rv)
	return rv
}

func (this *jsFunc) Accept(visitor expression.Visitor) (interface{}, error) {
	return visitor.VisitFunction(this)
}

func (this *jsFunc) Type() value.Type { return value.JSON }

func (this *jsFunc) MinArgs() int { return 0 }

func (this *jsFunc) MaxArgs() int { return math.MaxInt16 }

func (this *jsFunc) Constructor() expression.FunctionConstructor {
	name := this.Name()
	return func(operands ...expression.Expression) expression.Function {
		return newJSFunc(name, operands...)
	}
}

func (this *jsFunc) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	return runJSUDF(this.Name(), this.Operands(), item, context)
}

// registerJSFunc compiles source (which must define a function named name) and
// registers it so the parser resolves name(args) to the JS UDF and every
// per-query runtime defines it. A first-time registration that would shadow a
// stock cbq builtin or aggregate is refused (patch-05's RegisterFunction
// overwrites unconditionally, so the guard lives here); re-registering a name we
// previously installed is allowed (directory re-scan / hot reload).
func registerJSFunc(name, source string) error {
	name = strings.ToLower(name)

	if !extOurs[name] {
		if _, ok := expression.GetFunction(name); ok {
			return fmt.Errorf("JS UDF %q collides with a builtin function name", name)
		}
		if _, ok := algebra.GetAggregate(name, false, false, false); ok {
			return fmt.Errorf("JS UDF %q collides with an aggregate function name", name)
		}
	}

	prog, err := goja.Compile(name+".js", source, true)
	if err != nil {
		return fmt.Errorf("goja compile of %q: %w", name, err)
	}

	// Compile-check that the source actually defines a callable of this name.
	check := goja.New()
	if _, err := check.RunProgram(prog); err != nil {
		return fmt.Errorf("JS UDF %q: %w", name, err)
	}
	if _, ok := goja.AssertFunction(check.Get(name)); !ok {
		return fmt.Errorf("JS UDF %q: source does not define a function named %q", name, name)
	}

	if _, exists := jsPrograms[name]; !exists {
		jsProgramOrder = append(jsProgramOrder, name)
	}
	jsPrograms[name] = prog
	expression.RegisterFunction(name, newJSFunc(name))
	extOurs[name] = true
	return nil
}

// unregisterJSFunc drops the program from the registry (so future per-query
// runtimes won't define it) and overwrites the name in cbq's function registry
// with a stub that errors when called (cbq's registry has no delete, so the name
// still parses). extOurs stays set so a later reload isn't seen as a collision.
func unregisterJSFunc(name string) {
	name = strings.ToLower(name)
	delete(jsPrograms, name)
	for i, n := range jsProgramOrder {
		if n == name {
			jsProgramOrder = append(jsProgramOrder[:i], jsProgramOrder[i+1:]...)
			break
		}
	}
	expression.RegisterFunction(name, newUnloadedFunc(name))
}

// unloadedFunc replaces an unloaded extension in cbq's function registry: the
// name still parses (cbq's registry has no delete), but calling it errors
// instead of running stale code. A subsequent (re)load overwrites it again.
type unloadedFunc struct {
	expression.FunctionBase
}

func newUnloadedFunc(name string) expression.Function {
	rv := &unloadedFunc{}
	rv.Init(name)
	rv.SetExpr(rv)
	return rv
}

func (this *unloadedFunc) Accept(visitor expression.Visitor) (interface{}, error) {
	return visitor.VisitFunction(this)
}
func (this *unloadedFunc) Type() value.Type { return value.JSON }
func (this *unloadedFunc) MinArgs() int     { return 0 }
func (this *unloadedFunc) MaxArgs() int     { return math.MaxInt16 }
func (this *unloadedFunc) Constructor() expression.FunctionConstructor {
	name := this.Name()
	return func(operands ...expression.Expression) expression.Function { return newUnloadedFunc(name) }
}
func (this *unloadedFunc) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	return nil, fmt.Errorf("extension function %q was unloaded", this.Name())
}
