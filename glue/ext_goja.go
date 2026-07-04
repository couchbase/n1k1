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
	"math"
	"strings"
	"sync"
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
// NAME(args) to it. At Evaluate time the args are marshaled to goja values, the
// JS runs, and its return value is marshaled back -- flowing through the same
// interpreted/boxed lane n1k1 already uses for un-ported cbq expressions (via
// ExprTree -> Expression.Evaluate), not the zero-alloc byte fast path.
//
// goja (github.com/dop251/goja) is pure-Go / MIT / cgo-free, so it preserves
// n1k1's CGO_ENABLED=0 static-binary property.

// JSCallTimeout bounds a single UDF invocation: a runaway script (infinite loop,
// pathologically slow) is interrupted after this long and the call fails cleanly
// rather than hanging the actor goroutine forever. 0 disables the guard. An
// embedder may tune it before enabling UDFs. (goja is in-process and cannot be
// memory-capped like a Wasm guest -- see the DESIGN-extensions.md Caveats.)
var JSCallTimeout = 5 * time.Second

// jsRegistered records the UDF names we ourselves installed, so a re-scan of the
// same directory can re-register (overwrite) its own functions, while a *first*
// registration is still refused if it would shadow a stock builtin/aggregate.
var jsRegistered = map[string]bool{}

// jsProgram is a compiled JS UDF shared across a query. goja.Runtime is NOT
// goroutine-safe and n1k1 evaluates expressions from concurrent actor
// goroutines (base.Stage), so each goroutine borrows its own runtime from a
// pool. The *goja.Program (compiled once) is safely shared to seed each runtime.
type jsProgram struct {
	name   string // SQL++ (and JS) function name.
	prog   *goja.Program
	source string

	pool sync.Pool // of *jsRuntime; heavyweight interpreter state, not a hot buffer.
}

// jsRuntime is one goroutine-private goja runtime with the UDF resolved to a
// callable, plus reusable per-call scratch (goja runs single-threaded on it, so
// the scratch is never touched concurrently).
type jsRuntime struct {
	rt      *goja.Runtime
	fn      goja.Callable
	argVals []value.Value // scratch: evaluated cbq operand values.
	jsArgs  []goja.Value  // scratch: marshaled goja arguments.
}

func newJSProgram(name, source string) (*jsProgram, error) {
	prog, err := goja.Compile(name+".js", source, true)
	if err != nil {
		return nil, fmt.Errorf("goja compile of %q: %w", name, err)
	}
	jp := &jsProgram{name: name, prog: prog, source: source}
	jp.pool.New = func() interface{} { return jp.newRuntime() }

	// Compile-check that the source actually defines a callable of this name.
	jr := jp.newRuntime()
	if jr.fn == nil {
		return nil, fmt.Errorf("JS UDF %q: source does not define a function named %q", name, name)
	}
	jp.pool.Put(jr)
	return jp, nil
}

func (jp *jsProgram) newRuntime() *jsRuntime {
	rt := goja.New()
	if _, err := rt.RunProgram(jp.prog); err != nil {
		return &jsRuntime{rt: rt} // fn stays nil -> Evaluate reports a clean error.
	}
	fn, _ := goja.AssertFunction(rt.Get(jp.name))
	return &jsRuntime{rt: rt, fn: fn}
}

// call evaluates operands and runs the UDF on a pooled runtime. A runtime whose
// call panicked or was interrupted is DROPPED (not returned to the pool) because
// its VM state may be inconsistent; a fresh one is created on the next call.
func (jp *jsProgram) call(operands expression.Expressions,
	item value.Value, context expression.Context) (result value.Value, err error) {
	jr := jp.pool.Get().(*jsRuntime)
	poolable := true
	defer func() {
		if r := recover(); r != nil {
			result, err = value.NULL_VALUE, fmt.Errorf("JS UDF %q panicked: %v", jp.name, r)
			poolable = false
		}
		if poolable {
			jp.pool.Put(jr)
		}
	}()

	if jr.fn == nil {
		return value.NULL_VALUE, fmt.Errorf("JS UDF %q: not callable", jp.name)
	}

	// Evaluate operands then marshal, reusing the runtime's scratch slices (this
	// goroutine is the only user of jr) to avoid a per-row allocation.
	argVals := jr.argVals[:0]
	for _, op := range operands {
		v, e := op.Evaluate(item, context)
		if e != nil {
			return value.NULL_VALUE, e
		}
		argVals = append(argVals, v)
	}
	jr.argVals = argVals

	jsArgs := jr.jsArgs[:0]
	for _, a := range argVals {
		jsArgs = append(jsArgs, toGoja(jr.rt, a))
	}
	jr.jsArgs = jsArgs

	// Timeout guard: a timer interrupts the runtime if the call overruns. On the
	// normal path timer.Stop() + ClearInterrupt() drop any interrupt the timer
	// delivered late (between fn returning and Stop), keeping the pooled runtime
	// clean; timedOut disambiguates a timeout from an ordinary JS throw.
	var timedOut int32
	if JSCallTimeout > 0 {
		timer := time.AfterFunc(JSCallTimeout, func() {
			atomic.StoreInt32(&timedOut, 1)
			jr.rt.Interrupt("n1k1: JS UDF time limit exceeded")
		})
		defer func() {
			timer.Stop()
			jr.rt.ClearInterrupt()
		}()
	}

	res, callErr := jr.fn(goja.Undefined(), jsArgs...)
	if atomic.LoadInt32(&timedOut) == 1 {
		poolable = false
		return value.NULL_VALUE, fmt.Errorf("JS UDF %q exceeded the %s time limit", jp.name, JSCallTimeout)
	}
	if callErr != nil {
		return value.NULL_VALUE, fmt.Errorf("JS UDF %q: %w", jp.name, callErr)
	}
	return fromGoja(res), nil
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

type jsFunc struct {
	expression.FunctionBase
	js *jsProgram
}

func newJSFunc(js *jsProgram, operands ...expression.Expression) expression.Function {
	rv := &jsFunc{js: js}
	rv.Init(js.name, operands...)
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
	js := this.js
	return func(operands ...expression.Expression) expression.Function {
		return newJSFunc(js, operands...)
	}
}

func (this *jsFunc) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	return this.js.call(this.Operands(), item, context)
}

// registerJSFunc compiles source (which must define a function named name) and
// registers it as a builtin so the parser resolves name(args) to the JS UDF.
// A first-time registration that would shadow a stock cbq builtin or aggregate
// is refused (patch-05's RegisterFunction overwrites unconditionally, so the
// guard lives here); re-registering a name we previously installed is allowed
// (directory re-scan / hot reload).
func registerJSFunc(name, source string) error {
	name = strings.ToLower(name)

	if !jsRegistered[name] {
		if _, ok := expression.GetFunction(name); ok {
			return fmt.Errorf("JS UDF %q collides with a builtin function name", name)
		}
		if _, ok := algebra.GetAggregate(name, false, false, false); ok {
			return fmt.Errorf("JS UDF %q collides with an aggregate function name", name)
		}
	}

	jp, err := newJSProgram(name, source)
	if err != nil {
		return err
	}
	expression.RegisterFunction(name, newJSFunc(jp))
	jsRegistered[name] = true
	return nil
}
