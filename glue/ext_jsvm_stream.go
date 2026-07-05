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
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/dop251/goja"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// A JS *streaming table-valued source* (a `*.stream.js` file) is a function used
// in a FROM clause that pushes rows one at a time via an injected `emit`
// callback, instead of returning one big array. So `FROM gen(5) AS x` never
// materializes an array: each `emit(row)` flows straight into the pipeline (with
// backpressure), so a huge/endless source composes with WHERE/GROUP BY/LIMIT at
// bounded memory. See DESIGN-extensions.md "Streaming JS/goja functions".
//
// Shape: the JS defines `function NAME(emit, ...args) { … emit(row) … }`. n1k1
// injects `emit` as the first argument; the query's SQL args follow. `emit`
// accepts one row per argument (so `emit(a)` yields one row, `emit(a, b, c)`
// yields three — a simple, unambiguous batch form) and returns `false` once the
// consumer wants no more (e.g. a LIMIT is satisfied), so the JS loop can stop.
//
// The parser resolves NAME(args) via a jsStreamFunc registered in cbq's builtin
// registry; conv.go's VisitExpressionScan recognizes that type and routes FROM to
// the js-stream op (below) rather than the materializing expr-scan.

// jsStreamNames records registered streaming-source names (for reload idempotency
// and diagnostics).
var jsStreamNames = map[string]bool{}

// jsStreamPollEvery is how often (in rows emitted) the source polls YieldStats
// for an early-exit request (e.g. LIMIT satisfied), mirroring op_scan.
const jsStreamPollEvery = 256

// RegisterJSStream registers a streaming table-valued source NAME from source,
// which must define `function NAME(emit, ...args)`. NAME(args) then works in a
// FROM clause as a row source.
func RegisterJSStream(name, source string) error {
	name = strings.ToLower(name)

	if !extOurs[name] {
		if _, ok := expression.GetFunction(name); ok {
			return fmt.Errorf("JS streaming source %q collides with a builtin function name", name)
		}
		if _, ok := algebra.GetAggregate(name, false, false, false); ok {
			return fmt.Errorf("JS streaming source %q collides with an aggregate function name", name)
		}
	}

	prog, err := goja.Compile(name+".stream.js", source, true)
	if err != nil {
		return fmt.Errorf("goja compile of %q: %w", name, err)
	}
	check := goja.New()
	if _, err := check.RunProgram(prog); err != nil {
		return fmt.Errorf("JS streaming source %q: %w", name, err)
	}
	if _, ok := goja.AssertFunction(check.Get(name)); !ok {
		return fmt.Errorf("JS streaming source %q: source must define a function named %q", name, name)
	}

	key := "stream:" + name
	if _, exists := jsPrograms[key]; !exists {
		jsProgramOrder = append(jsProgramOrder, key)
	}
	jsPrograms[key] = prog
	expression.RegisterFunction(name, newJSStreamFunc(name))
	extOurs[name] = true
	jsStreamNames[name] = true
	extLoaded[name] = ExtensionInfo{Name: name, Kind: "javascript-stream", Source: "(inline)"}
	return nil
}

// --------------------------------------------------------
// jsStreamFunc: the expression.Function the parser instantiates for a streaming
// source call. It exists so NAME(args) parses in FROM; execution is handled by
// the js-stream op (VisitExpressionScan routes to it), so Evaluate is only hit if
// the function is (mis)used outside a FROM clause -- which we reject clearly.

type jsStreamFunc struct {
	expression.FunctionBase
}

func newJSStreamFunc(name string, operands ...expression.Expression) expression.Function {
	rv := &jsStreamFunc{}
	rv.Init(strings.ToLower(name), operands...)
	rv.SetExpr(rv)
	return rv
}

func (this *jsStreamFunc) Accept(visitor expression.Visitor) (interface{}, error) {
	return visitor.VisitFunction(this)
}
func (this *jsStreamFunc) Type() value.Type { return value.ARRAY } // set-returning
func (this *jsStreamFunc) MinArgs() int     { return 0 }
func (this *jsStreamFunc) MaxArgs() int     { return math.MaxInt16 }
func (this *jsStreamFunc) Constructor() expression.FunctionConstructor {
	name := this.Name()
	return func(operands ...expression.Expression) expression.Function {
		return newJSStreamFunc(name, operands...)
	}
}
func (this *jsStreamFunc) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	return nil, fmt.Errorf("streaming source function %q can only be used in a FROM clause (FROM %s(...) AS x)",
		this.Name(), this.Name())
}

// --------------------------------------------------------
// JSStreamOp: the FROM-clause streaming source operator.

// JSStreamOp runs a streaming JS source (from a *.stream.js): it evaluates the
// SQL args, calls the JS function with an injected emit callback, and yields one
// row per emit. Rows flow as they're produced, so memory stays BOUNDED (no giant
// array is ever built) and the source composes with WHERE/GROUP BY/LIMIT like any
// scan. Unlike scalar/aggregate UDFs the whole-source call runs for the scan's
// duration, so it is NOT bounded by JSCallTimeout.
//
// Caveat (matches every n1k1 source today): the source runs to completion --
// there is no per-producer early-exit yet, so `LIMIT k` yields the right rows but
// only by dropping extras downstream, not by stopping the JS loop. Don't hand an
// *unbounded* source a query without a producer-side bound; when engine-wide
// early-exit lands (the YieldStats hook below, currently inert), emit will start
// returning false and the loop can stop.
func JSStreamOp(o *base.Op, vars *base.Vars, yieldVals base.YieldVals, yieldErr base.YieldErr) {
	idx, ok := o.Params[0].(int)
	if !ok {
		yieldErr(fmt.Errorf("js-stream: expected int Temps index, got %T", o.Params[0]))
		return
	}
	sf, ok := vars.Temps[idx].(*jsStreamFunc)
	if !ok {
		yieldErr(fmt.Errorf("js-stream: no streaming source function at Temps[%d]", idx))
		return
	}

	// Eval context + correlation scope, as ExprScanOp resolves them.
	var ctx expression.Context
	var item value.Value
	var gc *GlueContext
	if c, ok := vars.Temps[0].(*GlueContext); ok {
		gc, ctx, item = c, c, c.corrParent
	} else if c, ok := vars.Temps[0].(expression.Context); ok {
		ctx = c
	}

	sr := newJSSharedRuntime()
	if gc != nil {
		sr = gc.jsShared()
	}
	fn := sr.callable(sf.Name())
	if fn == nil {
		yieldErr(fmt.Errorf("js-stream: %q is not callable", sf.Name()))
		return
	}

	// stopErr carries an early-exit sentinel (from YieldStats, e.g. LIMIT), a
	// marshal error, or stays nil for a clean end-of-stream.
	var stopErr error
	rowsOut := 0

	// pollStop asks whether a downstream consumer wants us to stop (LIMIT), the
	// same YieldStats gate scans use. Only active when stats are on; otherwise the
	// source runs to completion and LIMIT still drops extra rows downstream.
	pollStop := func() bool {
		if vars.Ctx != nil && vars.Ctx.YieldStats != nil && vars.Ctx.Stats != nil {
			if err := vars.Ctx.YieldStats(vars.Ctx.Stats); err != nil {
				stopErr = err
				return true
			}
		}
		return false
	}

	// emit(row[, row2, ...]) — one row per argument; returns false to tell the JS
	// loop to stop (early-exit already signalled, or a marshal failure).
	emit := func(call goja.FunctionCall) goja.Value {
		if stopErr != nil {
			return sr.rt.ToValue(false)
		}
		for _, a := range call.Arguments {
			jv, err := json.Marshal(a.Export())
			if err != nil {
				stopErr = fmt.Errorf("js-stream %q: cannot marshal emitted row: %w", sf.Name(), err)
				return sr.rt.ToValue(false)
			}
			yieldVals(base.Vals{base.Val(jv)})
			rowsOut++
			if rowsOut%jsStreamPollEvery == 0 && pollStop() {
				return sr.rt.ToValue(false)
			}
		}
		return sr.rt.ToValue(true)
	}

	// Args: emit first, then the evaluated SQL operands.
	jsArgs := make([]goja.Value, 0, len(sf.Operands())+1)
	jsArgs = append(jsArgs, sr.rt.ToValue(emit))
	for _, op := range sf.Operands() {
		v, e := op.Evaluate(item, ctx)
		if e != nil {
			yieldErr(e)
			return
		}
		jsArgs = append(jsArgs, toGoja(sr.rt, v))
	}

	// Run the source, containing any panic/throw so a source can't crash the
	// engine; a poisoned runtime is dropped so the next query rebuilds a clean one.
	func() {
		defer func() {
			if r := recover(); r != nil {
				stopErr = fmt.Errorf("js-stream %q panicked: %v", sf.Name(), r)
				if gc != nil {
					gc.dropJSShared()
				}
			}
		}()
		if _, callErr := fn(goja.Undefined(), jsArgs...); callErr != nil && stopErr == nil {
			stopErr = fmt.Errorf("js-stream %q: %w", sf.Name(), callErr)
		}
	}()

	// Propagate the early-exit sentinel / error / clean nil, as scans do.
	yieldErr(stopErr)
}
