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
// registry; jsStreamFunc implements StreamSource, so conv.go's VisitExpressionScan
// routes FROM to the generic stream-fn op (op_stream_fn.go) rather than the
// materializing expr-scan. All the fan-out/backpressure lives in that one op; here
// we only produce rows (via StreamRows below).

// jsStreamNames records registered streaming-source names (for reload idempotency
// and diagnostics).
var jsStreamNames = map[string]bool{}

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
	recordExtExamples("javascript-stream", name, readJSExamples(check, name)) // inline goldens.

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
// StreamRows: jsStreamFunc's StreamSource implementation (the JS row producer).

// StreamRows evaluates the SQL args, calls the JS function with an injected emit
// callback, and produces one row per emitted value. Rows flow as they're produced
// (via the generic op's emit), so memory stays BOUNDED. Unlike scalar/aggregate
// UDFs the whole-source call runs for the scan's duration, so it is NOT bounded by
// JSCallTimeout. The JS `emit(a, b, c)` batch form yields one row per argument and
// returns false once the consumer wants no more, so the JS loop can stop.
func (this *jsStreamFunc) StreamRows(vars *base.Vars, gc *GlueContext,
	ctx expression.Context, item value.Value, emit func(base.Val) bool) error {
	sr := newJSSharedRuntime()
	if gc != nil {
		sr = gc.jsShared()
	}
	fn := sr.callable(this.Name())
	if fn == nil {
		return fmt.Errorf("js-stream: %q is not callable", this.Name())
	}

	var streamErr error

	// gojaEmit(row[, row2, ...]) — one row per argument; returns false to tell the
	// JS loop to stop (the consumer wants no more, or a marshal failure).
	gojaEmit := func(call goja.FunctionCall) goja.Value {
		for _, a := range call.Arguments {
			jv, err := json.Marshal(a.Export())
			if err != nil {
				streamErr = fmt.Errorf("js-stream %q: cannot marshal emitted row: %w", this.Name(), err)
				return sr.rt.ToValue(false)
			}
			if !emit(base.Val(jv)) {
				return sr.rt.ToValue(false)
			}
		}
		return sr.rt.ToValue(true)
	}

	// Args: emit first, then the evaluated SQL operands.
	jsArgs := make([]goja.Value, 0, len(this.Operands())+1)
	jsArgs = append(jsArgs, sr.rt.ToValue(gojaEmit))
	for _, op := range this.Operands() {
		v, e := op.Evaluate(item, ctx)
		if e != nil {
			return e
		}
		jsArgs = append(jsArgs, toGoja(sr.rt, v))
	}

	// Run the source, containing any panic/throw so a source can't crash the
	// engine; a poisoned runtime is dropped so the next query rebuilds a clean one.
	func() {
		defer func() {
			if r := recover(); r != nil {
				streamErr = fmt.Errorf("js-stream %q panicked: %v", this.Name(), r)
				if gc != nil {
					gc.dropJSShared()
				}
			}
		}()
		if _, callErr := fn(goja.Undefined(), jsArgs...); callErr != nil && streamErr == nil {
			streamErr = fmt.Errorf("js-stream %q: %w", this.Name(), callErr)
		}
	}()

	return streamErr
}
