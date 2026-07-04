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

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// extAggregate is a generic algebra.Aggregate used ONLY for parse + plan. The
// cbq parser resolves an aggregate name (e.g. sparkline / histogram) to a
// registered prototype and calls Constructor()(operand) to build the call node;
// conv.go's VisitGroup then reads Name()/Operands()/Distinct()/String() off it
// and routes the actual computation to n1k1's native, zero-garbage aggregation
// (base.Agg -- see base/agg_ext.go), keyed by base.AggCatalog[name]. n1k1 never
// runs cbq's execution engine over these nodes, so the Cumulate*/ComputeFinal/
// Default/Evaluate methods are never invoked and are safe stubs.
//
// This is the aggregate counterpart of the scalar-UDF bridge in ext_js.go, and
// the parser-side half of the extension seam described in DESIGN-extensions.md.
type extAggregate struct {
	algebra.AggregateBase
}

// makeExtAggregate builds an extAggregate with the exact back-pointer wiring the
// concrete cbq aggregates use (SetExpr(rv), and BaseCopy in Copy()).
func makeExtAggregate(name string, operands expression.Expressions,
	flags uint32, filter expression.Expression, wTerm *algebra.WindowTerm) *extAggregate {
	rv := &extAggregate{*algebra.NewAggregateBase(name, operands, flags, filter, wTerm)}
	rv.SetExpr(rv)
	return rv
}

func (this *extAggregate) Accept(visitor expression.Visitor) (interface{}, error) {
	return visitor.VisitFunction(this)
}

// Type is STRING: the extension aggregates render a unicode chart string.
func (this *extAggregate) Type() value.Type { return value.STRING }

func (this *extAggregate) Constructor() expression.FunctionConstructor {
	name := this.Name()
	return func(operands ...expression.Expression) expression.Function {
		return makeExtAggregate(name, operands, uint32(0), nil, nil)
	}
}

func (this *extAggregate) Copy() expression.Expression {
	rv := makeExtAggregate(this.Name(),
		expression.CopyExpressions(this.Operands()),
		this.Flags(), expression.Copy(this.Filter()),
		algebra.CopyWindowTerm(this.WindowTerm()))
	rv.BaseCopy(this)
	rv.SetExpr(rv)
	return rv
}

// --- execution-only methods: never called by n1k1 (native agg computes the
// result); safe stubs kept only to satisfy the algebra.Aggregate interface. ---

func (this *extAggregate) Default(item value.Value, context algebra.Context) (value.Value, error) {
	return value.NULL_VALUE, nil
}

// Evaluate reads the value n1k1's group op already computed for this aggregate
// and stashed in the row's ATT_AGGREGATES attachment, keyed by String() (the
// projection re-evaluates the aggregate expression to pull it back). This
// mirrors unexported AggregateBase.evaluate, which we can't call across the
// package boundary. NOT the aggregation itself -- that is base/agg_ext.go.
func (this *extAggregate) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	av, ok := item.(value.AnnotatedValue)
	if !ok {
		return value.NULL_VALUE, nil
	}
	if aggregates := av.GetAttachment(value.ATT_AGGREGATES); aggregates != nil {
		if aggs, ok := aggregates.(map[string]value.Value); ok {
			if result := aggs[this.String()]; result != nil {
				return result, nil
			}
		}
	}
	return value.NULL_VALUE, nil
}

func (this *extAggregate) CumulateInitial(item, cumulative value.Value, context algebra.Context) (value.Value, error) {
	return nil, fmt.Errorf("%s: CumulateInitial not supported (n1k1 computes natively)", this.Name())
}

func (this *extAggregate) CumulateIntermediate(part, cumulative value.Value, context algebra.Context) (value.Value, error) {
	return nil, fmt.Errorf("%s: CumulateIntermediate not supported (n1k1 computes natively)", this.Name())
}

func (this *extAggregate) ComputeFinal(cumulative value.Value, context algebra.Context) (value.Value, error) {
	return nil, fmt.Errorf("%s: ComputeFinal not supported (n1k1 computes natively)", this.Name())
}

// --- registration ---

// registerExtAggregate wires an extension aggregate name into the cbq parser so
// NAME(expr) parses as an aggregate. The name MUST also be registered in
// base.AggCatalog (base package) so the engine can compute it. property is the
// algebra.AGGREGATE_ALLOWS_* bitmask.
func registerExtAggregate(name string, property uint32) {
	algebra.RegisterAggregate(name, property, makeExtAggregate(name, nil, 0, nil, nil))
}
