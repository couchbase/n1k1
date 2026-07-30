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

// `node('<name>')` is the table-valued reference a `.multi compose` DAG node uses to
// read an upstream node's materialized rows: `-- needs: rollup` then
// `FROM node('rollup') AS r`. It replaces the old `FROM pack_<name>` keyspace ref,
// which shared the keyspace name-space (a real keyspace named `pack_x` could collide)
// AND misnamed a node's RESULT as a "pack". A function reference is collision-free (it
// can never clash with a keyspace name) and self-documenting.
//
// It is a StreamSource (like a JS *.stream.js source), so conv.go's VisitExpressionScan
// routes `FROM node(...)` to the generic stream-fn op. StreamRows resolves the name to
// the node's temp keyspace (materialized by compose under ComposeKeyspace) via the
// GlueContext and emits its rows verbatim — the same {label,result,fingerprint} rows a
// `FROM pack_<name>` scan yielded. Meaningful only inside a compose DAG; elsewhere the
// node isn't materialized and it errors clearly.

import (
	"fmt"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
)

func init() { expression.RegisterFunction("node", newNodeFunc()) }

type nodeFunc struct {
	expression.FunctionBase
}

func newNodeFunc(operands ...expression.Expression) expression.Function {
	rv := &nodeFunc{}
	rv.Init("node", operands...)
	rv.SetExpr(rv)
	return rv
}

func (this *nodeFunc) Accept(visitor expression.Visitor) (interface{}, error) {
	return visitor.VisitFunction(this)
}
func (this *nodeFunc) Type() value.Type { return value.ARRAY } // set-returning
func (this *nodeFunc) MinArgs() int     { return 1 }
func (this *nodeFunc) MaxArgs() int     { return 1 }
func (this *nodeFunc) Constructor() expression.FunctionConstructor {
	return func(operands ...expression.Expression) expression.Function { return newNodeFunc(operands...) }
}

// Evaluate is only reached if node() is (mis)used outside a FROM clause; execution is
// the stream-fn op (VisitExpressionScan routes it), so reject clearly here.
func (this *nodeFunc) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	return nil, fmt.Errorf("node(...) is a table source: use it in a FROM clause (FROM node('name') AS x)")
}

// StreamRows resolves the node name to its compose temp keyspace and emits each stored
// row (verbatim), so a downstream node sees exactly what `FROM pack_<name>` used to.
func (this *nodeFunc) StreamRows(vars *base.Vars, gc *GlueContext,
	ctx expression.Context, item value.Value, emit func(base.Val) bool) error {
	ops := this.Operands()
	if len(ops) != 1 {
		return fmt.Errorf("node() takes one argument, the node name, e.g. node('rollup')")
	}
	nv, err := ops[0].Evaluate(item, ctx)
	if err != nil {
		return err
	}
	name, _ := nv.Actual().(string)
	if name == "" {
		return fmt.Errorf("node() needs a non-empty node name, e.g. node('rollup')")
	}
	if gc == nil || gc.subq == nil || gc.subq.store == nil || gc.subq.store.Temp == nil {
		return fmt.Errorf("node(%q): no compose context (node() is only valid within a .multi compose DAG)", name)
	}
	tk, ok := gc.subq.store.Temp.get(ComposeKeyspace(name))
	if !ok {
		return fmt.Errorf("node(%q): no such node materialized in this DAG (declare the dependency with `-- needs: %s`)", name, name)
	}
	if tk.heap == nil {
		return nil // an empty node produces no rows
	}
	for i := int64(0); i < int64(tk.heap.Len()); i++ {
		row, err := tk.heap.Get(i)
		if err != nil {
			return err
		}
		if !emit(base.Val(row)) {
			return nil // consumer wants no more (e.g. a satisfied LIMIT)
		}
	}
	return nil
}
