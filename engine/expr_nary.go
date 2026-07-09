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

package engine

import (
	"strconv"

	"github.com/couchbase/n1k1/base"
)

// NaryCaptureChildren builds the child ExprFuncs for an eager variadic expr. It
// runs entirely at build time (`// !lz`): for the compiler its role is the
// per-operand EmitPush/EmitPop captures (so each operand can later be inlined by
// `// <== emitCaptured`); for the interpreter it returns the real child slice.
//
// The eager nary exprs (ExprConcat, ExprGreatest/ExprLeast, the ifnull/coalesce
// family) share this, then each writes its own eval closure inline: evaluate
// each operand ONE AT A TIME into the shared lzVal register (emitCaptured, the
// mechanism the binary/ternary exprs use), append it to lzValsReduce (the
// per-expr reused buffer of evaluated operand values), then a plain (lazy) call
// to a cbq-free base reducer (base.NaryConcatVals / GreatestLeastVals /
// NaryFirstKeptVals) over lzValsReduce. Keeping that reduce line -- and
// lzValsReduce's varLift -- IN the expr's own function is deliberate: varLift
// renames per function+path, so each nary gets a distinct hoisted buffer and
// nested/sibling narys never clobber each other.
//
// Why eager (evaluate every operand): these exprs evaluate all operands anyway
// (see the base *Vals reducers), so pre-evaluating each operand into a value --
// instead of handing child closures to the reducer -- is semantics-preserving
// AND lets the compiler inline each operand with no nested func literal for a
// parent expr's emitCaptured to trip over. CASE, being lazy, can't pre-evaluate;
// it emits a flat lzMatched-guarded short-circuit instead (see ExprCase).
func NaryCaptureChildren(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzChildren []base.ExprFunc) {
	for lzI := range params { // !lz
		lzChild := MakeExprFunc(lzVars, labels, params[lzI].([]interface{}), path, strconv.Itoa(lzI)) // !lz

		lzChildren = append(lzChildren, lzChild) // !lz
	} // !lz

	return lzChildren // !lz
}
