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
	"github.com/couchbase/n1k1/base"
)

// The conditional-unknown selectors: IFNULL / IFMISSING / IFMISSINGORNULL (and
// COALESCE == IFMISSINGORNULL). All variadic: return the first operand "kept"
// under the mode, else NULL; the result is the chosen operand's bytes verbatim
// (zero-copy). Mirrors cbq expression/func_cond_unknown.go. N-ary (any operand
// count) via MakeNaryExprFunc.

// condFuncs maps each conditional-unknown selector to its base.CondIf* mode
// (passed to base.NaryFirstKept). NVL(a, b) returns b when a is NULL *or* MISSING
// (cbq NVL.Evaluate: `first.Type() > NULL ? first : second`), i.e. it's 2-arg
// IFMISSINGORNULL -- not IFNULL, which keeps a MISSING first operand.
var condFuncs = map[string]int{
	"ifnull": base.CondIfNull, "ifmissing": base.CondIfMissing,
	"ifmissingornull": base.CondIfMissingOrNull, "nvl": base.CondIfMissingOrNull,
}

func init() {
	for name, mode := range condFuncs {
		ExprCatalog[name] = exprCondOp(mode)
	}
}

// exprCondOp closes over a conditional-unknown mode: it builds the n-ary reducer
// (base.NaryFirstKept with that mode) and defers to MakeNaryExprFunc. Plain
// (non-lz) Go, so the interpreter path is unchanged from the old per-op funcs.
func exprCondOp(mode int) base.ExprCatalogFunc {
	reduce := func(children []base.ExprFunc, vals base.Vals, yieldErr base.YieldErr) base.Val {
		return base.NaryFirstKept(children, vals, yieldErr, mode)
	}
	return func(lzVars *base.Vars, labels base.Labels, params []interface{}, path string) base.ExprFunc {
		return MakeNaryExprFunc(lzVars, labels, params, path, reduce)
	}
}
