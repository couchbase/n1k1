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

func init() {
	ExprCatalog["ifnull"] = ExprIfNull
	ExprCatalog["ifmissing"] = ExprIfMissing
	ExprCatalog["ifmissingornull"] = ExprIfMissingOrNull
	// NVL(a, b) returns b when a is NULL *or* MISSING (cbq NVL.Evaluate:
	// `first.Type() > NULL ? first : second`), i.e. it's 2-arg IFMISSINGORNULL --
	// not IFNULL, which keeps a MISSING first operand.
	ExprCatalog["nvl"] = ExprIfMissingOrNull
}

// Per-mode reducers, passed directly to the n-ary harness.
func naryIfNull(children []base.ExprFunc, vals base.Vals, yieldErr base.YieldErr) base.Val {
	return base.NaryFirstKept(children, vals, yieldErr, base.CondIfNull)
}

func naryIfMissing(children []base.ExprFunc, vals base.Vals, yieldErr base.YieldErr) base.Val {
	return base.NaryFirstKept(children, vals, yieldErr, base.CondIfMissing)
}

func naryIfMissingOrNull(children []base.ExprFunc, vals base.Vals, yieldErr base.YieldErr) base.Val {
	return base.NaryFirstKept(children, vals, yieldErr, base.CondIfMissingOrNull)
}

func ExprIfNull(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return MakeNaryExprFunc(lzVars, labels, params, path, naryIfNull)
}

func ExprIfMissing(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return MakeNaryExprFunc(lzVars, labels, params, path, naryIfMissing)
}

func ExprIfMissingOrNull(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return MakeNaryExprFunc(lzVars, labels, params, path, naryIfMissingOrNull)
}
