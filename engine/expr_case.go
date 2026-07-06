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

// ExprCase handles searched and simple CASE. The glue optimizer lowers both to a
// flat [cond, then, cond, then, ..., else?] param list (simple CASE's conds are
// eq(searchTerm, when)), so this is just MakeNaryExprFunc over base.CaseReduce.

func init() {
	ExprCatalog["case"] = ExprCase
}

func ExprCase(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return MakeNaryExprFunc(lzVars, labels, params, path, base.CaseReduce)
}
