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

package base

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// LzExprFmt renders a compile-time-known "live" expression for the lz codegen
// (the intermed compiler calls this in place of a raw %#v). Everything except a
// func renders exactly as %#v did before -- so ints/bools/strings are unchanged.
//
// A FUNC value renders as its qualified Go name (e.g. base.StrCaseUpper, math.Abs,
// base.Num.Div), so a harness can take an op as a real func param and have the
// compiled path emit a genuine call rather than an un-compilable pointer literal.
// This only works for NAMED, exported funcs in a package the generated `tmp` file
// imports (base.*, math.*, ...) -- not closures (whose runtime name is uncallable)
// nor unexported engine-local funcs. Put the leaf logic in base and pass base.Foo.
func LzExprFmt(v interface{}) string {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Func && !rv.IsNil() {
		if fn := runtime.FuncForPC(rv.Pointer()); fn != nil {
			if name := fn.Name(); name != "" {
				// Strip the import path, keeping the package-qualified name:
				//   github.com/couchbase/n1k1/base.StrCaseUpper -> base.StrCaseUpper
				//   github.com/couchbase/n1k1/base.Num.Div  -> base.Num.Div (method expr)
				//   math.Abs                                -> math.Abs
				if i := strings.LastIndex(name, "/"); i >= 0 {
					name = name[i+1:]
				}
				return name
			}
		}
	}
	// Non-func, or a nil / unresolvable func: %#v (unchanged from before). A nil
	// func renders as a valid `(T)(nil)`, e.g. in a `_ = fn` unused-suppression.
	return fmt.Sprintf("%#v", v)
}
