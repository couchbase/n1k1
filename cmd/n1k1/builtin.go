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

package main

// Built-in macros ship inside the n1k1 binary (go:embed in extensions/macros): they are
// registered at startup so @vectorize_field & friends are always available, no -ext
// needed. A user -ext load of a same-named macro overrides (RegisterMacro replaces).

import (
	"github.com/couchbase/n1k1/extensions/macros"
	"github.com/couchbase/n1k1/glue"
)

// builtinMacroNames marks which loaded macros are the shipped built-ins (for `.macro
// list`/`show` labeling). Populated by registerBuiltinMacros.
var builtinMacroNames = map[string]bool{}

// registerBuiltinMacros registers the embedded built-in macros. Best-effort: a macro
// that fails to compile is skipped (returned in the error list) rather than aborting
// startup, since a user's own extensions/queries shouldn't be blocked by one bad
// built-in.
func registerBuiltinMacros() []error {
	var errs []error
	for _, m := range macros.Builtins() {
		if err := glue.RegisterJSMacro(m.Name, m.Source); err != nil {
			errs = append(errs, err)
			continue
		}
		builtinMacroNames[m.Name] = true
	}
	return errs
}
