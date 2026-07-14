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

// Package macros bundles the *.macro.js files in this directory into the n1k1 binary
// (go:embed), so they ship built-in and are always available without -ext. To ship a
// new built-in macro, just add a well-tested `<name>.macro.js` here -- the embed glob
// picks it up automatically. The CLI registers these at startup; a user -ext load of a
// same-named macro simply overrides.
package macros

import (
	"embed"
	"sort"
	"strings"
)

//go:embed *.macro.js
var builtinFS embed.FS

// Builtin is one shipped macro: its invocation name (the filename stem) and JS source.
type Builtin struct {
	Name   string
	Source string
}

// Builtins returns the embedded built-in macros, sorted by name.
func Builtins() []Builtin {
	entries, err := builtinFS.ReadDir(".")
	if err != nil {
		return nil
	}
	out := make([]Builtin, 0, len(entries))
	for _, e := range entries {
		fn := e.Name()
		if e.IsDir() || !strings.HasSuffix(fn, ".macro.js") {
			continue
		}
		src, err := builtinFS.ReadFile(fn)
		if err != nil {
			continue
		}
		out = append(out, Builtin{Name: strings.TrimSuffix(fn, ".macro.js"), Source: string(src)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
