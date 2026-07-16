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

// Package builtinjs embeds the shipped builtin_*.js JS modules (e.g. builtin_decimal,
// builtin_ejson) so the n1k1 binary can auto-register them at startup — "builtins" are
// available with no -ext flag (mirrors extensions/macros for the built-in macros). The
// non-builtin demo files in this directory (add_two_numbers.js, slugify.js, …) are NOT
// embedded; they load via -ext as examples.
package builtinjs

import (
	"embed"
	"sort"
	"strings"
)

//go:embed builtin_*.js
var builtinFS embed.FS

// Builtin is one embedded builtin module: its bundle name (filename stem) + source.
type Builtin struct {
	Name   string
	Source string
}

// Builtins returns the embedded builtin_*.js modules, sorted by name.
func Builtins() []Builtin {
	entries, err := builtinFS.ReadDir(".")
	if err != nil {
		return nil
	}
	out := make([]Builtin, 0, len(entries))
	for _, e := range entries {
		fn := e.Name()
		if e.IsDir() || !strings.HasPrefix(fn, "builtin_") || !strings.HasSuffix(fn, ".js") {
			continue
		}
		src, err := builtinFS.ReadFile(fn)
		if err != nil {
			continue
		}
		out = append(out, Builtin{Name: strings.TrimSuffix(fn, ".js"), Source: string(src)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
