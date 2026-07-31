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

import "strings"

// JSFrontMatter reads `// key: value` front-matter from the TOP of a JS artifact
// (*.macro.js, *.extract.js, UDF module) — the JS mirror of the *.sql++ front-matter
// convention, so every artifact family can declare the same advisory metadata:
//
//	// version:     v1.2        the ARTIFACT's version (not n1k1's)
//	// description: one line
//	// tags:        a, b
//
// Rules mirror ParseMultiQueryEntry's: the front-matter is the leading run of blank
// and `// <bare-ident>: value` comment lines; the first line that is neither — code,
// or a prose comment ("// This macro rewrites…", whose first word is not key-shaped) —
// ends it, and everything after is ignored. Keys are lowercased; values verbatim.
// ALL keys are captured (not just the known ones): labels/annotations/whatever a
// team invents ride along for future filtering/analysis, never silently dropped.
// Returns nil when there is no front-matter.
func JSFrontMatter(src string) map[string]string {
	var out map[string]string
	for _, ln := range strings.Split(src, "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue // blank lines are skipped while still in front-matter
		}
		body, ok := strings.CutPrefix(ln, "//")
		if !ok {
			break // code: front-matter over
		}
		key, val, ok := strings.Cut(body, ":")
		if !ok {
			break // a prose comment ends the front-matter
		}
		key = strings.TrimSpace(key)
		if key == "" || strings.ContainsAny(key, " \t") {
			break // "key" with spaces is prose ("// NOTE about: x"), not front-matter
		}
		if out == nil {
			out = map[string]string{}
		}
		out[strings.ToLower(key)] = strings.TrimSpace(val)
	}
	return out
}
