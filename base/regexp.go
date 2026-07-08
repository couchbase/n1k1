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
	"regexp"
)

// Regexp is a native regexp predicate's per-expression compiled-pattern cache.
// The pattern for a native REGEXP_CONTAINS / REGEXP_LIKE is ALWAYS a compile-time
// constant (the glue optimizer keeps a dynamic or invalid pattern boxed, so cbq's
// runtime error semantics are preserved -- see glue/expr_optimize.go). The
// handler bakes that constant pattern SOURCE as a varLift'd string and carries a
// Regexp across rows, so the compiled *regexp.Regexp lives entirely inside base
// (the generated compiled code references only base.*, never the regexp package)
// and is built ONCE, lazily, on the first row -- after which every row reuses it.
type Regexp struct {
	re  *regexp.Regexp
	set bool // whether the one-time compile of src has run
}

// StrRegexpMatch reports whether the JSON-string val matches src as a regular
// expression -- REGEXP_CONTAINS (src is the raw pattern) or REGEXP_LIKE (src is
// the caller-anchored "^pattern$" full-match form). MISSING -> MISSING, a
// non-string val -> NULL (via StrDecode). src is a baked compile-time constant,
// so r compiles it once (on the first row) and reuses it thereafter; a src that
// fails to compile (unreachable -- the optimizer only lowers a src that already
// compiled) yields NULL. re.Match runs on the decoded bytes directly; Go's regexp
// pools its matching machine, so matching allocates nothing per row.
func StrRegexpMatch(val Val, src string, r *Regexp) Val {
	decoded, sentinel, ok := StrDecode(val)
	if !ok {
		return sentinel
	}

	if !r.set {
		r.set = true
		r.re, _ = regexp.Compile(src) // src is a constant; compiled once
	}

	if r.re == nil {
		return ValNull
	}

	if r.re.Match(decoded) {
		return ValTrue
	}
	return ValFalse
}
