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

// Package builtins bundles the *.sql++ files in this directory into the n1k1 binary
// (go:embed) as PARAMETERIZED, VERSIONED builtin queries — the SQL++ sibling of
// extensions/macros. To ship a new builtin, add a well-tested `<name>.sql++` here:
// the embed glob picks it up, `--queries builtin:<name>.sql++?k=v` runs it, and
// `.multi show` prints its (rendered) source. Each file is an ordinary pack entry —
// front-matter + one SQL++ statement (+ optional -- @fixture / -- @expect goldens) —
// plus two extra front-matter keys this package owns:
//
//	-- version: v1.3
//	    The ARTIFACT's version (not n1k1's) — semver-style, bumped when the file's
//	    meaning/output changes, so a user (or a durable artifact like a cursor) can
//	    pin `builtin:<name>@<version>` and detect incompatibility instead of
//	    silently running a different query than the one they validated against.
//
//	-- param: <name> <type> [= <default>]
//	    One declared substitution parameter (repeatable). A `$name` reference in the
//	    SQL body — SQL++'s own named-parameter syntax, ONE grammar — is replaced by
//	    the (validated, safely rendered) value from the URI query-string; no `=`
//	    means REQUIRED. Types:
//	      ident  an identifier/path rendered `backticked` (keyspace, field names)
//	      int    an integer rendered bare
//	      list   a comma-separated list rendered as a JSON string array (empty -> [])
//
// The syntax is SQL++ named parameters; the BINDING is early (pre-parse constant
// folding) rather than engine-runtime, because a pack's economics are literal-driven
// (the predicate index prunes on plan-time literals; the compiled lane bakes them) —
// a runtime $param gate would degrade to always-wake. Resolution is quote-aware
// (a $ inside a string/backtick literal is data) and matches the LONGEST declared
// name with a non-name-character boundary (so `$type-field` binds the declared
// `type-field`; arithmetic needs spacing: `$depth - 1`, since `$depth-1` reads as
// an undeclared name and errors). Value-only substitution is deliberate: the
// template's SQL STRUCTURE is static, so what `.multi show` prints is exactly what
// runs (and exactly what a user forks) — structural variation is expressed IN SQL++
// (e.g. `WHEN $depth >= 2 AND ...`), never in Go string-building.
package builtins

import (
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

//go:embed *.sql++
var builtinFS embed.FS

// Param is one declared substitution parameter of a builtin query (the shared
// pack-parameter machinery in glue/params.go — file packs use the identical
// declaration, syntax, and rendering via `--param k=v`).
type Param = glue.QueryParam

// Query is one embedded builtin *.sql++: its identity, artifact version, declared
// params, and the SQL template (front-matter + fixture sections stripped).
type Query struct {
	Name        string // the file name, e.g. "census.sql++" (the builtin: ref name)
	Version     string // `-- version:` front-matter (the ARTIFACT's version), "" if undeclared
	Description string
	Params      []Param
	Template    string               // the SQL body with $name references
	Entry       glue.MultiQueryEntry // the full parsed entry (fixtures ride along)
}

var (
	queries    []Query
	queriesErr error
)

func init() { queries, queriesErr = load() }

func load() ([]Query, error) {
	entries, err := builtinFS.ReadDir(".")
	if err != nil {
		return nil, err
	}
	out := make([]Query, 0, len(entries))
	for _, de := range entries {
		fn := de.Name()
		if de.IsDir() || !strings.HasSuffix(fn, ".sql++") {
			continue
		}
		src, rerr := builtinFS.ReadFile(fn)
		if rerr != nil {
			return nil, fmt.Errorf("builtin %s: %v", fn, rerr)
		}
		e, perr := glue.ParseMultiQueryEntry(fn, string(src))
		if perr != nil {
			return nil, fmt.Errorf("builtin %s: %v", fn, perr)
		}
		out = append(out, Query{
			Name: fn, Version: e.Version, Description: e.Description,
			Params: e.Params, Template: e.Stmt, Entry: e,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// All returns the embedded builtin queries, sorted by name. The error is non-nil
// only when an embedded file itself is malformed — a build-time defect, surfaced
// loudly at first use rather than panicking at init.
func All() ([]Query, error) { return queries, queriesErr }

// Lookup finds an embedded builtin by its ref name (e.g. "census.sql++").
func Lookup(name string) (Query, bool) {
	for _, q := range queries {
		if q.Name == name {
			return q, true
		}
	}
	return Query{}, false
}

// Resolve merges the caller's URI params over the declared defaults: an unknown
// key is a loud error (a typo'd param silently ignored is how a census runs with
// depth=2 when you asked depht=1), and a missing REQUIRED param names itself.
func (q Query) Resolve(given map[string]string) (map[string]string, error) {
	for k := range given {
		if _, ok := glue.ParamKeyResolve(q.Params, k); !ok {
			return nil, fmt.Errorf("builtin %s has no param %q (declared: %s)", q.Name, k, q.paramNames())
		}
	}
	return glue.ParamsResolve("builtin "+q.Name, q.Params, given)
}

// Render substitutes the resolved params into the template's `$name` references
// via the shared pack-parameter renderer (glue.RenderStmtParams): SQL++
// named-parameter syntax bound early, quote- and comment-aware, typed rendering.
func (q Query) Render(resolved map[string]string) (string, error) {
	return glue.RenderStmtParams("builtin "+q.Name, q.Template, q.Params, resolved)
}

func (q Query) paramNames() string {
	names := make([]string, len(q.Params))
	for i, p := range q.Params {
		names[i] = p.Name
	}
	return strings.Join(names, ", ")
}
