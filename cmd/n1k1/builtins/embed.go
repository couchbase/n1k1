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
//	    One declared substitution parameter (repeatable). A `$(name)` placeholder in
//	    the SQL body is replaced by the (validated, safely rendered) value from the
//	    URI query-string; no `=` means REQUIRED. Types:
//	      ident  an identifier/path rendered `backticked` (keyspace, field names)
//	      int    an integer rendered bare
//	      list   a comma-separated list rendered as a JSON string array (empty -> [])
//
// Value-only substitution is deliberate: the template's SQL STRUCTURE is static, so
// what `.multi show` prints is exactly what runs (and exactly what a user forks) —
// structural variation is expressed IN SQL++ (e.g. `WHEN $(depth) >= 2 AND ...`),
// never in Go string-building.
package builtins

import (
	"embed"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

//go:embed *.sql++
var builtinFS embed.FS

// Param is one declared substitution parameter of a builtin query.
type Param struct {
	Name     string // the $(name) placeholder / URI query-string key
	Type     string // "ident" | "int" | "list"
	Default  string // raw default value ("" is a valid default)
	Required bool   // no `= default` was declared
}

// Query is one embedded builtin *.sql++: its identity, artifact version, declared
// params, and the SQL template (front-matter + fixture sections stripped).
type Query struct {
	Name        string // the file name, e.g. "census.sql++" (the builtin: ref name)
	Version     string // `-- version:` front-matter (the ARTIFACT's version), "" if undeclared
	Description string
	Params      []Param
	Template    string               // the SQL body with $(name) placeholders
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
		params, serr := scanParams(string(src))
		if serr != nil {
			return nil, fmt.Errorf("builtin %s: %v", fn, serr)
		}
		out = append(out, Query{
			Name: fn, Version: e.Version, Description: e.Description,
			Params: params, Template: e.Stmt, Entry: e,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// scanParams collects every `-- param: <name> <type> [= <default>]` front-matter
// line. Scanned from the raw source (not the front-matter map) because `param` is
// repeatable and a map keeps only the last.
func scanParams(src string) ([]Param, error) {
	var out []Param
	for _, ln := range strings.Split(src, "\n") {
		ln = strings.TrimSpace(ln)
		if !strings.HasPrefix(ln, "--") {
			break // front-matter over: params must precede the SQL body
		}
		body := strings.TrimSpace(strings.TrimPrefix(ln, "--"))
		val, ok := strings.CutPrefix(body, "param:")
		if !ok {
			continue
		}
		spec, dflt, hasDflt := strings.Cut(val, "=")
		fields := strings.Fields(spec)
		if len(fields) != 2 {
			return nil, fmt.Errorf("bad param line %q (want: param: <name> <type> [= <default>])", ln)
		}
		typ := fields[1]
		if typ != "ident" && typ != "int" && typ != "list" {
			return nil, fmt.Errorf("param %q: unknown type %q (want ident|int|list)", fields[0], typ)
		}
		out = append(out, Param{
			Name: fields[0], Type: typ,
			Default: strings.TrimSpace(dflt), Required: !hasDflt,
		})
	}
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
	out := map[string]string{}
	for _, p := range q.Params {
		out[p.Name] = p.Default
	}
	for k, v := range given {
		if _, declared := out[k]; !declared {
			return nil, fmt.Errorf("builtin %s has no param %q (declared: %s)", q.Name, k, q.paramNames())
		}
		out[k] = v
	}
	for _, p := range q.Params {
		if p.Required && out[p.Name] == "" {
			return nil, fmt.Errorf("builtin %s needs %s=..., e.g. builtin:%s?%s=<%s>",
				q.Name, p.Name, q.Name, p.Name, p.Name)
		}
	}
	return out, nil
}

var placeholderRE = regexp.MustCompile(`\$\(([A-Za-z0-9_-]+)\)`)

// Render substitutes the resolved params into the template, each value validated
// and rendered per its declared type (ident -> `backticked`, int -> bare digits,
// list -> a JSON string array). A leftover placeholder (template names a param it
// never declared) is an error — an embedded-file defect, never silent.
func (q Query) Render(resolved map[string]string) (string, error) {
	byName := map[string]Param{}
	for _, p := range q.Params {
		byName[p.Name] = p
	}
	var rerr error
	sql := placeholderRE.ReplaceAllStringFunc(q.Template, func(m string) string {
		name := placeholderRE.FindStringSubmatch(m)[1]
		p, ok := byName[name]
		if !ok {
			rerr = fmt.Errorf("builtin %s: template placeholder $(%s) has no declared param", q.Name, name)
			return m
		}
		v, err := renderValue(p, resolved[p.Name])
		if err != nil {
			rerr = err
			return m
		}
		return v
	})
	return sql, rerr
}

func renderValue(p Param, val string) (string, error) {
	switch p.Type {
	case "ident":
		if val == "" || strings.ContainsAny(val, "`\n") {
			return "", fmt.Errorf("param %s: %q is not a usable identifier", p.Name, val)
		}
		return "`" + val + "`", nil
	case "int":
		n, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			return "", fmt.Errorf("param %s: %q is not an integer", p.Name, val)
		}
		return strconv.Itoa(n), nil
	case "list":
		var quoted []string
		for _, e := range strings.Split(val, ",") {
			if e = strings.TrimSpace(e); e != "" {
				quoted = append(quoted, strconv.Quote(e))
			}
		}
		return "[" + strings.Join(quoted, ", ") + "]", nil
	}
	return "", fmt.Errorf("param %s: unknown type %q", p.Name, p.Type)
}

func (q Query) paramNames() string {
	names := make([]string, len(q.Params))
	for i, p := range q.Params {
		names[i] = p.Name
	}
	return strings.Join(names, ", ")
}
