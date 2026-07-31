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

// Query parameters (DESIGN-cep.md "Parameterized packs"): a *.sql++ entry declares
// typed parameters in front-matter and references them with SQL++'s own
// named-parameter syntax — ONE grammar:
//
//	-- param: threshold int = 5
//	SELECT ... WHERE t.cost > $threshold
//
// n1k1 binds pack parameters EARLY (pre-parse constant folding) rather than at
// engine runtime, because a pack's economics are literal-driven: the predicate
// index prunes on plan-time literals and the compiled lane bakes them — a runtime
// $param gate would degrade to always-wake. The engine's runtime named args
// (Session.NamedArgs) remain the mechanism for ad-hoc single statements.
//
// HOW STANDARD IS THIS? Verified against the engine's parser, by position:
//   - VALUE position (`x > $threshold`, `k NOT IN $exclude`) — genuinely standard
//     named-parameter syntax; a stock engine parses it and binds at runtime. Our
//     int/list(/future str) params live here: a pack using only value-typed params
//     is standard-parseable SQL++.
//   - FIELD position — write the STANDARD dynamic-field form `obj.[$field]`
//     (dot-bracket; bare `obj.$field` is a stock-engine SYNTAX ERROR). It parses and
//     runs on a stock engine, and n1k1's early binding COLLAPSES it to the static
//     field path `obj.`value`` the optimizer prunes on — standard-portable file,
//     optimal rendered SQL (see RenderStmtParams). Bare `obj.$field` also still
//     renders (a pack-only convenience), but prefer the portable form.
//   - FROM position (`FROM $keyspace`) — parses on a stock engine as
//     expression-as-datasource (the bound VALUE is the data, e.g. an inline array of
//     docs), which is not the same meaning as n1k1's early binding (a keyspace named
//     by the value). Parse-portable, semantics diverge; there is no stock runtime
//     equivalent for a parameterized keyspace name.
//   - HYPHENATED names (`$type-field`) — standard lexing reads `$type - field`, so
//     declare underscores (`type_field`) for standard-portable files; caller-supplied
//     keys in either spelling reach the declared param (ParamKeyResolve).
//
// Types close the injection surface: `ident` renders `backticked` (and rejects
// backticks), `int` must parse, `list` renders as a JSON string array. Substitution
// is quote- AND comment-aware (a $ inside a string literal or a `--` comment is
// data), and a reference binds the LONGEST declared name with a non-name-character
// boundary ($type-field binds `type-field`; arithmetic needs spacing: `$depth - 1`).

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// QueryParam is one declared parameter of a *.sql++ entry (or embedded builtin).
type QueryParam struct {
	Name     string // the $name reference / --param key
	Type     string // "ident" | "int" | "list"
	Default  string // raw default value ("" is a valid default)
	Required bool   // no `= default` was declared
}

// ScanQueryParams collects every `-- param: <name> <type> [= <default>]`
// front-matter line from raw *.sql++ source. Scanned from the raw text (not the
// front-matter map) because `param` is repeatable and a map keeps only the last.
func ScanQueryParams(src string) ([]QueryParam, error) {
	var out []QueryParam
	for _, ln := range strings.Split(src, "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue // blank lines are skipped while still in front-matter
		}
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
		out = append(out, QueryParam{
			Name: fields[0], Type: typ,
			Default: strings.TrimSpace(dflt), Required: !hasDflt,
		})
	}
	return out, nil
}

// ParamsResolve merges the caller's values over one declaring unit's defaults.
// Keys in `given` that this unit doesn't declare are IGNORED here (a pack-level
// caller checks unknowns against the union of all entries — see ApplyParams; the
// builtins caller checks against its single query). A missing REQUIRED param names
// itself, prefixed by `what` (e.g. `query CC-SPEND`, `builtin census.sql++`).
func ParamsResolve(what string, params []QueryParam, given map[string]string) (map[string]string, error) {
	out := map[string]string{}
	for _, p := range params {
		out[p.Name] = p.Default
	}
	for k, v := range given {
		if name, ok := ParamKeyResolve(params, k); ok {
			out[name] = v
		}
	}
	for _, p := range params {
		if p.Required && out[p.Name] == "" {
			return nil, fmt.Errorf("%s needs param %s=... (declared: %s)", what, p.Name, paramNames(params))
		}
	}
	return out, nil
}

// ParamKeyResolve maps a caller-supplied key to a declared param name: exact match
// wins, else the hyphen/underscore-swapped spelling. URI surfaces conventionally use
// hyphens (?type-field=...) while standard-portable $ references need underscores
// ($type_field — standard lexing reads a hyphen as minus), so both spellings reach
// the same declared param.
func ParamKeyResolve(params []QueryParam, key string) (string, bool) {
	for _, p := range params {
		if p.Name == key {
			return p.Name, true
		}
	}
	for _, alt := range []string{strings.ReplaceAll(key, "-", "_"), strings.ReplaceAll(key, "_", "-")} {
		for _, p := range params {
			if p.Name == alt {
				return p.Name, true
			}
		}
	}
	return "", false
}

// paramNameCharRE matches characters that may appear in a $name reference.
var paramNameCharRE = regexp.MustCompile(`^[A-Za-z0-9_-]`)

// RenderStmtParams substitutes resolved params into the statement's `$name`
// references (SQL++ named-parameter syntax, bound early), each value validated and
// rendered per its declared type. Quote-aware (a $ inside a "…" / '…' / `…` literal
// is data) and comment-aware (a `--` line comment is copied verbatim — an
// apostrophe in prose must not open a string). Each reference binds the LONGEST
// declared name it prefixes, bounded by a non-name character. A $word matching NO
// declared param is an error — an undeclared parameter silently passed through
// would surface later as an engine named-arg the pack never supplies.
func RenderStmtParams(what, stmt string, params []QueryParam, resolved map[string]string) (string, error) {
	sorted := append([]QueryParam(nil), params...)
	sort.Slice(sorted, func(i, j int) bool { return len(sorted[i].Name) > len(sorted[j].Name) })

	var out strings.Builder
	t := stmt
	var quote byte // active string/ident delimiter, 0 when outside
	for i := 0; i < len(t); i++ {
		ch := t[i]
		switch {
		case quote != 0:
			if ch == quote {
				quote = 0
			}
		case ch == '-' && i+1 < len(t) && t[i+1] == '-':
			for i < len(t) && t[i] != '\n' {
				out.WriteByte(t[i])
				i++
			}
			if i < len(t) {
				out.WriteByte('\n')
			}
			continue
		case ch == '"' || ch == '\'' || ch == '`':
			quote = ch
		case ch == '$' && i+1 < len(t) && paramNameCharRE.MatchString(t[i+1:]):
			matched := false
			for _, p := range sorted {
				rest := t[i+1:]
				if strings.HasPrefix(rest, p.Name) &&
					(len(rest) == len(p.Name) || !paramNameCharRE.MatchString(rest[len(p.Name):])) {
					v, err := renderParamValue(p, resolved[p.Name])
					if err != nil {
						return "", fmt.Errorf("%s: %v", what, err)
					}
					// The STANDARD dynamic-field idiom `obj.[$field]` (dot-bracket, the
					// stock-parseable spelling — bare `obj.$field` is a stock syntax
					// error) collapses under early binding to the STATIC field path
					// `obj.`value`` the optimizer wants: a file can be written in 100%
					// standard SQL++ and still render to the fast form.
					cur := out.String()
					if p.Type == "ident" && strings.HasSuffix(cur, ".[") &&
						len(rest) > len(p.Name) && rest[len(p.Name)] == ']' {
						out.Reset()
						out.WriteString(cur[:len(cur)-1]) // drop the "["
						out.WriteString(v)
						i += len(p.Name) + 1 // skip the "]" too
						matched = true
						break
					}
					out.WriteString(v)
					i += len(p.Name)
					matched = true
					break
				}
			}
			if !matched {
				end := i + 1
				for end < len(t) && paramNameCharRE.MatchString(t[end:]) {
					end++
				}
				return "", fmt.Errorf("%s: $%s matches no declared param (declared: %s)",
					what, t[i+1:end], paramNames(params))
			}
			continue
		}
		out.WriteByte(ch)
	}
	return out.String(), nil
}

func renderParamValue(p QueryParam, val string) (string, error) {
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

func paramNames(params []QueryParam) string {
	if len(params) == 0 {
		return "(none)"
	}
	names := make([]string, len(params))
	for i, p := range params {
		names[i] = p.Name
	}
	return strings.Join(names, ", ")
}

// ApplyParams renders a PACK's parameterized entries (DESIGN-cep.md): each entry
// resolves the caller's --param values over its own declared defaults and has its
// Stmt rewritten in place (entries declaring no params pass through untouched). It
// happens at LOAD, before compilation AND before QueriesID hashes the statement —
// so parameters land inside a cursor's delta identity with no new hash input.
//
// Returns the pack-wide RESOLVED union (what a cursor persists as CursorState.Params
// and replays on every peek/advance): a name resolving to DIFFERENT values in two
// entries (divergent defaults, not overridden) is an error — a single stored value
// couldn't replay both. A given key that NO entry declares is a loud error (a
// typo'd --param silently ignored is how a threshold stays at its default).
func ApplyParams(dets []MultiQueryEntry, given map[string]string) ([]MultiQueryEntry, map[string]string, error) {
	var union []QueryParam
	seenName := map[string]bool{}
	for _, e := range dets {
		for _, p := range e.Params {
			if !seenName[p.Name] {
				seenName[p.Name] = true
				union = append(union, p)
			}
		}
	}
	sort.Slice(union, func(i, j int) bool { return union[i].Name < union[j].Name })
	for k := range given {
		if _, ok := ParamKeyResolve(union, k); !ok {
			return nil, nil, fmt.Errorf("no entry declares param %q (declared across the pack: %s)",
				k, paramNames(union))
		}
	}

	out := append([]MultiQueryEntry(nil), dets...)
	resolved := map[string]string{}
	for i, e := range out {
		if len(e.Params) == 0 {
			continue
		}
		what := "query " + e.Label
		res, err := ParamsResolve(what, e.Params, given)
		if err != nil {
			return nil, nil, err
		}
		rendered, err := RenderStmtParams(what, e.Stmt, e.Params, res)
		if err != nil {
			return nil, nil, err
		}
		out[i].Stmt = rendered
		for k, v := range res {
			if prev, seen := resolved[k]; seen && prev != v {
				return nil, nil, fmt.Errorf("param %q resolves to conflicting values across entries "+
					"(%q vs %q — divergent defaults); pass --param %s=<value> to pin one", k, prev, v, k)
			}
			resolved[k] = v
		}
	}
	if len(resolved) == 0 {
		resolved = nil
	}
	return out, resolved, nil
}
