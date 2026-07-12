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

// macro.go is n1k1's pre-parse SQL++ MACRO expander (DESIGN-extensions.md
// "Macros"). A macro is user-authored (JS: ext_macro_jsvm.go) source-to-source
// sugar: an `@name(args)` invocation in a statement is replaced with SQL++ text
// BEFORE cbq's parser runs (glue.ParseStatement calls ExpandMacros at the top,
// right before n1ql.ParseStatement2). After expansion the statement is ordinary
// SQL++, so the planner / CSE / MQO / ASOF lowering / analyzer codegen see only
// hand-written-shaped SQL and never know a macro produced it -- macros add zero
// downstream complexity.
//
// The value it buys: WINDOW-heavy shapes (grep -A/-B/-C context, top-per-group,
// sessionize) that are painful to hand-write become one-liners. A JS scalar/table
// UDF can't help -- a WINDOW clause is *syntax*, not a value or a table -- so a
// text-level generator is the right (and only) layer.
//
// This file owns the SCANNER + REGISTRY + argument semantics (pure Go, no goja);
// ext_macro_jsvm.go owns the JS `expand(args, ctx)` binding. The split mirrors
// extract recipes (macro.go : ext_macro_jsvm.go :: records recipe : ext_extract_jsvm.go).

import (
	"fmt"
	"strconv"
	"strings"
)

// maxMacroExpansions caps the total number of `@name(...)` substitutions in one
// statement. Argument-nesting strictly shrinks the `@` count so it can't loop;
// only a macro whose BODY re-emits a macro can grow it, so this bound turns a
// recursive/runaway macro into a clean error instead of a hang. Far above any
// real statement's macro count.
const maxMacroExpansions = 1000

// MacroParam is one declared parameter of a macro (its optional `macro.params`
// signature): enables positional->named mapping, defaults, arity/keyword checks,
// and `.macro help`. JSON tags match the JS field names.
type MacroParam struct {
	Name     string      `json:"name"`
	Required bool        `json:"required"`
	Default  interface{} `json:"default"`
}

// MacroArgs is the raw, parsed argument list of one `@name(...)` invocation.
// Values are the RAW SQL++ source substrings of each argument (a macro
// manipulates syntax): a `src` arg arrives as the identifier text `logs`, a
// `when` arg as the unparsed predicate `sev = "ERROR"`, spliced verbatim.
type MacroArgs struct {
	Positional []string          // in call order, before any named arg
	Named      map[string]string // key => raw source text
}

// MacroCtx is the per-EXPANSION-PASS context handed to every expand() call in one
// ExpandMacros run. Gensym's counter is shared across the whole pass so inner,
// outer, and body-introduced expansions all draw disjoint names -- no alias
// collision however macros nest.
type MacroCtx struct {
	gensymN *int
}

// Gensym returns a fresh, collision-free SQL identifier derived from prefix,
// unique within the expansion pass (prefix__mN). This is n1k1's macro hygiene
// primitive: a macro introduces every internal alias/CTE/subquery name via
// Gensym so two uses (or nesting) of the same macro never clash.
func (c *MacroCtx) Gensym(prefix string) string {
	*c.gensymN++
	return sanitizeIdent(prefix) + "__m" + strconv.Itoa(*c.gensymN)
}

// macroEntry is one registered macro.
type macroEntry struct {
	name   string // registry key == invocation name, lowercased
	params []MacroParam
	expand func(*MacroArgs, *MacroCtx) (string, error)
	source string // originating file path, or "(inline)"
	hash   string // short source-hash (for listing / future memoization)
}

// macroRegistry / macroOrder hold the loaded macros. Mutated only by
// RegisterMacro (startup / between queries, never concurrently with parsing), so
// no lock -- matching ext_jsvm.go / ext_extract_jsvm.go. ExpandMacros only READS
// the registry, and concurrent reads (parallel parses) are safe.
var macroRegistry = map[string]*macroEntry{}
var macroOrder []string

// MacroInfo describes one loaded macro (for `.macro list`/`help`).
type MacroInfo struct {
	Name   string
	Source string
	Params []MacroParam
}

// RegisterMacro registers (or replaces) a macro under name. expand takes the
// parsed args + pass context and returns SQL++ text. Called by the JS loader
// (ext_macro_jsvm.go) and usable directly for native/test macros.
func RegisterMacro(name string, params []MacroParam,
	expand func(*MacroArgs, *MacroCtx) (string, error), source, hash string) {
	lname := strings.ToLower(name)
	if _, exists := macroRegistry[lname]; !exists {
		macroOrder = append(macroOrder, lname)
	}
	macroRegistry[lname] = &macroEntry{
		name: lname, params: params, expand: expand, source: source, hash: hash,
	}
}

// ListMacros returns the loaded macros in load order.
func ListMacros() []MacroInfo {
	out := make([]MacroInfo, 0, len(macroOrder))
	for _, n := range macroOrder {
		if e := macroRegistry[n]; e != nil {
			out = append(out, MacroInfo{Name: e.name, Source: e.source, Params: e.params})
		}
	}
	return out
}

// macroNames returns the loaded macro names (for error messages).
func macroNames() []string {
	out := make([]string, 0, len(macroOrder))
	for _, n := range macroOrder {
		out = append(out, "@"+n)
	}
	return out
}

// resetMacroRegistry clears all registered macros. Test-only.
func resetMacroRegistry() {
	macroRegistry = map[string]*macroEntry{}
	macroOrder = nil
}

// ExpandMacros expands every `@name(...)` macro invocation in stmt to SQL++ text,
// leftmost-innermost first (so a macro used as an argument to another expands
// before the enclosing macro's expand() runs -- applicative order), re-scanning
// each macro's output so macros can build on macros. Returns stmt unchanged when
// no macros are loaded or the statement contains no `@` (the common path is a
// single byte scan).
func ExpandMacros(stmt string) (string, error) {
	if len(macroRegistry) == 0 || strings.IndexByte(stmt, '@') < 0 {
		return stmt, nil
	}
	gensymN := 0
	ctx := &MacroCtx{gensymN: &gensymN}
	s := stmt
	for n := 0; ; n++ {
		call, ok, err := findMacro(s, 0)
		if err != nil {
			return "", fmt.Errorf("macro expansion: %w", err)
		}
		if !ok {
			return s, nil
		}
		if n >= maxMacroExpansions {
			return "", fmt.Errorf("macro expansion did not terminate after %d expansions "+
				"(recursive macro @%s?)", maxMacroExpansions, call.name)
		}
		entry := macroRegistry[strings.ToLower(call.name)]
		if entry == nil {
			return "", fmt.Errorf("unknown macro @%s (loaded: %s)",
				call.name, strings.Join(macroNames(), ", "))
		}
		pos, named, err := splitMacroArgs(s[call.argFrom:call.argTo])
		if err != nil {
			return "", fmt.Errorf("macro @%s: %w", call.name, err)
		}
		repl, err := entry.expand(&MacroArgs{Positional: pos, Named: named}, ctx)
		if err != nil {
			return "", fmt.Errorf("macro @%s: %w", call.name, err)
		}
		// Wrap in parens: safe in both expression and FROM-subquery position.
		s = s[:call.start] + "(" + repl + ")" + s[call.argTo+1:]
	}
}

// ------------------------------------------------------------------
// Scanner: string/comment-aware, so a paren/comma/@ inside a SQL string or
// comment is never miscounted.

type macroCall struct {
	start   int    // index of '@'
	name    string // identifier after '@'
	argFrom int    // index just after '('
	argTo   int    // index of the matching ')'
}

// findMacro returns the leftmost-innermost `@name(...)` at/after from. A call
// whose argument list contains another macro call yields that inner call first
// (so arguments expand before their enclosing macro). A bare `@` not forming a
// `@ident(` call is passed through untouched.
func findMacro(s string, from int) (macroCall, bool, error) {
	i := from
	for i < len(s) {
		if j, ok := skipStringOrComment(s, i); ok {
			i = j
			continue
		}
		if s[i] == '@' && i+1 < len(s) && isIdentStart(s[i+1]) {
			k := i + 1
			for k < len(s) && isIdentPart(s[k]) {
				k++
			}
			name := s[i+1 : k]
			p := k
			for p < len(s) && isSpace(s[p]) {
				p++
			}
			if p < len(s) && s[p] == '(' {
				closeIdx, err := matchParen(s, p)
				if err != nil {
					return macroCall{}, false, err
				}
				// Descend: a macro call inside our own arg list is more-inner.
				if inner, ok, err := findMacro(s, p+1); err != nil {
					return macroCall{}, false, err
				} else if ok && inner.start < closeIdx {
					return inner, true, nil
				}
				return macroCall{start: i, name: name, argFrom: p + 1, argTo: closeIdx}, true, nil
			}
			i = k // `@name` not a call -> pass through
			continue
		}
		i++
	}
	return macroCall{}, false, nil
}

// matchParen returns the index of the ')' matching the '(' at open, skipping
// parens inside strings/comments.
func matchParen(s string, open int) (int, error) {
	depth := 0
	for i := open; i < len(s); {
		if j, ok := skipStringOrComment(s, i); ok {
			i = j
			continue
		}
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
		i++
	}
	return -1, fmt.Errorf("unbalanced parentheses in macro arguments")
}

// splitMacroArgs splits a macro's argument text into positional args (in order)
// and named args (`name => value`), at top-level commas only. Named args must
// follow all positional args. Values keep their raw source text.
func splitMacroArgs(s string) (positional []string, named map[string]string, err error) {
	named = map[string]string{}
	pieces := splitTopLevel(s, ',')
	if len(pieces) == 1 && strings.TrimSpace(pieces[0]) == "" {
		return nil, named, nil // @name() -- zero args
	}
	sawNamed := false
	for _, pc := range pieces {
		if name, val, isNamed := splitNamed(pc); isNamed {
			name = strings.TrimSpace(name)
			if _, dup := named[name]; dup {
				return nil, nil, fmt.Errorf("duplicate named argument %q", name)
			}
			named[name] = strings.TrimSpace(val)
			sawNamed = true
		} else {
			if sawNamed {
				return nil, nil, fmt.Errorf("positional argument %q after a named argument",
					strings.TrimSpace(pc))
			}
			positional = append(positional, strings.TrimSpace(pc))
		}
	}
	return positional, named, nil
}

// splitTopLevel splits s at sep occurrences that are at paren/bracket/brace
// depth 0 and outside strings/comments.
func splitTopLevel(s string, sep byte) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); {
		if j, ok := skipStringOrComment(s, i); ok {
			i = j
			continue
		}
		switch s[i] {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case sep:
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
		i++
	}
	return append(out, s[start:])
}

// splitNamed detects a `name => value` argument: a bare identifier, then a
// top-level `=>`, then the value. The `=>` sigil (not `=` or `:`) is chosen so a
// predicate-valued arg (`sev = "ERROR"`, a keyspace path `ns:bucket`) is never
// mistaken for a named arg.
func splitNamed(pc string) (name, val string, ok bool) {
	depth := 0
	for i := 0; i+1 < len(pc); {
		if j, ok := skipStringOrComment(pc, i); ok {
			i = j
			continue
		}
		switch pc[i] {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case '=':
			if depth == 0 && pc[i+1] == '>' {
				left := strings.TrimSpace(pc[:i])
				if isBareIdent(left) {
					return left, pc[i+2:], true
				}
				return "", "", false
			}
		}
		i++
	}
	return "", "", false
}

// skipStringOrComment: if s[i] begins a SQL string ('...', "...", `...`) or a
// comment (-- to EOL, /* */), returns the index just past it and true. An
// unterminated string/comment consumes to end-of-input (cbq's parser then
// reports the real syntax error).
func skipStringOrComment(s string, i int) (int, bool) {
	if i >= len(s) {
		return i, false
	}
	switch s[i] {
	case '\'', '"':
		q := s[i]
		for j := i + 1; j < len(s); j++ {
			if s[j] == '\\' {
				j++ // skip escaped char
				continue
			}
			if s[j] == q {
				return j + 1, true
			}
		}
		return len(s), true
	case '`':
		for j := i + 1; j < len(s); j++ {
			if s[j] == '`' {
				if j+1 < len(s) && s[j+1] == '`' { // doubled backtick escape
					j++
					continue
				}
				return j + 1, true
			}
		}
		return len(s), true
	case '-':
		if i+1 < len(s) && s[i+1] == '-' {
			j := i + 2
			for j < len(s) && s[j] != '\n' {
				j++
			}
			return j, true
		}
	case '/':
		if i+1 < len(s) && s[i+1] == '*' {
			for j := i + 2; j+1 < len(s); j++ {
				if s[j] == '*' && s[j+1] == '/' {
					return j + 2, true
				}
			}
			return len(s), true
		}
	}
	return i, false
}

// ------------------------------------------------------------------
// Argument resolution + literal coercion (shared with the JS binding).

// Resolve maps this call's positional args onto params by position, overlays the
// named args, and fills declared defaults -- erroring on arity overflow, an
// unknown named arg, a doubly-supplied arg, or a missing required arg. With no
// declared params it just returns the named args (positional stay index-only).
func (a *MacroArgs) Resolve(params []MacroParam) (map[string]string, error) {
	out := make(map[string]string, len(params))
	if len(params) == 0 {
		for k, v := range a.Named {
			out[k] = v
		}
		return out, nil
	}
	if len(a.Positional) > len(params) {
		return nil, fmt.Errorf("too many positional arguments: got %d, macro takes %d",
			len(a.Positional), len(params))
	}
	seen := map[string]bool{}
	for i, v := range a.Positional {
		out[params[i].Name] = v
		seen[params[i].Name] = true
	}
	for k, v := range a.Named {
		known := false
		for _, p := range params {
			if p.Name == k {
				known = true
				break
			}
		}
		if !known {
			return nil, fmt.Errorf("unknown named argument %q", k)
		}
		if seen[k] {
			return nil, fmt.Errorf("argument %q given both positionally and by name", k)
		}
		out[k] = v
		seen[k] = true
	}
	for _, p := range params {
		if seen[p.Name] {
			continue
		}
		if p.Default != nil {
			out[p.Name] = defaultRaw(p.Default)
			seen[p.Name] = true
			continue
		}
		if p.Required {
			return nil, fmt.Errorf("missing required argument %q", p.Name)
		}
	}
	return out, nil
}

// defaultRaw renders a JSON-decoded default value as the raw SQL++ text spliced
// for an absent argument (numbers/bools as literals; strings verbatim).
func defaultRaw(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'g', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

// coerceLit best-effort coerces a raw argument's source text to a JS-friendly
// literal for args.$lit: an int/float, a bool, null, or an unquoted string when
// the arg is a single quoted literal; otherwise the raw text unchanged.
func coerceLit(raw string) interface{} {
	s := strings.TrimSpace(raw)
	if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') && s[len(s)-1] == s[0] {
		if inner, err := unquoteSQL(s); err == nil {
			return inner
		}
	}
	switch strings.ToLower(s) {
	case "true":
		return true
	case "false":
		return false
	case "null", "missing":
		return nil
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return raw
}

// unquoteSQL strips one layer of SQL string quotes, honoring backslash escapes,
// for a string that is exactly one quoted literal.
func unquoteSQL(s string) (string, error) {
	q := s[0]
	var b strings.Builder
	for i := 1; i < len(s)-1; i++ {
		if s[i] == '\\' && i+1 < len(s)-1 {
			i++
			b.WriteByte(s[i])
			continue
		}
		if s[i] == q {
			return "", fmt.Errorf("not a single quoted literal")
		}
		b.WriteByte(s[i])
	}
	return b.String(), nil
}

// ------------------------------------------------------------------

func isIdentStart(b byte) bool {
	return b == '_' || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func isIdentPart(b byte) bool {
	return isIdentStart(b) || (b >= '0' && b <= '9')
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func isBareIdent(s string) bool {
	if s == "" || !isIdentStart(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		if !isIdentPart(s[i]) {
			return false
		}
	}
	return true
}

// sanitizeIdent keeps only identifier-legal chars from prefix (for Gensym),
// falling back to "g" if nothing survives.
func sanitizeIdent(prefix string) string {
	var b strings.Builder
	for i := 0; i < len(prefix); i++ {
		if (i == 0 && isIdentStart(prefix[i])) || (i > 0 && isIdentPart(prefix[i])) {
			b.WriteByte(prefix[i])
		}
	}
	if b.Len() == 0 {
		return "g"
	}
	return b.String()
}
