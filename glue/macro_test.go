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

import (
	"strings"
	"testing"
)

// regMacro registers a Go-closure macro for testing (no goja): expand returns
// the given render(args, ctx).
func regMacro(name string, params []MacroParam, render func(*MacroArgs, *MacroCtx) (string, error)) {
	RegisterMacro(name, params, render, "(test)", "")
}

func TestExpandMacrosIdentityWhenNoneLoaded(t *testing.T) {
	resetMacroRegistry()
	// With no macros loaded, even text containing `@` is returned verbatim.
	for _, s := range []string{
		"SELECT 1",
		`SELECT "@box(1)" AS x`,
		"SELECT * FROM @box(1)",
	} {
		if got, err := ExpandMacros(s); err != nil || got != s {
			t.Errorf("ExpandMacros(%q) = %q, %v; want unchanged", s, got, err)
		}
	}
}

func TestExpandMacrosPositionalAndNamed(t *testing.T) {
	resetMacroRegistry()
	regMacro("w", []MacroParam{
		{Name: "src", Required: true},
		{Name: "when", Required: true},
		{Name: "before", Default: float64(2)},
	}, func(a *MacroArgs, c *MacroCtx) (string, error) {
		r, err := a.Resolve([]MacroParam{
			{Name: "src", Required: true},
			{Name: "when", Required: true},
			{Name: "before", Default: float64(2)},
		})
		if err != nil {
			return "", err
		}
		return "src=" + r["src"] + "|when=" + r["when"] + "|before=" + r["before"], nil
	})

	// Positional src, positional predicate (no => -> positional, `=` inside is fine),
	// named before. Default after is untouched (not declared here).
	got, err := ExpandMacros(`FROM @w(logs, sev = "ERROR", before => 5)`)
	if err != nil {
		t.Fatal(err)
	}
	want := `FROM (src=logs|when=sev = "ERROR"|before=5)`
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}

	// Default fills an absent named arg.
	got, err = ExpandMacros(`@w(logs, sev = 1)`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "before=2") {
		t.Errorf("default not applied: %q", got)
	}
}

func TestExpandMacrosNestingApplicativeOrder(t *testing.T) {
	resetMacroRegistry()
	// inner() -> INNER ; wrap(x) -> [x]. wrap sees inner's EXPANDED text (applicative).
	regMacro("inner", nil, func(a *MacroArgs, c *MacroCtx) (string, error) { return "INNER", nil })
	regMacro("wrap", nil, func(a *MacroArgs, c *MacroCtx) (string, error) {
		return "[" + a.Positional[0] + "]", nil
	})
	got, err := ExpandMacros("@wrap(@inner())")
	if err != nil {
		t.Fatal(err)
	}
	// inner -> (INNER); wrap's arg is "(INNER)" -> "[(INNER)]"; wrapped -> "([(INNER)])".
	if got != "([(INNER)])" {
		t.Errorf("applicative-order nesting: got %q", got)
	}
}

func TestExpandMacrosBodyReemission(t *testing.T) {
	resetMacroRegistry()
	regMacro("inner", nil, func(a *MacroArgs, c *MacroCtx) (string, error) { return "INNER", nil })
	regMacro("emit", nil, func(a *MacroArgs, c *MacroCtx) (string, error) { return "@inner()", nil })
	got, err := ExpandMacros("@emit()")
	if err != nil {
		t.Fatal(err)
	}
	// emit -> (@inner()); re-scan -> ((INNER)).
	if got != "((INNER))" {
		t.Errorf("body re-emission: got %q", got)
	}
}

func TestExpandMacrosHygieneGensym(t *testing.T) {
	resetMacroRegistry()
	regMacro("g", nil, func(a *MacroArgs, c *MacroCtx) (string, error) {
		return c.Gensym("t"), nil
	})
	got, err := ExpandMacros("@g() @g()")
	if err != nil {
		t.Fatal(err)
	}
	// Two uses in one pass draw disjoint names.
	if got != "(t__m1) (t__m2)" {
		t.Errorf("gensym collision: got %q", got)
	}
}

func TestExpandMacrosStringAndCommentSafety(t *testing.T) {
	resetMacroRegistry()
	regMacro("box", nil, func(a *MacroArgs, c *MacroCtx) (string, error) { return "X", nil })
	for _, s := range []string{
		`SELECT '@box()' AS a`,           // single-quoted string
		`SELECT "@box()" AS a`,           // double-quoted string
		"SELECT 1 -- @box()\nFROM t",     // line comment
		"SELECT 1 /* @box() */ FROM t",   // block comment
		"SELECT `@box()` AS a",           // backtick identifier
	} {
		if got, err := ExpandMacros(s); err != nil || got != s {
			t.Errorf("ExpandMacros(%q) = %q, %v; want unchanged (macro inside string/comment)", s, got, err)
		}
	}
	// But a real call adjacent to a string IS expanded, and the string is preserved.
	got, err := ExpandMacros(`@box() WHERE x = '@box()'`)
	if err != nil {
		t.Fatal(err)
	}
	if got != `(X) WHERE x = '@box()'` {
		t.Errorf("got %q", got)
	}
}

func TestExpandMacrosUnknownMacro(t *testing.T) {
	resetMacroRegistry()
	regMacro("known", nil, func(a *MacroArgs, c *MacroCtx) (string, error) { return "K", nil })
	_, err := ExpandMacros("@nope()")
	if err == nil || !strings.Contains(err.Error(), "unknown macro @nope") {
		t.Errorf("want unknown-macro error, got %v", err)
	}
}

func TestExpandMacrosRecursionCapped(t *testing.T) {
	resetMacroRegistry()
	regMacro("loop", nil, func(a *MacroArgs, c *MacroCtx) (string, error) { return "@loop()", nil })
	_, err := ExpandMacros("@loop()")
	if err == nil || !strings.Contains(err.Error(), "did not terminate") {
		t.Errorf("want non-termination error, got %v", err)
	}
}

func TestExpandMacrosArityErrors(t *testing.T) {
	resetMacroRegistry()
	params := []MacroParam{{Name: "a", Required: true}, {Name: "b"}}
	regMacro("m", params, func(ar *MacroArgs, c *MacroCtx) (string, error) {
		_, err := ar.Resolve(params)
		return "ok", err
	})
	cases := map[string]string{
		"@m()":                "missing required argument",
		"@m(1, 2, 3)":         "too many positional",
		"@m(1, c => 2)":       "unknown named argument",
		"@m(1, a => 2)":       "given both positionally and by name",
		"@m(a => 1, 2)":       "positional argument \"2\" after a named",
	}
	for call, want := range cases {
		if _, err := ExpandMacros(call); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("ExpandMacros(%q): want error containing %q, got %v", call, want, err)
		}
	}
}

func TestCoerceLit(t *testing.T) {
	cases := []struct {
		in   string
		want interface{}
	}{
		{"2", int64(2)},
		{"3.5", 3.5},
		{"true", true},
		{"false", false},
		{"null", nil},
		{`"ERROR"`, "ERROR"},
		{`'x'`, "x"},
		{`sev = "ERROR"`, `sev = "ERROR"`}, // not a bare literal -> raw text
	}
	for _, c := range cases {
		if got := coerceLit(c.in); got != c.want {
			t.Errorf("coerceLit(%q) = %#v, want %#v", c.in, got, c.want)
		}
	}
}

func TestJSMacroGrepContext(t *testing.T) {
	resetMacroRegistry()
	src := `
		var macro = { name: "grep_context",
		  params: [ {name:"src",required:true}, {name:"when",required:true},
		            {name:"before",default:2}, {name:"after",default:2} ] };
		function expand(args, ctx) {
		  if (typeof args.$lit.before !== "number") ctx.error("before must be numeric");
		  var sub = ctx.gensym("ctx"), near = ctx.gensym("near");
		  return "SELECT * FROM (SELECT line, "
		    + "MAX(CASE WHEN (" + args.when + ") THEN 1 ELSE 0 END) "
		    + "OVER (ORDER BY pos ROWS BETWEEN " + args.before + " PRECEDING AND "
		    + args.after + " FOLLOWING) AS " + near
		    + " FROM " + args.src + ") " + sub + " WHERE " + sub + "." + near + " = 1";
		}`
	if err := RegisterJSMacro("grep_context", src); err != nil {
		t.Fatal(err)
	}
	out, err := ExpandMacros(`SELECT * FROM @grep_context(logs, when => sev = "ERROR", before => 3)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"3 PRECEDING", "2 FOLLOWING", // named + default coerced as raw
		"ctx__m1", "near__m2", // hygiene gensym, pass-global counter
		`sev = "ERROR"`,       // predicate spliced verbatim
		"FROM logs",           // identifier arg spliced verbatim
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expansion missing %q in:\n%s", want, out)
		}
	}

	// ctx.error surfaces as a mapped macro error.
	if _, err := ExpandMacros(`@grep_context(logs, when => x, before => "oops")`); err == nil ||
		!strings.Contains(err.Error(), "before must be numeric") {
		t.Errorf("ctx.error not mapped: %v", err)
	}
}

func TestListMacros(t *testing.T) {
	resetMacroRegistry()
	regMacro("aaa", []MacroParam{{Name: "x"}}, func(a *MacroArgs, c *MacroCtx) (string, error) { return "", nil })
	regMacro("bbb", nil, func(a *MacroArgs, c *MacroCtx) (string, error) { return "", nil })
	got := ListMacros()
	if len(got) != 2 || got[0].Name != "aaa" || got[1].Name != "bbb" {
		t.Fatalf("ListMacros load-order = %+v", got)
	}
}
