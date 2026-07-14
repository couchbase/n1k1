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

import (
	"fmt"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// cmdMacro dispatches ".macro [list | help [<name>] | show <name> | expand <statement>]":
// pre-parse SQL++ macros. Some macros ship built-in (embedded in the binary, registered
// at startup); more load from "*.macro.js" files via -ext / .extensions. No argument (or
// "list") lists all loaded macros.
func (c *cli) cmdMacro(arg string) {
	arg = strings.TrimSpace(arg)
	sub := arg
	rest := ""
	if i := strings.IndexAny(arg, " \t"); i >= 0 {
		sub, rest = arg[:i], strings.TrimSpace(arg[i+1:])
	}
	switch strings.ToLower(sub) {
	case "", "list":
		c.macroList()
	case "help":
		c.macroHelp(rest)
	case "show", "source", "cat":
		c.macroShow(rest)
	case "expand":
		c.macroExpand(rest)
	default:
		fmt.Fprintln(c.stderr, "usage: .macro [list | help [<name>] | show <name> | expand <statement>]")
	}
}

func (c *cli) macroList() {
	macros := glue.ListMacros()
	if len(macros) == 0 {
		fmt.Fprintln(c.stderr, "no macros loaded (load a *.macro.js via -ext or .extensions)")
		return
	}
	fmt.Fprintf(c.stderr, "%d loaded macro(s):  (.macro show <name> for the source)\n", len(macros))
	for _, m := range macros {
		tag := ""
		if builtinMacroNames[m.Name] {
			tag = c.style.Dim("  [built-in]")
		}
		fmt.Fprintf(c.stderr, "  @%-24s %s%s\n", m.Name+"("+macroParamList(m.Params)+")", macroBlurb(m.Source), tag)
	}
}

// macroInfoHeader prints the "@name(params) [built-in]" line, the one-line blurb, and
// each parameter's required/optional + default -- shared by help and show.
func (c *cli) macroInfoHeader(m glue.MacroInfo) {
	tag := ""
	if builtinMacroNames[m.Name] {
		tag = "  [built-in]"
	}
	fmt.Fprintf(c.stderr, "@%s(%s)%s\n", m.Name, macroParamList(m.Params), tag)
	if b := macroBlurb(m.Source); b != "" {
		fmt.Fprintf(c.stderr, "  %s\n", b)
	}
	for _, p := range m.Params {
		req := "optional"
		if p.Required {
			req = "required"
		}
		def := ""
		if p.Default != nil {
			def = fmt.Sprintf(", default %v", p.Default)
		}
		fmt.Fprintf(c.stderr, "  %-16s %s%s\n", p.Name, req, def)
	}
}

func (c *cli) macroHelp(name string) {
	if name == "" {
		fmt.Fprintln(c.stderr, strings.TrimSpace(macroHelpText))
		return
	}
	name = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(name)), "@")
	for _, m := range glue.ListMacros() {
		if m.Name == name {
			c.macroInfoHeader(m)
			fmt.Fprintln(c.stderr, c.style.Dim("  (.macro show "+name+" for the full source)"))
			return
		}
	}
	fmt.Fprintf(c.stderr, "no such macro @%s -- try .macro list\n", name)
}

// macroShow prints a macro's header/params plus its full source code -- so a user can
// read exactly what @name(...) does and adapt it. The source goes to stdout (pipeable);
// the header to stderr.
func (c *cli) macroShow(name string) {
	if name == "" {
		fmt.Fprintln(c.stderr, "usage: .macro show <name>")
		return
	}
	name = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(name)), "@")
	for _, m := range glue.ListMacros() {
		if m.Name == name {
			c.macroInfoHeader(m)
			fmt.Fprintln(c.stderr, "\nsource:")
			fmt.Fprintln(c.out, m.Source)
			return
		}
	}
	fmt.Fprintf(c.stderr, "no such macro @%s -- try .macro list\n", name)
}

// macroBlurb extracts a macro's one-line description: the first line of its source
// comment after the "<name>.macro.js — " prefix. "" if the source has no leading comment.
func macroBlurb(source string) string {
	for _, ln := range strings.Split(source, "\n") {
		t := strings.TrimSpace(ln)
		if t == "" {
			continue
		}
		if !strings.HasPrefix(t, "//") {
			break // first real code line -> no doc comment
		}
		t = strings.TrimSpace(strings.TrimPrefix(t, "//"))
		for _, sep := range []string{" — ", " -- ", " - "} {
			if i := strings.Index(t, sep); i >= 0 && strings.Contains(t[:i], ".macro.js") {
				t = strings.TrimSpace(t[i+len(sep):])
				break
			}
		}
		return truncateRunes(t, 66)
	}
	return ""
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

func (c *cli) macroExpand(stmt string) {
	if stmt == "" {
		fmt.Fprintln(c.stderr, "usage: .macro expand <statement>")
		return
	}
	out, err := glue.ExpandMacros(stmt)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .macro expand: %v\n", c.prog, err)
		return
	}
	// Pretty-print the expansion: a macro produces a gensym-heavy nested-subquery wall
	// on one line -- PrettySQL re-lays it out (whitespace only, same statement) so the
	// generated shape is readable (IDEA-0037 "cheap first slice").
	fmt.Fprintln(c.out, glue.PrettySQL(out))
}

// macroParamList renders a macro's params as a compact "src, when, before=2" hint.
func macroParamList(params []glue.MacroParam) string {
	if len(params) == 0 {
		return "..."
	}
	parts := make([]string, len(params))
	for i, p := range params {
		parts[i] = p.Name
		if p.Default != nil {
			parts[i] = fmt.Sprintf("%s=%v", p.Name, p.Default)
		}
	}
	return strings.Join(parts, ", ")
}

const macroHelpText = `
.macro -- pre-parse and transform SQL++

A macro turns a compact @name(...) call into generated SQL++, expanded BEFORE the
SQL++ parser. Some macros ship built-in with n1k1 (e.g. @vectorize_field); more load
from your own "*.macro.js" files via -ext / .extensions.

  .macro                       list loaded macros (built-in + your own)
  .macro list                  same
  .macro help [<name>]         this help, or one macro's parameters
  .macro show <name>           print a macro's full source code (read/adapt it)
  .macro expand <statement>    print the fully-expanded SQL++ (debugging)

Invocation syntax (in any SQL++ statement):

  SELECT ... FROM @grep_context(logs, when => sev = "ERROR", before => 2, after => 2);

  - positional args, then named args written  name => value  (the => sigil, so a
    predicate value like  sev = "ERROR"  is never mistaken for a named arg).
  - macros compose: a macro may be an argument to another (@outer(@inner(...))),
    and a macro's output may itself contain @calls.

Authoring: see  .extract help  for the sibling *.extract.js surface;
a *.macro.js defines  expand(args, ctx) -> SQL++ string  (optional module-scope
"macro" object with "params"). Use ctx.gensym("x") to generate unique symbols so
that expansions never collide.

Example -- the whole of a "recent.macro.js" (keeps rows at/after a time):

  // @recent(logs, since => 1700000000)  ->  rows whose ts >= since
  var macro = {
    name: "recent",
    params: [ { name: "src",   required: true },   // keyspace / subquery
              { name: "ts",    default: "ts" },     // the time column
              { name: "since", required: true } ]   // lower bound (raw SQL++)
  };
  function expand(args, ctx) {
    var s = ctx.gensym("r");                        // unique alias (hygiene)
    return "(SELECT " + s + ".* FROM " + args.src + " AS " + s +
           " WHERE " + s + "." + args.ts + " >= (" + args.since + "))";
  }

  # load it and use it (the paren-wrapped expansion is a subquery -> alias it):
  .ext ./recent.macro.js
  SELECT r.* FROM @recent(logs, since => 1700000000) AS r;

  # see exactly what it expands to:
  .macro expand SELECT r.* FROM @recent(logs, since => 1700000000) AS r`
