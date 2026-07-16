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

// Loading query extensions into the CLI, via the repeatable -ext / -extensions
// startup flag and the .extensions dot-command (list | load | unload). The kind
// of each extension is auto-detected from its file extension (today: ".js" = a
// JavaScript scalar UDF). See DESIGN-extensions.md and extensions/functions/.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// extPathsFlag collects -ext / -extensions occurrences. It is repeatable AND
// accepts a comma-separated list per occurrence, so all of these work:
//
//	-ext a -ext b            -ext a,b            -extensions a -ext b
type extPathsFlag []string

func (e *extPathsFlag) String() string { return strings.Join(*e, ",") }

func (e *extPathsFlag) Set(v string) error {
	*e = append(*e, splitPaths(v)...)
	return nil
}

// splitPaths splits a comma- and/or whitespace-separated path list, dropping
// empties.
func splitPaths(s string) []string {
	var out []string
	for _, f := range strings.Fields(s) {
		for _, p := range strings.Split(f, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

// loadExtensions registers query extensions from a list of directories and/or
// individual files. A directory is scanned for recognized extension files
// (glue.RegisterExtensionDir); a file is registered directly
// (glue.RegisterExtensionFile), its kind auto-detected from the extension.
// Returns the registered function names (in the order encountered).
func loadExtensions(paths []string) ([]string, error) {
	var names []string
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			return names, err
		}
		if fi.IsDir() {
			ns, err := glue.RegisterExtensionDir(p)
			names = append(names, ns...)
			if err != nil {
				return names, err
			}
			continue
		}
		name, err := glue.RegisterExtensionFile(p)
		if err != nil {
			return names, err
		}
		names = append(names, name)
	}
	return names, nil
}

// cmdExtensions implements the ".extensions [help | list | show <name> | load <path...> |
// unload <name...> | examples [name] | test [name]]" dot-command (alias ".ext"). No
// argument (or "list") lists all loaded extensions (every kind, incl. built-in macros);
// "help" prints the same guide as ".help extensions".
func (c *cli) cmdExtensions(arg string) {
	fields := strings.Fields(strings.TrimSpace(arg))
	sub := ""
	if len(fields) > 0 {
		sub = strings.ToLower(fields[0])
	}
	switch sub {
	case "", "list":
		c.extList()
	case "help":
		c.helpExtensions() // `.extensions help` == `.help extensions`
	case "show", "source", "cat":
		c.extShow(strings.Join(fields[1:], " "))
	case "load":
		c.extLoad(fields[1:])
	case "unload":
		c.extUnload(fields[1:])
	case "examples", "example":
		c.extExamples(strings.Join(fields[1:], " "))
	case "test":
		c.extTest(strings.Join(fields[1:], " "))
	default:
		// Friendly shorthand: ".extensions <dir-or-file>" == "... load <dir-or-file>".
		c.extLoad(fields)
	}
}

// extEntry is one loaded extension of ANY kind (scalar UDF / aggregate / table source /
// extract recipe / macro), unified for `.extensions list` and `.extensions show`.
type extEntry struct {
	name, kind, origin string   // origin: file path, "(inline)", "(built-in)", or "(loaded)"
	code               string   // full source, or "" when not retrievable
	nExamples          int      //
	fns                []string // for a "javascript-module" row: the SQL functions it defines
}

// gatherExtensions collects EVERY loaded extension across the kinds -- scalar/aggregate/
// stream (glue.ListExtensions), extract recipes (ListExtractRecipes), and macros
// (ListMacros, incl. the built-ins) -- so all are visible in one place. Source code is
// the macro registry's stored JS for a macro, else the file at its origin path.
func (c *cli) gatherExtensions() []extEntry {
	readCode := func(origin string) string {
		if origin == "" || strings.HasPrefix(origin, "(") { // (inline)/(built-in)/(loaded): no file
			return ""
		}
		if b, err := os.ReadFile(origin); err == nil {
			return string(b)
		}
		return ""
	}
	var out []extEntry
	// Collapse a multi-export module's functions into ONE bundle row (so a loaded
	// builtin_decimal.js shows as `builtin_decimal [javascript-module]` with its
	// functions listed, not as N scattered rows). A single-function UDF stays its own row.
	type modAgg struct {
		origin string
		nEx    int
		fns    []string
	}
	mods := map[string]*modAgg{}
	var modOrder []string
	for _, e := range glue.ListExtensions() {
		if b := glue.JSModuleOf(e.Name); b != "" {
			m := mods[b]
			if m == nil {
				m = &modAgg{origin: e.Source}
				mods[b] = m
				modOrder = append(modOrder, b)
			}
			m.nEx += len(glue.ExtExamplesFor(e.Kind, e.Name))
			m.fns = append(m.fns, e.Name)
			continue
		}
		out = append(out, extEntry{name: e.Name, kind: e.Kind, origin: e.Source,
			code: readCode(e.Source), nExamples: len(glue.ExtExamplesFor(e.Kind, e.Name))})
	}
	for _, b := range modOrder {
		m := mods[b]
		sort.Strings(m.fns)
		code := readCode(m.origin) // a file-loaded module reads from disk...
		if code == "" {            // ...an inline/embedded-builtin module has its source stashed.
			if s, ok := glue.ModuleSource(b); ok {
				code = s
			}
		}
		out = append(out, extEntry{name: b, kind: "javascript-module", origin: m.origin,
			code: code, nExamples: m.nEx, fns: m.fns})
	}
	for _, r := range glue.ListExtractRecipes() {
		out = append(out, extEntry{name: r.Name, kind: "extract", origin: r.Source,
			code: readCode(r.Source), nExamples: len(glue.ExtExamplesFor("extract", r.Name))})
	}
	for _, m := range glue.ListMacros() {
		origin := "(loaded)"
		if builtinMacroNames[m.Name] {
			origin = "(built-in)"
		}
		out = append(out, extEntry{name: "@" + m.Name, kind: "macro", origin: origin,
			code: m.Source, nExamples: len(glue.ExtExamplesFor("macro", m.Name))})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].kind != out[j].kind {
			return out[i].kind < out[j].kind
		}
		return out[i].name < out[j].name
	})
	return out
}

func (c *cli) extList() {
	exts := c.gatherExtensions()
	if len(exts) == 0 {
		fmt.Fprintln(c.stderr, "no extensions loaded")
		return
	}
	fmt.Fprintf(c.stderr, "%d loaded extension(s):  (.extensions show <name> for the source)\n", len(exts))
	for _, e := range exts {
		ex := ""
		if e.nExamples > 0 {
			ex = fmt.Sprintf("  (%d example%s)", e.nExamples, plural(e.nExamples))
		}
		fmt.Fprintf(c.stderr, "  %-22s %-22s %s%s\n", e.name, e.kind, e.origin, ex)
		if len(e.fns) > 0 { // a module: list the functions it defines
			fmt.Fprintln(c.stderr, c.style.Dim("      fns: "+strings.Join(e.fns, ", ")))
		}
	}
	fmt.Fprintln(c.stderr, c.style.Dim("(.extensions examples [name] to see them; .extensions test [name] to run them)"))
}

// extShow prints one extension's header + full source code (source to stdout, pipeable;
// header to stderr) -- the cross-kind sibling of `.macro show`.
func (c *cli) extShow(name string) {
	if name == "" {
		fmt.Fprintln(c.stderr, "usage: .extensions show <name>")
		return
	}
	q := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(name)), "@")
	for _, e := range c.gatherExtensions() {
		if strings.TrimPrefix(e.name, "@") == q {
			fmt.Fprintf(c.stderr, "%s  [%s]  %s", e.name, e.kind, e.origin)
			if e.nExamples > 0 {
				fmt.Fprintf(c.stderr, "  (%d example%s)", e.nExamples, plural(e.nExamples))
			}
			fmt.Fprintln(c.stderr)
			if e.code == "" {
				fmt.Fprintln(c.stderr, c.style.Dim("  (source not available for an inline extension)"))
				return
			}
			fmt.Fprintln(c.stderr, "\nsource:")
			fmt.Fprintln(c.out, e.code)
			return
		}
	}
	// A module FUNCTION name (e.g. "decimal_add") is collapsed into its bundle row, so
	// resolve it to the bundle and show that (the shared file).
	if b := glue.JSModuleOf(q); b != "" {
		c.extShow(b)
		return
	}
	fmt.Fprintf(c.stderr, "no extension %q -- try .extensions list\n", name)
}

// extExamples prints the inline examples declared in the loaded extension files --
// self-documenting: what each UDF / aggregate / stream source / macro / extract recipe
// DOES, read straight from the file without running it. `only` filters to one name.
// extResolveNames maps a `.extensions <cmd> <name>` argument to the set of extension
// names it targets: nil for "" (means all), otherwise the name itself PLUS — if it names
// a loaded multi-export JS module bundle (a file stem like "decimal") — that bundle's
// function names. So `.extensions examples decimal` / `test decimal` cover DECIMAL_ADD,
// DECIMAL_CMP, … even though those are the actual registered names.
func extResolveNames(only string) map[string]bool {
	if only == "" {
		return nil
	}
	set := map[string]bool{strings.ToLower(only): true}
	for _, fn := range glue.JSModuleFunctions(only) {
		set[strings.ToLower(fn)] = true
	}
	return set
}

func (c *cli) extExamples(only string) {
	sets := glue.ListExtExampleSets()
	want := extResolveNames(only)
	shown := 0
	for _, s := range sets {
		if want != nil && !want[strings.ToLower(s.Name)] {
			continue
		}
		shown++
		fmt.Fprintf(c.stderr, "%s %s — %d example%s:\n",
			c.style.Bold(s.Name), c.style.Dim("("+s.Kind+")"), len(s.Examples), plural(len(s.Examples)))
		for _, ex := range s.Examples {
			if d := exampleDesc(ex); d != "" {
				fmt.Fprintf(c.stderr, "    %s\n", c.style.Dim("# "+d))
			}
			fmt.Fprintf(c.stderr, "    %s  =>  %s\n", compactJSON(ex.In), compactJSON(ex.Out))
		}
	}
	if shown == 0 {
		if only != "" {
			fmt.Fprintf(c.stderr, "no examples for %q (loaded with -ext?)\n", only)
		} else {
			fmt.Fprintln(c.stderr, "no extension examples loaded (add an `examples` array to a *.js file; see .help extensions)")
		}
	}
}

// extTest runs every loaded extension's inline examples and checks each against its
// expected output -- the JS analog of `.multi test`. A failure latches c.failed so a
// non-interactive run exits non-zero (CI). `only` filters to one extension name.
func (c *cli) extTest(only string) {
	var results []glue.ExampleResult
	if fns := glue.JSModuleFunctions(only); len(fns) > 0 {
		// A module bundle: run every function's examples (RunExtensionExamples filters by
		// a single extension name, and the functions are the registered names).
		for _, fn := range fns {
			results = append(results, glue.RunExtensionExamples(fn)...)
		}
	} else {
		results = glue.RunExtensionExamples(only)
	}
	if len(results) == 0 {
		if only != "" {
			fmt.Fprintf(c.stderr, "no examples for %q to test\n", only)
		} else {
			fmt.Fprintln(c.stderr, "no extension examples to test")
		}
		return
	}
	pass, fail := 0, 0
	for _, r := range results {
		switch {
		case r.Err != "":
			fail++
			fmt.Fprintf(c.stderr, "  %s %s/%s [%s]: %s\n",
				c.style.Red("✗"), r.Kind, r.Name, r.Label, r.Err)
		case r.Pass:
			pass++
			fmt.Fprintf(c.stderr, "  %s %s [%s]  %s => %s\n",
				c.style.Green("✓"), r.Name, r.Label, r.In, r.Want)
		default:
			fail++
			fmt.Fprintf(c.stderr, "  %s %s [%s]  %s\n      got:  %s\n      want: %s\n",
				c.style.Red("✗"), r.Name, r.Label, r.In, r.Got, r.Want)
		}
	}
	summary := fmt.Sprintf("%d/%d example(s) passed", pass, pass+fail)
	if fail > 0 {
		c.failed = true
		fmt.Fprintf(c.stderr, "%s %s\n", c.icon("❌ "), c.style.Red(summary))
	} else {
		fmt.Fprintf(c.stderr, "%s %s\n", c.icon("✅ "), c.style.Green(summary))
	}
}

// compactJSON renders raw JSON on a single line (whitespace removed) for display.
func compactJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "null"
	}
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		return string(raw)
	}
	return b.String()
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func exampleDesc(ex glue.ExtExample) string {
	if ex.Desc != "" {
		return ex.Desc
	}
	return ex.Name
}

func (c *cli) extLoad(args []string) {
	var paths []string
	for _, a := range args {
		paths = append(paths, splitPaths(a)...)
	}
	if len(paths) == 0 {
		fmt.Fprintln(c.stderr, "usage: .extensions load <dir-or-file>[,<dir-or-file>...]")
		return
	}
	names, err := loadExtensions(paths)
	if len(names) > 0 {
		fmt.Fprintf(c.stderr, "loaded: %s\n", strings.Join(names, ", "))
	}
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .extensions load: %v\n", c.prog, err)
	}
}

func (c *cli) extUnload(args []string) {
	var names []string
	for _, a := range args {
		names = append(names, splitPaths(a)...)
	}
	if len(names) == 0 {
		fmt.Fprintln(c.stderr, "usage: .extensions unload <name>[,<name>...]")
		return
	}
	for _, n := range names {
		if err := glue.UnloadExtension(n); err != nil {
			fmt.Fprintf(c.stderr, "%s: .extensions unload: %v\n", c.prog, err)
		} else {
			fmt.Fprintf(c.stderr, "unloaded: %s\n", strings.ToLower(n))
		}
	}
}
