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

// cli dot-command dispatch (.help/.open/.output/...).
package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/cmd"
	"github.com/couchbase/n1k1/glue"
	"github.com/couchbase/n1k1/records"
)

// ---- dot-commands ---------------------------------------------------------

// dot handles a meta command line (it always starts with '.'). Returns true to
// quit the REPL.
func (c *cli) dot(line string) bool {
	name, arg := splitFirst(line)
	switch name {
	case ".quit", ".exit":
		return true
	case ".help":
		c.printHelp()
	case ".version":
		printVersion(c.stderr)
	case ".open":
		c.cmdOpen(arg)
	case ".tables", ".keyspaces":
		c.cmdKeyspaces()
	case ".index":
		c.cmdIndex(arg)
	case ".indexes": // alias for ".index list"
		c.cmdIndex("list")
	case ".schema":
		c.cmdSchema(arg)
	case ".mode":
		if cmd.ValidMode(arg) {
			c.mode = arg
		} else {
			fmt.Fprintf(c.stderr, "modes: %s\n", strings.Join(cmd.OutputModes, " "))
		}
	case ".timer":
		switch strings.ToLower(arg) {
		case "":
			fmt.Fprintf(c.stderr, "timer %s\n", onOff(c.timer))
		case "on":
			c.timer = true
		case "off":
			c.timer = false
		default:
			fmt.Fprintf(c.stderr, "usage: .timer [on|off] (currently %s)\n", onOff(c.timer))
		}
	case ".meta":
		// Toggle/check the _meta sub-object (path/name/ext/size/mtime). The engine
		// reads glue.ScanWalkOptions.Meta per query, so mutating it here takes
		// effect for subsequent statements.
		switch a := strings.ToLower(strings.TrimSpace(arg)); a {
		case "":
			fmt.Fprintf(c.stderr, "meta %s\n", glue.ScanWalkOptions.Meta)
		default:
			mm, err := records.ParseMetaMode(a)
			if err != nil {
				fmt.Fprintf(c.stderr, "usage: .meta [on|off|auto]  (currently %s)\n", glue.ScanWalkOptions.Meta)
			} else {
				glue.ScanWalkOptions.Meta = mm
				fmt.Fprintf(c.stderr, "meta %s\n", glue.ScanWalkOptions.Meta)
			}
		}
	case ".formats":
		// Check/set which file formats (+ gzip/zstd/recurse) scanning considers.
		// The engine reads glue.ScanWalkOptions per query, so a change takes effect
		// for subsequent statements. No arg shows the current setting.
		if a := strings.TrimSpace(arg); a == "" {
			c.printFormats()
		} else if opts, err := records.ParseModes(a); err != nil {
			fmt.Fprintf(c.stderr, "usage: .formats [all|json|jsonl|csv|tsv|extract|doc|text|image|video|gzip|recurse]  (currently %s)\n",
				glue.ScanWalkOptions.Spec)
		} else {
			opts.Meta = glue.ScanWalkOptions.Meta // keep the current .meta setting
			glue.ScanWalkOptions = opts
			fmt.Fprintf(c.stderr, "formats: %s\n", glue.ScanWalkOptions.Spec)
			// Persist to the datastore's catalog.json so it's remembered next open
			// (directory datastores only; a single-file arg has no sidecar of its own).
			if fi, serr := os.Stat(c.dir); serr == nil && fi.IsDir() {
				if err := glue.CatalogSetFormats(c.dir, a); err != nil {
					fmt.Fprintf(c.stderr, "  (not saved to %s: %v)\n", c.catalogPath(), err)
				} else {
					fmt.Fprintf(c.stderr, "  saved to %s\n", c.catalogPath())
				}
			}
		}
	case ".verbose":
		// Check/set the verbose diagnostics level: 0=off; >0 logs info (query plans,
		// extract/describe diagnostics via base.Logf); >1 logs more detail. Accepts
		// off|on|debug or a number; no arg shows the current.
		switch a := strings.ToLower(strings.TrimSpace(arg)); a {
		case "":
			// show only
		case "off":
			c.verbose = 0
		case "on":
			c.verbose = 1
		case "debug":
			c.verbose = 2
		default:
			if n, err := strconv.Atoi(a); err == nil && n >= 0 {
				c.verbose = n
			} else {
				fmt.Fprintf(c.stderr, "usage: .verbose [off|on|debug|<n>]  (currently %s)\n", verboseName(c.verbose))
				return false
			}
		}
		base.LogLevel = c.verbose // route base.Logf through the same knob
		fmt.Fprintf(c.stderr, "verbose %s\n", verboseName(c.verbose))
	case ".explain":
		switch strings.ToLower(arg) {
		case "":
			fmt.Fprintf(c.stderr, "explain %s\n", onOff(c.explain))
		case "on":
			c.explain = true
		case "off":
			c.explain = false
		default:
			fmt.Fprintf(c.stderr, "usage: .explain [on|off] (currently %s)\n", onOff(c.explain))
		}
	case ".prepare":
		// .prepare               show the current -prepare ceiling level.
		// .prepare <level>       set it: interpreted | data | full (on=full, off=interpreted).
		// .prepare <statement>   one-shot: emit the generated Go for <statement> (like
		//                        EXPLAIN, orthogonal to the ceiling), then run it. A
		//                        statement that needs cbq (a boxed expression, or a
		//                        non-bakeable datastore op) can't compile: it prints the
		//                        reason and falls back to the interpreter, never failing.
		// (PREPARE/EXECUTE are also plain SQL statements -- just run them directly.)
		switch a := strings.TrimSpace(arg); {
		case a == "":
			fmt.Fprintf(c.stderr, "prepare %s\n", c.prepareLevel)
		case isPrepareLevelToken(a):
			c.prepareLevel, _ = glue.PrepareLevelParse(a)
			fmt.Fprintf(c.stderr, "prepare %s\n", c.prepareLevel)
		default:
			// Treat the arg as a one-shot statement: emit its Go, then run it.
			c.prepareStmt(a)
			c.exec(a)
		}
	case ".stats":
		switch a := strings.ToLower(strings.TrimSpace(arg)); a {
		case "":
			fmt.Fprintf(c.stderr, "stats %s\n", c.statsMode)
		case "about", "help":
			// Glossary of every registered counter (known at startup), a reference
			// for the names shown in the footer.
			for _, ln := range statsAbout() {
				fmt.Fprintln(c.stderr, ln)
			}
		default:
			if m, err := parseStatsMode(a); err == nil {
				c.statsMode = m
				fmt.Fprintf(c.stderr, "stats %s\n", c.statsMode)
			} else {
				fmt.Fprintf(c.stderr, "usage: .stats [on|off|final|about] (currently %s)\n", c.statsMode)
			}
		}
	case ".maxrows":
		if arg == "" {
			fmt.Fprintf(c.stderr, "maxrows %s\n", c.maxRowsDesc())
		} else if n, err := strconv.Atoi(arg); err == nil {
			c.maxRows = n
			fmt.Fprintf(c.stderr, "maxrows %s\n", c.maxRowsDesc())
		} else {
			fmt.Fprintf(c.stderr, "usage: .maxrows <n>  (0 = all; negative = last |n| rows)\n")
		}
	case ".maxwidth":
		if arg == "" {
			fmt.Fprintf(c.stderr, "maxwidth %s\n", c.maxWidthDesc())
		} else if strings.EqualFold(arg, "auto") {
			c.maxWidth = -1
			fmt.Fprintf(c.stderr, "maxwidth %s\n", c.maxWidthDesc())
		} else if n, err := strconv.Atoi(arg); err == nil && n >= 0 {
			c.maxWidth = n
			fmt.Fprintf(c.stderr, "maxwidth %s\n", c.maxWidthDesc())
		} else {
			fmt.Fprintf(c.stderr, "usage: .maxwidth <n|auto>  (0 = uncapped; auto = fit terminal)\n")
		}
	case ".print":
		// Emit text (a script progress marker, e.g. "STARTING big query..."). Goes
		// to stderr so it interleaves with other diagnostics and never pollutes the
		// query results on stdout. sqlite/duckdb call this .print.
		fmt.Fprintln(c.stderr, arg)
	case ".echo":
		// Echo each input line as it's read (great for logging what a -f/.read
		// script ran). sqlite/duckdb call this .echo.
		switch strings.ToLower(strings.TrimSpace(arg)) {
		case "":
			fmt.Fprintf(c.stderr, "echo %s\n", onOff(c.echo))
		case "on":
			c.echo = true
		case "off":
			c.echo = false
		default:
			fmt.Fprintf(c.stderr, "usage: .echo [on|off] (currently %s)\n", onOff(c.echo))
		}
	case ".bail":
		// Stop running input (a -f/.read script, stdin, or the REPL) on the first
		// statement error, instead of plowing on. sqlite/duckdb call this .bail.
		switch strings.ToLower(strings.TrimSpace(arg)) {
		case "":
			fmt.Fprintf(c.stderr, "bail %s\n", onOff(c.bail))
		case "on":
			c.bail = true
		case "off":
			c.bail = false
		default:
			fmt.Fprintf(c.stderr, "usage: .bail [on|off] (currently %s)\n", onOff(c.bail))
		}
	case ".read":
		c.readFile(arg)
	case ".rules":
		c.cmdRules(arg)
	case ".extensions", ".ext":
		c.cmdExtensions(arg)
	case ".extract":
		c.cmdExtract(arg)
	case ".output":
		c.cmdOutput(arg)
	default:
		fmt.Fprintf(c.stderr, "unknown command %q -- try .help\n", name)
	}
	return false
}

func (c *cli) printHelp() {
	// The current value of each on|off-style setting is highlighted in its choice
	// list below, so the active setting stands out at a glance.
	mode, _, _ := cmd.ParseMode(c.mode) // current output mode, minus any |pretty
	vcur := "off"                       // .verbose level -> its choice token
	if c.verbose == 1 {
		vcur = "on"
	} else if c.verbose >= 2 {
		vcur = "debug"
	}
	// Each line begins with its ".command" token, so a lexicographic sort lists
	// them in command-name order. Choice lists keep a fixed visible width (the
	// highlight is zero-width ANSI), so the description column stays aligned.
	lines := []string{
		".help                 show this help",
		".open <dir>           open a different datastore directory",
		".tables / .keyspaces  list keyspaces + SQL++ example",
		".index [list|show <name>|rebuild [<n>]|help]  secondary indexes (run .index help for details)",
		".schema [<keyspace>]  sampled shape (keys + JSON types) of a keyspace",
		".mode <m>             output mode (append |pretty to indent JSON): " + c.highlightCurrent(mode, " ", cmd.OutputModes...),
		".meta " + c.helpOpts(glue.ScanWalkOptions.Meta.String(), "on", "off", "auto") + "   add a _meta sub-object to records (no arg shows the current setting)",
		".formats [<set>]      restrict files scanned to formats/modes, e.g. json,csv,gzip (no arg shows current)",
		".timer " + c.helpOpts(onOff(c.timer), "on", "off") + "       elapsed-time reporting (no arg shows the current setting)",
		".stats " + c.helpOpts(c.statsMode, "on", "off", "final", "about") + " query stats: on=live footer, final=totals at end only (about=glossary)",
		".explain " + c.helpOpts(onOff(c.explain), "on", "off") + "     print " + prog + "'s converted plan per query",
		".prepare [interpreted|data|full | <stmt>]  set the max compile level, or emit generated Go for a one-shot <stmt>",
		".verbose " + c.helpOpts(vcur, "off", "on", "debug", "n") + "  diagnostics level (n>1 provides more info; no arg shows current)",
		".maxrows <n>          box: cap rows shown (0 = all; negative = last |n| rows)",
		".maxwidth <n|auto>    box: cap column width (0 = uncapped; auto = fit terminal)",
		".rules [list|run|lint|test|help] --queries <dir>  run a collection of tagged *.sql++ queries over the datastore (.rules help)",
		".extensions [list | load <dir>... | unload <name>...]  extensions (*.js = JavaScript)",
		".extract [help|list]  author *.extract.js recipes that frame files into rows (.extract help for details)",
		".read <file>          run statements/dot-commands from a file",
		".bail " + c.helpOpts(onOff(c.bail), "on", "off") + "        stop on the first statement error (handy for scripts)",
		".echo " + c.helpOpts(onOff(c.echo), "on", "off") + "        echo each input line as it's read (handy for scripts)",
		".print <text>         emit text to stderr (e.g. for script progress / debugging)",
		".output [<file>]      send results to a file, or to stdout if omitted",
		".version              show version + build info",
		".quit / .exit         leave",
	}
	sort.Strings(lines)
	for _, l := range lines {
		fmt.Fprintln(c.stderr, l)
	}
	// Show the current datastore + a live example over a real keyspace, or a hint
	// to open one when there's no datastore.
	fmt.Fprintf(c.stderr, "\ndatastore: %s\n", c.dataLoc())
	if ex := c.exampleQuery(); ex != "" {
		fmt.Fprintf(c.stderr, "Statements are SQL++; terminate with ';'. Example: %s\n", ex)
	} else {
		fmt.Fprintln(c.stderr, "Statements are SQL++; terminate with ';'. Open a datastore with .open <dir> to query it.")
	}
	// Materialization (staged/hierarchical analysis): keep a query's results as a
	// queryable keyspace for later statements -- session-scoped in-memory, or a file.
	fmt.Fprintln(c.stderr, "Materialize results into a keyspace you can query again:")
	fmt.Fprintln(c.stderr, "  CREATE TEMP KEYSPACE <name> AS <select>   (session-scoped, in-memory; DROP TEMP KEYSPACE <name>)")
	fmt.Fprintln(c.stderr, "  INSERT INTO `<name>/data.jsonl` (KEY UUID(), VALUE self) <select>   (persisted as a jsonl file)")
}

// highlightCurrent joins opts with sep, rendering the token equal to current in a
// highlighted style so the .help listing shows the active setting. Token text is
// unchanged (the highlight is zero-width ANSI), so column alignment is preserved,
// and it degrades to plain text when styling is off (piped/redirected output).
func (c *cli) highlightCurrent(current, sep string, opts ...string) string {
	parts := make([]string, len(opts))
	for i, o := range opts {
		if o == current {
			parts[i] = c.style.Bold(c.style.Cyan(o))
		} else {
			parts[i] = o
		}
	}
	return strings.Join(parts, sep)
}

// helpOpts renders a "[a|b|c]" choice list for .help with the current value
// highlighted.
func (c *cli) helpOpts(current string, opts ...string) string {
	return "[" + c.highlightCurrent(current, "|", opts...) + "]"
}

// printFormats shows the current .formats/-formats setting, then a grouped
// reference of every supported format/mode token (with its file extensions and a
// short explanation), so users can see what to pass to restrict scanning.
func (c *cli) printFormats() {
	fmt.Fprintf(c.stderr, "formats: %s\n", glue.ScanWalkOptions.Spec)
	fmt.Fprintln(c.stderr, "\nsupported (comma-separate to restrict, e.g. -formats json,csv,gzip):")

	modes := records.Modes()
	name := func(m records.ModeInfo) string { // "jsonl/ndjson", "gzip/gz", ...
		return strings.Join(append([]string{m.Token}, m.Aliases...), "/")
	}
	nameW, extW := 0, 0 // column widths
	for _, m := range modes {
		if n := len(name(m)); n > nameW {
			nameW = n
		}
		if e := len(strings.Join(m.Exts, " ")); e > extW {
			extW = e
		}
	}

	groups := []struct{ kind, title string }{
		{"structured", "structured (parsed into rows):"},
		{"extract", "extract (text + metadata from unstructured files, one record each):"},
		{"modifier", "modifiers:"},
		{"meta", ""},
	}
	for _, g := range groups {
		if g.title != "" {
			fmt.Fprintf(c.stderr, "  %s\n", g.title)
		}
		for _, m := range modes {
			if m.Kind != g.kind {
				continue
			}
			fmt.Fprintf(c.stderr, "    %-*s  %-*s  %s\n",
				nameW, name(m), extW, strings.Join(m.Exts, " "), m.Desc)
		}
	}
	fmt.Fprintln(c.stderr, "  (individual extensions also work as tokens, e.g. pdf, docx, png)")
}

func (c *cli) cmdOpen(dir string) {
	if dir == "" {
		fmt.Fprintln(c.stderr, "usage: .open <dir>")
		return
	}
	sess, err := glue.OpenSession(dir, defaultNamespace)
	if err != nil {
		fmt.Fprintf(c.stderr, "cannot open %q: %v\n", dir, err)
		return
	}
	c.sess, c.dir = sess, dir
	c.eagerBuildIndexes() // re-apply -index=eager to the newly opened datastore
	fmt.Fprintf(c.stderr, "opened %s\n", dir)
}

func (c *cli) cmdOutput(path string) {
	if c.outFile != nil {
		c.outFile.Close()
		c.outFile = nil
	}
	if path == "" {
		c.out = os.Stdout
		c.style.On = c.fancyTTY // restore styling for the terminal
		return
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(c.stderr, "cannot create %q: %v\n", path, err)
		return
	}
	c.outFile, c.out = f, f
	c.style.On = false // never write ANSI codes to a file
}
