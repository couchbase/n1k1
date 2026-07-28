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

// cmd_settings.go holds the REPL setting/toggle dot-command bodies (.mode, .timer,
// .meta, .formats, .verbose, .explain, .prepare, .stats, .maxrows, .maxwidth, .print,
// .echo, .bail). Each is invoked by the registry table in cmd_registry.go. They mutate
// cli/session state and manage their own stderr + c.failed, exactly as before.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/cmd"
	"github.com/couchbase/n1k1/glue"
	"github.com/couchbase/n1k1/records"
)

// cmdMode sets the output mode (box/json/csv/..., with an optional |pretty). No arg
// (or a bad one) lists the modes.
func (c *cli) cmdMode(arg string) {
	if cmd.ValidMode(arg) {
		c.mode = arg
	} else {
		fmt.Fprintf(c.stderr, "modes: %s\n", strings.Join(cmd.OutputModes, " "))
	}
}

// cmdTimer toggles elapsed-time reporting. No arg shows the current setting.
func (c *cli) cmdTimer(arg string) {
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
}

// cmdMeta toggles/checks the _meta sub-object (path/name/ext/size/mtime). The engine
// reads glue.ScanWalkOptions.Meta per query, so mutating it here takes effect for
// subsequent statements.
func (c *cli) cmdMeta(arg string) {
	switch a := strings.ToLower(strings.TrimSpace(arg)); a {
	case "":
		fmt.Fprintf(c.stderr, "meta %s\n", glue.ScanWalkOptions.Meta)
	case "help":
		c.helpMeta() // `.meta help` == `.help meta`
	default:
		mm, err := records.ParseMetaMode(a)
		if err != nil {
			fmt.Fprintf(c.stderr, "usage: .meta [on|off|auto]  (currently %s)\n", glue.ScanWalkOptions.Meta)
		} else {
			glue.ScanWalkOptions.Meta = mm
			fmt.Fprintf(c.stderr, "meta %s\n", glue.ScanWalkOptions.Meta)
		}
	}
}

// cmdFormats checks/sets which file formats (+ gzip/zstd/recurse) scanning considers.
// The engine reads glue.ScanWalkOptions per query, so a change takes effect for
// subsequent statements. No arg shows the current setting.
func (c *cli) cmdFormats(arg string) {
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
}

// printFormats shows the current .formats/-formats setting, then a grouped reference
// of every supported format/mode token (with its file extensions and a short
// explanation), so users can see what to pass to restrict scanning.
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

// cmdVerbose checks/sets the verbose diagnostics level: 0=off; >0 logs info (query
// plans, extract/describe diagnostics via base.Logf); >1 logs more detail. Accepts
// off|on|debug or a number; no arg shows the current.
func (c *cli) cmdVerbose(arg string) {
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
			return
		}
	}
	base.LogLevel = c.verbose // route base.Logf through the same knob
	fmt.Fprintf(c.stderr, "verbose %s\n", verboseName(c.verbose))
}

// cmdExplain toggles printing the converted plan per query. No arg shows the current.
func (c *cli) cmdExplain(arg string) {
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
}

// cmdPrepare handles:
//
//	.prepare               show the current -prepare ceiling level.
//	.prepare <level>       set it: interpreted | data | full (on=full, off=interpreted).
//	.prepare <statement>   one-shot: emit the generated Go for <statement> (like
//	                       EXPLAIN, orthogonal to the ceiling), then run it. A statement
//	                       that needs cbq (a boxed expression, or a non-bakeable
//	                       datastore op) can't compile: it prints the reason and falls
//	                       back to the interpreter, never failing.
//
// (PREPARE/EXECUTE are also plain SQL statements -- just run them directly.)
func (c *cli) cmdPrepare(arg string) {
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
}

// cmdStats checks/sets the query-stats mode (on=live footer, final=totals at end,
// about=glossary). No arg shows the current.
func (c *cli) cmdStats(arg string) {
	switch a := strings.ToLower(strings.TrimSpace(arg)); a {
	case "":
		fmt.Fprintf(c.stderr, "stats %s\n", c.statsMode)
	case "about", "help":
		// Glossary of every registered counter (known at startup), a reference for
		// the names shown in the footer.
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
}

// cmdMaxRows sets the box row cap (0 = all; negative = last |n| rows). No arg shows
// the current.
func (c *cli) cmdMaxRows(arg string) {
	if arg == "" {
		fmt.Fprintf(c.stderr, "maxrows %s\n", c.maxRowsDesc())
	} else if n, err := strconv.Atoi(arg); err == nil {
		c.maxRows = n
		fmt.Fprintf(c.stderr, "maxrows %s\n", c.maxRowsDesc())
	} else {
		fmt.Fprintf(c.stderr, "usage: .maxrows <n>  (0 = all; negative = last |n| rows)\n")
	}
}

// cmdMaxWidth sets the box per-column width cap (0 = uncapped; auto = fit terminal).
// No arg shows the current.
func (c *cli) cmdMaxWidth(arg string) {
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
}

// cmdPrint emits text (a script progress marker, e.g. "STARTING big query..."). Goes
// to stderr so it interleaves with other diagnostics and never pollutes the query
// results on stdout. sqlite/duckdb call this .print.
func (c *cli) cmdPrint(arg string) {
	fmt.Fprintln(c.stderr, arg)
}

// cmdEcho toggles echoing each input line as it's read (great for logging what a
// -f/.read script ran). sqlite/duckdb call this .echo.
func (c *cli) cmdEcho(arg string) {
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
}

// cmdBail toggles stopping input (a -f/.read script, stdin, or the REPL) on the first
// statement error, instead of plowing on. sqlite/duckdb call this .bail.
func (c *cli) cmdBail(arg string) {
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
}
