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

// cmd_registry.go is the dot-command registry: a small Cmd interface, a data table
// of the built-ins (replacing a hand-written dispatch switch + a parallel help
// table), and .help generation driven from that table. Commands are values, so a
// future plugin -- e.g. a *.cmd.js backed by goja -- can implement Cmd and register
// alongside the built-ins via (*cli).registerCmd, dispatching and self-describing
// its .help line identically.
package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/couchbase/n1k1/cmd"
	"github.com/couchbase/n1k1/glue"
)

// Cmd is one dot-command (or a family sharing a token prefix, like .index). Built-ins
// and plugins implement it the same way.
type Cmd interface {
	// Names lists the tokens that invoke this command (e.g. {".tables", ".keyspaces"}).
	// The first is canonical; the rest are aliases.
	Names() []string

	// Run executes the command. name is the exact token used (so an alias can vary
	// behavior, e.g. ".indexes" == ".index list"); arg is the rest of the line. It
	// reports failure the way the CLI always has -- print to c.stderr and set c.failed
	// (so .bail and the process exit code observe it) -- and returns quit=true to leave
	// the REPL.
	Run(c *cli, name, arg string) (quit bool)

	// Help returns this command's one-line .help entry, rendered against the current
	// settings (so the active choice can be highlighted). "" hides it from .help
	// (used for pure aliases folded into another command's line).
	Help(c *cli) string
}

// cmdSpec is the adapter that expresses a built-in as data (names + two closures),
// so builtinCmds below reads as a table rather than a switch. A plugin command is a
// different type implementing Cmd (it need not use cmdSpec).
type cmdSpec struct {
	names []string
	run   func(c *cli, name, arg string) bool
	help  func(c *cli) string
}

func (s cmdSpec) Names() []string                   { return s.names }
func (s cmdSpec) Run(c *cli, name, arg string) bool { return s.run(c, name, arg) }
func (s cmdSpec) Help(c *cli) string {
	if s.help == nil {
		return ""
	}
	return s.help(c)
}

// builtinCmdList is the registration-order table of built-in commands; builtinCmdByName
// indexes it (including aliases) for dispatch. Both are lazily built once (not package
// var initializers -- builtinCmds transitively refers back to them via printHelp, which
// Go's init-cycle check would reject). Being package-level, a bare &cli{} (as several
// tests construct) still dispatches built-ins without any setup.
var (
	builtinOnce      sync.Once
	builtinCmdList   []Cmd
	builtinCmdByName map[string]Cmd
)

func builtins() ([]Cmd, map[string]Cmd) {
	builtinOnce.Do(func() {
		builtinCmdList = builtinCmds()
		builtinCmdByName = make(map[string]Cmd, len(builtinCmdList)*2)
		for _, cm := range builtinCmdList {
			for _, n := range cm.Names() {
				builtinCmdByName[n] = cm
			}
		}
	})
	return builtinCmdList, builtinCmdByName
}

// registerCmd adds a plugin command to this cli, so a loaded *.cmd.js can extend the
// dot-command set at runtime. A per-cli registration shadows a built-in of the same
// name and appears in .help alongside the built-ins.
func (c *cli) registerCmd(cm Cmd) {
	if c.cmds == nil {
		c.cmds = map[string]Cmd{}
	}
	for _, n := range cm.Names() {
		c.cmds[n] = cm
	}
	c.cmdExtra = append(c.cmdExtra, cm)
}

// lookupCmd resolves a dot-command token to its handler: a per-cli (plugin) command
// first, then a built-in. nil if unknown.
func (c *cli) lookupCmd(name string) Cmd {
	if c.cmds != nil {
		if cm := c.cmds[name]; cm != nil {
			return cm
		}
	}
	_, byName := builtins()
	return byName[name]
}

// dot handles a meta command line (it always starts with '.'). Returns true to quit
// the REPL.
func (c *cli) dot(line string) bool {
	name, arg := splitFirst(line)
	if cm := c.lookupCmd(name); cm != nil {
		return cm.Run(c, name, arg)
	}
	fmt.Fprintf(c.stderr, "unknown command %q -- try .help\n", name)
	return false
}

// builtinCmds is the built-in dot-command table. Each entry names its tokens and gives
// a run closure (delegating to a c.cmdXxx method) plus a help-line closure. The bodies
// live in cmd_settings.go / cmd_open.go / the topical cmd*.go files; keeping the table
// declarative makes the whole command surface scannable in one place.
func builtinCmds() []Cmd {
	table := []cmdSpec{
		{names: []string{".help"},
			run: func(c *cli, _, arg string) bool { c.cmdHelp(arg); return false },
			help: func(c *cli) string {
				return ".help [<topic>]       show this help, or a deep-dive (reserved-words|quoting|keyspaces|meta|temp-keyspaces)"
			}},

		{names: []string{".open"},
			run:  func(c *cli, _, arg string) bool { c.cmdOpen(arg); return false },
			help: func(c *cli) string { return ".open <dir>           open a different datastore directory" }},

		{names: []string{".tables", ".keyspaces"},
			run: func(c *cli, _, arg string) bool {
				if strings.EqualFold(strings.TrimSpace(arg), "help") {
					c.helpKeyspaces() // `.keyspaces help` == `.help keyspaces`
				} else {
					c.cmdKeyspaces()
				}
				return false
			},
			help: func(c *cli) string { return ".tables / .keyspaces  list keyspaces + SQL++ example" }},

		{names: []string{".index", ".indexes"},
			run: func(c *cli, name, arg string) bool {
				if name == ".indexes" { // alias for `.index list`
					arg = "list"
				}
				c.cmdIndex(arg)
				return false
			},
			help: func(c *cli) string {
				return ".index [list|show <name>|rebuild [<n>]|help]  secondary indexes (run .index help for details)"
			}},

		{names: []string{".schema"},
			run:  func(c *cli, _, arg string) bool { c.cmdSchema(arg); return false },
			help: func(c *cli) string { return ".schema [<keyspace>]  sampled shape (keys + JSON types) of a keyspace" }},

		{names: []string{".mode"},
			run: func(c *cli, _, arg string) bool { c.cmdMode(arg); return false },
			help: func(c *cli) string {
				mode, _, _ := cmd.ParseMode(c.mode) // current output mode, minus any |pretty
				return ".mode <m>             output mode (append |pretty to indent JSON): " + c.highlightCurrent(mode, " ", cmd.OutputModes...)
			}},

		{names: []string{".meta"},
			run: func(c *cli, _, arg string) bool { c.cmdMeta(arg); return false },
			help: func(c *cli) string {
				return ".meta " + c.helpOpts(glue.ScanWalkOptions.Meta.String(), "on", "off", "auto") + "   add a _meta sub-object to records (no arg shows the current setting)"
			}},

		{names: []string{".formats"},
			run: func(c *cli, _, arg string) bool { c.cmdFormats(arg); return false },
			help: func(c *cli) string {
				return ".formats [<set>]      restrict files scanned to formats/modes, e.g. json,csv,gzip (no arg shows current)"
			}},

		{names: []string{".timer"},
			run: func(c *cli, _, arg string) bool { c.cmdTimer(arg); return false },
			help: func(c *cli) string {
				return ".timer " + c.helpOpts(onOff(c.timer), "on", "off") + "       elapsed-time reporting (no arg shows the current setting)"
			}},

		{names: []string{".stats"},
			run: func(c *cli, _, arg string) bool { c.cmdStats(arg); return false },
			help: func(c *cli) string {
				return ".stats " + c.helpOpts(c.statsMode, "on", "off", "final", "about") + " query stats: on=live footer, final=totals at end only (about=glossary)"
			}},

		{names: []string{".explain"},
			run: func(c *cli, _, arg string) bool { c.cmdExplain(arg); return false },
			help: func(c *cli) string {
				return ".explain " + c.helpOpts(onOff(c.explain), "on", "off") + "     print " + prog + "'s converted plan per query"
			}},

		{names: []string{".prepare"},
			run: func(c *cli, _, arg string) bool { c.cmdPrepare(arg); return false },
			help: func(c *cli) string {
				return ".prepare [interpreted|data|full | <stmt>]  set the max compile level, or emit generated Go for a one-shot <stmt>"
			}},

		{names: []string{".verbose"},
			run: func(c *cli, _, arg string) bool { c.cmdVerbose(arg); return false },
			help: func(c *cli) string {
				vcur := "off" // .verbose level -> its choice token
				if c.verbose == 1 {
					vcur = "on"
				} else if c.verbose >= 2 {
					vcur = "debug"
				}
				return ".verbose " + c.helpOpts(vcur, "off", "on", "debug", "n") + "  diagnostics level (n>1 provides more info; no arg shows current)"
			}},

		{names: []string{".maxrows"},
			run: func(c *cli, _, arg string) bool { c.cmdMaxRows(arg); return false },
			help: func(c *cli) string {
				return ".maxrows <n>          box: cap rows shown (0 = all; negative = last |n| rows)"
			}},

		{names: []string{".maxwidth"},
			run: func(c *cli, _, arg string) bool { c.cmdMaxWidth(arg); return false },
			help: func(c *cli) string {
				return ".maxwidth <n|auto>    box: cap column width (0 = uncapped; auto = fit terminal)"
			}},

		{names: []string{".multi"},
			run: func(c *cli, _, arg string) bool { c.cmdMulti(arg); return false },
			help: func(c *cli) string {
				return ".multi [list|run|lint|test|cursor|compose|help] --queries <dir>  run a multi-query pack of tagged *.sql++ queries over the datastore, shared execution; cursor = named \"what's new\" cursors; compose = a DAG of packs (.multi help)"
			}},

		{names: []string{".extensions", ".ext"},
			run: func(c *cli, _, arg string) bool { c.cmdExtensions(arg); return false },
			help: func(c *cli) string {
				return ".extensions [list | load <dir>... | unload <name>... | examples | test]  extensions (*.js); test runs inline examples"
			}},

		{names: []string{".extract"},
			run: func(c *cli, _, arg string) bool { c.cmdExtract(arg); return false },
			help: func(c *cli) string {
				return ".extract [help|list]  author *.extract.js plugins that frame files into rows (.extract help for details)"
			}},

		{names: []string{".macro", ".macros"},
			run: func(c *cli, _, arg string) bool { c.cmdMacro(arg); return false },
			help: func(c *cli) string {
				return ".macro [help|list|expand <stmt>]  pre-parse SQL++ macros: @name(...) -> generated SQL++ (.macro help for details)"
			}},

		{names: []string{".read"},
			run:  func(c *cli, _, arg string) bool { c.readFile(arg); return false },
			help: func(c *cli) string { return ".read <file>          run statements/dot-commands from a file" }},

		{names: []string{".bail"},
			run: func(c *cli, _, arg string) bool { c.cmdBail(arg); return false },
			help: func(c *cli) string {
				return ".bail " + c.helpOpts(onOff(c.bail), "on", "off") + "        stop on the first statement error (handy for scripts)"
			}},

		{names: []string{".echo"},
			run: func(c *cli, _, arg string) bool { c.cmdEcho(arg); return false },
			help: func(c *cli) string {
				return ".echo " + c.helpOpts(onOff(c.echo), "on", "off") + "        echo each input line as it's read (handy for scripts)"
			}},

		{names: []string{".print"},
			run: func(c *cli, _, arg string) bool { c.cmdPrint(arg); return false },
			help: func(c *cli) string {
				return ".print <text>         emit text to stderr (e.g. for script progress / debugging)"
			}},

		{names: []string{".output"},
			run:  func(c *cli, _, arg string) bool { c.cmdOutput(arg); return false },
			help: func(c *cli) string { return ".output [<file>]      send results to a file, or to stdout if omitted" }},

		{names: []string{".version"},
			run:  func(c *cli, _, _ string) bool { printVersion(c.stderr); return false },
			help: func(c *cli) string { return ".version              show version + build info" }},

		{names: []string{".quit", ".exit"},
			run:  func(c *cli, _, _ string) bool { return true },
			help: func(c *cli) string { return ".quit / .exit         leave" }},
	}
	// The table is []cmdSpec for elided literals; expose it as []Cmd (cmdSpec is the
	// adapter that satisfies the interface -- a plugin command is a different type).
	out := make([]Cmd, len(table))
	for i := range table {
		out[i] = table[i]
	}
	return out
}

func (c *cli) printHelp() {
	// Each command self-describes its .help line (rendering current on|off-style
	// settings highlighted), so the listing can't drift from the dispatch table.
	var lines []string
	list, _ := builtins()
	for _, cm := range list {
		if h := cm.Help(c); h != "" {
			lines = append(lines, h)
		}
	}
	for _, cm := range c.cmdExtra { // plugin (*.cmd.js) commands, if any
		if h := cm.Help(c); h != "" {
			lines = append(lines, h)
		}
	}
	// Each line begins with its ".command" token, so a lexicographic sort lists them
	// in command-name order. Choice lists keep a fixed visible width (the highlight is
	// zero-width ANSI), so the description column stays aligned.
	sort.Strings(lines)
	for _, l := range lines {
		fmt.Fprintln(c.stderr, l)
	}
	fmt.Fprintf(c.stderr, "\n")
	if ex := c.exampleQuery(); ex != "" {
		fmt.Fprintf(c.stderr, "Statements are SQL++; terminate with ';'. Example: %s\n\n", ex)
	} else {
		fmt.Fprintf(c.stderr, "Statements are SQL++; terminate with ';'. Open a datastore with .open <dir> to query it.\n\n")
	}
	// Materialization (staged/hierarchical analysis): keep a query's results as a
	// queryable keyspace for later statements -- session-scoped in-memory, or a file.
	fmt.Fprintln(c.stderr, "Materialize results into a keyspace you can query again:")
	fmt.Fprintln(c.stderr, "  CREATE TEMP KEYSPACE <name> AS <select>   (session-scoped, in-memory + spills to disk if large; DROP TEMP KEYSPACE <name>)")
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
