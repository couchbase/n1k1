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
	"strconv"
	"strings"

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
			fmt.Fprintf(c.stderr, "formats: %s\n", glue.ScanWalkOptions.Describe())
		} else if opts, err := records.ParseModes(a); err != nil {
			fmt.Fprintf(c.stderr, "usage: .formats [all|json|jsonl|csv|tsv|extract|doc|text|image|video|gzip|recurse]  (currently %s)\n",
				glue.ScanWalkOptions.Describe())
		} else {
			opts.Meta = glue.ScanWalkOptions.Meta // keep the current .meta setting
			glue.ScanWalkOptions = opts
			fmt.Fprintf(c.stderr, "formats: %s\n", glue.ScanWalkOptions.Describe())
		}
	case ".explain":
		c.explain = !c.explain
		fmt.Fprintf(c.stderr, "explain %s\n", onOff(c.explain))
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
	case ".read":
		c.readFile(arg)
	case ".output":
		c.cmdOutput(arg)
	default:
		fmt.Fprintf(c.stderr, "unknown command %q -- try .help\n", name)
	}
	return false
}

func (c *cli) printHelp() {
	fmt.Fprint(c.stderr, `.help                 show this help
.open <dir>           open a different file datastore directory
.tables / .keyspaces  list keyspaces (with a copy-paste example each)
.index [list|show <name>|rebuild [<n>]]  secondary indexes (run .index help for details)
.schema [<keyspace>]  sampled shape (keys + JSON types) of a keyspace
.mode <m>             output mode (append |pretty to indent JSON): `+strings.Join(cmd.OutputModes, " ")+`
.meta [on|off|auto]   add a _meta sub-object to records (no arg shows the current setting)
.formats [<set>]      restrict scanning to formats/modes, e.g. json,csv,gzip (no arg shows current)
.timer [on|off]       elapsed-time reporting (no arg shows the current setting)
.explain              toggle printing EXPLAIN PLAN per query
.maxrows <n>          box: cap rows shown (0 = all; negative = last |n| rows)
.maxwidth <n|auto>    box: cap column width (0 = uncapped; auto = fit terminal)
.read <file>          run statements/dot-commands from a file
.output [<file>]      send results to a file, or back to stdout if omitted
.version              show version + build info (incl. dependency SHAs)
.quit / .exit         leave
`)
	// Show the current datastore + a live example over a real keyspace, or a hint
	// to open one when there's no datastore.
	fmt.Fprintf(c.stderr, "\ndatastore: %s\n", c.dataLoc())
	if ex := c.exampleQuery(); ex != "" {
		fmt.Fprintf(c.stderr, "Statements are SQL++; terminate with ';'. Example: %s\n", ex)
	} else {
		fmt.Fprintln(c.stderr, "Statements are SQL++; terminate with ';'. Open a datastore with .open <dir> to query it.")
	}
}

func (c *cli) cmdOpen(dir string) {
	if dir == "" {
		fmt.Fprintln(c.stderr, "usage: .open <dir>")
		return
	}
	sess, err := glue.OpenSession(dir, c.ns)
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
