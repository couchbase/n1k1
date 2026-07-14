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

// `.help` topic system (IDEA-0028): `.help` lists the dot-commands (printHelp) plus an
// index of deep-dive topics; `.help <topic>` prints a topic. Concept topics come first
// (each sorted A→Z), then the command guides that also answer to `.<command> help`.
// One topic worth noting is `reserved-words`, which checks a name against cbq's LIVE
// parser (glue.IsReserved) -- never a hardcoded list -- so `.help reserved-words <name>`
// tells an author up front whether a field/alias/keyspace name needs backticking,
// turning the reactive parse-error hint into a proactive lookup.

import (
	"fmt"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// helpTopic is one entry in the `.help` index. `alias`, when set, notes the
// equivalent command-scoped help (e.g. `.help multi` == `.multi help`) -- those
// topics DELEGATE to the same guide, so there is one source of truth and two ways in.
type helpTopic struct {
	name, blurb, alias string
}

// helpTopics is the topic index shown by `.help`: concept deep-dives first, then the
// command guides (also reachable as `.<command> help`). Each group is sorted A→Z so
// the list is scannable.
var helpTopics = []helpTopic{
	{name: "extensions", blurb: "user functions (*.js UDFs/aggregates/sources) loaded via -ext"},
	{name: "extract", blurb: "*.extract.js extensions that frame files into rows", alias: ".extract help"},
	{name: "index", blurb: "secondary/FTS indexes: the catalog + .index commands", alias: ".index help"},
	{name: "keyspaces", blurb: "how files/dirs become keyspaces; dotted names"},
	{name: "macro", blurb: "*.macro.js macros that expand @name(...) into SQL++", alias: ".macro help"},
	{name: "meta", blurb: "the _meta record fields + external follow-up"},
	{name: "multi", blurb: "authoring & running a multi-query pack of *.sql++ queries (shared execution)", alias: ".multi help"},
	{name: "quoting", blurb: "backticks vs the shell vs dot-command args"},
	{name: "reserved-words", blurb: "the SQL++ keywords you must backtick as identifiers (full list)"},
	{name: "temp-keyspaces", blurb: "CREATE TEMP KEYSPACE such as for staged analysis pipelines"},
}

// cmdHelp implements `.help [<topic> [<arg>]]`.
func (c *cli) cmdHelp(arg string) {
	topic, rest := splitFirst(arg)
	topic = strings.ToLower(strings.TrimSpace(topic))
	switch topic {
	case "":
		c.printHelp()
		fmt.Fprintln(c.stderr, "\nDeep-dive topics — .help <topic>:")
		for _, t := range helpTopics {
			line := fmt.Sprintf("  .help %-15s %s", t.name, t.blurb)
			if t.alias != "" {
				line += " (= " + t.alias + ")"
			}
			fmt.Fprintln(c.stderr, line)
		}
		// Show the current datastore + a live example over a real keyspace, or a hint
		// to open one when there's no datastore.
		fmt.Fprintf(c.stderr, "\ndatastore: %s\n", c.dataLoc())

	case "extensions", "ext", "udf", "udfs", "functions":
		c.helpExtensions()
	case "keyspaces", "tables":
		c.helpKeyspaces()
	case "meta", "_meta":
		c.helpMeta()
	case "quoting", "quotes":
		c.helpQuoting()
	case "reserved-words", "reserved", "keywords":
		c.helpReserved(strings.TrimSpace(rest))
	case "temp-keyspaces", "temp-keyspace", "temp", "materialize":
		c.helpTempKeyspaces()
	// Command guides: delegate to the SAME help the command-scoped form prints, so
	// `.help multi` and `.multi help` are one guide reached two ways.
	case "multi":
		c.cmdRulesHelp()
	case "extract":
		c.cmdExtract("help")
	case "macro", "macros":
		c.cmdMacro("help")
	case "index":
		c.cmdIndexHelp()
	default:
		fmt.Fprintf(c.stderr, "unknown help topic %q — run %s for the topic list\n", topic, ".help")
	}
}

func (c *cli) hline(s string) { fmt.Fprintln(c.stderr, s) }

// helpReserved handles `.help reserved-words [<name>...]`: with no name it prints the
// FULL reserved-word list (so an author reads it once, no whack-a-mole); with names it
// live-checks just those. Both come from cbq's own parser -- never a hardcoded list.
func (c *cli) helpReserved(rest string) {
	if rest != "" {
		for _, name := range strings.Fields(rest) {
			n := strings.Trim(name, "`")
			if glue.IsReserved(n) {
				c.hline(fmt.Sprintf("  %-14s RESERVED — quote it as `%s` (and single-quote the -c arg in a shell)", n, n))
			} else {
				c.hline(fmt.Sprintf("  %-14s ok — usable unquoted as a field/alias/keyspace name", n))
			}
		}
		return
	}

	words := glue.ReservedWords()
	c.hline(fmt.Sprintf("reserved words — %d SQL++ keywords that must be backticked as identifiers", len(words)))
	c.hline("")
	c.hline("Using a reserved word as a field, alias, or temp-keyspace name is a common")
	c.hline("SQL++ authoring issue. Fix: backtick it — `level` — and in a shell single-quote")
	c.hline("the whole -c arg so the backticks survive (see .help quoting). Example, a log")
	c.hline("keyspace has a `level` field, so a query with  WHERE log.`level` = \"error\".")
	c.hline("")
	c.hline("To check specific words:  .help reserved-words <word>")
	c.hline("")
	c.printWordGrid(words)
}

// printWordGrid prints words in aligned columns sized to the terminal width cap.
func (c *cli) printWordGrid(words []string) {
	if len(words) == 0 {
		return
	}
	col := 0
	for _, w := range words { // widest word sets the column width
		if len(w) > col {
			col = len(w)
		}
	}
	col += 2
	perRow := 76 / col
	if perRow < 1 {
		perRow = 1
	}
	var b strings.Builder
	for i, w := range words {
		b.WriteString(fmt.Sprintf("%-*s", col, w))
		if (i+1)%perRow == 0 || i == len(words)-1 {
			c.hline("  " + strings.TrimRight(b.String(), " "))
			b.Reset()
		}
	}
}

func (c *cli) helpExtensions() {
	c.hline("extensions — user functions loaded from files")
	c.hline("")
	c.hline("Load at startup with  -ext <dir-or-file>  (repeatable, comma-ok), or at the prompt")
	c.hline("with  .extensions load <path>. A directory is scanned for every kind below.")
	c.hline("  .extensions list             — what's loaded (name · kind · source)")
	c.hline("  .extensions unload <name>    — drop one")
	c.hline("")
	c.hline("Each file's suffix specifies its kind; the function name is the file's stem:")
	c.hline("  foo.js         scalar UDF    function foo(a, b) { ... }")
	c.hline("  foo.agg.js     aggregate     foo_init / foo_update(s,v) / foo_final(s)")
	c.hline("  foo.stream.js  table-valued source function foo(emit, args) { emit({...}) }")
	c.hline("     SELECT foo(x.a, x.b) ...  -- scalar / aggregate, called by stem")
	c.hline("     FROM foo(1, 10) AS x      -- streaming source, one row at a time")
	c.hline("")
	c.hline("Additional file extension kinds, each with its own guide:")
	c.hline("  foo.extract.js  frame a file into rows        — see .help extract")
	c.hline("  foo.macro.js    expand @foo(...) into SQL++   — see .help macro")
	c.hline("")
	c.hline("Inline examples ({in, out} in an `examples` array) self-document + golden-test:")
	c.hline("  .extensions examples [name]  — print them")
	c.hline("  .extensions test [name]      — run + check")
	c.hline("")
	c.hline("See also: .help extract, .help macro, .help multi.")
}

func (c *cli) helpQuoting() {
	c.hline("quoting — tips")
	c.hline("")
	c.hline("1) In SQL++: backtick a dotted keyspace or a reserved/odd field name:")
	c.hline("     SELECT COUNT(*) FROM `ap_server.error`;")
	c.hline("     SELECT errs.`level` FROM `app_server.error` errs")
	c.hline("")
	c.hline("2) In a shell -c: backticks are command-substitution inside \"double quotes\", so")
	c.hline("   'single-quote' the whole -c arg (backticks stay literal), or use -f <file>:")
	c.hline("     n1k1 -c 'SELECT COUNT(*) FROM `app_server.error`' my-logs")
	c.hline("")
	c.hline("See also: .help reserved-words, .help keyspaces.")
}

func (c *cli) helpKeyspaces() {
	c.hline("keyspaces — how files become queryable tables")
	c.hline("")
	c.hline("• A directory <namespace>/<keyspace>/ is a keyspace (its record files are unioned).")
	c.hline("• A single file arg is a keyspace named by its stem: app_server.error.log ->")
	c.hline("  keyspace `app_server.error` (dotted -> must be backticked; see .help quoting).")
	c.hline("• A flat dir of loose files: one keyspace named after the dir; a grab-bag dir")
	c.hline("  (files + subdirs): one keyspace per top-level file, by stem.")
	c.hline("• `FROM `./data/**/*.json`` — an inline doublestar glob keyspace.")
	c.hline("")
	c.hline(".tables / .keyspaces lists them with a framing tag (jsonl / recipe=<name> /")
	c.hline("whole-file / temp · session) and a copy/paste example per keyspace.")
}

func (c *cli) helpMeta() {
	c.hline("_meta — per-record provenance (add with -meta=on, or auto for extracted docs)")
	c.hline("")
	c.hline("Each record can carry a reserved `_meta` sub-object (query via _meta.`path`):")
	c.hline("  path name ext size mtime   — the source file")
	c.hline("  pos                        — the record's 0-based ordinal in its file")
	c.hline("  byte_offset byte_len       — the record's byte span in the ORIGINAL source")
	c.hline("  line_start line_end        — its raw 1-based line range")
	c.hline("")
	c.hline("So a finding is externally chase-able: dd/tail -c+<byte_offset>, sed -n <line_start>p,")
	c.hline("or rg -n land on the exact raw record (offsets are the original stream, pre-framing).")
	c.hline("  SELECT META(x).id, x._meta.byte_offset, x._meta.line_start FROM <ks> x WHERE ...")
}

func (c *cli) helpTempKeyspaces() {
	c.hline("temp-keyspaces — keep a query's results as a queryable keyspace for later statements")
	c.hline("")
	c.hline("  CREATE [OR REPLACE] TEMP KEYSPACE <name> AS <select> -- session-scoped, in-memory")
	c.hline("                                                          (spills to disk if large)")
	c.hline("  DROP TEMP KEYSPACE [IF EXISTS] <name>")
	c.hline("")
	c.hline("Later statements SELECT ... FROM <name> — JOINable, aggregable, and chainable (a")
	c.hline("temp keyspace built FROM other temp keyspaces). The staged-analysis pattern: scan")
	c.hline("big files once into small finding keyspaces, then correlate them, all in one")
	c.hline("session. (File-backed sibling: INSERT INTO `<name>/data.jsonl` ... SELECT.)")
}
