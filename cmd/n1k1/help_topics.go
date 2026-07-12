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
// index of deep-dive topics; `.help <topic>` prints a topic. The headline topic is
// `reserved-words`, which checks a name against cbq's LIVE parser (glue.IsReserved) --
// never a hardcoded list -- so `.help reserved-words <name>` tells an author (or an AI
// agent authoring detectors) up front whether a field/alias/keyspace name needs
// backticking, turning the reactive parse-error hint into a proactive lookup.

import (
	"fmt"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// helpTopic is one deep-dive entry for the `.help` index.
type helpTopic struct {
	name, blurb string
}

// helpTopics is the ordered topic index shown by `.help`.
var helpTopics = []helpTopic{
	{"reserved-words", "the SQL++ keywords you must backtick as identifiers (full list)"},
	{"quoting", "backticks vs the shell vs dot-command args"},
	{"keyspaces", "how files/dirs become keyspaces; dotted names"},
	{"meta", "the _meta record fields + external follow-up"},
	{"temp-keyspaces", "CREATE TEMP KEYSPACE staged pipelines"},
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
			fmt.Fprintf(c.stderr, "  .help %-15s %s\n", t.name, t.blurb)
		}
	case "reserved-words", "reserved", "keywords":
		c.helpReserved(strings.TrimSpace(rest))
	case "quoting", "quotes":
		c.helpQuoting()
	case "keyspaces", "tables":
		c.helpKeyspaces()
	case "meta", "_meta":
		c.helpMeta()
	case "temp-keyspaces", "temp-keyspace", "temp", "materialize":
		c.helpTempKeyspaces()
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
	c.hline("Using one as a field, alias, or temp-keyspace name is the most common authoring")
	c.hline("error. Fix: backtick it — `level` — and in a shell single-quote the whole -c arg so")
	c.hline("the backticks survive (see .help quoting). The built-in log recipe EMITS a `level`")
	c.hline("field, so a detector writes  WHERE l.`level` = \"error\".")
	c.hline("")
	c.hline("This list is cbq's own (probed live from its parser, not hand-maintained). To check")
	c.hline("one name:  .help reserved-words <name>")
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

func (c *cli) helpQuoting() {
	c.hline("quoting — the 3 contexts that trip people up")
	c.hline("")
	c.hline("1) In SQL++: backtick a dotted keyspace or a reserved/odd field name:")
	c.hline("     SELECT COUNT(*) FROM `ns_server.error`;   SELECT l.`level` FROM `ns_server.error` l")
	c.hline("2) In a shell -c: backticks are command-substitution inside \"double quotes\", so")
	c.hline("   'single-quote' the whole -c arg (backticks stay literal), or use -f <file>:")
	c.hline("     n1k1 -c 'SELECT COUNT(*) FROM `ns_server.error`' <bundle>")
	c.hline("3) In a dot-command arg: NO backticks — .schema/.index take the bare name:")
	c.hline("     .schema ns_server.error       (not  .schema `ns_server.error`)")
	c.hline("")
	c.hline("See also: .help reserved-words, .help keyspaces.")
}

func (c *cli) helpKeyspaces() {
	c.hline("keyspaces — how files become queryable tables")
	c.hline("")
	c.hline("• A directory <ns>/<keyspace>/ is a keyspace (its record files are unioned).")
	c.hline("• A single file arg is a keyspace named by its stem: ns_server.error.log ->")
	c.hline("  keyspace `ns_server.error` (dotted -> must be backticked; see .help quoting).")
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
	c.hline("  CREATE [OR REPLACE] TEMP KEYSPACE <name> AS <select>   -- session-scoped, in-memory")
	c.hline("                                                          (spills to disk if large)")
	c.hline("  DROP TEMP KEYSPACE [IF EXISTS] <name>")
	c.hline("")
	c.hline("Later statements SELECT ... FROM <name> — JOINable, aggregable, and chainable (a")
	c.hline("temp keyspace built FROM other temp keyspaces). The staged-analysis pattern: scan")
	c.hline("the big bundle once into small finding keyspaces, then correlate them, all in one")
	c.hline("session and one dialect. (File-backed sibling: INSERT INTO `<name>/data.jsonl` ... SELECT.)")
}
