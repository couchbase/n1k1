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
	{name: "extensions", blurb: "user functions (*.js UDFs/aggregates/sources/modules) loaded via -ext", alias: ".extensions help"},
	{name: "extract", blurb: "*.extract.js extensions that frame files into rows", alias: ".extract help"},
	{name: "index", blurb: "secondary/FTS indexes: the catalog + .index commands", alias: ".index help"},
	{name: "keyspaces", blurb: "how files/dirs become keyspaces; dotted names", alias: ".keyspaces help"},
	{name: "macro", blurb: "*.macro.js macros that expand @name(...) into SQL++", alias: ".macro help"},
	{name: "meta", blurb: "the _meta record fields + external follow-up", alias: ".meta help"},
	{name: "multi", blurb: "authoring & running a multi-query pack of *.sql++ queries (shared execution)", alias: ".multi help"},
	{name: "quoting", blurb: "backticks vs the shell vs dot-command args"},
	{name: "reserved-words", blurb: "the SQL++ keywords you must backtick as identifiers (full list)"},
	{name: "temp-keyspaces", blurb: "CREATE TEMP KEYSPACE such as for staged analysis pipelines"},
	{name: "vectors", blurb: "semantic / similarity search: embed text, store vectors, find nearest"},
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
	case "vectors", "vector", "embedding", "embeddings", "search":
		c.helpVectors()
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

func (c *cli) helpVectors() {
	c.hline("vectors — semantic / similarity search over your data")
	c.hline("")
	c.hline("Turn text into embedding vectors, store them, and find the nearest matches to a")
	c.hline("query. Three pieces:")
	c.hline("")
	c.hline("  VECTORIZE_BATCH(array, opts)   embed an array of {text} objects into vectors,")
	c.hline("                                 one model call per batch. Offline deterministic")
	c.hline("                                 \"fake\" vectors by default (no model); point opts")
	c.hline("                                 at a real model to embed for real.")
	c.hline("  VECTOR_DISTANCE(a, b, metric)  distance between two vectors -- metric is")
	c.hline("                                 \"cosine\", \"l2\", \"l2_squared\", or \"dot\". Smaller =")
	c.hline("                                 closer, so ORDER BY VECTOR_DISTANCE(...) ASC")
	c.hline("                                 LIMIT k gives the k nearest.")
	c.hline("  @vectorize_field(...)          built-in macro that embeds a text field of a")
	c.hline("                                 keyspace in batches (.macro show vectorize_field).")
	c.hline("")
	c.hline("Quick try — rank a tiny corpus against a query; no model, no files:")
	c.hline("")
	c.hline("  WITH docs AS (VECTORIZE_BATCH([{\"id\":1,\"text\":\"the disk is full\"},")
	c.hline("                                 {\"id\":2,\"text\":\"sunny weather today\"},")
	c.hline("                                 {\"id\":3,\"text\":\"no space left on device\"}],")
	c.hline("                                {\"text\":\"text\",\"dim\":16})),")
	c.hline("       q AS (VECTORIZE_BATCH([{\"text\":\"out of disk space\"}],")
	c.hline("                             {\"text\":\"text\",\"dim\":16})[0].vec)")
	c.hline("  SELECT d.id, d.text,")
	c.hline("         ROUND(VECTOR_DISTANCE(d.vec, q, \"cosine\"), 3) AS dist")
	c.hline("    FROM docs d ORDER BY dist ASC;")
	c.hline("")
	c.hline("  (fake vectors are deterministic but not meaningful; use a real model for true")
	c.hline("  semantic matches -- same call, with an endpoint + model in opts:)")
	c.hline("")
	c.hline("  # once: install a local embedding server -- example: ollama pull nomic-embed-text")
	c.hline("  {\"text\":\"text\",")
	c.hline("   \"endpoint\":\"http://localhost:11434/api/embed\",")
	c.hline("   \"model\":\"nomic-embed-text\"}")
	c.hline("")
	c.hline("Store once, search many — build a vector keyspace, then search it:")
	c.hline("")
	c.hline("  # ingest: embed a `line` field, write a Parquet vec keyspace (fast search)")
	c.hline("  INSERT INTO `vecs/data.parquet` (KEY UUID(), VALUE self)")
	c.hline("  SELECT r.id, r.vec")
	c.hline("    FROM @vectorize_field(logs, field => line, id => id,")
	c.hline("                          batch => 256, opts => {\"dim\":16}) AS r;")
	c.hline("")
	c.hline("  # search: embed the query the SAME way -> the 5 nearest ids")
	c.hline("  WITH q AS (VECTORIZE_BATCH([{\"text\":\"disk full\"}],")
	c.hline("                             {\"text\":\"text\",\"dim\":16})[0].vec)")
	c.hline("  SELECT v.id, VECTOR_DISTANCE(v.vec, q, \"cosine\") AS dist")
	c.hline("    FROM vecs v ORDER BY dist ASC LIMIT 5;")
	c.hline("")
	c.hline("Tips:")
	c.hline("  - Embed the corpus and the query with the SAME model + opts, or the distances")
	c.hline("    are meaningless.")
	c.hline("  - Store as `.parquet` for fast search over large vector sets; `.jsonl` works too.")
	c.hline("  - Keep an id next to the vector (a string doc key or a number -- both are fine)")
	c.hline("    so queries return which rows matched, then fetch the full docs by id.")
	c.hline("  - See  .macro show vectorize_field  for the ingest macro's full options.")
}

func (c *cli) helpExtensions() {
	c.hline("extensions — your own JS functions, loaded from files")
	c.hline("")
	c.hline("Write a small JS file; its SUFFIX picks the kind; the function name is the file")
	c.hline("stem. Load a dir or file at startup with  -ext <path>  (repeatable, comma-ok), or")
	c.hline("at the prompt with  .extensions load <path>.")
	c.hline("")
	c.hline("SCALAR UDF  (foo.js) — a value in, a value out. File  initials.js :")
	c.hline("  function initials(name) {")
	c.hline("    return name.split(\" \").map(function(w){ return w[0]; }).join(\"\").toUpperCase();")
	c.hline("  }")
	c.hline("  initials.examples = [ { in: [\"Ada Lovelace\"], out: \"AL\" } ];  // self-doc + golden")
	c.hline("")
	c.hline("  SELECT p.name, initials(p.name) AS ini FROM people p;")
	c.hline("")
	c.hline("AGGREGATE  (foo.agg.js) — init / update(state,value) / final(state). File  maxlen.agg.js :")
	c.hline("  function maxlen_init()       { return 0; }")
	c.hline("  function maxlen_update(s, v) { return v.length > s ? v.length : s; }")
	c.hline("  function maxlen_final(s)     { return s; }")
	c.hline("  var examples = [ { in: [\"a\",\"bbb\",\"cc\"], out: 3 } ];")
	c.hline("")
	c.hline("  SELECT maxlen(l.msg) AS longest FROM logs l;")
	c.hline("")
	c.hline("KEYSPACE / STREAMING  (foo.stream.js) — emit rows, used in FROM. File  ints.stream.js :")
	c.hline("  function ints(emit, n) { for (var i = 1; i <= n; i++) emit({ i: i, sq: i*i }); }")
	c.hline("  ints.examples = [ { in: [3], out: [ {i:1,sq:1},{i:2,sq:4},{i:3,sq:9} ] } ];")
	c.hline("")
	c.hline("  SELECT x.i, x.sq FROM ints(5) AS x WHERE x.i > 2;")
	c.hline("")
	c.hline("MODULE  (one file, many functions) — a whole family in one namespace file. Set")
	c.hline("exports.functions to an array of {name, fn, marshal?, examples?} entries; the filename")
	c.hline("is just a bundle, each entry names its own SQL function. File  decimal.js :")
	c.hline("  function add(a, b) { /* ...exact base-10 via BigInt... */ }")
	c.hline("  exports.functions = [")
	c.hline("    { name: \"DECIMAL_ADD\", marshal: \"variant\", fn: add,")
	c.hline("      examples: [ { in: [\"0.1\", \"0.2\"], out: { \"$numberDecimal\": \"0.3\" } } ] },")
	c.hline("    { name: \"DECIMAL_CMP\", marshal: \"json\", fn: cmp },")
	c.hline("  ];")
	c.hline("")
	c.hline("  SELECT DECIMAL_ADD(\"0.1\", \"0.2\");  -- exact 0.3  (a plain + drifts to 0.30000000000000004)")
	c.hline("")
	c.hline("  `marshal` is how values cross the JS boundary: \"json\" (default), \"variant\"")
	c.hline("  (VARIANT-typed values as EJSON-tagged JSON, e.g. {\"$numberDecimal\":\"...\"}), or \"raw\".")
	c.hline("  A .js file that does NOT set exports.functions stays a single scalar UDF (above).")
	c.hline("  An entry's `kind` may be \"aggregate\" (init/update/final callbacks) or \"stream\" (a")
	c.hline("  source fn using emit), so one module can mix scalar + aggregate + streaming, e.g.:")
	c.hline("    { name: \"DECIMAL_SUM\", kind: \"aggregate\", init: fi, update: fu, final: ff }")
	c.hline("    { name: \"GEN\",         kind: \"stream\",    fn: function(emit, n){ ... } }")
	c.hline("")
	c.hline("  EJSON tags: in a UDF the host `ejson` helper wraps/reads them — ejson.decimal(x),")
	c.hline("  ejson.unwrap(x), ejson.decode(x) (strip tags → plain JSON). SQL++ has the same via")
	c.hline("  the shipped builtin_ejson module: EJSON_DECODE / EJSON_DECIMAL / EJSON_UNWRAP.")
	c.hline("  Shipped builtin modules are named builtin_*.js so an embedder can load them by glob")
	c.hline(fmt.Sprintf("  (%s -ext '<dir>/builtin_*.js'): builtin_decimal (DECIMAL_*), builtin_ejson.", prog))
	c.hline("")
	c.hline("Two more kinds have their own guides:")
	c.hline("  foo.extract.js  frame a raw file into rows      — see .help extract")
	c.hline("  foo.macro.js    expand @foo(...) into SQL++     — see .help macro")
	c.hline("")
	c.hline("GOLDEN EXAMPLES — every kind (scalar, aggregate, source, extract recipe, macro) can")
	c.hline("carry an `examples` array of {in, out} for self-documentation of the function AND")
	c.hline("for quick sanity tests; a MODULE declares them per entry (examples: [...] on each).")
	c.hline(fmt.Sprintf("  %s -ext ./my-udfs -c 'SELECT initials(\"Grace Hopper\")'   # load + run", prog))
	c.hline("  .extensions list             # all loaded, every kind (incl. built-in macros)")
	c.hline("  .extensions show [name]      # print one extension's full source")
	c.hline("  .extensions examples [name]  # print the examples for an extension")
	c.hline("  .extensions test [name]      # run an extension + check every in -> out")
	c.hline("  .extensions unload <name>    # drop an extension from the session")
}

func (c *cli) helpQuoting() {
	c.hline("quoting — tips")
	c.hline("")
	c.hline("1) In SQL++: backtick a dotted keyspace or a reserved/odd field name:")
	c.hline("")
	c.hline("     SELECT COUNT(*) FROM `ap_server.error`;")
	c.hline("     SELECT errs.`level` FROM `app_server.error` errs")
	c.hline("")
	c.hline("2) In a shell -c: backticks are command-substitution inside \"double quotes\", so")
	c.hline("   'single-quote' the whole -c arg (backticks stay literal), or use -f <file>:")
	c.hline("")
	c.hline(fmt.Sprintf("     %s -c 'SELECT COUNT(*) FROM `app_server.error`' my-logs", prog))
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
	c.hline("")
	c.hline("Example queries (say a keyspace `logs` of {sev, node, msg} records):")
	c.hline("  SELECT COUNT(*) FROM logs;                                 -- how many records")
	c.hline("  SELECT l.* FROM logs l WHERE l.sev = \"ERROR\" LIMIT 10;     -- filter + peek")
	c.hline("  SELECT l.node, COUNT(*) AS n FROM logs l                   -- group + rank")
	c.hline("    GROUP BY l.node ORDER BY n DESC LIMIT 5;")
	c.hline("  SELECT l.msg FROM logs l WHERE l.msg LIKE \"%disk%\";        -- substring match")
	c.hline("  SELECT RAW l.msg FROM logs l WHERE l.sev = \"ERROR\";        -- bare values, no wrapper")
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
