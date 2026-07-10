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

// cmdExtract dispatches the .extract command family: an authoring reference for
// *.extract.js recipes (help) and an inventory of the loaded ones (list). It's the
// extract-recipe analogue of .rules -- a single place documenting the ExtractSpec
// surface so writing the first recipe doesn't require reading records/spec.go.
func (c *cli) cmdExtract(arg string) {
	sub, _ := splitFirst(arg)
	switch strings.ToLower(sub) {
	case "", "help":
		fmt.Fprintf(c.out, "%s", extractHelpText)
	case "list", "ls":
		c.extractList()
	default:
		fmt.Fprintf(c.stderr, "unknown subcommand %q; try .extract help\n", sub)
	}
}

// extractList inventories the loaded *.extract.js recipes: each recipe's name, what it
// claims (exts / name-regexps / priority), and where it came from. Goes to stderr so
// it interleaves with other diagnostics. A recipe frames the files it claims into rows
// (see .extract help); a file no recipe claims stays a whole-file blob.
func (c *cli) extractList() {
	recipes := glue.ListExtractRecipes()
	if len(recipes) == 0 {
		fmt.Fprintln(c.stderr, "no *.extract.js recipes loaded (load one with -ext <dir>, or see .extract help)")
		return
	}
	fmt.Fprintf(c.stderr, "%d extract recipe(s) loaded:\n", len(recipes))
	for _, r := range recipes {
		claims := ""
		if len(r.Exts) > 0 {
			claims += "exts=" + strings.Join(r.Exts, ",")
		}
		if len(r.Names) > 0 {
			if claims != "" {
				claims += " "
			}
			claims += "names=" + strings.Join(r.Names, ",")
		}
		fmt.Fprintf(c.stderr, "  %-24s %-40s priority=%d  %s\n", r.Name, claims, r.Priority, r.Source)
	}
}

// extractHelpText is the self-contained *.extract.js authoring reference (DOC-1). No
// backticks so it stays one clean raw string; inline code is quoted or indented.
const extractHelpText = `.extract -- author *.extract.js recipes that frame files into queryable rows

An EXTRACT RECIPE teaches n1k1 how to turn a file the built-ins don't understand (a
log, a command dump, an app-specific format) into RECORDS you can SELECT over. Drop a
"<name>.extract.js" file in a dir and pass it with "-ext <dir>"; it's picked up before
the datastore opens, so a matched file becomes a keyspace (see .tables).

A recipe supplies ONE function, describe(file), run ONCE per matched file (cold path)
in JavaScript; it returns a DECLARATIVE spec that n1k1 then applies NATIVELY per record
-- no per-row JS, so a 400 MB log frames at full speed.

COMMANDS
  .extract help            this guide
  .extract list            the loaded recipes: what each claims (exts/names) + source

RECIPE SHAPE (module scope)
  // WHICH files this recipe claims (records.ExtractMatch). Highest priority wins on
  // overlap; a file matches if its ext is in exts (when given) AND some names regexp
  // matches its dataset-relative path (when given).
  var match = { exts: [".log"], names: ["ns_server\\..*\\.log$"], priority: 20 };

  // describe(file) -> an ExtractSpec object. file = { path, name, ext, head } where
  // head is a decompressed head sample (use it to sniff a format/timezone; describe
  // runs once per file, so this is a cold path -- reading head is fine).
  function describe(file) {
    return { format: "my_log", framing: {...}, fields: {...}, time: {...}, order: {...} };
  }

FRAMING (how a file's bytes split into records) -- framing.kind is one of:
  line        one record per line.                         { kind: "line" }
  multiline   a lead line + continuation lines; a line is  { kind: "multiline",
              a lead iff it matches fields.pattern (robust    continuation: "^\\s|^\\[" }
              even when a continuation starts with '[').
  json        JSONL: one JSON object per line; time field  { kind: "json" }
              is normalized in place to the int64 sort key.
  section     ====-banner blocks -> one {title,text} record { kind: "section",
              per section (cbcollect couchbase.log). title    section: "^={10,}$" }
              is the command between banner rules.
  whole       one record for the whole file (office/PDF     { kind: "whole" }
              baseline; text under "text").

FIELDS (lift typed columns out of each framed record) -- native byte-regex, off the
boxed lane. One regexp with named captures; each (?P<name>...) becomes a field:
  fields: { pattern: "^(?P<ts>\\S+) (?P<level>\\S+) (?P<node>\\S+) (?P<msg>.*)" }
A record that doesn't match degrades to {"text": <raw>} so nothing is dropped.

TIME (normalize the timestamp field to one sortable int64 epoch-NANOS key, so ORDER BY
/ ASOF / merges work across files & nodes). time.field names the captured field;
time.layout is one of:
  "RFC3339"   2026-05-17T15:36:11.198+02:00      "epoch_ms"  milliseconds since epoch
  "epoch_s"   seconds since epoch (may be frac)  "epoch_us"  microseconds since epoch
  "epoch_ns"  nanoseconds since epoch            <other>     a Go reference-time layout
                                                             ("02/Jan/2006:15:04:05 -0700")
  time.tz_default (e.g. "+02:00" / "UTC") is applied when a value carries no zone.

ORDER (declare the file's sortedness so temporal ops can plan) -- order.by is usually
time.field; order.sorted is "strict" | "near" | "none"; order.disorder bounds a "near"
source. describe MEASURES the real sortedness from the head sample, refining this.

PROVENANCE (optional): provenance:{k:v,...} constants lifted once, riding every record.

ANNOTATED EXAMPLE (myapp.log lines: "<RFC3339> <LEVEL> <node> <msg>")
  var match = { exts: [".log"], names: ["myapp\\..*\\.log$"], priority: 20 };
  function describe(file) {
    return {
      format:  "myapp_log",
      framing: { kind: "line" },
      fields:  { pattern: "^(?P<ts>\\S+) (?P<level>\\S+) (?P<node>\\S+) (?P<msg>.*)" },
      time:    { field: "ts", layout: "RFC3339" },
      order:   { by: "ts", sorted: "near" }
    };
  }
  # then:  SELECT a.node, a.msg FROM myapp a WHERE a.` + "`level`" + ` = "ERROR" ORDER BY a.ts

Shipped examples: extensions/extract_recipes/apache_access.extract.js (line + Go layout)
and couchbase_log.extract.js (section framing). Struct source of truth: records/spec.go.

Non-interactive (CI / agent):
  n1k1 -ext ./extractors -c "SELECT COUNT(*) FROM myapp" <data-dir>
`
