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
// extract-recipe analogue of .multi -- a single place documenting the ExtractSpec
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

A recipe supplies describe(file), extract(file, emit), or extractStream(file, emit):
  - describe(file), run ONCE per matched file (cold path), returns a DECLARATIVE spec
    n1k1 then applies NATIVELY per record -- no per-row JS, so a 400 MB log frames at
    full speed. This is the preferred path for line/multiline/section-framed text.
  - extract(file, emit) is the imperative escape hatch: JS gets the WHOLE file and
    emits records itself, so it owns framing AND parsing -- for a self-contained or
    irregular format a declarative spec can't frame (see IMPERATIVE EXTRACT below).
  - extractStream(file, emit) is the STREAMING form: JS reads incrementally
    (file.readLine) and emits records that flow out one at a time with backpressure,
    so a large multi-record file frames at bounded memory (see IMPERATIVE EXTRACT).
A recipe's match may claim a BRAND-NEW extension (e.g. ".toml2"); the claim is what
makes such files records at all.

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
    return { format: "my_log",
             framing: {...},
             fields: {...},
             time: {...},
             order: {...}
           };
  }

FRAMING (how a file's bytes split into records) -- framing.kind is one of:
  line       one record per line.                          { kind: "line" }
  multiline  a lead line + continuation lines; a line is   { kind: "multiline",
             a lead iff it matches fields.pattern (robust    continuation: "^\\s|^\\[" }
             even when a continuation starts with '[').
  json       JSONL: one JSON object per line; time field   { kind: "json" }
             is normalized in place to the int64 sort key.
  section    ====-banner blocks -> one {title,text} record { kind: "section",
             per section (cbcollect couchbase.log). title    section: "^={10,}$" }
             is the command between banner rules.
  whole      one record for the whole file (office/PDF     { kind: "whole" }
             baseline; text under "text").
  opaque     intentionally UNframable (a binary profile,   { kind: "opaque",
             a compressed blob): ONE {kind:"opaque",note}    note: "binary CPU profile" }
             row, no content read. Keeps the file out of
             .tables' "add a recipe" nudge + documents it.

An OPTIONAL framing.banner regexp (line/multiline) drops a non-data separator line
(cbbrowse_logs' "==== couchbase logs ====" header) so it doesn't inflate COUNT(*)/.schema.

FIELDS (lift typed columns out of each framed record) -- native byte-regex, off the
boxed lane. One regexp with named captures; each (?P<name>...) becomes a field:
  fields: { pattern: "^(?P<ts>\\S+) (?P<level>\\S+) (?P<node>\\S+) (?P<msg>.*)" }
A record that doesn't match degrades to {"text": <raw>} so nothing is dropped.
Captures are STRINGS by default; fields.types declares a numeric/bool field so it's a
JSON number/bool -- "WHERE count > 1000" then compares numerically & stays native:
  fields: { pattern: "...", types: { count: "int", inuse_bytes: "int", ratio: "float" } }

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

IMPERATIVE EXTRACT (extract(file, emit)) -- for a self-contained/irregular format a
declarative spec can't frame (a whole document like TOML, a stateful multiline, a blob
you crack yourself). Define extract INSTEAD OF (or alongside) describe:
  file  = { path, name, ext, stem, text }  -- text is the WHOLE decompressed file.
  emit(doc [, id])  -- push one record. doc is any JSON-able value; the optional id
                       overrides the default (the file stem for a single record, else
                       "<prefix>#<n>"). The host JSON-canonicalizes doc (sorted keys).
  // Parse a whole document yourself and emit it as one record keyed by the stem:
  var match = { exts: [".toml2"], priority: 10 };
  function extract(file, emit) { emit(parseTOML(file.text), file.stem); }
extract buffers its records, paying the JS boundary once per file (not per row). See
extensions/extract_recipes/toml2.extract.js for a full TOML parser that matches n1k1's
native .toml reader.

STREAMING (extractStream(file, emit)) -- for a LARGE multi-record file that shouldn't be
buffered. Instead of file.text, read incrementally:
  file.readLine()      -- the next line (without newline), or null at EOF; "" for a blank
                          line (use it as a record boundary).
  file.readAll()       -- the rest of the file as one string.
  file.readBytes(n)    -- up to n RAW bytes as an ArrayBuffer (or null at EOF) -- the
                          GENERAL primitive: frame any binary/length-prefixed/fixed-width
                          format. Wrap it in JS: new Uint8Array(buf) / new DataView(buf).
                          (readLine/readAll are the text conveniences built on this idea.)
Emitted records flow out one at a time with BACKPRESSURE (bounded memory, any file size);
emit(doc[, id]) returns FALSE once the consumer stops (a LIMIT is met, the query is
cancelled), so your loop can break. ids default to "<prefix>#<n>". Example (blank-line-
delimited "key: value" stanzas):
  var match = { exts: [".stanza"], priority: 10 };
  function extractStream(file, emit) {
    var rec = null, line;
    while ((line = file.readLine()) !== null) {
      if (line.trim() === "") { if (rec && !emit(rec)) return; rec = null; continue; }
      var i = line.indexOf(":"); if (i < 0) continue;
      (rec = rec || {})[line.slice(0,i).trim()] = line.slice(i+1).trim();
    }
    if (rec) emit(rec);
  }
Define describe, extract, OR extractStream (extract and extractStream are mutually
exclusive). See extensions/extract_recipes/stanza.extract.js.

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
  # then: SELECT a.node, a.msg FROM myapp a WHERE a.` + "`level`" + ` = "ERROR" ORDER BY a.ts

Golden examples: an "examples" array ({in: "<sample file text>", out: [rows]}) both
documents a recipe and golden-tests it -- run with  .extensions test [name].
`
