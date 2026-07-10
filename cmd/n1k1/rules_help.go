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

import "fmt"

// cmdRulesHelp prints the self-contained .rules guide to c.out: the subcommand +
// flag one-liners, a sample collection directory layout, an annotated sample recipe (the
// real front-matter / SQL / @fixture / @expect format), TRUTHFUL example outputs (the
// exact shapes .rules list/run/lint/test produce over the shipped testdata collection),
// and authoring tips for getting the best out of a collection. It goes to stdout (not
// stderr) so it can be piped/paged like any other document.
func (c *cli) cmdRulesHelp() {
	// Fprintf with "%s" (not Fprint) because the text embeds "%" tokens (e.g. LIKE
	// '%panic%'), which vet's printf check would otherwise flag as format directives.
	fmt.Fprintf(c.out, "%s", rulesHelpText)
}

// rulesHelpText avoids backticks so it can be one clean raw string literal; inline
// code is shown quoted or as indented blocks.
const rulesHelpText = `.rules -- run a collection of SQL++ detectors over a dataset

A COLLECTION is a directory of *.sql++ RECIPE files. Each recipe is a single SQL++ SELECT
(a "detector") plus optional "-- key: value" front-matter and an optional inline golden
fixture. Run a collection over an open dataset to get tagged findings; lint it for an
authoring report card; unit-test each detector against its golden fixture (CI).

The same findings are also available directly in SQL++ as a composable FROM source --
the RULE_MATCHES() table-valued function -- so they can be sliced with WHERE / GROUP BY /
ORDER BY / JOIN and PREPARE'd / EXECUTE'd, e.g.:
  SELECT f.tag, COUNT(*) AS hits FROM RULE_MATCHES('detectors/') AS f GROUP BY f.tag;
(.rules run streams; RULE_MATCHES() materializes the whole result set as one array.)

COMMANDS
  .rules list  [--queries <dir>]                     inventory the collection (metadata only: no dataset, no compile)
  .rules run   --queries <dir> [--bind <manifest>]   compile the collection over the open dataset -> findings
  .rules lint  --queries <dir> [--bind <manifest>]   authoring report card (compiles, does NOT run)
  .rules test  [--queries <dir>] [--update]          golden-fixture runner (CI): check @fixture vs @expect
  .rules help                                        this guide

FLAGS
  --queries <dir>     the directory of *.sql++ recipe files (required)
  --bind <manifest>  map LOGICAL keyspace names (FROM <logical>) to per-dataset globs, so one collection
                     runs across differently-named datasets unchanged (run / lint). Manifest is either
                     "logical = glob" lines ('#' comments + blanks ignored), or a JSON object
                     {"logical":"glob", ...}. A logical keyspace matching 0 files is a hard error.
  --update           .rules test only: (re-)record each fixture's produced findings as its @expect golden

COLLECTION LAYOUT
  detectors/
    disk_full.sql++      one recipe per file (the filename stem is the fallback tag)
    slow_request.sql++
    manifest             optional --bind map, e.g.:
                           logs     = **/*.log
                           requests = http/*.json

ANNOTATED RECIPE (detectors/disk_full.sql++)
  -- ticket:   ET-12345        # front-matter (leading -- key: value lines):
  -- severity: high            #   ticket   -> the finding Tag (else the filename stem)
  -- source:   logs            #   source   -> the LOGICAL keyspace this detector reads (FROM logs)
  -- gate:     l.sev = "ERROR" #   gate     -> a cheap NECESSARY precondition (see GATE below)
  -- versions: ["7.2","7.6"]   #   severity -> advisory, reported by list / lint
  -- tags:     ["disk","io"]   #   versions -> software versions this detector targets
  -- A disk-full error detector.  # tags     -> freeform labels
  SELECT l.msg, l.ts FROM logs l WHERE l.sev = "ERROR"   # the detector: ONE SELECT
                                 # (the first non key:value line ends the front-matter)
  -- @fixture                  # golden test INPUT: one JSON doc per line (fed as the source keyspace)
  {"sev":"ERROR","msg":"disk full","ts":3}
  {"sev":"WARN","msg":"ok","ts":5}
  {"sev":"ERROR","msg":"oom","ts":9}
  -- @expect                   # golden findings the detector MUST reproduce (compared as a set)
  {"tag":"ET-12345","evidence":{"msg":"disk full","sev":"ERROR","ts":3}}
  {"tag":"ET-12345","evidence":{"msg":"oom","sev":"ERROR","ts":9}}

EXAMPLE: .rules list --queries ./detectors   (box at a TTY; jsonlines when piped)
  {"tag":"ET-12345","source":"logs","severity":"high","versions":"7.2,7.6","fixture?":"yes","golden?":"yes","path":".../disk_full.sql++"}
  {"tag":"ET-20001","source":"requests","severity":"medium","versions":"-","fixture?":"yes","golden?":"yes","path":".../slow_request.sql++"}
  {"tag":"ET-30002","source":"logs","severity":"low","versions":"-","fixture?":"no","golden?":"no","path":".../warn_no_fixture.sql++"}
  3 detector(s) in ./detectors -- 2 with a fixture, 2 with a golden (run .rules lint for a health report)

EXAMPLE: .rules run --queries ./detectors   (over a dataset with a "logs" keyspace)
  loaded: 3 detector(s) -- 2 fused, 0 standalone, 1 rejected
    ET-20001: plan error: Keyspace not found requests
        not a runnable detector: plan error: Keyspace not found requests. A detector is a single SELECT, ...
  {"tag":"ET-12345","evidence":{"sev":"ERROR","msg":"disk full","ts":3}}
  {"tag":"ET-12345","evidence":{"sev":"ERROR","msg":"timeout","ts":5}}
  2 finding(s) from 3 detector(s)

EXAMPLE: .rules lint --queries ./detectors   (a report-card row + the score line)
  {"detector":"ET-12345","class":"fused","keyspace":"default:logs","lane":"native","index":"literal \"ERROR\"","reason":"-","advice":"-"}
  ...
  score: 66% fused (2/3), 100% native (2/2 converted), 100% index-pruned (2/2 fused)  [0 standalone, 1 rejected]

EXAMPLE: .rules test --queries ./detectors
  ET-12345: PASS (2 finding(s))
  ET-20001: PASS (2 finding(s))
  ET-30002: no fixture
  2 passed / 0 failed / 1 no-fixture / 0 skipped
  # A mismatch prints a per-finding diff plus: "re-record the golden: .rules test --update".
  # A fixture with no @expect FAILs with: "Capture them: .rules test --update".

TIPS (get the best out of a collection)
  - Lead a predicate with a DISCRIMINATING LITERAL as a top-level AND conjunct so the predicate
    index prunes wake-ups, e.g. "... AND msg LIKE '%panic%'" -- otherwise the detector wakes on every row.
  - Keep a detector SINGLE-SOURCE filter+project (SELECT ... FROM one WHERE ...) so it FUSES into
    the shared scan. A GROUP BY / window / join / DISTINCT / ORDER-LIMIT / index-scan runs standalone.
  - For grep -A/-B/-C style CONTEXT (the matching line + surrounding lines), use a sliding-window
    match flag (see CONTEXT below) -- and PARTITION BY _meta.` + "`path`" + ` on a multi-file keyspace, or
    context LEAKS across rotated files.
  - Prefer NATIVE expressions over boxed ones: "msg LIKE '%x%'", CONTAINS or "regexp_contains(msg,'x')" instead of
    a multi-wildcard "msg LIKE '%a%b%'". A boxed expression falls back to cbq and caps the compile level.
  - Give EVERY detector a golden fixture (-- @fixture / -- @expect) so CI (.rules test) protects it
    against a regression. Capture the first golden with ".rules test --update".
  - Author against LOGICAL keyspaces + a --bind manifest, so ONE collection runs across differently-named
    datasets (indexer.log vs projector.log) unchanged.
  - Version-tag detectors ("versions:") -- field-shape changes across releases are handled by evolving
    the collection, not by writing per-version adapters.
  - RESERVED WORDS: field names that are SQL++ keywords must be BACKTICKED, or the detector fails to
    parse. The built-in log recipe emits "level" (reserved: ISOLATION LEVEL) -- write WHERE l.` + "`level`" + ` = "error".
    Common offenders: "level", "keys", and natural aliases like "prev" (... AS ` + "`prev`" + `).

TEMPORAL (ASOF) -- nearest-preceding correlation across two log streams (correlate an
error with the step that preceded it) lowers a correlated argmax subquery to a streaming
merge-join instead of an O(n^2) scan. To QUALIFY, the subquery must be:
  - a BARE subquery term (SELECT ... ), single keyspace, ONE scalar projection;
  - ORDER BY <key> DESC (nearest-preceding) or ASC (following), LIMIT 1, no OFFSET;
  - WHERE r.<key> <= e.<key> (preceding) with the direction matching the ORDER BY;
  - and BOTH keyspaces must expose SORTED-SOURCE metadata -- i.e. an extract recipe with
    a "time:" field (normalized to the int64 sort key) and an "order:" (.extract help).
An outer WHERE on the driving stream is fine. If it does NOT lower, ".verbose on" prints
"argmax subquery NOT lowered to ASOF ...: <the gate that stopped it>", e.g. an unproven
sort key -- so you know before running a slow query. Example:
  SELECT e.ts, (SELECT r.msg FROM state r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1) AS prev
  FROM errors e WHERE regexp_contains(e.msg, "Terminate")

CONTEXT (grep -A/-B/-C) -- emit the matching line PLUS N lines of surrounding context, the
way "grep -C2" does. A sliding window computes a "near a match" FLAG per line; a wrapping
query keeps the lit rows:
  SELECT p, pos, line FROM (
    SELECT _meta.` + "`path`" + ` AS p, _meta.pos AS pos, line,
           MAX(CASE WHEN sev = "ERROR" THEN 1 ELSE 0 END)
             OVER (PARTITION BY _meta.` + "`path`" + ` ORDER BY _meta.pos
                   ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS near
    FROM logs) sub
  WHERE sub.near = 1 ORDER BY p, pos
The frame ends set the context width: "2 PRECEDING" = grep -B2, "2 FOLLOWING" = -A2, both
= -C2. The line ordinal is _meta.pos (each record's 0-based position within its file; run
with "-meta on", or it is present for extracted docs).
  *** PARTITION BY _meta.` + "`path`" + ` IS REQUIRED for a multi-file keyspace (rotated logs:
  indexer.log, indexer.log.1, ...). _meta.pos restarts at 0 per file, so a bare
  ORDER BY _meta.pos INTERLEAVES the files -- a match near the top of one file then pulls
  in unrelated lines from another, so context LEAKS across files (WRONG evidence).
  Partitioning by _meta.` + "`path`" + ` isolates each file's context. ***
("path" is a reserved word, hence backticked. A context detector has an OVER clause, so it
runs standalone -- its own scan, not fused; GATE it, below, so it only sorts keyspaces that
can match.) For CHRONOLOGICAL context that spans rotated files, order instead by an
extract-recipe "time:" key (.extract help) -- one sortable timeline across the whole keyspace.

GATE (index-gate a standalone detector) -- a fused filter+project detector is pruned per
row by the predicate index, but a STANDALONE detector (window / GROUP BY / join -- anything
with its own scan) is not. A "gate:" front-matter line gives it a cheap NECESSARY
precondition: a boolean SQL++ expression over its "source" keyspace that MUST hold for any
finding. Before running the (expensive) detector, .rules run probes
"SELECT 1 FROM <source> WHERE <gate> LIMIT 1"; if no row matches, the detector is SKIPPED --
its sort/window never touches a keyspace that cannot produce a finding. Example, gating the
CONTEXT detector above so it only sorts files that actually contain an ERROR:
  -- source: logs
  -- gate:   sev = "ERROR"
Needs "source:". The gate must be NECESSARY (skipping is only correct if no finding is
possible without it) -- e.g. do NOT gate an ABSENCE detector ("... HAVING COUNT(...) = 0")
on the thing it counts. A skipped detector is reported ("gated: N skipped"), never silent;
a gate that errors runs the detector anyway (safe). Gate literals are pushed to the probe's
scan, so a discriminating gate is itself index-pruned.

Non-interactive (CI / agent):
  n1k1 -c '.rules run --queries ./detectors --bind ./manifest' <data-dir>
`
