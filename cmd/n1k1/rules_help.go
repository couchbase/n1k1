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

// cmdRulesHelp prints the self-contained .multi guide to c.out: the subcommand +
// flag one-liners, a sample collection directory layout, an annotated sample recipe (the
// real front-matter / SQL / @fixture / @expect format), TRUTHFUL example outputs (the
// exact shapes .multi list/run/lint/test produce over the shipped testdata collection),
// and authoring tips for getting the best out of a collection. It goes to stdout (not
// stderr) so it can be piped/paged like any other document.
func (c *cli) cmdRulesHelp() {
	// Fprintf with "%s" (not Fprint) because the text embeds "%" tokens (e.g. LIKE
	// '%panic%'), which vet's printf check would otherwise flag as format directives.
	fmt.Fprintf(c.out, "%s", rulesHelpText)
}

// rulesHelpText avoids backticks so it can be one clean raw string literal; inline
// code is shown quoted or as indented blocks.
const rulesHelpText = `.multi -- run a multi-query pack of SQL++ queries over a dataset

"multi" is short for multi-query: a pack of related SELECTs run together with SHARED
execution (multi-query optimization -- one scan feeds many queries: broadcast, predicate
index, common-subexpression sharing). The queries are provided as a directory of *.sql++
files. Each *.sql++ file is a single SQL++ SELECT query plus optional "-- key: value"
front-matter and an optional inline golden fixture. Run the pack over a dataset to get
tagged findings; lint the queries for a report card; unit-test each query against its
golden fixture (such as for CI). (Renamed from ".rules" / RULE_MATCHES.)

The same feature is also available directly in SQL++ as a composable FROM source --
the MULTI_MATCHES() table-valued function -- so that results can be further queried with
WHERE / GROUP BY / ORDER BY / JOIN and PREPARE'd / EXECUTE'd, e.g.:
  SELECT f.label, COUNT(*) AS hits FROM MULTI_MATCHES('my-queries/') AS f GROUP BY f.label;

COMMANDS
  .multi list --queries <dir>                      inventory the queries (metadata only: no dataset, no compile)
  .multi run  --queries <dir> [--bind <manifest>]  compile & execute the queries over the open dataset
  .multi lint --queries <dir> [--bind <manifest>]  authoring report card (compiles, does NOT run)
  .multi explain --queries <dir> [--bind <manifest>]  show the fused shared-scan plan + fusion map (compiles, does NOT run)
  .multi test --queries <dir> [--update]           golden-fixture runner (CI): check @fixture vs @expect
  .multi help                                      this guide

FLAGS
  --queries <dir>    directory of *.sql++ query files (required). REPEATABLE, and accepts a comma-list, so
                     several tiers fuse into ONE shared-scan pack: --queries a --queries b, or --queries a,b
  --bind <manifest>  map LOGICAL keyspace names (FROM <logical>) to per-dataset globs, so one collection
                     runs across differently-named datasets unchanged (run / lint). Manifest is either
                     "logical = glob" lines ('#' comments + blanks ignored), or a JSON object
                     {"logical":"glob", ...}. A logical keyspace matching 0 files is a hard error.
  --update           .multi test only: (re-)record each fixture's produced findings as its @expect golden

QUERIES DIRECTORY LAYOUT
  my-queries/
    disk_full.sql++      one SQL++ per file (the filename stem is the fallback label)
    slow_request.sql++
    manifest             optional --bind map, e.g.:
                           logs     = **/*.log
                           requests = http/*.json

ANNOTATED RECIPE (my-queries/disk_full.sql++)
The front-matter of a *.sql++ file has leading '-- key: value' lines...
  -- label:       ET-12345          # label       -> the finding Label (else the filename stem)
  -- description: disk-full errors  # description -> a free-form summary, reported by list / lint
  -- source:      logs              # source      -> the LOGICAL keyspace this SQL++ reads (FROM logs)
  -- gate:        l.sev = "ERROR"   # gate        -> a cheap NECESSARY precondition (see GATE below)
  -- tags:        ["disk","io"]     # tags        -> freeform labels (a JSON array or comma-separated)
  -- A disk-full error query.       # free form comment lines (ignored)
  SELECT l.msg, l.ts FROM logs l WHERE l.sev = "ERROR"   # the query: ONE SELECT
                                 # (the first non key:value line ends the front-matter)
  -- @fixture                    # golden test INPUT: one JSON doc per line (fed as the source keyspace)
  -- {"sev":"ERROR","msg":"disk full","ts":3}   # data lines are SQL comments, so the
  -- {"sev":"WARN","msg":"ok","ts":5}           # whole *.sql++ file stays valid SQL++
  -- {"sev":"ERROR","msg":"oom","ts":9}
  -- @expect                     # golden findings the query MUST reproduce (compared as a set)
  -- {"label":"ET-12345","result":{"msg":"disk full","ts":3}}
  -- {"label":"ET-12345","result":{"msg":"oom","ts":9}}

EXAMPLE: .multi list --queries ./my-queries   (box at a TTY; jsonlines when piped)
  {"label":"ET-12345","source":"logs","description":"disk-full errors","tags":"disk,io","fixture?":"yes","golden?":"yes","path":".../disk_full.sql++"}
  {"label":"ET-20001","source":"requests","description":"-","tags":"-","fixture?":"yes","golden?":"yes","path":".../slow_request.sql++"}
  {"label":"ET-30002","source":"logs","description":"-","tags":"-","fixture?":"no","golden?":"no","path":".../warn_no_fixture.sql++"}
  3 query/queries in ./my-queries -- 2 with a fixture, 2 with a golden (run .multi lint for a health report)

EXAMPLE: .multi run --queries ./my-queries   (over a dataset with a "logs" keyspace)
  loaded: 3 query/queries -- 2 fused, 0 standalone, 1 rejected
    ET-20001: plan error: Keyspace not found requests
        not a runnable query: plan error: Keyspace not found requests. A query is a single SELECT, ...
  {"label":"ET-12345","result":{"sev":"ERROR","msg":"disk full","ts":3}}
  {"label":"ET-12345","result":{"sev":"ERROR","msg":"timeout","ts":5}}
  2 finding(s) from 3 query/queries

EXAMPLE: .multi lint --queries ./my-queries   (a report-card row + the score line)
  {"query":"ET-12345","class":"fused","keyspace":"default:logs","lane":"native","index":"literal \"ERROR\"","reason":"-","advice":"-"}
  ...
  score: 66% fused (2/3), 100% native (2/2 converted), 100% index-pruned (2/2 fused)  [0 standalone, 1 rejected]

EXAMPLE: .multi test --queries ./my-queries
  ET-12345: PASS (2 finding(s))
  ET-20001: PASS (2 finding(s))
  ET-30002: no fixture
  2 passed / 0 failed / 1 no-fixture / 0 skipped
  # A mismatch prints a per-finding diff plus: "re-record the golden: .multi test --update".
  # A fixture with no @expect FAILs with: "Capture them: .multi test --update".

TIPS (get the best out of a collection)
  - Lead a predicate with a DISCRIMINATING LITERAL as a top-level AND conjunct so the predicate
    index prunes wake-ups, e.g. "... AND msg LIKE '%panic%'" -- otherwise the query wakes on every row.
  - Keep a query SINGLE-SOURCE filter+project (SELECT ... FROM one WHERE ...) so it FUSES into
    the shared scan. A GROUP BY / window / join / DISTINCT / ORDER-LIMIT / index-scan runs standalone.
  - For grep -A/-B/-C style CONTEXT (the matching line + surrounding lines), use a sliding-window
    match flag (see CONTEXT below) -- and PARTITION BY _meta.` + "`path`" + ` on a multi-file keyspace, or
    context LEAKS across rotated files.
  - Prefer NATIVE expressions over boxed ones: "msg LIKE '%x%'", CONTAINS or "regexp_contains(msg,'x')" instead of
    a multi-wildcard "msg LIKE '%a%b%'". A boxed expression falls back to cbq and caps the compile level.
  - Give EVERY query *.sql a golden fixture (-- @fixture / -- @expect) so CI (.multi test) protects it
    against a regression. Capture the first golden with ".multi test --update".
  - Author against LOGICAL keyspaces + a --bind manifest, so ONE collection of *.sql++ queries can run
    across differently-named datasets (indexer.log vs indexer.0023.log) unchanged.
  - Data drift -- field-shape changes across source data releases are handled by evolving the
    queries (or the *.extract.js recipes), not by writing per-version adapters.
  - RESERVED WORDS: field names that are SQL++ keywords must be BACKTICKED, or the query fails to
    parse. The built-in log recipe emits "level" (reserved: ISOLATION LEVEL) -- write WHERE l.` + "`level`" + ` = "error".
    Common offenders: "level", "keys", and natural aliases like "prev" (... AS ` + "`prev`" + `).

TEMPORAL (ASOF) -- nearest-preceding correlation across two log streams (correlate an
error with the step that preceded it) lowers a correlated argmax subquery to a streaming
merge-join instead of an O(n^2) scan. To QUALIFY, the subquery must be:
  - a BARE subquery term (SELECT ... ), single keyspace, ONE scalar projection;
  - LIMIT 1, no OFFSET, and either PRECEDING (ORDER BY <key> DESC + WHERE r.<key> <= e.<key>)
    or FOLLOWING (ORDER BY <key> ASC + WHERE r.<key> >= e.<key>) -- "the nearest R at/before"
    or "at/after" each E row;
  - optional partition equalities r.<eqk> = e.<eqk>; and a soft bound (a max gap Δt in ns):
    preceding look-BACK r.<key> >= e.<key> - Δt, or following look-AHEAD r.<key> <= e.<key>
    + Δt ("the nearest R within Δt after each E" -- the XYZ-then-ABC-soon-after shape);
  - optional CONTENT filter(s) on the inner stream referencing ONLY r (e.g.
    r.msg LIKE "%ABC%", r.state = "done") -- pushed onto the build scan so the merge finds
    the nearest matching row ("the last <ABC> before this error" / "the next <ABC> after");
  - and BOTH keyspaces must expose SORTED-SOURCE metadata -- i.e. an extract recipe with
    a "time:" field (normalized to the int64 sort key) and an "order:" (.extract help).
An outer WHERE on the driving stream is fine. If it does NOT lower, ".verbose on" prints
"argmax subquery NOT lowered to ASOF ...: <the gate that stopped it>", e.g. an unproven
sort key -- so you know before running a slow query. Examples:
  -- PRECEDING: the last rebalance state in effect when each error was logged.
  SELECT e.ts, (SELECT r.msg FROM state r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1) AS prev
  FROM errors e WHERE regexp_contains(e.msg, "Terminate")

  -- FOLLOWING + look-ahead + content residual: "XYZ then ABC within 5s after, same node"
  -- (did an ABC recovery line follow each XYZ error soon after, on that node?). ASC + >=
  -- is following; "r.ts <= e.ts + 5000000000" is the within-5s look-ahead; "r.node =
  -- e.node" partitions; "r.msg LIKE ..." is a right-only residual pushed to the build.
  SELECT e.ts, e.node,
    (SELECT r.msg FROM recovery r
     WHERE r.ts >= e.ts AND r.ts <= e.ts + 5000000000
       AND r.node = e.node AND r.msg LIKE "%ABC%"
     ORDER BY r.ts ASC LIMIT 1) AS abc_after
  FROM errors e WHERE e.msg LIKE "%XYZ%"

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
  in unrelated lines from another, so context LEAKS across files (WRONG result).
  Partitioning by _meta.` + "`path`" + ` isolates each file's context. ***
("path" is a reserved word, hence backticked. A context query has an OVER clause, so it
runs standalone -- its own scan, not fused; GATE it, below, so it only sorts keyspaces that
can match.) For CHRONOLOGICAL context that spans rotated files, order instead by an
extract-recipe "time:" key (.extract help) -- one sortable timeline across the whole keyspace.

GATE (index-gate a standalone query) -- a fused filter+project query is pruned per
row by the predicate index, but a STANDALONE query (window / GROUP BY / join -- anything
with its own scan) is not. A "gate:" front-matter line gives it a cheap NECESSARY
precondition: a boolean SQL++ expression over its "source" keyspace that MUST hold for any
finding. Before running the (expensive) query, .multi run probes
"SELECT 1 FROM <source> WHERE <gate> LIMIT 1"; if no row matches, the query is SKIPPED --
its sort/window never touches a keyspace that cannot produce a finding. Example, gating the
CONTEXT query above so it only sorts files that actually contain an ERROR:
  -- source: logs
  -- gate:   sev = "ERROR"
Needs "source:". The gate must be NECESSARY (skipping is only correct if no finding is
possible without it) -- e.g. do NOT gate an ABSENCE query ("... HAVING COUNT(...) = 0")
on the thing it counts. A skipped query is reported ("gated: N skipped"), never silent;
a gate that errors runs the query anyway (safe). Gate literals are pushed to the probe's
scan, so a discriminating gate is itself index-pruned.

Non-interactive (CI / agent):
  n1k1 -c '.multi run --queries ./my-queries --bind ./manifest' <data-dir>
`
