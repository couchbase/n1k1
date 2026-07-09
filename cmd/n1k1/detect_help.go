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

// cmdDetectHelp prints the self-contained .detect guide to c.out: the subcommand +
// flag one-liners, a sample corpus directory layout, an annotated sample recipe (the
// real front-matter / SQL / @fixture / @expect format), TRUTHFUL example outputs (the
// exact shapes .detect list/run/lint/test produce over the shipped testdata corpus),
// and authoring tips for getting the best out of a corpus. It goes to stdout (not
// stderr) so it can be piped/paged like any other document.
func (c *cli) cmdDetectHelp() {
	// Fprintf with "%s" (not Fprint) because the text embeds "%" tokens (e.g. LIKE
	// '%panic%'), which vet's printf check would otherwise flag as format directives.
	fmt.Fprintf(c.out, "%s", detectHelpText)
}

// detectHelpText avoids backticks so it can be one clean raw string literal; inline
// code is shown quoted or as indented blocks.
const detectHelpText = `.detect -- PREPARE++ detector corpus (DESIGN-prepare.md phases 6-7)

A CORPUS is a directory of *.sql++ RECIPE files. Each recipe is a single SQL++ SELECT
(a "detector") plus optional "-- key: value" front-matter and an optional inline golden
fixture. Run a corpus over an open support bundle to get tagged findings; lint it for an
authoring report card; unit-test each detector against its golden fixture (CI).

COMMANDS
  .detect list  [--corpus <dir>]                     inventory the corpus (metadata only: no bundle, no compile)
  .detect run   --corpus <dir> [--bind <manifest>]   compile the corpus over the open bundle -> findings
  .detect lint  --corpus <dir> [--bind <manifest>]   authoring report card (compiles, does NOT run)
  .detect test  [--corpus <dir>] [--update]          golden-fixture runner (CI): check @fixture vs @expect
  .detect help                                        this guide

FLAGS
  --corpus <dir>     the directory of *.sql++ recipe files (required)
  --bind <manifest>  map LOGICAL keyspace names (FROM <logical>) to per-bundle globs, so one corpus
                     runs across differently-named bundles unchanged (run / lint). Manifest is either
                     "logical = glob" lines ('#' comments + blanks ignored), or a JSON object
                     {"logical":"glob", ...}. A logical keyspace matching 0 files is a hard error.
  --update           .detect test only: (re-)record each fixture's produced findings as its @expect golden

CORPUS LAYOUT
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

EXAMPLE: .detect list --corpus ./detectors   (box at a TTY; jsonlines when piped)
  {"tag":"ET-12345","source":"logs","severity":"high","versions":"7.2,7.6","fixture?":"yes","golden?":"yes","path":".../disk_full.sql++"}
  {"tag":"ET-20001","source":"requests","severity":"medium","versions":"-","fixture?":"yes","golden?":"yes","path":".../slow_request.sql++"}
  {"tag":"ET-30002","source":"logs","severity":"low","versions":"-","fixture?":"no","golden?":"no","path":".../warn_no_fixture.sql++"}
  3 detector(s) in ./detectors -- 2 with a fixture, 2 with a golden (run .detect lint for a health report)

EXAMPLE: .detect run --corpus ./detectors   (over a bundle with a "logs" keyspace)
  corpus: 3 detector(s) -- 2 fused, 0 standalone, 1 rejected
    ET-20001: plan error: Keyspace not found requests
        not a runnable detector: plan error: Keyspace not found requests. A detector is a single SELECT, ...
  {"tag":"ET-12345","evidence":{"sev":"ERROR","msg":"disk full","ts":3}}
  {"tag":"ET-12345","evidence":{"sev":"ERROR","msg":"timeout","ts":5}}
  2 finding(s) from 3 detector(s)

EXAMPLE: .detect lint --corpus ./detectors   (a report-card row + the score line)
  {"detector":"ET-12345","class":"fused","keyspace":"default:logs","lane":"native","index":"literal \"ERROR\"","reason":"-","advice":"-"}
  ...
  score: 66% fused (2/3), 100% native (2/2 converted), 100% index-pruned (2/2 fused)  [0 standalone, 1 rejected]

EXAMPLE: .detect test --corpus ./detectors
  ET-12345: PASS (2 finding(s))
  ET-20001: PASS (2 finding(s))
  ET-30002: no fixture
  2 passed / 0 failed / 1 no-fixture / 0 skipped
  # A mismatch prints a per-finding diff plus: "re-record the golden: .detect test --update".
  # A fixture with no @expect FAILs with: "Capture them: .detect test --update".

TIPS (get the best out of a corpus)
  - Lead a predicate with a DISCRIMINATING LITERAL as a top-level AND conjunct so the predicate
    index prunes wake-ups, e.g. "... AND msg LIKE '%panic%'" -- otherwise the detector wakes on every row.
  - Keep a detector SINGLE-SOURCE filter+project (SELECT ... FROM one WHERE ...) so it FUSES into
    the shared scan. A GROUP BY / window / join / DISTINCT / ORDER-LIMIT / index-scan runs standalone.
  - Prefer NATIVE expressions over boxed ones: "msg LIKE '%x%'" or "regexp_contains(msg,'x')" instead of
    "UPPER(msg) LIKE '%X%'". A boxed expression falls back to cbq and caps the compile level.
  - Give EVERY detector a golden fixture (-- @fixture / -- @expect) so CI (.detect test) protects it
    against a regression. Capture the first golden with ".detect test --update".
  - Author against LOGICAL keyspaces + a --bind manifest, so ONE corpus runs across differently-named
    bundles (indexer.log vs projector.log) unchanged.
  - Version-tag detectors ("versions:") -- field-shape changes across releases are handled by evolving
    the corpus, not by writing per-version adapters.

Non-interactive (CI / agent):
  n1k1 -c '.detect run --corpus ./detectors --bind ./manifest' <bundle-dir>
`
