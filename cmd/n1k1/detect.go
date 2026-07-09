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

// cli .detect command family (PREPARE++ detector corpus: run + lint).
//
// .detect brings the corpus machinery (glue.CorpusCompile / glue.CorpusLint;
// DESIGN-prepare.md phases 6-7) to the CLI so a tech-support team -- or an AI support
// agent -- can run a corpus of SQL++ "detectors" over a support bundle (the open
// datastore) and get findings, and lint the corpus for authoring feedback. It runs
// interactively AND non-interactively (n1k1 <bundle> -c '.detect run --corpus ./det'),
// so CI / an agent drives it the same way.
//
// A CORPUS is a flat directory of *.sql++ files: each file is one detector; its Tag
// is the filename stem; its statement is the file contents. (Front-matter / golden
// fixtures -- phase 7's recipe format -- are DEFERRED; a corpus is just SQL++ files.)
//
// SUBCOMMANDS:
//
//	.detect run  --corpus <dir> [--bind <manifest>]  -- compile the corpus over the
//	    open bundle, print a fail-loud coverage/health summary to stderr, then render
//	    the tagged findings to stdout in the current output mode.
//	.detect lint --corpus <dir> [--bind <manifest>]  -- the authoring report card:
//	    per-detector class (fused/standalone/rejected), target keyspace, eval lane
//	    (native/boxed), predicate-index verdict (literal vs always-wake) and advice,
//	    plus a corpus score (% fused / native / index-pruned).
//
// DEFERRED (noted): .detect test (golden fixtures, phase 7); .detect bind (dry-run --
// binding already fails loud at run); per-finding STREAMING (findings are batch-
// rendered via the current output mode -- jsonlines still streams the row table; a
// per-finding OnRow hook is a nice-to-have); the SHA-keyed build cache; and the
// re-run delta report.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/couchbase/n1k1/glue"
)

// cmdDetect dispatches the .detect command family (run | lint | help).
func (c *cli) cmdDetect(arg string) {
	sub, rest := splitFirst(arg)
	switch strings.ToLower(sub) {
	case "run":
		c.cmdDetectRun(rest)
	case "lint":
		c.cmdDetectLint(rest)
	case "", "help":
		c.cmdDetectHelp()
	default:
		fmt.Fprintf(c.stderr, "unknown subcommand %q; try .detect help\n", sub)
	}
}

func (c *cli) cmdDetectHelp() {
	fmt.Fprint(c.stderr, `.detect commands (PREPARE++ detector corpus -- DESIGN-prepare.md):
  .detect run  --corpus <dir> [--bind <manifest>]  run the corpus over the open bundle -> findings
  .detect lint --corpus <dir> [--bind <manifest>]  authoring report card (compile, don't run)
  .detect help                                      this help

A corpus is a flat directory of *.sql++ files: each file is one detector, its Tag is
the filename stem, its body is the SQL++ statement.

--bind <manifest> maps LOGICAL keyspace names (FROM <logical>) to per-bundle globs, so
one corpus runs against differently-named bundles unchanged. Manifest format (one of):
  line form:  logical = glob        (one per line; '#' comments and blanks ignored)
  json  form: {"logical":"glob", ...}
Globs are bundle-root-relative (bare), or ./ ../ (cwd) or / (absolute), like an inline
glob. A logical keyspace that resolves to NO files is a hard error (fail-loud) -- never
a silently "clean" bundle.

Non-interactive (CI / agent):
  n1k1 <bundle-dir> -c '.detect run --corpus ./detectors --bind ./manifest'
`)
}

// detectArgs is the parsed flag set shared by run + lint: the corpus dir and an
// optional bind manifest path.
type detectArgs struct {
	corpus string
	bind   string
}

// parseDetectArgs parses `--corpus <dir> [--bind <file>]` (also accepting the
// bare/`=` forms `-corpus=x`). Unknown tokens are an error so a typo fails loudly
// rather than being silently ignored.
func parseDetectArgs(arg string) (detectArgs, error) {
	var a detectArgs
	toks := strings.Fields(arg)
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		key, val, hasEq := t, "", false
		if eq := strings.IndexByte(t, '='); eq >= 0 {
			key, val, hasEq = t[:eq], t[eq+1:], true
		}
		switch strings.TrimLeft(key, "-") {
		case "corpus":
			if !hasEq {
				i++
				if i >= len(toks) {
					return a, fmt.Errorf("--corpus needs a directory")
				}
				val = toks[i]
			}
			a.corpus = val
		case "bind":
			if !hasEq {
				i++
				if i >= len(toks) {
					return a, fmt.Errorf("--bind needs a manifest file")
				}
				val = toks[i]
			}
			a.bind = val
		default:
			return a, fmt.Errorf("unknown flag %q (want --corpus <dir> [--bind <manifest>])", t)
		}
	}
	if a.corpus == "" {
		return a, fmt.Errorf("--corpus <dir> is required")
	}
	return a, nil
}

// loadCorpus reads every *.sql++ file in dir as one detector: Tag = filename stem,
// Stmt = file contents. Returned sorted by Tag for deterministic output. An empty
// corpus (no *.sql++ files) is an error -- a silent no-op corpus would falsely read
// as a clean bundle.
func loadCorpus(dir string) ([]glue.CorpusDetector, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.sql++"))
	if err != nil {
		return nil, fmt.Errorf("scanning corpus %q: %v", dir, err)
	}
	sort.Strings(paths)
	var dets []glue.CorpusDetector
	for _, p := range paths {
		body, rerr := os.ReadFile(p)
		if rerr != nil {
			return nil, fmt.Errorf("reading %q: %v", p, rerr)
		}
		tag := strings.TrimSuffix(filepath.Base(p), ".sql++")
		dets = append(dets, glue.CorpusDetector{Tag: tag, Stmt: string(body)})
	}
	if len(dets) == 0 {
		return nil, fmt.Errorf("no *.sql++ detectors in %q", dir)
	}
	return dets, nil
}

// loadBinding reads a per-bundle manifest into a glue.Binding. Two minimal formats:
// a JSON object {"logical":"glob", ...}, or a line form `logical = glob` (one per
// line; '#' comments and blank lines ignored). An empty/missing path yields a nil
// binding (the plain, binding-free path).
func loadBinding(path string) (glue.Binding, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading manifest %q: %v", path, err)
	}
	b := glue.Binding{}
	if s := strings.TrimSpace(string(raw)); strings.HasPrefix(s, "{") {
		if jerr := json.Unmarshal([]byte(s), &b); jerr != nil {
			return nil, fmt.Errorf("manifest %q (JSON): %v", path, jerr)
		}
		return b, nil
	}
	for i, ln := range strings.Split(string(raw), "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" || strings.HasPrefix(ln, "#") {
			continue
		}
		eq := strings.IndexByte(ln, '=')
		if eq < 0 {
			return nil, fmt.Errorf("manifest %q line %d: want `logical = glob`, got %q", path, i+1, ln)
		}
		logical := strings.TrimSpace(ln[:eq])
		glob := strings.TrimSpace(ln[eq+1:])
		if logical == "" || glob == "" {
			return nil, fmt.Errorf("manifest %q line %d: empty logical or glob in %q", path, i+1, ln)
		}
		b[logical] = glob
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("manifest %q has no bindings", path)
	}
	return b, nil
}

// detectSession opens a fresh session over the open bundle (c.dir), bound with the
// manifest when --bind was given. It is separate from c.sess so .detect never
// disturbs the interactive session's state.
func (c *cli) detectSession(bind string) (*glue.Session, glue.Binding, error) {
	if c.dir == "" {
		return nil, nil, fmt.Errorf("no bundle open -- open a datastore directory first (.open <dir>)")
	}
	b, err := loadBinding(bind)
	if err != nil {
		return nil, nil, err
	}
	sess, err := glue.OpenSessionBound(c.dir, defaultNamespace, b)
	if err != nil {
		return nil, nil, fmt.Errorf("opening bundle %q: %v", c.dir, err)
	}
	return sess, b, nil
}

// cmdDetectRun implements `.detect run`: compile the corpus over the open bundle,
// print a fail-loud coverage/health summary to stderr, then render the tagged
// findings to stdout in the current output mode.
func (c *cli) cmdDetectRun(arg string) {
	args, err := parseDetectArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .detect run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	dets, err := loadCorpus(args.corpus)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .detect run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	sess, binding, err := c.detectSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .detect run: %v\n", c.prog, err)
		c.failed = true
		return
	}

	// Fail-loud binding coverage FIRST (before compile): probe every logical keyspace
	// in the manifest against this bundle. An unresolved/empty-glob keyspace is a GAP
	// -- surface it and refuse to render a (falsely clean) findings table.
	if gap := c.reportBindingCoverage(sess, binding); gap {
		fmt.Fprintf(c.stderr, "%s: .detect run: aborting -- unresolved logical keyspace(s) above (a bundle gap, not a clean run)\n", c.prog)
		c.failed = true
		return
	}

	cc, err := sess.CorpusCompile(dets)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .detect run: compile: %v\n", c.prog, err)
		c.failed = true
		return
	}
	c.reportCorpusHealth(cc, len(dets))

	findings, err := cc.Run()
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .detect run: %v\n", c.prog, err)
		c.failed = true
		return
	}

	// Render findings as JSON rows {"tag":..., "evidence":...} in the current output
	// mode (box at a TTY, jsonlines when piped -- reusing renderRows). Streaming each
	// finding as it is produced (Session.OnRow-style) is a noted nice-to-have; the
	// MVP batch-renders the whole set.
	rows := make([]json.RawMessage, 0, len(findings))
	for _, f := range findings {
		rows = append(rows, orderedJSONRow(
			[2]interface{}{"tag", f.Tag},
			[2]interface{}{"evidence", f.Evidence},
		))
	}
	c.renderRows(rows, "", false)
	fmt.Fprintf(c.stderr, "%s%d finding(s) from %d detector(s)\n", c.icon("🔎 "), len(findings), len(dets))
}

// reportBindingCoverage probes each manifest logical keyspace against the bundle and
// reports resolved-vs-errored to stderr (the fail-loud coverage block). Returns true
// if ANY logical keyspace failed to resolve (a gap). A nil/empty binding is a no-op
// (returns false) -- an unbound corpus references real keyspace names directly.
func (c *cli) reportBindingCoverage(sess *glue.Session, binding glue.Binding) bool {
	if len(binding) == 0 {
		return false
	}
	names := make([]string, 0, len(binding))
	for n := range binding {
		names = append(names, n)
	}
	sort.Strings(names)

	ns, nerr := sess.Store.Datastore.NamespaceByName(defaultNamespace)
	if nerr != nil {
		fmt.Fprintf(c.stderr, "%s: binding: cannot open namespace: %v\n", c.prog, nerr)
		return true
	}
	fmt.Fprintf(c.stderr, "%sbinding coverage (%d logical keyspace(s)):\n", c.icon("🔗 "), len(names))
	gap := false
	for _, n := range names {
		if _, err := ns.KeyspaceByName(n); err != nil {
			fmt.Fprintf(c.stderr, "  %s %s = %q -> %s\n", c.icon("✗"), n, binding[n],
				c.style.Red("UNRESOLVED: "+tidyMsg(err.Error())))
			gap = true
		} else {
			fmt.Fprintf(c.stderr, "  %s %s = %q -> resolved\n", c.icon("✓"), n, binding[n])
		}
	}
	return gap
}

// reportCorpusHealth prints the coverage/health summary to stderr: fused / standalone
// / rejected counts, and each rejected detector's tag + reason (surfaced, never
// silently dropped). total is the number of detectors loaded.
func (c *cli) reportCorpusHealth(cc *glue.CompiledCorpus, total int) {
	fused := total - len(cc.Standalone) - len(cc.Rejected)
	fmt.Fprintf(c.stderr, "%scorpus: %d detector(s) -- %d fused, %d standalone, %d rejected\n",
		c.icon("📋 "), total, fused, len(cc.Standalone), len(cc.Rejected))
	for _, r := range cc.Rejected {
		fmt.Fprintf(c.stderr, "  %s %s: %s\n", c.icon("✗"), r.Tag, c.style.Yellow(r.Reason))
	}
}

// cmdDetectLint implements `.detect lint`: the authoring report card. It compiles
// (does not run) each detector via glue.CorpusLint and renders a per-detector table
// in the current output mode (box at a TTY, jsonlines when piped), then a corpus
// score line to stderr.
func (c *cli) cmdDetectLint(arg string) {
	args, err := parseDetectArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .detect lint: %v\n", c.prog, err)
		c.failed = true
		return
	}
	dets, err := loadCorpus(args.corpus)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .detect lint: %v\n", c.prog, err)
		c.failed = true
		return
	}
	sess, binding, err := c.detectSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .detect lint: %v\n", c.prog, err)
		c.failed = true
		return
	}
	// Lint compiles (plans) each detector, which resolves keyspaces -- so report the
	// same fail-loud binding coverage, but here it is advisory (lint still reports the
	// report card, where an unresolved keyspace shows up as a rejected row).
	c.reportBindingCoverage(sess, binding)

	report, score, err := sess.CorpusLint(dets)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .detect lint: %v\n", c.prog, err)
		c.failed = true
		return
	}

	rows := make([]json.RawMessage, 0, len(report))
	for _, d := range report {
		index := "always-wake"
		if d.Indexed {
			index = fmt.Sprintf("literal %q", d.Literal)
		} else if d.Class != glue.LintFused {
			index = "-" // only a fused detector uses the predicate index
		}
		rows = append(rows, orderedJSONRow(
			[2]interface{}{"detector", d.Tag},
			[2]interface{}{"class", d.Class},
			[2]interface{}{"keyspace", orEmptyDash(d.Keyspace)},
			[2]interface{}{"lane", orEmptyDash(d.Lane)},
			[2]interface{}{"index", index},
			[2]interface{}{"reason", orEmptyDash(d.Reason)},
			[2]interface{}{"advice", strings.Join(d.Advice, "; ")},
		))
	}
	c.renderRows(rows, "", false)

	// The corpus score line -- the guardrail against an AI-authored corpus silently
	// bloating (all always-wake) or lying (rejected -> no findings).
	fmt.Fprintf(c.stderr,
		"%sscore: %d%% fused (%d/%d), %d%% native (%d/%d converted), %d%% index-pruned (%d/%d fused)  [%d standalone, %d rejected]\n",
		c.icon("📊 "),
		score.PctFused(), score.Fused, score.Total,
		score.PctNative(), score.Native, score.Converted,
		score.PctIndexPruned(), score.IndexPruned, score.FusedForIndex,
		score.Standalone, score.Rejected)
}

// orEmptyDash renders an empty string as "-" so a blank cell reads clearly in the
// box/jsonlines table.
func orEmptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
