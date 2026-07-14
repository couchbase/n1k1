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

// cli .multi command family (PREPARE++ detector corpus: run + lint).
//
// .multi brings the corpus machinery (glue.CorpusCompile / glue.CorpusLint;
// DESIGN-prepare.md phases 6-7) to the CLI so a tech-support team -- or an AI support
// agent -- can run a corpus of SQL++ "detectors" over a support bundle (the open
// datastore) and get findings, and lint the corpus for authoring feedback. It runs
// interactively AND non-interactively (n1k1 <bundle> -c '.multi run --queries ./det'),
// so CI / an agent drives it the same way.
//
// A CORPUS is a directory of *.sql++ RECIPE files (glue.LoadCorpus / glue.ParseRecipe).
// A recipe is SQL++ plus optional `-- key: value` front-matter (label -> Label, source,
// description, tags) and an optional inline golden fixture (`-- @fixture` JSONL
// input rows + `-- @expect` golden findings). A plain *.sql++ with none of these still
// loads (Label = filename stem, Stmt = whole body) -- backward compatible.
//
// SUBCOMMANDS:
//
//	.multi run  --queries <dir> [--bind <manifest>]  -- compile the corpus over the
//	    open bundle, print a fail-loud coverage/health summary to stderr, then render
//	    the tagged findings to stdout in the current output mode.
//	.multi lint --queries <dir> [--bind <manifest>]  -- the authoring report card:
//	    per-detector class (fused/standalone/rejected), target keyspace, eval lane
//	    (native/boxed), predicate-index verdict (literal vs always-wake) and advice,
//	    plus a corpus score (% fused / native / index-pruned).
//	.multi test [--queries <dir>] [--update]         -- the golden-fixture runner (CI):
//	    for each recipe with a `-- @fixture`, build a temp keyspace from its input rows,
//	    run JUST that detector, and (check mode) assert the produced findings equal the
//	    recipe's `-- @expect` golden as a set -- or (--update) record the produced
//	    findings back into the recipe's @expect block. Signals failure via c.failed so a
//	    caller (make rules-test) exits non-zero on any FAIL. Hermetic: builds its own
//	    temp datastores, so it needs no open bundle.
//
// DEFERRED (noted): .multi bind (dry-run -- binding already fails loud at run);
// per-finding STREAMING (findings are batch-rendered via the current output mode --
// jsonlines still streams the row table; a per-finding OnRow hook is a nice-to-have);
// the SHA-keyed build cache; the re-run delta report; and multi-keyspace / version-
// specific fixtures (a fixture feeds the detector's single `source` keyspace).
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

// cmdRules dispatches the .multi command family (list | run | lint | explain | test | help).
func (c *cli) cmdRules(arg string) {
	sub, rest := splitFirst(arg)
	switch strings.ToLower(sub) {
	case "list", "ls":
		c.cmdRulesList(rest)
	case "run":
		c.cmdRulesRun(rest)
	case "lint":
		c.cmdRulesLint(rest)
	case "explain":
		c.cmdRulesExplain(rest)
	case "test":
		c.cmdRulesTest(rest)
	case "", "help":
		c.cmdRulesHelp()
	default:
		fmt.Fprintf(c.stderr, "unknown subcommand %q; try .multi help\n", sub)
	}
}

// rulesArgs is the parsed flag set shared by run + lint + test: the queries dirs (each
// a directory of *.sql++ files), an optional bind manifest path (run/lint), and the
// --update boolean (test).
type rulesArgs struct {
	queries []string
	bind    string
	update  bool // .multi test: record produced findings back into each recipe's @expect
	sql     bool // .multi explain: render the pretty SQL++ + provenance view instead of the op tree
}

// parseRulesArgs parses `--queries <dir>... [--bind <file>] [--update]` (also accepting
// the bare/`=` forms `-queries=x`). --queries is REPEATABLE and accepts a comma-separated
// list, so several query tiers (`--queries a --queries b`, or `--queries a,b`) compile
// into one shared-scan multi-query pack (IDEA-0034). Unknown tokens are an error so a typo
// fails loudly rather than being silently ignored. --queries is required (validated by the
// caller for run/lint; test errors on its absence too).
func parseRulesArgs(arg string) (rulesArgs, error) {
	var a rulesArgs
	toks := strings.Fields(arg)
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		key, val, hasEq := t, "", false
		if eq := strings.IndexByte(t, '='); eq >= 0 {
			key, val, hasEq = t[:eq], t[eq+1:], true
		}
		switch strings.TrimLeft(key, "-") {
		case "queries":
			if !hasEq {
				i++
				if i >= len(toks) {
					return a, fmt.Errorf("--queries needs a directory")
				}
				val = toks[i]
			}
			for _, d := range strings.Split(val, ",") {
				if d = strings.TrimSpace(d); d != "" {
					a.queries = append(a.queries, d)
				}
			}
		case "bind":
			if !hasEq {
				i++
				if i >= len(toks) {
					return a, fmt.Errorf("--bind needs a manifest file")
				}
				val = toks[i]
			}
			a.bind = val
		case "update":
			// A boolean flag: bare `--update`, or `--update=true|false`.
			a.update = !hasEq || val == "true" || val == "1"
		case "sql":
			// A boolean flag (.multi explain): bare `--sql`, or `--sql=true|false`.
			a.sql = !hasEq || val == "true" || val == "1"
		default:
			return a, fmt.Errorf("unknown flag %q (want --queries <dir> [--bind <manifest>] [--update] [--sql])", t)
		}
	}
	if len(a.queries) == 0 {
		return a, fmt.Errorf("--queries <dir> is required")
	}
	return a, nil
}

// loadRecipes loads one or more corpus dirs as parsed recipes (front-matter + fixtures),
// the reusable glue loader. loadCorpus below projects these onto the Label+Stmt detectors
// run/lint consume; .multi test needs the full recipe (source, fixture, expect). Multiple
// dirs concatenate into one pack (IDEA-0034).
func loadRecipes(dirs []string) ([]glue.Recipe, error) {
	return glue.LoadCorpusDirs(dirs)
}

// loadCorpus reads corpus dirs as the Label+Stmt detectors run/lint consume: it loads
// the recipes (front-matter + fixtures stripped from the SQL body) via loadRecipes and
// projects each onto its CorpusDetector. Returned sorted by path for deterministic
// output. An empty corpus (no *.sql++ files) is an error -- a silent no-op corpus would
// falsely read as a clean bundle.
func loadCorpus(dirs []string) ([]glue.CorpusDetector, error) {
	recipes, err := loadRecipes(dirs)
	if err != nil {
		return nil, err
	}
	dets := make([]glue.CorpusDetector, 0, len(recipes))
	for i := range recipes {
		dets = append(dets, recipes[i].AsDetector())
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

// rulesSession opens a fresh session over the open bundle (c.dir), bound with the
// manifest when --bind was given. It is separate from c.sess so .multi never
// disturbs the interactive session's state.
func (c *cli) rulesSession(bind string) (*glue.Session, glue.Binding, error) {
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

// cmdRulesList implements `.multi list`: a metadata-only inventory of the corpus --
// one row per recipe (label / source / description / tags / fixture? / golden? / path),
// rendered in the current output mode (box at a TTY, jsonlines when piped). It is the
// fast "what's in my corpus" landing page: it only reads recipe front-matter (pure
// glue.LoadCorpus), so it needs NO open bundle and does NOT compile -- distinct from
// `lint`, which compiles for a health report card.
func (c *cli) cmdRulesList(arg string) {
	args, err := parseRulesArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi list: %v\n", c.prog, err)
		c.failed = true
		return
	}
	recipes, err := loadRecipes(args.queries)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi list: %v\n", c.prog, err)
		c.failed = true
		return
	}
	// LoadCorpus returns recipes sorted by path (deterministic); sort by label with path
	// as the tiebreak so the inventory reads in label order regardless of file naming.
	sort.SliceStable(recipes, func(i, j int) bool {
		if recipes[i].Label != recipes[j].Label {
			return recipes[i].Label < recipes[j].Label
		}
		return recipes[i].Path < recipes[j].Path
	})

	rows := make([]json.RawMessage, 0, len(recipes))
	fixtures, goldens := 0, 0
	for i := range recipes {
		r := recipes[i]
		if r.HasFixture {
			fixtures++
		}
		if r.HasExpect {
			goldens++
		}
		rows = append(rows, orderedJSONRow(
			[2]interface{}{"label", r.Label},
			[2]interface{}{"source", orEmptyDash(r.Source)},
			[2]interface{}{"description", orEmptyDash(r.Description)},
			[2]interface{}{"tags", orEmptyDash(strings.Join(r.Tags, ","))},
			[2]interface{}{"fixture?", yesNo(r.HasFixture)},
			[2]interface{}{"golden?", yesNo(r.HasExpect)},
			[2]interface{}{"path", r.Path},
		))
	}
	c.renderRows(rows, "", false)
	fmt.Fprintf(c.stderr, "%s%d query/queries in %s -- %d with a fixture, %d with a golden (run .multi lint for a health report)\n",
		c.icon("📋 "), len(recipes), strings.Join(args.queries, ", "), fixtures, goldens)
}

// yesNo renders a boolean flag column as "yes"/"no" (kept short so the box stays tight).
func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// cmdRulesRun implements `.multi run`: compile the corpus over the open bundle,
// print a fail-loud coverage/health summary to stderr, then render the tagged
// findings to stdout in the current output mode.
func (c *cli) cmdRulesRun(arg string) {
	args, err := parseRulesArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	dets, err := loadCorpus(args.queries)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	sess, binding, err := c.rulesSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
		c.failed = true
		return
	}

	// Fail-loud binding coverage FIRST (before compile): probe every logical keyspace
	// in the manifest against this bundle. An unresolved/empty-glob keyspace is a GAP
	// -- surface it and refuse to render a (falsely clean) findings table.
	if gap := c.reportBindingCoverage(sess, binding); gap {
		fmt.Fprintf(c.stderr, "%s: .multi run: aborting -- unresolved logical keyspace(s) above (a bundle gap, not a clean run)\n", c.prog)
		c.failed = true
		return
	}

	cc, err := sess.CorpusCompile(dets)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: compile: %v\n", c.prog, err)
		c.failed = true
		return
	}
	c.reportCorpusHealth(cc, len(dets))

	findings, report, err := cc.RunReport()
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi run: %v\n", c.prog, err)
		c.failed = true
		return
	}

	// Render findings as JSON rows {"label":..., "result":...} in the current output
	// mode (box at a TTY, jsonlines when piped -- reusing renderRows). Streaming each
	// finding as it is produced (Session.OnRow-style) is a noted nice-to-have; the
	// MVP batch-renders the whole set.
	rows := make([]json.RawMessage, 0, len(findings))
	for _, f := range findings {
		rows = append(rows, orderedJSONRow(
			[2]interface{}{"label", f.Label},
			[2]interface{}{"result", f.Result},
		))
	}
	c.renderRows(rows, "", false)
	fmt.Fprintf(c.stderr, "%s%d finding(s) from %d query/queries\n", c.icon("🔎 "), len(findings), len(dets))
	if n := len(cc.GatedSkipped); n > 0 {
		// A gated skip means the detector's `gate:` precondition matched no row in its
		// keyspace, so its (expensive, standalone) sort/window was not run. Surfaced so
		// the skip is visible -- a mis-declared gate reads as "0 findings", not silence.
		fmt.Fprintf(c.stderr, "  %s\n", c.style.Dim(fmt.Sprintf(
			"gated: %d standalone query/queries skipped (gate precondition absent): %s",
			n, strings.Join(cc.GatedSkipped, ", "))))
	}
	if shareable, nDets := correlationShareable(cc.CorrelationGroups); shareable > 0 {
		// A group of >1 correlation detector over the same (left,right,key) shares ONE
		// sorted scan+decode of each keyspace via the corpus scan cache (Part B).
		fmt.Fprintf(c.stderr, "  %s\n", c.style.Dim(fmt.Sprintf(
			"correlation: %d query/queries in %d shareable group(s) -- sharing a sorted scan per keyspace",
			nDets, shareable)))
	}
	if line := mergeStatsLine(cc.MergeStats); line != "" {
		fmt.Fprintf(c.stderr, "  %s\n", c.style.Dim(line))
	}
	c.reportDetectorHits(dets, findings, cc, report)
}

// mergeStatsLine summarizes the run's sorted-merge behavior for the user (memory-relevant:
// which joins/scans streamed vs materialized, how much a materialized build spilled, and
// how many keyless log lines were skipped). Empty when no merge ran. The full breakdown is
// available via N1K1_MEM_STATS.
func mergeStatsLine(m *base.MergeStats) string {
	if m == nil || (m.JoinCount.Load() == 0 && m.ScanStreamed.Load() == 0 && m.ScanMaterialized.Load() == 0) {
		return ""
	}
	var b strings.Builder
	b.WriteString("merge: ")
	if j := m.JoinCount.Load(); j > 0 {
		fmt.Fprintf(&b, "%d join(s) [%d streamed, %d materialized]", j, m.JoinStreamed.Load(), j-m.JoinStreamed.Load())
		if sp := m.JoinSpillCount.Load(); sp > 0 {
			fmt.Fprintf(&b, " (%d spilled build(s), peak %.0f MiB)", sp, float64(m.BuildBytesPeak.Load())/(1<<20))
		}
	}
	if s := m.ScanStreamed.Load() + m.ScanMaterialized.Load(); s > 0 {
		if m.JoinCount.Load() > 0 {
			b.WriteString("; ")
		}
		fmt.Fprintf(&b, "%d sorted-scan(s) [%d streamed, %d materialized]", s, m.ScanStreamed.Load(), m.ScanMaterialized.Load())
	}
	if nk := m.NoKeySkipped.Load(); nk > 0 {
		fmt.Fprintf(&b, "; %d keyless log line(s) skipped", nk)
	}
	return b.String()
}

// reportDetectorHits prints the per-detector hit stats (IDEA-0015): for each detector,
// how many findings it matched and -- for a fused detector -- how many rows its
// keyspace scanned. The point is a debuggable 0-findings run: a detector that matched
// 0 gets an annotation distinguishing "the keyspace scanned ~0 rows" (a whole-file
// blob / empty scan -- the real cause is upstream framing) from "the predicate matched
// none of N scanned rows" (a predicate bug). Goes to stderr so it never pollutes the
// findings on stdout.
// correlationShareable counts, over the correlation groups, the groups with >1 detector
// (the ones that could share a scan) and the total detectors in those groups.
func correlationShareable(groups map[string][]string) (shareableGroups, detectors int) {
	for _, tags := range groups {
		if len(tags) > 1 {
			shareableGroups++
			detectors += len(tags)
		}
	}
	return shareableGroups, detectors
}

func (c *cli) reportDetectorHits(dets []glue.CorpusDetector, findings []glue.Finding,
	cc *glue.CompiledCorpus, report *glue.CorpusRunReport) {
	if len(dets) == 0 {
		return
	}
	matched := make(map[string]int, len(dets))
	for _, f := range findings {
		matched[f.Label]++
	}
	fmt.Fprintf(c.stderr, "  %s\n", c.style.Dim("per-query hits (scanned = keyspace rows; woken = rows that woke it; matched = findings):"))
	for _, d := range dets {
		ks, fused := cc.DetKeyspace[d.Label]
		m := matched[d.Label]
		var line string
		if fused {
			scanned := report.ScannedByKeyspace[ks]
			woken := report.WokenByDetector[d.Label]
			line = fmt.Sprintf("%-24s matched=%-5d woken=%-7d %s scanned=%d", d.Label, m, woken, ks, scanned)
			if m == 0 {
				line += "   " + zeroMatchHint(scanned, woken)
			}
		} else {
			// Standalone (GROUP BY / window / ASOF / ...) or rejected: no shared scan.
			line = fmt.Sprintf("%-24s matched=%-5d (standalone/non-fused)", d.Label, m)
		}
		fmt.Fprintf(c.stderr, "    %s\n", c.style.Dim(line))
	}
}

// zeroMatchHint explains a 0-findings fused detector from its keyspace's scanned-row
// count and how many rows woke it: ~0 scanned means the data never reached the
// predicate (an empty scan, or a whole-file blob that isn't framed -- see .tables);
// 0 woken over a scanned keyspace means the predicate-index literal never appears (a
// typo, or genuinely absent); woken>0 with 0 matched means the predicate was evaluated
// but never held (a predicate-logic bug).
func zeroMatchHint(scanned, woken int64) string {
	switch {
	case scanned == 0:
		return "← 0 matched: keyspace scanned 0 rows (empty or unresolved)"
	case scanned == 1:
		return "← 0 matched: keyspace scanned 1 row — likely a whole-file blob, not framed into rows (see .tables)"
	case woken == 0:
		return fmt.Sprintf("← 0 matched, 0 woken: the index literal never appears in %d scanned rows — a typo, or genuinely absent", scanned)
	default:
		return fmt.Sprintf("← 0 matched: predicate woke on %d row(s) but never held — check the predicate logic", woken)
	}
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
			fmt.Fprintf(c.stderr, "      %s\n", rulesFix(fixUnresolved, n))
			gap = true
		} else {
			fmt.Fprintf(c.stderr, "  %s %s = %q -> resolved\n", c.icon("✓"), n, binding[n])
		}
	}
	return gap
}

// reportCorpusHealth prints the coverage/health summary to stderr: fused / standalone
// / rejected counts, and each rejected detector's label + reason (surfaced, never
// silently dropped). total is the number of detectors loaded.
func (c *cli) reportCorpusHealth(cc *glue.CompiledCorpus, total int) {
	fused := total - len(cc.Standalone) - len(cc.Rejected)
	fmt.Fprintf(c.stderr, "%sloaded: %d query/queries -- %d fused, %d standalone, %d rejected\n",
		c.icon("📋 "), total, fused, len(cc.Standalone), len(cc.Rejected))
	// A rejected detector never runs, so it can never fire: surface it with the reason
	// AND the fix snippet (what a runnable detector looks like), never silently drop it.
	for _, r := range cc.Rejected {
		fmt.Fprintf(c.stderr, "  %s %s: %s\n", c.icon("✗"), r.Label, c.style.Yellow(r.Reason))
		fmt.Fprintf(c.stderr, "      %s\n", rulesFix(fixRejected, r.Reason))
	}
	// A standalone detector still runs (its own scan), just not fused into the shared
	// scan -- name each so the author knows it opted out of fusion, with the why/how.
	for _, d := range cc.Standalone {
		fmt.Fprintf(c.stderr, "  %s %s: %s\n", c.icon("• "), d.Label, rulesFix(fixStandalone, ""))
	}
}

// cmdRulesLint implements `.multi lint`: the authoring report card. It compiles
// (does not run) each detector via glue.CorpusLint and renders a per-detector table
// in the current output mode (box at a TTY, jsonlines when piped), then a corpus
// score line to stderr.
func (c *cli) cmdRulesLint(arg string) {
	args, err := parseRulesArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi lint: %v\n", c.prog, err)
		c.failed = true
		return
	}
	dets, err := loadCorpus(args.queries)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi lint: %v\n", c.prog, err)
		c.failed = true
		return
	}
	sess, binding, err := c.rulesSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi lint: %v\n", c.prog, err)
		c.failed = true
		return
	}
	// Lint compiles (plans) each detector, which resolves keyspaces -- so report the
	// same fail-loud binding coverage, but here it is advisory (lint still reports the
	// report card, where an unresolved keyspace shows up as a rejected row).
	c.reportBindingCoverage(sess, binding)

	report, score, err := sess.CorpusLint(dets)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi lint: %v\n", c.prog, err)
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
			[2]interface{}{"query", d.Label},
			[2]interface{}{"class", d.Class},
			[2]interface{}{"keyspace", orEmptyDash(d.Keyspace)},
			[2]interface{}{"lane", orEmptyDash(d.Lane)},
			[2]interface{}{"index", index},
			[2]interface{}{"reason", orEmptyDash(d.Reason)},
			[2]interface{}{"advice", orEmptyDash(lintAdvice(d))},
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

// cmdRulesExplain implements `.multi explain`: it surfaces the fused MQO / shared-scan
// PLAN that a `MULTI_MATCHES()` query otherwise hides behind one opaque `stream-fn` node
// (IDEA-0036 -- the machinery `.multi` advertises was invisible in `.explain`). It
// compiles (does NOT run) the corpus and prints three things:
//
//   - the fused op tree: the union-all(broadcast-indexed(cse(scan))) shape, ONE shared
//     scan per keyspace, with per-expression native/boxed verdicts (via FormatConvPlan);
//   - the fusion map: which queries share each keyspace scan, the Aho-Corasick predicate
//     index literal each is keyed on (or "always-wake"), and its eval lane;
//   - the standalone / rejected queries that fell out of fusion, with why.
//
// It is the observability companion to `.multi lint` (which gives the scores): here you
// see the actual plan, so you can confirm CSE + the shared scan + which literal the index
// picked. The per-query facts come from CorpusLint, whose classifier mirrors CorpusCompile
// exactly, so the fusion map is faithful to the tree above.
func (c *cli) cmdRulesExplain(arg string) {
	args, err := parseRulesArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi explain: %v\n", c.prog, err)
		c.failed = true
		return
	}
	dets, err := loadCorpus(args.queries)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi explain: %v\n", c.prog, err)
		c.failed = true
		return
	}
	sess, binding, err := c.rulesSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi explain: %v\n", c.prog, err)
		c.failed = true
		return
	}
	// Compiling plans each detector, resolving keyspaces -- report the same fail-loud
	// binding coverage as lint, advisory here (an unresolved keyspace shows up as a
	// rejected query in the map below).
	c.reportBindingCoverage(sess, binding)

	cc, err := sess.CorpusCompile(dets)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi explain: compile: %v\n", c.prog, err)
		c.failed = true
		return
	}
	report, score, err := sess.CorpusLint(dets)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi explain: %v\n", c.prog, err)
		c.failed = true
		return
	}
	if args.sql {
		c.renderCorpusExplainSQL(dets, report, score)
		return
	}
	c.renderCorpusExplain(cc, report, score)
}

// renderCorpusExplain prints the fused plan, the per-keyspace fusion map, and the
// standalone/rejected queries. Free-form text to c.out (like `.explain`): a plan tree
// isn't tabular, so it is not routed through renderRows.
func (c *cli) renderCorpusExplain(cc *glue.CompiledCorpus, report []glue.DetectorLint, score glue.CorpusScore) {
	w := c.out

	// Fused detectors grouped by their shared-scan keyspace, in first-seen order (so
	// the map lists keyspaces in the order queries reference them). A fused detector
	// always has a keyspace; guard defensively.
	ksOrder := []string{}
	byKS := map[string][]glue.DetectorLint{}
	for _, d := range report {
		if d.Class != glue.LintFused {
			continue
		}
		ks := orEmptyDash(d.Keyspace)
		if _, seen := byKS[ks]; !seen {
			ksOrder = append(ksOrder, ks)
		}
		byKS[ks] = append(byKS[ks], d)
	}

	fmt.Fprintf(w, "%s%d query/queries → %d fused across %d shared scan(s), %d standalone, %d rejected\n",
		c.icon("\U0001F4CB "), score.Total, score.Fused, len(ksOrder), score.Standalone, score.Rejected)

	// The fused op tree -- the shape MULTI_MATCHES's stream-fn node hides. nil when
	// nothing fused (each query runs its own scan; there is no shared-scan plan).
	fmt.Fprintln(w)
	if cc.Plan != nil {
		fmt.Fprintln(w, c.style.Bold("FUSED PLAN (shared-scan MQO):"))
		tree := glue.FormatConvPlan(cc.Plan)
		fmt.Fprint(w, indentLines(tree, "  "))
		if legend := glue.ConvPlanLegendFor(tree); legend != "" {
			fmt.Fprint(w, "\n"+indentLines(legend, "  "))
		}
	} else {
		fmt.Fprintf(w, "%s\n", c.style.Dim("FUSED PLAN: none -- no query fused (nothing shares a scan); each runs standalone below"))
	}

	// The fusion map: which queries share each keyspace scan, keyed on which literal.
	if len(ksOrder) > 0 {
		fmt.Fprintf(w, "\n%s\n", c.style.Bold("FUSION MAP (queries sharing each scan):"))
		for _, ks := range ksOrder {
			dls := byKS[ks]
			fmt.Fprintf(w, "  %s  %s\n", c.style.Cyan("shared scan: "+ks),
				c.style.Dim(fmt.Sprintf("(%d query/queries, one scan)", len(dls))))
			for _, d := range dls {
				fmt.Fprintf(w, "    %s %-24s %-14s %s\n",
					c.icon("•"), d.Label, explainIndexCell(d), c.style.Dim("["+orEmptyDash(d.Lane)+"]"))
			}
		}
	}

	// Standalone: valid, still runs, just not fused (its own scan).
	if score.Standalone > 0 {
		fmt.Fprintf(w, "\n%s\n", c.style.Bold("STANDALONE (own scan, not fused):"))
		for _, d := range report {
			if d.Class == glue.LintStandalone {
				fmt.Fprintf(w, "  %s %-24s %s\n", c.icon("•"), d.Label, c.style.Dim(orEmptyDash(d.Reason)))
			}
		}
	}

	// Rejected: never runs, so it can never fire -- surfaced, never silently dropped.
	if score.Rejected > 0 {
		fmt.Fprintf(w, "\n%s\n", c.style.Bold("REJECTED (never runs):"))
		for _, d := range report {
			if d.Class == glue.LintRejected {
				fmt.Fprintf(w, "  %s %-24s %s\n", c.icon("✗"), d.Label, c.style.Yellow(orEmptyDash(d.Reason)))
			}
		}
	}
}

// renderCorpusExplainSQL is `.multi explain --sql` (IDEA-0037): the author-facing
// companion to the op tree. For each query it prints a provenance header comment (how
// CorpusCompile classifies it: fused into which shared-scan keyspace / standalone /
// rejected, its eval lane, and the index literal it keys on), any mechanical lint hints
// as `-- hint:` comments, then the query itself re-laid-out by PrettySQL so a
// gensym-heavy / nested statement reads as a plan. The rendered SQL++ is the SAME
// statement (whitespace only), so it stays copy-paste runnable. (Deeper per-expression
// CSE-origin attribution -- which shared sub-expression came from which query -- is
// noted future work in corpus_lint.go; this surfaces the provenance that already exists.)
func (c *cli) renderCorpusExplainSQL(dets []glue.CorpusDetector, report []glue.DetectorLint, score glue.CorpusScore) {
	w := c.out
	byLabel := make(map[string]glue.DetectorLint, len(report))
	for _, d := range report {
		byLabel[d.Label] = d
	}

	// Partition by how MQO classified each query; group the FUSED ones by the shared scan
	// (keyspace) they fuse into, so the SQL view SHOWS which queries share one pass -- the
	// point the flat list missed. Preserve detector order within each group.
	fusedByKS := map[string][]glue.CorpusDetector{}
	var ksOrder []string
	var standalone, rejected []glue.CorpusDetector
	for _, det := range dets {
		switch byLabel[det.Label].Class {
		case glue.LintFused:
			ks := byLabel[det.Label].Keyspace
			if _, seen := fusedByKS[ks]; !seen {
				ksOrder = append(ksOrder, ks)
			}
			fusedByKS[ks] = append(fusedByKS[ks], det)
		case glue.LintRejected:
			rejected = append(rejected, det)
		default:
			standalone = append(standalone, det)
		}
	}
	sort.Strings(ksOrder)

	fmt.Fprintf(w, "%s%d query/queries → %d fused across %d shared scan(s), %d standalone, %d rejected\n",
		c.icon("\U0001F4CB "), score.Total, score.Fused, len(ksOrder), score.Standalone, score.Rejected)

	for _, ks := range ksOrder {
		members := fusedByKS[ks]
		fmt.Fprintln(w)
		fmt.Fprintln(w, c.style.Bold(fmt.Sprintf("═══ shared scan · %s · %d query/queries → ONE pass ═══",
			orEmptyDash(ks), len(members))))
		for _, ln := range sharedScanNotes(members, byLabel) {
			fmt.Fprintln(w, c.style.Dim(ln))
		}
		for _, det := range members {
			c.explainSQLOne(det, byLabel[det.Label])
		}
	}
	if len(standalone) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, c.style.Bold("═══ standalone · own scan, not fused ═══"))
		for _, det := range standalone {
			c.explainSQLOne(det, byLabel[det.Label])
		}
	}
	if len(rejected) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, c.style.Bold("═══ rejected · never runs ═══"))
		for _, det := range rejected {
			c.explainSQLOne(det, byLabel[det.Label])
		}
	}
}

// explainSQLOne prints one query's provenance comment, any lint hints, and its FINAL
// SQL++ (macros expanded) re-laid-out by PrettySQL. When the query invoked macro(s), the
// expanded SQL is bracketed by `-- BEGIN/END expansion of @macro` so the generated region
// is obvious (and the original @call is shown first, so the before→after is legible).
func (c *cli) explainSQLOne(det glue.CorpusDetector, d glue.DetectorLint) {
	w := c.out
	fmt.Fprintln(w)
	fmt.Fprintln(w, c.style.Cyan(explainProvenanceComment(d, det.Label)))
	if adv := lintAdvice(d); adv != "" {
		fmt.Fprintln(w, c.style.Dim("-- hint: "+adv))
	}

	used := macrosUsed(det.Stmt)
	final := det.Stmt
	if expanded, err := glue.ExpandMacros(det.Stmt); err == nil {
		final = expanded
	}
	if len(used) == 0 {
		fmt.Fprintln(w, glue.PrettySQL(final)) // no macro: the query IS its final SQL++
		return
	}
	names := "@" + strings.Join(used, ", @")
	fmt.Fprintln(w, c.style.Dim("-- as written (before expansion):"))
	fmt.Fprintln(w, c.style.Dim(glue.PrettySQL(det.Stmt)))
	fmt.Fprintln(w, c.style.Dim("-- BEGIN expansion of "+names))
	fmt.Fprintln(w, glue.PrettySQL(final))
	fmt.Fprintln(w, c.style.Dim("-- END expansion of "+names))
}

// macrosUsed returns the registered macro names a statement invokes ("@name(" call), in
// load order -- so the --sql view can name + bracket the expanded region.
func macrosUsed(stmt string) []string {
	var out []string
	for _, m := range glue.ListMacros() {
		if strings.Contains(stmt, "@"+m.Name+"(") {
			out = append(out, m.Name)
		}
	}
	return out
}

// sharedScanNotes summarizes the ONE shared pass MQO built: the union of predicate-index
// wake literals across the fused members (the shared gate that makes the single scan cheap),
// or that it must wake every row when a member has no necessary literal.
func sharedScanNotes(members []glue.CorpusDetector, byLabel map[string]glue.DetectorLint) []string {
	seen := map[string]bool{}
	var lits []string
	always := false
	for _, det := range members {
		d := byLabel[det.Label]
		if d.Indexed && d.Literal != "" {
			if !seen[d.Literal] {
				seen[d.Literal] = true
				lits = append(lits, fmt.Sprintf("%q", d.Literal))
			}
		} else {
			always = true
		}
	}
	if always {
		return []string{
			"-- one pass over the keyspace; a query has no necessary literal, so the shared scan wakes EVERY row",
			"-- each query below independently filters + projects the shared rows (the MQO / shared-scan win):",
		}
	}
	return []string{
		"-- one pass; the shared predicate index wakes only rows matching any of: " + strings.Join(lits, ", "),
		"-- each query below independently filters + projects those shared rows (the MQO / shared-scan win):",
	}
}

// explainProvenanceComment is the one-line `--` header above a query in the --sql view:
// how it is classified, the shared-scan keyspace it fuses into (or its own), its eval
// lane, and its predicate-index literal (or always-wake). label is passed explicitly so
// a detector missing from the lint report (should not happen) still prints its name.
func explainProvenanceComment(d glue.DetectorLint, label string) string {
	if label == "" {
		label = orEmptyDash(d.Label)
	}
	switch d.Class {
	case glue.LintFused:
		return fmt.Sprintf("-- %s  ·  fused → shared scan %s  ·  %s  ·  %s",
			label, orEmptyDash(d.Keyspace), orEmptyDash(d.Lane), explainIndexCell(d))
	case glue.LintStandalone:
		return fmt.Sprintf("-- %s  ·  standalone (own scan)  ·  %s  ·  %s",
			label, orEmptyDash(d.Lane), orEmptyDash(d.Reason))
	case glue.LintRejected:
		return fmt.Sprintf("-- %s  ·  REJECTED (never runs) — %s", label, orEmptyDash(d.Reason))
	default:
		return "-- " + label
	}
}

// explainIndexCell renders a fused detector's predicate-index status: the necessary
// literal the Aho-Corasick index keys on (so only rows carrying it wake the query), or
// "always-wake" when no discriminating literal was found (the query is evaluated on
// every scanned row -- the thing `.multi lint` advises adding a literal to fix).
func explainIndexCell(d glue.DetectorLint) string {
	if d.Indexed {
		return fmt.Sprintf("literal %q", d.Literal)
	}
	return "always-wake"
}

// indentLines prefixes every non-empty line of s with pad (used to nest the op tree /
// legend under their section headers). A trailing newline is preserved.
func indentLines(s, pad string) string {
	if s == "" {
		return s
	}
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, ln := range lines {
		if ln != "" {
			lines[i] = pad + ln
		}
	}
	return strings.Join(lines, "\n") + "\n"
}

// orEmptyDash renders an empty string as "-" so a blank cell reads clearly in the
// box/jsonlines table.
func orEmptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// cmdRulesTest implements `.multi test`: the golden-fixture runner (DESIGN-prepare.md
// phase 7, "a golden-fixture diff ... is the detector's unit test"; the AI-authoring CI
// point). For each recipe that carries a `-- @fixture`, it builds a temp keyspace from
// the fixture's input rows, runs JUST that detector (glue.Recipe.RunFixture -> the same
// CorpusCompile/Run path .multi run uses), and then:
//
//   - CHECK mode (default): asserts the produced findings equal the recipe's `-- @expect`
//     golden as a SORTED SET (order isn't guaranteed). A fixture with no @expect is a
//     FAIL ("no golden recorded"). A FAIL prints a compact missing/unexpected diff.
//   - --update mode: writes the produced findings back into the recipe's @expect block
//     (golden-master capture) so the author reviews the diff and commits.
//
// It is HERMETIC (each recipe runs over its own temp datastore), so it needs no open
// bundle. On any FAIL it sets c.failed so a non-interactive caller (make rules-test)
// exits non-zero. A recipe with no fixture is counted, never a hard failure; a fixture
// whose keyspace can't resolve (a deferred multi-source fixture) is SKIPPED with a note.
func (c *cli) cmdRulesTest(arg string) {
	args, err := parseRulesArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi test: %v\n", c.prog, err)
		c.failed = true
		return
	}
	recipes, err := loadRecipes(args.queries)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .multi test: %v\n", c.prog, err)
		c.failed = true
		return
	}

	var passed, failed, noFixture, skipped, updated int
	for i := range recipes {
		r := recipes[i]

		if !r.HasFixture {
			noFixture++
			fmt.Fprintf(c.stderr, "  %s %s: no fixture\n", c.icon("• "), r.Label)
			continue
		}

		actual, rerr := r.RunFixture()
		if rerr != nil {
			var unresolved *glue.ErrFixtureUnresolved
			if errors.As(rerr, &unresolved) {
				skipped++
				fmt.Fprintf(c.stderr, "  %s %s: %s -- %s\n", c.icon("⏭ "), r.Label,
					c.style.Yellow("SKIP"), tidyMsg(unresolved.Error()))
				continue
			}
			failed++
			fmt.Fprintf(c.stderr, "  %s %s: %s -- %s\n", c.icon("✗ "), r.Label,
				c.style.Red("FAIL"), tidyMsg(rerr.Error()))
			continue
		}

		if args.update {
			if uerr := updateRecipeExpect(r.Path, actual); uerr != nil {
				failed++
				fmt.Fprintf(c.stderr, "  %s %s: %s -- writing golden: %v\n", c.icon("✗ "), r.Label,
					c.style.Red("FAIL"), uerr)
				continue
			}
			updated++
			fmt.Fprintf(c.stderr, "  %s %s: recorded %d finding(s)\n", c.icon("📝 "), r.Label, len(actual))
			continue
		}

		if !r.HasExpect {
			failed++
			fmt.Fprintf(c.stderr, "  %s %s: %s -- %s\n",
				c.icon("✗ "), r.Label, c.style.Red("FAIL"), rulesFix(fixNoGolden, ""))
			continue
		}

		missing, unexpected := glue.DiffFindings(r.Fixture.Expect, actual)
		if len(missing) == 0 && len(unexpected) == 0 {
			passed++
			fmt.Fprintf(c.stderr, "  %s %s: %s (%d finding(s))\n", c.icon("✓ "), r.Label,
				c.style.Cyan("PASS"), len(actual))
			continue
		}
		failed++
		fmt.Fprintf(c.stderr, "  %s %s: %s (%d missing, %d unexpected)\n", c.icon("✗ "), r.Label,
			c.style.Red("FAIL"), len(missing), len(unexpected))
		for _, f := range missing {
			fmt.Fprintf(c.stderr, "      %s missing:    %s\n", c.style.Red("-"), findingLine(f))
		}
		for _, f := range unexpected {
			fmt.Fprintf(c.stderr, "      %s unexpected: %s\n", c.style.Cyan("+"), findingLine(f))
		}
		fmt.Fprintf(c.stderr, "      %s\n", rulesFix(fixFixtureFail, ""))
	}

	// Summary + CI signal. --update mode never "fails" a diff (it is recording), but a
	// write error or an unresolved fixture still counts.
	if args.update {
		fmt.Fprintf(c.stderr, "%s%d recorded / %d no-fixture / %d skipped / %d failed\n",
			c.icon("📋 "), updated, noFixture, skipped, failed)
	} else {
		fmt.Fprintf(c.stderr, "%s%d passed / %d failed / %d no-fixture / %d skipped\n",
			c.icon("📋 "), passed, failed, noFixture, skipped)
	}
	if failed > 0 {
		c.failed = true // non-interactive callers (make rules-test) exit non-zero.
	}
}

// updateRecipeExpect rewrites path's `-- @expect` block in place with findings (leaving
// everything before it byte-identical -- glue.RewriteExpect), the golden-master capture
// for `.multi test --update`.
func updateRecipeExpect(path string, findings []glue.Finding) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(glue.RewriteExpect(string(raw), findings)), 0o644)
}

// findingLine renders one finding as a compact {"label":...,"result":...} line for the
// check-mode diff.
func findingLine(f glue.Finding) string {
	label, _ := json.Marshal(f.Label)
	ev := f.Result
	if len(ev) == 0 {
		ev = json.RawMessage("null")
	}
	return fmt.Sprintf(`{"label":%s,"result":%s}`, label, string(ev))
}
