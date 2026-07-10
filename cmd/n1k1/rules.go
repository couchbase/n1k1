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

// cli .rules command family (PREPARE++ detector corpus: run + lint).
//
// .rules brings the corpus machinery (glue.CorpusCompile / glue.CorpusLint;
// DESIGN-prepare.md phases 6-7) to the CLI so a tech-support team -- or an AI support
// agent -- can run a corpus of SQL++ "detectors" over a support bundle (the open
// datastore) and get findings, and lint the corpus for authoring feedback. It runs
// interactively AND non-interactively (n1k1 <bundle> -c '.rules run --corpus ./det'),
// so CI / an agent drives it the same way.
//
// A CORPUS is a directory of *.sql++ RECIPE files (glue.LoadCorpus / glue.ParseRecipe).
// A recipe is SQL++ plus optional `-- key: value` front-matter (ticket -> Tag, source,
// severity, versions, tags) and an optional inline golden fixture (`-- @fixture` JSONL
// input rows + `-- @expect` golden findings). A plain *.sql++ with none of these still
// loads (Tag = filename stem, Stmt = whole body) -- backward compatible.
//
// SUBCOMMANDS:
//
//	.rules run  --corpus <dir> [--bind <manifest>]  -- compile the corpus over the
//	    open bundle, print a fail-loud coverage/health summary to stderr, then render
//	    the tagged findings to stdout in the current output mode.
//	.rules lint --corpus <dir> [--bind <manifest>]  -- the authoring report card:
//	    per-detector class (fused/standalone/rejected), target keyspace, eval lane
//	    (native/boxed), predicate-index verdict (literal vs always-wake) and advice,
//	    plus a corpus score (% fused / native / index-pruned).
//	.rules test [--corpus <dir>] [--update]         -- the golden-fixture runner (CI):
//	    for each recipe with a `-- @fixture`, build a temp keyspace from its input rows,
//	    run JUST that detector, and (check mode) assert the produced findings equal the
//	    recipe's `-- @expect` golden as a set -- or (--update) record the produced
//	    findings back into the recipe's @expect block. Signals failure via c.failed so a
//	    caller (make rules-test) exits non-zero on any FAIL. Hermetic: builds its own
//	    temp datastores, so it needs no open bundle.
//
// DEFERRED (noted): .rules bind (dry-run -- binding already fails loud at run);
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

	"github.com/couchbase/n1k1/glue"
)

// cmdRules dispatches the .rules command family (list | run | lint | test | help).
func (c *cli) cmdRules(arg string) {
	sub, rest := splitFirst(arg)
	switch strings.ToLower(sub) {
	case "list", "ls":
		c.cmdRulesList(rest)
	case "run":
		c.cmdRulesRun(rest)
	case "lint":
		c.cmdRulesLint(rest)
	case "test":
		c.cmdRulesTest(rest)
	case "", "help":
		c.cmdRulesHelp()
	default:
		fmt.Fprintf(c.stderr, "unknown subcommand %q; try .rules help\n", sub)
	}
}

// rulesArgs is the parsed flag set shared by run + lint + test: the corpus dir, an
// optional bind manifest path (run/lint), and the --update boolean (test).
type rulesArgs struct {
	corpus string
	bind   string
	update bool // .rules test: record produced findings back into each recipe's @expect
}

// parseRulesArgs parses `--corpus <dir> [--bind <file>] [--update]` (also accepting
// the bare/`=` forms `-corpus=x`). Unknown tokens are an error so a typo fails loudly
// rather than being silently ignored. --corpus is validated by the caller (required for
// run/lint; test errors on its absence too).
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
		case "update":
			// A boolean flag: bare `--update`, or `--update=true|false`.
			a.update = !hasEq || val == "true" || val == "1"
		default:
			return a, fmt.Errorf("unknown flag %q (want --corpus <dir> [--bind <manifest>] [--update])", t)
		}
	}
	if a.corpus == "" {
		return a, fmt.Errorf("--corpus <dir> is required")
	}
	return a, nil
}

// loadRecipes loads a corpus dir as parsed recipes (front-matter + fixtures), the
// reusable glue loader. loadCorpus below projects these onto the Tag+Stmt detectors
// run/lint consume; .rules test needs the full recipe (source, fixture, expect).
func loadRecipes(dir string) ([]glue.Recipe, error) {
	return glue.LoadCorpus(dir)
}

// loadCorpus reads a corpus dir as the Tag+Stmt detectors run/lint consume: it loads
// the recipes (front-matter + fixtures stripped from the SQL body) via loadRecipes and
// projects each onto its CorpusDetector. Returned sorted by path for deterministic
// output. An empty corpus (no *.sql++ files) is an error -- a silent no-op corpus would
// falsely read as a clean bundle.
func loadCorpus(dir string) ([]glue.CorpusDetector, error) {
	recipes, err := loadRecipes(dir)
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
// manifest when --bind was given. It is separate from c.sess so .rules never
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

// cmdRulesList implements `.rules list`: a metadata-only inventory of the corpus --
// one row per recipe (tag / source / severity / versions / fixture? / golden? / path),
// rendered in the current output mode (box at a TTY, jsonlines when piped). It is the
// fast "what's in my corpus" landing page: it only reads recipe front-matter (pure
// glue.LoadCorpus), so it needs NO open bundle and does NOT compile -- distinct from
// `lint`, which compiles for a health report card.
func (c *cli) cmdRulesList(arg string) {
	args, err := parseRulesArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules list: %v\n", c.prog, err)
		c.failed = true
		return
	}
	recipes, err := loadRecipes(args.corpus)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules list: %v\n", c.prog, err)
		c.failed = true
		return
	}
	// LoadCorpus returns recipes sorted by path (deterministic); sort by tag with path
	// as the tiebreak so the inventory reads in tag order regardless of file naming.
	sort.SliceStable(recipes, func(i, j int) bool {
		if recipes[i].Tag != recipes[j].Tag {
			return recipes[i].Tag < recipes[j].Tag
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
			[2]interface{}{"tag", r.Tag},
			[2]interface{}{"source", orEmptyDash(r.Source)},
			[2]interface{}{"severity", orEmptyDash(r.Severity)},
			[2]interface{}{"versions", orEmptyDash(strings.Join(r.Versions, ","))},
			[2]interface{}{"fixture?", yesNo(r.HasFixture)},
			[2]interface{}{"golden?", yesNo(r.HasExpect)},
			[2]interface{}{"path", r.Path},
		))
	}
	c.renderRows(rows, "", false)
	fmt.Fprintf(c.stderr, "%s%d detector(s) in %s -- %d with a fixture, %d with a golden (run .rules lint for a health report)\n",
		c.icon("📋 "), len(recipes), args.corpus, fixtures, goldens)
}

// yesNo renders a boolean flag column as "yes"/"no" (kept short so the box stays tight).
func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// cmdRulesRun implements `.rules run`: compile the corpus over the open bundle,
// print a fail-loud coverage/health summary to stderr, then render the tagged
// findings to stdout in the current output mode.
func (c *cli) cmdRulesRun(arg string) {
	args, err := parseRulesArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	dets, err := loadCorpus(args.corpus)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules run: %v\n", c.prog, err)
		c.failed = true
		return
	}
	sess, binding, err := c.rulesSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules run: %v\n", c.prog, err)
		c.failed = true
		return
	}

	// Fail-loud binding coverage FIRST (before compile): probe every logical keyspace
	// in the manifest against this bundle. An unresolved/empty-glob keyspace is a GAP
	// -- surface it and refuse to render a (falsely clean) findings table.
	if gap := c.reportBindingCoverage(sess, binding); gap {
		fmt.Fprintf(c.stderr, "%s: .rules run: aborting -- unresolved logical keyspace(s) above (a bundle gap, not a clean run)\n", c.prog)
		c.failed = true
		return
	}

	cc, err := sess.CorpusCompile(dets)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules run: compile: %v\n", c.prog, err)
		c.failed = true
		return
	}
	c.reportCorpusHealth(cc, len(dets))

	findings, err := cc.Run()
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules run: %v\n", c.prog, err)
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
			fmt.Fprintf(c.stderr, "      %s\n", rulesFix(fixUnresolved, n))
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
	// A rejected detector never runs, so it can never fire: surface it with the reason
	// AND the fix snippet (what a runnable detector looks like), never silently drop it.
	for _, r := range cc.Rejected {
		fmt.Fprintf(c.stderr, "  %s %s: %s\n", c.icon("✗"), r.Tag, c.style.Yellow(r.Reason))
		fmt.Fprintf(c.stderr, "      %s\n", rulesFix(fixRejected, r.Reason))
	}
	// A standalone detector still runs (its own scan), just not fused into the shared
	// scan -- name each so the author knows it opted out of fusion, with the why/how.
	for _, d := range cc.Standalone {
		fmt.Fprintf(c.stderr, "  %s %s: %s\n", c.icon("• "), d.Tag, rulesFix(fixStandalone, ""))
	}
}

// cmdRulesLint implements `.rules lint`: the authoring report card. It compiles
// (does not run) each detector via glue.CorpusLint and renders a per-detector table
// in the current output mode (box at a TTY, jsonlines when piped), then a corpus
// score line to stderr.
func (c *cli) cmdRulesLint(arg string) {
	args, err := parseRulesArgs(arg)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules lint: %v\n", c.prog, err)
		c.failed = true
		return
	}
	dets, err := loadCorpus(args.corpus)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules lint: %v\n", c.prog, err)
		c.failed = true
		return
	}
	sess, binding, err := c.rulesSession(args.bind)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules lint: %v\n", c.prog, err)
		c.failed = true
		return
	}
	// Lint compiles (plans) each detector, which resolves keyspaces -- so report the
	// same fail-loud binding coverage, but here it is advisory (lint still reports the
	// report card, where an unresolved keyspace shows up as a rejected row).
	c.reportBindingCoverage(sess, binding)

	report, score, err := sess.CorpusLint(dets)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules lint: %v\n", c.prog, err)
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

// orEmptyDash renders an empty string as "-" so a blank cell reads clearly in the
// box/jsonlines table.
func orEmptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// cmdRulesTest implements `.rules test`: the golden-fixture runner (DESIGN-prepare.md
// phase 7, "a golden-fixture diff ... is the detector's unit test"; the AI-authoring CI
// point). For each recipe that carries a `-- @fixture`, it builds a temp keyspace from
// the fixture's input rows, runs JUST that detector (glue.Recipe.RunFixture -> the same
// CorpusCompile/Run path .rules run uses), and then:
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
		fmt.Fprintf(c.stderr, "%s: .rules test: %v\n", c.prog, err)
		c.failed = true
		return
	}
	recipes, err := loadRecipes(args.corpus)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: .rules test: %v\n", c.prog, err)
		c.failed = true
		return
	}

	var passed, failed, noFixture, skipped, updated int
	for i := range recipes {
		r := recipes[i]

		if !r.HasFixture {
			noFixture++
			fmt.Fprintf(c.stderr, "  %s %s: no fixture\n", c.icon("• "), r.Tag)
			continue
		}

		actual, rerr := r.RunFixture()
		if rerr != nil {
			var unresolved *glue.ErrFixtureUnresolved
			if errors.As(rerr, &unresolved) {
				skipped++
				fmt.Fprintf(c.stderr, "  %s %s: %s -- %s\n", c.icon("⏭ "), r.Tag,
					c.style.Yellow("SKIP"), tidyMsg(unresolved.Error()))
				continue
			}
			failed++
			fmt.Fprintf(c.stderr, "  %s %s: %s -- %s\n", c.icon("✗ "), r.Tag,
				c.style.Red("FAIL"), tidyMsg(rerr.Error()))
			continue
		}

		if args.update {
			if uerr := updateRecipeExpect(r.Path, actual); uerr != nil {
				failed++
				fmt.Fprintf(c.stderr, "  %s %s: %s -- writing golden: %v\n", c.icon("✗ "), r.Tag,
					c.style.Red("FAIL"), uerr)
				continue
			}
			updated++
			fmt.Fprintf(c.stderr, "  %s %s: recorded %d finding(s)\n", c.icon("📝 "), r.Tag, len(actual))
			continue
		}

		if !r.HasExpect {
			failed++
			fmt.Fprintf(c.stderr, "  %s %s: %s -- %s\n",
				c.icon("✗ "), r.Tag, c.style.Red("FAIL"), rulesFix(fixNoGolden, ""))
			continue
		}

		missing, unexpected := glue.DiffFindings(r.Fixture.Expect, actual)
		if len(missing) == 0 && len(unexpected) == 0 {
			passed++
			fmt.Fprintf(c.stderr, "  %s %s: %s (%d finding(s))\n", c.icon("✓ "), r.Tag,
				c.style.Cyan("PASS"), len(actual))
			continue
		}
		failed++
		fmt.Fprintf(c.stderr, "  %s %s: %s (%d missing, %d unexpected)\n", c.icon("✗ "), r.Tag,
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
// for `.rules test --update`.
func updateRecipeExpect(path string, findings []glue.Finding) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(glue.RewriteExpect(string(raw), findings)), 0o644)
}

// findingLine renders one finding as a compact {"tag":...,"evidence":...} line for the
// check-mode diff.
func findingLine(f glue.Finding) string {
	tag, _ := json.Marshal(f.Tag)
	ev := f.Evidence
	if len(ev) == 0 {
		ev = json.RawMessage("null")
	}
	return fmt.Sprintf(`{"tag":%s,"evidence":%s}`, tag, string(ev))
}
