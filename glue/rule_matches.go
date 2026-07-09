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

package glue

// rule_matches.go exposes a detector corpus (corpus.go) as a plain, composable
// SQL++ table-valued source WITHOUT any cbq grammar change: an
// array-of-objects-returning scalar function used in a FROM expression-term.
//
//	SELECT f.tag, COUNT(*) AS hits
//	FROM RULE_MATCHES('detectors/') AS f
//	WHERE f.tag LIKE 'ET-%' GROUP BY f.tag ORDER BY hits DESC;
//
// cbq already accepts a function call that returns an array as a FROM source
// (e.g. `FROM split("a,b,c", ",") AS t`): conv.go's VisitExpressionScan routes a
// plain array-returning function through the MATERIALIZING "expr-scan" op, which
// evaluates it once and yields one row per array element. So RULE_MATCHES is just
// a scalar UDF whose Type() is ARRAY and whose Evaluate loads the corpus, runs it,
// and returns `[{"tag":..,"evidence":..}, ...]`. Because the result is one plain
// SELECT, RULE_MATCHES composes with WHERE / GROUP BY / ORDER BY / JOIN and is
// PREPARE-able / EXECUTE-able for free. Each row carries a `tag` naming which
// detector produced it, so the stream is a sliceable, discriminated union.
//
// MEMORY TRADEOFF: a scalar UDF returns ONE value, so RULE_MATCHES materializes
// the WHOLE result set into a single array value in memory (vs. the streaming
// `.detect run` CLI path). Fine for moderate corpora; for a huge result set prefer
// the streaming path. A scalar UDF cannot stream, by construction.
//
// SESSION AT EVAL TIME (the spike): CorpusCompile is a method on *Session, which
// needs a *Store (datastore + parser + PlanStatementQP). Evaluate runs during the
// outer query's execution, at which point the PROCESS-GLOBAL datastore is the
// current session's store (Session.Run's datastore.SetDatastore, mirrored by
// CorpusCompile/Run). So we reconstruct a *Store around datastore.GetDatastore()
// -- the Store struct is a trivial value type and the global datastore already
// carries every wrapper (glob / flat / binding / secondary-index), so detectors'
// `FROM <keyspace>` resolve exactly as the outer query would (Option A: no
// session.go edit, no shared package var, concurrency-neutral). The optional
// `bind` opt instead opens a FRESH OpenSessionBound rooted at the current data
// root (recovered from the datastore's file:// URL) with a manifest, so a corpus
// authored against a logical vocabulary resolves against this data source.
//
// RE-ENTRANCY: running the corpus during Evaluate is a nested sub-pipeline, just
// like GlueContext.EvaluateSubquery (which ExprScanOp already relies on):
// CorpusCompile.Run uses its OWN MakeVars/tmpDir and save/restores engine.ExecOpEx.
// We additionally save/restore the process-global datastore around the whole call
// (the base case reconstructs from -- and thus re-sets -- the same datastore; the
// bind case switches it), so the outer query's state is never left mutated.

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/util"
	"github.com/couchbase/query/value"
)

// RuleMatchesFuncName is the SQL++ name RULE_MATCHES registers under (cbq
// lower-cases function names, so this is the canonical spelling). It reads
// naturally as a FROM source: `FROM rule_matches('detectors/') AS f`.
const RuleMatchesFuncName = "rule_matches"

// registerRuleMatchesFunc wires RULE_MATCHES into the cbq parser so
// `rule_matches(dir[,opts])` resolves as a scalar (array-returning) function
// usable in a FROM clause. Called from ext.go's package init (always-on, like the
// sparkline/histogram aggregates). A collision with a stock cbq builtin would be a
// bug -- refuse rather than shadow.
func registerRuleMatchesFunc() {
	if _, ok := expression.GetFunction(RuleMatchesFuncName); ok {
		// A cbq builtin already owns this name: do NOT shadow it (would corrupt the
		// differential). This should never fire -- RULE_MATCHES is domain-specific.
		return
	}
	expression.RegisterFunction(RuleMatchesFuncName, newRuleMatchesFunc(RuleMatchesFuncName))
	extOurs[RuleMatchesFuncName] = true
}

// ruleMatchesFunc is the expression.Function the parser instantiates for a
// rule_matches(...) call. Modeled on jsStreamFunc / jsFunc (ext_jsvm*.go): Type
// ARRAY (so cbq accepts it as a FROM expression-term), Init/SetExpr back-pointer
// wiring, and a Constructor the parser clones per call site.
type ruleMatchesFunc struct {
	expression.FunctionBase
}

func newRuleMatchesFunc(name string, operands ...expression.Expression) expression.Function {
	rv := &ruleMatchesFunc{}
	rv.Init(strings.ToLower(name), operands...)
	rv.SetExpr(rv)
	return rv
}

func (this *ruleMatchesFunc) Accept(visitor expression.Visitor) (interface{}, error) {
	return visitor.VisitFunction(this)
}

// Type ARRAY: the array-of-objects return is what makes rule_matches(...) a valid,
// composable FROM source (VisitExpressionScan -> materializing expr-scan).
func (this *ruleMatchesFunc) Type() value.Type { return value.ARRAY }

func (this *ruleMatchesFunc) MinArgs() int { return 1 } // corpus dir
func (this *ruleMatchesFunc) MaxArgs() int { return 2 } // + optional opts object

func (this *ruleMatchesFunc) Constructor() expression.FunctionConstructor {
	name := this.Name()
	return func(operands ...expression.Expression) expression.Function {
		return newRuleMatchesFunc(name, operands...)
	}
}

// Evaluate loads the corpus at arg 0's directory, runs it against the current
// session/store, and returns the tagged matches as an ARRAY of
// {"tag":..,"evidence":..} objects (so f.tag / f.evidence are addressable).
func (this *ruleMatchesFunc) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	operands := this.Operands()

	dirV, err := operands[0].Evaluate(item, context)
	if err != nil {
		return nil, err
	}
	if dirV.Type() != value.STRING {
		return nil, fmt.Errorf("RULE_MATCHES: first argument (corpus dir) must be a string, got %s", dirV.Type())
	}
	dir, _ := dirV.Actual().(string)

	var opts ruleMatchesOpts
	if len(operands) >= 2 {
		optV, err := operands[1].Evaluate(item, context)
		if err != nil {
			return nil, err
		}
		opts = parseRuleMatchesOpts(optV)
	}

	return runRuleMatches(dir, opts)
}

// ruleMatchesOpts is the leniently-parsed arg-1 options object. Unknown keys are
// ignored; today only `bind` (a manifest path -> OpenSessionBound) changes
// behavior, while `versions` is accepted and recorded but not yet used to filter
// which detectors apply (parity with Recipe.Versions -- reporting-only for now).
type ruleMatchesOpts struct {
	bind     string   // a manifest path (logical->glob); "" = current session.
	versions []string // accepted, not yet applied (deferred, see corpus_recipe.go).
}

// parseRuleMatchesOpts reads the arg-1 OBJECT leniently: a non-object (or MISSING/
// NULL) yields zero options, and any unrecognized key is ignored. So a caller can
// pass forward-compatible opts without a hard error. It navigates via value.Value
// Field/Index (not v.Actual() type-asserts): an evaluated object literal's field
// values are themselves value.Value, so a raw map[string]interface{} assertion on
// its members would miss them (and silently drop `bind`).
func parseRuleMatchesOpts(v value.Value) ruleMatchesOpts {
	var o ruleMatchesOpts
	if v == nil || v.Type() != value.OBJECT {
		return o
	}
	if bv, ok := v.Field("bind"); ok && bv.Type() == value.STRING {
		o.bind, _ = bv.Actual().(string)
	}
	if vv, ok := v.Field("versions"); ok && vv.Type() == value.ARRAY {
		n := 0
		if act, ok := vv.Actual().([]interface{}); ok {
			n = len(act)
		}
		for i := 0; i < n; i++ {
			if ev, ok := vv.Index(i); ok && ev.Type() == value.STRING {
				if s, ok := ev.Actual().(string); ok {
					o.versions = append(o.versions, s)
				}
			}
		}
	}
	return o
}

// runRuleMatches loads the corpus at dir, compiles + runs it against the resolved
// session, and returns its matches as one materialized ARRAY value. It
// save/restores the process-global datastore around the whole run, since the bind
// path (and CorpusCompile/Run's idempotent SetDatastore) mutate it -- so the outer
// query's datastore is never left changed when we return.
func runRuleMatches(dir string, opts ruleMatchesOpts) (value.Value, error) {
	prevDS := datastore.GetDatastore()
	defer datastore.SetDatastore(prevDS)

	sess, err := ruleMatchesSession(opts, prevDS)
	if err != nil {
		return nil, err
	}

	// LoadCorpus fails loudly on an empty/missing corpus dir (no *.sql++ files) --
	// a silent empty result would falsely read as "clean" (fail-loud, the same
	// safety property as binding.go's empty-glob refusal).
	recipes, err := LoadCorpus(dir)
	if err != nil {
		return nil, fmt.Errorf("RULE_MATCHES: %w", err)
	}
	dets := make([]CorpusDetector, 0, len(recipes))
	for i := range recipes {
		dets = append(dets, recipes[i].AsDetector())
	}

	cc, err := sess.CorpusCompile(dets)
	if err != nil {
		return nil, fmt.Errorf("RULE_MATCHES: compiling corpus %q: %w", dir, err)
	}
	findings, err := cc.Run()
	if err != nil {
		return nil, fmt.Errorf("RULE_MATCHES: running corpus %q: %w", dir, err)
	}

	// One materialized array of {tag, evidence} objects. Evidence is the match's
	// raw JSON, re-parsed to a value so f.evidence navigates into it.
	arr := make([]interface{}, 0, len(findings))
	for _, f := range findings {
		arr = append(arr, map[string]interface{}{
			"tag":      f.Tag,
			"evidence": value.NewValue([]byte(f.Evidence)),
		})
	}
	return value.NewValue(arr), nil
}

// ruleMatchesSession resolves the *Session the corpus runs against. With no `bind`
// opt it wraps a *Store around the current process-global datastore (Option A: the
// global is already the outer session's fully-wrapped store, so detectors resolve
// keyspaces exactly as the outer query does). With `bind` it opens a FRESH bound
// session rooted at the current data root (recovered from the datastore's file://
// URL) with the manifest, so a logical-vocabulary corpus resolves against this data
// source. A nil global datastore (no active session) is a clear error.
func ruleMatchesSession(opts ruleMatchesOpts, ds datastore.Datastore) (*Session, error) {
	if ds == nil {
		return nil, fmt.Errorf("RULE_MATCHES: no active datastore (open a data source first)")
	}

	if opts.bind != "" {
		root := strings.TrimPrefix(ds.URL(), "file://")
		if root == "" {
			return nil, fmt.Errorf("RULE_MATCHES: bind requires a file datastore (got URL %q)", ds.URL())
		}
		b, err := loadRuleMatchesBinding(opts.bind)
		if err != nil {
			return nil, fmt.Errorf("RULE_MATCHES: %w", err)
		}
		sess, err := OpenSessionBound(root, "default", b)
		if err != nil {
			return nil, fmt.Errorf("RULE_MATCHES: opening bound session at %q: %w", root, err)
		}
		return sess, nil
	}

	store := &Store{
		Datastore:       ds,
		IndexApiVersion: datastore.INDEX_API_MAX,
		FeatureControls: util.DEF_N1QL_FEAT_CTRL,
	}
	return &Session{Store: store, Namespace: "default"}, nil
}

// loadRuleMatchesBinding reads a manifest into a Binding. Two minimal formats
// (matching the CLI's .detect --bind loader): a JSON object {"logical":"glob", ...},
// or a line form `logical = glob` (one per line, '#' comments and blanks ignored).
// An empty/malformed manifest is a hard error so a mistyped path is loud, never a
// silently binding-free run.
func loadRuleMatchesBinding(path string) (Binding, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading manifest %q: %v", path, err)
	}
	b := Binding{}
	if s := strings.TrimSpace(string(raw)); strings.HasPrefix(s, "{") {
		if jerr := json.Unmarshal([]byte(s), &b); jerr != nil {
			return nil, fmt.Errorf("manifest %q (JSON): %v", path, jerr)
		}
	} else {
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
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("manifest %q has no bindings", path)
	}
	return b, nil
}
