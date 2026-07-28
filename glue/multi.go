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

// multi.go exposes an entry pack (multiquery.go) as a plain, composable
// SQL++ table-valued source WITHOUT any cbq grammar change: a set-returning function
// used in a FROM expression-term.
//
//	SELECT f.label, COUNT(*) AS hits
//	FROM MULTI_MATCHES('entries/') AS f
//	WHERE f.label LIKE 'ET-%' GROUP BY f.label ORDER BY hits DESC;
//
// MULTI_MATCHES is a set-returning function (Type() ARRAY) usable as a FROM source.
// cbq accepts a FROM function-call term (e.g. `FROM split("a,b,c", ",") AS t`), and
// because multiMatchesFunc implements StreamSource, conv.go's VisitExpressionScan
// routes `FROM multi_matches(...)` to the generic STREAMING stream-fn op
// (op_stream_fn.go): each finding flows straight into the pipeline as the pack
// produces it, so memory stays BOUNDED and the source composes with WHERE / GROUP BY
// / ORDER BY / JOIN and is PREPARE/EXECUTE-able for free. Each row carries a `label`
// naming which entry produced it, so the stream is a sliceable, discriminated
// union. The streaming producer is StreamRows (below); the shared, spillable
// compile+run machinery is CompiledMultiQueryEntries.RunStream (multiquery.go).
//
// SCALAR-CONTEXT FALLBACK: if multi_matches(...) is (mis)used OUTSIDE a FROM clause
// -- as a scalar expression -- there is no pipeline to stream into, so Evaluate
// MATERIALIZES the whole result set into one array value (runMultiMatches). That is
// the only materializing path; the FROM path never builds the whole array.
//
// SESSION AT EVAL TIME (the spike): MultiQueryCompile is a method on *Session, which
// needs a *Store (datastore + parser + PlanStatementQP). The pack runs during the
// outer query's execution, at which point the PROCESS-GLOBAL datastore is the
// current session's store (Session.Run's datastore.SetDatastore, mirrored by
// MultiQueryCompile/Run). So we reconstruct a *Store around datastore.GetDatastore()
// -- the Store struct is a trivial value type and the global datastore already
// carries every wrapper (glob / flat / binding / secondary-index), so entries'
// `FROM <keyspace>` resolve exactly as the outer query would (Option A: no
// session.go edit, no shared package var, concurrency-neutral). The optional
// `bind` opt instead opens a FRESH OpenSessionBound rooted at the current data
// root (recovered from the datastore's file:// URL) with a manifest, so a pack
// authored against a logical vocabulary resolves against this data source.
//
// RE-ENTRANCY: running the pack during the outer query is a nested sub-pipeline,
// just like GlueContext.EvaluateSubquery (which ExprScanOp already relies on):
// RunStream uses its OWN MakeVars/tmpDir and save/restores engine.ExecOpEx.
// We additionally save/restore the process-global datastore around the whole call
// (the base case reconstructs from -- and thus re-sets -- the same datastore; the
// bind case switches it), so the outer query's state is never left mutated.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/util"
	"github.com/couchbase/query/value"
)

// MultiMatchesFuncName is the user-facing TVF name: MULTI_MATCHES (cbq lower-cases
// function names, so "multi_matches" is the canonical spelling). "multi" is short for
// multi-query -- the batch of related SQL++ queries this runs with shared execution
// (MQO). Renamed from MULTI_MATCHES in 2026 (a hard cut: no back-compat alias). Reads
// naturally as a FROM source: `FROM multi_matches('entries/') AS f`. Arg-0 may name
// SEVERAL query dirs -- a comma-list `'a,b'` or an array `['a','b']` -- which fuse into
// one shared-scan pack, so tiers stay organized on disk yet run as one entry set.
const MultiMatchesFuncName = "multi_matches"

// registerMultiMatchesFunc wires MULTI_MATCHES into the cbq parser so
// `multi_matches(dir[,opts])` resolves as a scalar (array-returning) function usable in a
// FROM clause. Called from ext.go's package init (always-on, like the sparkline/histogram
// aggregates). A collision with a stock cbq builtin would be a bug -- refuse rather than
// shadow.
func registerMultiMatchesFunc() {
	if _, ok := expression.GetFunction(MultiMatchesFuncName); ok {
		// A cbq builtin already owns this name: do NOT shadow it (would corrupt the
		// differential). This should never fire -- MULTI_MATCHES is domain-specific.
		return
	}
	expression.RegisterFunction(MultiMatchesFuncName, newMultiMatchesFunc(MultiMatchesFuncName))
	extOurs[MultiMatchesFuncName] = true
}

// multiMatchesFunc is the expression.Function the parser instantiates for a
// multi_matches(...) call. Modeled on jsStreamFunc / jsFunc (ext_jsvm*.go): Type
// ARRAY (so cbq accepts it as a FROM expression-term), Init/SetExpr back-pointer
// wiring, and a Constructor the parser clones per call site.
type multiMatchesFunc struct {
	expression.FunctionBase
}

func newMultiMatchesFunc(name string, operands ...expression.Expression) expression.Function {
	rv := &multiMatchesFunc{}
	rv.Init(strings.ToLower(name), operands...)
	rv.SetExpr(rv)
	return rv
}

func (this *multiMatchesFunc) Accept(visitor expression.Visitor) (interface{}, error) {
	return visitor.VisitFunction(this)
}

// Type ARRAY marks multi_matches(...) set-returning so cbq accepts it as a FROM
// expression-term; conv.go routes the FROM to the streaming stream-fn op (via the
// StreamSource impl below), and Evaluate is only the scalar-context fallback.
func (this *multiMatchesFunc) Type() value.Type { return value.ARRAY }

func (this *multiMatchesFunc) MinArgs() int { return 1 } // pack dir
func (this *multiMatchesFunc) MaxArgs() int { return 2 } // + optional opts object

func (this *multiMatchesFunc) Constructor() expression.FunctionConstructor {
	name := this.Name()
	return func(operands ...expression.Expression) expression.Function {
		return newMultiMatchesFunc(name, operands...)
	}
}

// evalArgs evaluates the pack dir(s) (arg 0) and the optional opts object (arg 1).
// Shared by the streaming StreamRows path and the scalar Evaluate. Arg 0 is a query-dir
// string, a comma-separated list, OR an array of dir strings -- multiple dirs concatenate
// into one shared-scan multi-query pack (IDEA-0034).
func (this *multiMatchesFunc) evalArgs(item value.Value, context expression.Context) ([]string, multiMatchesOpts, error) {
	operands := this.Operands()

	dirV, err := operands[0].Evaluate(item, context)
	if err != nil {
		return nil, multiMatchesOpts{}, err
	}
	dirs, err := multiMatchesDirs(dirV)
	if err != nil {
		return nil, multiMatchesOpts{}, err
	}

	var opts multiMatchesOpts
	if len(operands) >= 2 {
		optV, err := operands[1].Evaluate(item, context)
		if err != nil {
			return nil, multiMatchesOpts{}, err
		}
		opts = parseMultiMatchesOpts(optV)
	}
	return dirs, opts, nil
}

// multiMatchesDirs reads MULTI_MATCHES's arg-0 into the list of pack dirs: a STRING
// (optionally comma-separated) or an ARRAY of strings. Blanks are dropped; an empty
// result is an error. (Navigates an ARRAY via value.Index -- an evaluated array literal's
// elements are themselves value.Value, so an .Actual() type-assert would miss them.)
func multiMatchesDirs(v value.Value) ([]string, error) {
	var out []string
	add := func(s string) {
		for _, d := range strings.Split(s, ",") {
			if d = strings.TrimSpace(d); d != "" {
				out = append(out, d)
			}
		}
	}
	switch v.Type() {
	case value.STRING:
		s, _ := v.Actual().(string)
		add(s)
	case value.ARRAY:
		n := 0
		if act, ok := v.Actual().([]interface{}); ok {
			n = len(act)
		}
		for i := 0; i < n; i++ {
			ev, ok := v.Index(i)
			if !ok || ev.Type() != value.STRING {
				return nil, fmt.Errorf("MULTI_MATCHES: query-dir array must be all strings")
			}
			s, _ := ev.Actual().(string)
			add(s)
		}
	default:
		return nil, fmt.Errorf("MULTI_MATCHES: first argument (query dir) must be a string or array of strings, got %s", v.Type())
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("MULTI_MATCHES: first argument (query dir) is empty")
	}
	return out, nil
}

// Evaluate is the SCALAR-context fallback (multi_matches(...) used OUTSIDE a FROM
// clause): with no pipeline to stream into, it materializes the whole result set as
// one ARRAY of {label, result} objects. The FROM path never reaches here -- it
// streams via StreamRows.
func (this *multiMatchesFunc) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	dirs, opts, err := this.evalArgs(item, context)
	if err != nil {
		return nil, err
	}
	return runMultiMatches(dirs, opts, warnSink(context))
}

// StreamRows is multiMatchesFunc's StreamSource implementation: the STREAMING FROM
// producer. It loads + compiles the pack and emits each finding row as the pack
// produces it (bounded memory), instead of materializing the whole result set the
// way the scalar Evaluate fallback does. Each row is a {"label":..,"result":..}
// object matching the materialized array's element shape, so f.label / f.result
// navigate identically whichever path ran.
func (this *multiMatchesFunc) StreamRows(vars *base.Vars, gc *GlueContext,
	ctx expression.Context, item value.Value, emit func(base.Val) bool) error {
	dirs, opts, err := this.evalArgs(item, ctx)
	if err != nil {
		return err
	}

	// Save/restore the process-global datastore around the run: the bind path (and
	// MultiQueryCompile/RunStream's idempotent SetDatastore) mutate it, so the outer
	// query's datastore is never left changed when we return.
	prevDS := datastore.GetDatastore()
	defer datastore.SetDatastore(prevDS)

	cc, err := multiMatchesEntries(dirs, opts, func(s string) { gc.Warnf("%s", s) })
	if err != nil {
		return err
	}

	stopped := false
	rerr := cc.RunStream(func(f Finding) error {
		jv, e := json.Marshal(ruleMatchRow{Label: f.Label, Result: f.Result})
		if e != nil {
			return fmt.Errorf("MULTI_MATCHES: marshaling finding: %w", e)
		}
		if !emit(base.Val(jv)) {
			stopped = true
			return errStreamStop // the consumer wants no more (e.g. LIMIT satisfied).
		}
		return nil
	})
	if stopped {
		return nil // clean early-exit; errStreamStop was ours, not a real failure.
	}
	return rerr
}

// ruleMatchRow is the per-row shape MULTI_MATCHES yields: the finding's label plus its
// result JSON, so `FROM multi_matches(...) AS f` exposes f.label and f.result. It
// marshals to the same {"label":..,"result":..} object the materialized array uses.
type ruleMatchRow struct {
	Label  string          `json:"label"`
	Result json.RawMessage `json:"result"`
}

// errStreamStop is the sentinel RunStream's callback returns to abort once the
// consumer wants no more rows (emit returned false); StreamRows swallows it.
var errStreamStop = errors.New("multi_matches: consumer requested stop")

// multiMatchesOpts is the leniently-parsed arg-1 options object. Unknown keys are
// ignored; today only `bind` (a manifest path -> OpenSessionBound) changes behavior.
type multiMatchesOpts struct {
	bind string // a manifest path (logical->glob); "" = current session.
}

// parseMultiMatchesOpts reads the arg-1 OBJECT leniently: a non-object (or MISSING/
// NULL) yields zero options, and any unrecognized key is ignored. So a caller can
// pass forward-compatible opts without a hard error. It navigates via value.Value
// Field/Index (not v.Actual() type-asserts): an evaluated object literal's field
// values are themselves value.Value, so a raw map[string]interface{} assertion on
// its members would miss them (and silently drop `bind`).
func parseMultiMatchesOpts(v value.Value) multiMatchesOpts {
	var o multiMatchesOpts
	if v == nil || v.Type() != value.OBJECT {
		return o
	}
	if bv, ok := v.Field("bind"); ok && bv.Type() == value.STRING {
		o.bind, _ = bv.Actual().(string)
	}
	return o
}

// multiMatchesEntries resolves the target session (Option A, or `bind`), loads the
// pack at dir, and compiles it. The CALLER save/restores the process-global
// datastore around the returned pack's Run/RunStream (the bind path switches it).
// Shared by the streaming StreamRows path and the materializing Evaluate fallback.
func multiMatchesEntries(dirs []string, opts multiMatchesOpts, warn func(string)) (*CompiledMultiQueryEntries, error) {
	sess, err := multiMatchesSession(opts, datastore.GetDatastore())
	if err != nil {
		return nil, err
	}

	// LoadMultiQueryEntriesDirs fails loudly on an empty/missing pack dir (no *.sql++ files) --
	// a silent empty result would falsely read as "clean" (fail-loud, the same
	// safety property as binding.go's empty-glob refusal). Multiple dirs concatenate
	// into one shared-scan pack (IDEA-0034).
	entries, err := LoadMultiQueryEntriesDirs(dirs)
	if err != nil {
		return nil, fmt.Errorf("MULTI_MATCHES: %w", err)
	}
	dets := make([]MultiQueryEntry, 0, len(entries))
	for i := range entries {
		dets = append(dets, entries[i])
	}

	label := strings.Join(dirs, ", ")
	cc, err := sess.MultiQueryCompile(dets)
	if err != nil {
		return nil, fmt.Errorf("MULTI_MATCHES: compiling pack %q: %w", label, err)
	}
	if err := reportMultiQueryRejects(cc, label, opts, warn); err != nil {
		return nil, err
	}
	return cc, nil
}

// reportCorpusRejects surfaces entries that FAILED to compile (IDEA-0017): unlike
// .multi run, the MULTI_MATCHES TVF otherwise drops them silently, so a pack whose
// entries don't resolve (e.g. LOGICAL keyspaces with no {"bind":...}) returned an
// empty array indistinguishable from "ran, matched nothing." When EVERY entry
// rejected (nothing runnable) that's a hard ERROR -- a misconfigured pack must not
// read as a clean bundle. When only SOME rejected, a WARNING lists them (the runnable
// rest still stream). A keyspace-resolution reject with no bind gets a bind hint.
func reportMultiQueryRejects(cc *CompiledMultiQueryEntries, label string, opts multiMatchesOpts, warn func(string)) error {
	if len(cc.Rejected) == 0 {
		return nil
	}
	const cap = 6
	parts := make([]string, 0, cap+1)
	for i, r := range cc.Rejected {
		if i == cap {
			parts = append(parts, fmt.Sprintf("+%d more", len(cc.Rejected)-cap))
			break
		}
		parts = append(parts, fmt.Sprintf("%s (%s)", r.Label, r.Reason))
	}
	list := strings.Join(parts, "; ")

	hint := ""
	if opts.bind == "" && rejectsMentionKeyspace(cc.Rejected) {
		hint = " -- queries over LOGICAL keyspaces need a {\"bind\":\"<manifest>\"} option"
	}

	// Runnable iff something will actually execute: a fused plan or any standalone.
	if cc.Plan == nil && len(cc.Standalone) == 0 {
		return fmt.Errorf("MULTI_MATCHES: no query in %q compiled -- all %d rejected: %s%s",
			label, len(cc.Rejected), list, hint)
	}
	if warn != nil {
		warn(fmt.Sprintf("MULTI_MATCHES: %d query/queries in %q skipped (did not compile): %s%s",
			len(cc.Rejected), label, list, hint))
	}
	return nil
}

// rejectsMentionKeyspace reports whether any reject reason looks like a keyspace-
// resolution failure -- the signature of a LOGICAL-keyspace pack run without a bind.
func rejectsMentionKeyspace(rejected []RejectedEntry) bool {
	for _, r := range rejected {
		if strings.Contains(strings.ToLower(r.Reason), "keyspace") {
			return true
		}
	}
	return false
}

// warnSink returns a warning callback backed by the eval context's GlueContext (so a
// MULTI_MATCHES warning reaches the request's warning collector), or a no-op when the
// context isn't a *GlueContext.
func warnSink(context expression.Context) func(string) {
	if gc, ok := context.(*GlueContext); ok {
		return func(s string) { gc.Warnf("%s", s) }
	}
	return func(string) {}
}

// runMultiMatches is the scalar-context fallback: compile + Run the pack and return
// its matches as one MATERIALIZED array value. It save/restores the process-global
// datastore around the run (the bind path and MultiQueryCompile/Run's idempotent
// SetDatastore mutate it) -- so the outer query's datastore is never left changed.
func runMultiMatches(dirs []string, opts multiMatchesOpts, warn func(string)) (value.Value, error) {
	prevDS := datastore.GetDatastore()
	defer datastore.SetDatastore(prevDS)

	cc, err := multiMatchesEntries(dirs, opts, warn)
	if err != nil {
		return nil, err
	}
	findings, err := cc.Run()
	if err != nil {
		return nil, fmt.Errorf("MULTI_MATCHES: running pack %q: %w", strings.Join(dirs, ", "), err)
	}

	// One materialized array of {label, result} objects. Result is the match's
	// raw JSON, re-parsed to a value so f.result navigates into it.
	arr := make([]interface{}, 0, len(findings))
	for _, f := range findings {
		arr = append(arr, map[string]interface{}{
			"label":  f.Label,
			"result": value.NewValue([]byte(f.Result)),
		})
	}
	return value.NewValue(arr), nil
}

// multiMatchesSession resolves the *Session the pack runs against. With no `bind`
// opt it wraps a *Store around the current process-global datastore (Option A: the
// global is already the outer session's fully-wrapped store, so entries resolve
// keyspaces exactly as the outer query does). With `bind` it opens a FRESH bound
// session rooted at the current data root (recovered from the datastore's file://
// URL) with the manifest, so a logical-vocabulary pack resolves against this data
// source. A nil global datastore (no active session) is a clear error.
func multiMatchesSession(opts multiMatchesOpts, ds datastore.Datastore) (*Session, error) {
	if ds == nil {
		return nil, fmt.Errorf("MULTI_MATCHES: no active datastore (open a data source first)")
	}

	if opts.bind != "" {
		root := strings.TrimPrefix(ds.URL(), "file://")
		if root == "" {
			return nil, fmt.Errorf("MULTI_MATCHES: bind requires a file datastore (got URL %q)", ds.URL())
		}
		b, err := loadMultiMatchesBinding(opts.bind)
		if err != nil {
			return nil, fmt.Errorf("MULTI_MATCHES: %w", err)
		}
		sess, err := OpenSessionBound(root, "default", b)
		if err != nil {
			return nil, fmt.Errorf("MULTI_MATCHES: opening bound session at %q: %w", root, err)
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

// loadMultiMatchesBinding reads a manifest into a Binding. Two minimal formats
// (matching the CLI's .multi --bind loader): a JSON object {"logical":"glob", ...},
// or a line form `logical = glob` (one per line, '#' comments and blanks ignored).
// An empty/malformed manifest is a hard error so a mistyped path is loud, never a
// silently binding-free run.
func loadMultiMatchesBinding(path string) (Binding, error) {
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
