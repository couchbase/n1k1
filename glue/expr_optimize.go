//go:build n1ql

//  Copyright (c) 2019 Couchbase, Inc.
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

import (
	"bytes"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

var OptimizableFuncs = map[string]string{}

func init() {
	// The common case: the cbq Function.Name() and the n1k1 ExprCatalog name are
	// identical, so optSelf registers name -> name. Grouped by family; see the
	// referenced engine files for each.
	optSelf(
		"eq", "lt", "le", "gt", "ge", // comparisons (expr_cmp.go)
		"add", "sub", "mult", "div", "mod", "idiv", "imod", "neg", // arithmetic (expr_arith.go)
		"abs", "ceil", "floor", "sqrt", "exp", "ln", "log", "sign", // unary math (expr_math.go)
		"degrees", "radians", "sin", "cos", "tan", "asin", "acos", "atan",
		"upper", "lower", "length", "title", "trim", "ltrim", "rtrim", "reverse", // unary string (expr_str.go)
		"contains", "position0", "position1", // binary string (expr_str.go)
		"regexp_contains", "regexp_like", // regexp predicates, constant-pattern only (expr_str.go)
		"replace",            // ternary string, 3-arg form (expr_str.go)
		"substr0", "substr1", // SUBSTR, arity-dispatched below (expr_str.go)
		"split",        // SPLIT, arity-dispatched below (expr_str.go)
		"lpad", "rpad", // LPAD/RPAD, arity-dispatched below (expr_str.go)
		"power", "atan2", // binary math (expr_math.go)
		"round", "trunc", // ROUND/TRUNC, arity-dispatched below (expr_math.go)
		"date_part_millis", // DATE_PART_MILLIS 2-arg (expr_date.go); 3-arg (tz) falls back
		"date_add_millis",  // DATE_ADD_MILLIS 3-arg (expr_date.go)
		"to_boolean", "to_string", "to_number", // type conversions (expr_type.go)
		"array_length", "array_count", "array_sum", "array_avg", // array readers (expr_array.go)
		"array_min", "array_max", "array_contains", "array_position", // (expr_array.go)
		"object_length", "poly_length", // object/collection readers (expr_object.go)
		"and", "or", // three-valued logical (expr_logic.go)
		"not",                                           // unary predicate (expr_pred.go)
		"ifnull", "ifmissing", "ifmissingornull", "nvl", // conditional-unknown (expr_cond.go)
		"between",             // ternary (expr_between.go)
		"in",                  // membership (expr_in.go)
		"concat",              // string concat `||` (expr_concat.go)
		"nullif", "missingif", // (expr_null.go)
		"greatest", "least", // (expr_greatest.go)
		"element",                                                                  // array element `arr[idx]` (expr_nav.go)
		"is_array", "is_number", "is_string", "is_boolean", "is_object", "is_atom", // type checks (expr_type.go)
	)

	// The unknown predicates (expr_pred.go): cbq Function.Name() is the
	// no-underscore form, but the n1k1 ExprCatalog name has underscores.
	OptimizableFuncs["isnull"] = "is_null"
	OptimizableFuncs["isnotnull"] = "is_not_null"
	OptimizableFuncs["ismissing"] = "is_missing"
	OptimizableFuncs["isnotmissing"] = "is_not_missing"
	OptimizableFuncs["isvalued"] = "is_valued"
	OptimizableFuncs["isnotvalued"] = "is_not_valued"

	// IS [NOT] DISTINCT FROM: cbq Function.Name() is the no-underscore form.
	OptimizableFuncs["isdistinctfrom"] = "is_distinct_from"
	OptimizableFuncs["isnotdistinctfrom"] = "is_not_distinct_from"
}

// optSelf registers each name as optimizable to a native ExprCatalog func of the
// same name -- the common case where cbq's Function.Name() equals the n1k1 name.
func optSelf(names ...string) {
	for _, name := range names {
		OptimizableFuncs[name] = name
	}
}

// ExprTreeOptimize attempts to optimize a N1QL query/expression.Expression tree
// into a n1k1 expr params tree. It tries the native lowering first
// (exprTreeOptimizeNative); if native can't express e but e is a runtime-constant
// (row-independent, non-volatile) expression, it folds e to a ["json", value] leaf
// (exprConstFold). Native is tried first so a constant with a native handler
// (e.g. GREATEST(1,2)) keeps its tested handler rather than cbq's static fold.
//
// strict, when true, makes a Field reference that does NOT match a real label
// prefix (i.e. one that would fall back to the whole-row "." / ".*" default) a
// hard failure rather than a local-row navigation. Callers pass strict=true when
// a scope is active (correlated subquery / WITH / recursive CTE): there, an
// identifier absent from the local labels may belong to the parent scope, and
// the native path can't see the parent -- so we must only take it when every
// field reference provably resolves to a local label. See ExprTree's scoped gate.
func ExprTreeOptimize(labels base.Labels, e expression.Expression,
	buf *bytes.Buffer, strict bool) (params []interface{}, ok bool) {
	if params, ok := exprTreeOptimizeNative(labels, e, buf, strict); ok {
		return params, true
	}

	return exprConstFold(e, buf)
}

// exprConstFold bakes a runtime-constant expression's value as a native
// ["json", ...] leaf, evaluated once here at codegen time -- so no cbq is needed
// in the emitted program, and (because ExprTreeOptimize recurses) a constant
// SUBTREE folds wherever it appears, lifting an otherwise-boxed enclosing expr to
// native (e.g. the LENGTH(SUFFIXES("abc")) in b.i + LENGTH(SUFFIXES("abc"))).
//
// e.Value() != nil is the constant DETECTOR: cbq returns nil for anything
// row-dependent OR volatile (NOW/CLOCK/UUID/RANDOM), so we never fold a per-row or
// non-deterministic expr. But the VALUE comes from Evaluate(), NOT Value(): cbq's
// static Value() folding disagrees with runtime Evaluate() for some functions
// (e.g. GREATEST(9,null).Value()==null but Evaluate()==9), and only Evaluate()
// matches what the boxed lane would compute per row. e being non-volatile, the
// context's clock is irrelevant. See DESIGN-prepare.md "const-fold".
func exprConstFold(e expression.Expression, buf *bytes.Buffer) (params []interface{}, ok bool) {
	if e.Value() == nil {
		return nil, false // row-dependent or volatile -- not a constant
	}

	v, err := e.Evaluate(value.NULL_VALUE, NewExprGlueContext(time.Time{}))
	if err != nil || v == nil {
		return nil, false
	}

	// Refuse a non-finite number (NaN / +-Inf, e.g. from NaN()/PosInf()). JSON has
	// no such literal, so cbq's WriteJSON emits it as the STRING "NaN"/"+Inf" --
	// changing its type from number to string. Baking that would corrupt an
	// enclosing native op (e.g. sign(NaN()): folding the NaN() SUBTREE to "NaN"
	// makes the native sign see a string). Keeping it boxed lets cbq's runtime
	// evaluate the whole expression as it does today -- and the enclosing expr can
	// still fold if IT evaluates to a finite value (sign(NaN()) -> 0).
	if v.Type() == value.NUMBER {
		if f, ok := v.Actual().(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
			return nil, false
		}
	}

	// MISSING has no JSON form -- WriteJSON emits "null", which would wrongly
	// become NULL. Emit an empty json constant; ExprJson yields a zero-length Val.
	if v.Type() == value.MISSING {
		return []interface{}{"json", ""}, true
	}

	buf.Reset()

	if v.WriteJSON(nil, buf, "", "", true) != nil {
		return nil, false
	}

	return []interface{}{"json", buf.String()}, true
}

func exprTreeOptimizeNative(labels base.Labels, e expression.Expression,
	buf *bytes.Buffer, strict bool) (params []interface{}, ok bool) {
	if agg, ok := e.(algebra.Aggregate); ok {
		// A grouped aggregate (count(*), sum(x), ...) is already computed by the
		// group op, which appends each finalized Result as JSON bytes under the
		// label "^aggregates|"+agg.String() (see VisitGroup / op_group.go). Read
		// that value natively instead of boxing the grouped row to re-invoke
		// cbq's Aggregate.Evaluate -- which just fetches the same precomputed
		// value from the row's aggregates attachment. This also makes
		// aggregate-containing expressions (e.g. count(*)+1, sum(a)/count(*)) go
		// native, since the surrounding operators recurse through here.
		//
		// Only when that label is actually present in the input: an aggregate
		// referenced where it isn't materialized (no matching label) keeps the
		// boxed path. Must precede the *expression.Function case below, since
		// aggregates also satisfy that interface.
		aggLabel := "^aggregates|" + agg.String()
		if labels.IndexOf(aggLabel) >= 0 {
			return []interface{}{"labelPath", aggLabel}, true
		}
		return nil, false
	}

	if c, ok := e.(*expression.Constant); ok {
		// A MISSING constant has no JSON form -- value.WriteJSON emits "null",
		// which would wrongly become NULL. Emit an empty json constant instead;
		// ExprJson yields a zero-length Val, i.e. MISSING.
		if c.Value().Type() == value.MISSING {
			return []interface{}{"json", ""}, true
		}

		buf.Reset()

		if c.Value().WriteJSON(nil, buf, "", "", true) != nil {
			return nil, false
		}

		return []interface{}{"json", buf.String()}, true
	}

	if field, ok := e.(*expression.Field); ok {
		// A case-insensitive field reference (`name`i) matches a field name
		// ignoring case. Our native path navigation below is case-sensitive, so
		// hand any case-insensitive step in the chain to query's Field.Evaluate
		// (the general expr path), which does the case-insensitive lookup.
		if fieldChainCaseInsensitive(field) {
			return nil, false
		}

		fieldPath, ok := ExprFieldPath(field)
		if !ok {
			return nil, false
		}

		// Default to the whole-row label present in labels: "." normally, or
		// the ".*" star-spread row (SELECT path.*), whose stored val is the
		// object itself. A field with no more-specific path label match then
		// resolves against that whole row rather than a missing "." label.
		labelBest := "."
		if labels.IndexOf(".") < 0 && labels.IndexOf(".*") >= 0 {
			labelBest = ".*"
		}
		iBest := -1
	OUTER:
		for i := 0; i < len(fieldPath); i++ {
			labelMaybe := "." + LabelSuffix(strings.Join(fieldPath[0:i+1], `","`))

			for _, label := range labels {
				if label == labelMaybe {
					labelBest = label
					iBest = i
					continue OUTER
				}
			}
		}

		// Under an active scope, a field that matched no real label prefix
		// (iBest < 0, so labelBest is the whole-row "." / ".*" default) may be a
		// parent-scope identifier that the native labelPath can't resolve -- it
		// would silently navigate the local row and yield MISSING. Refuse it so
		// the caller keeps the (parent-aware) cbq fallback for this expression.
		if strict && iBest < 0 {
			return nil, false
		}

		params = []interface{}{"labelPath", labelBest}
		for _, x := range fieldPath[iBest+1:] {
			params = append(params, x)
		}

		return params, true
	}

	// CASE is not an expression.Function; lower both forms to a flat native
	// "case" param list [cond, then, cond, then, ..., else?]. Children() gives:
	//   SearchedCase: [when1, then1, ..., else?]
	//   SimpleCase:   [searchTerm, when1, then1, ..., else?]
	// SimpleCase desugars to searched form with cond = eq(searchTerm, when).
	if sc, ok := e.(*expression.SearchedCase); ok {
		params = []interface{}{"case"}
		for _, child := range sc.Children() {
			cp, ok := ExprTreeOptimize(labels, child, buf, strict)
			if !ok {
				return nil, false
			}
			params = append(params, cp)
		}
		return params, true
	}

	if sc, ok := e.(*expression.SimpleCase); ok {
		children := sc.Children()
		searchP, ok := ExprTreeOptimize(labels, children[0], buf, strict)
		if !ok {
			return nil, false
		}
		params = []interface{}{"case"}
		i := 1
		for i+1 < len(children) { // (when, then) pairs
			whenP, ok := ExprTreeOptimize(labels, children[i], buf, strict)
			if !ok {
				return nil, false
			}
			thenP, ok := ExprTreeOptimize(labels, children[i+1], buf, strict)
			if !ok {
				return nil, false
			}
			params = append(params, []interface{}{"eq", searchP, whenP}, thenP)
			i += 2
		}
		if i < len(children) { // trailing else
			elseP, ok := ExprTreeOptimize(labels, children[i], buf, strict)
			if !ok {
				return nil, false
			}
			params = append(params, elseP)
		}
		return params, true
	}

	f, ok := e.(expression.Function)
	if !ok {
		return nil, false
	}

	name, ok := OptimizableFuncs[f.Name()]
	if !ok {
		return nil, false
	}

	// The native arithmetic harness (engine/expr_arith.go) handles the binary
	// operators and unary neg only. cbq's add/mult are n-ary; the >2-operand
	// forms fall back to cbq rather than silently dropping operands.
	operands := f.Operands()

	// Logical AND / OR are n-ary in cbq but the native harness is binary
	// (engine/expr_logic.go). Fold into right-nested binary applications, which
	// is exact under three-valued logic (base.LogicAnd2/LogicOr2 short-circuit on
	// a decided operand and the unknown-precedence is idempotent under nesting).
	// e.g. AND(a,b,c) -> ["and", a, ["and", b, c]].
	if name == "and" || name == "or" {
		n := len(operands)
		if n < 2 {
			return nil, false
		}
		acc, ok := ExprTreeOptimize(labels, operands[n-1], buf, strict)
		if !ok {
			return nil, false
		}
		for i := n - 2; i >= 0; i-- {
			lhs, ok := ExprTreeOptimize(labels, operands[i], buf, strict)
			if !ok {
				return nil, false
			}
			acc = []interface{}{name, lhs, acc}
		}
		return acc, true
	}

	// SUBSTR0/SUBSTR1 are 2-or-3 arg; dispatch to an arity-specific native name
	// (substr0_2 / substr0_3 / ...) so each rides a fixed-arity harness. Any other
	// arity (or the rune-based MB variants, which aren't recognized) falls back.
	if name == "substr0" || name == "substr1" {
		switch len(operands) {
		case 2:
			name += "_2"
		case 3:
			name += "_3"
		default:
			return nil, false
		}
	}

	// SPLIT is 1-arg (whitespace) or 2-arg (explicit sep); dispatch to a
	// fixed-arity native name. Any other arity falls back.
	if name == "split" {
		switch len(operands) {
		case 1:
			name = "split_1"
		case 2:
			name = "split_2"
		default:
			return nil, false
		}
	}

	// LPAD/RPAD are 2-arg (default space pad) or 3-arg (explicit pad); dispatch to
	// a fixed-arity native name.
	if name == "lpad" || name == "rpad" {
		switch len(operands) {
		case 2:
			name += "_2"
		case 3:
			name += "_3"
		default:
			return nil, false
		}
	}

	// ROUND/TRUNC are 1-arg (precision 0) or 2-arg (explicit precision); dispatch
	// to a fixed-arity native name.
	if name == "round" || name == "trunc" {
		switch len(operands) {
		case 1:
			name += "_1"
		case 2:
			name += "_2"
		default:
			return nil, false
		}
	}

	// REGEXP_CONTAINS / REGEXP_LIKE: native only when the pattern (2nd operand) is
	// a compile-time-constant string that COMPILES. A dynamic pattern would force a
	// per-row recompile (allocating), and cbq raises a runtime ERROR on a bad
	// pattern -- so a dynamic OR invalid-constant pattern stays boxed, preserving
	// cbq's semantics. The native handler bakes the constant pattern and compiles
	// it once (see engine.exprRegexpMatch / base.StrRegexpMatch).
	if name == "regexp_contains" || name == "regexp_like" {
		if len(operands) != 2 {
			return nil, false
		}
		pv := operands[1].Value()
		if pv == nil || pv.Type() != value.STRING {
			return nil, false
		}
		if _, err := regexp.Compile(pv.ToString()); err != nil {
			return nil, false
		}
	}

	switch name {
	case "add", "mult", "sub", "div", "mod", "idiv", "imod", "in",
		"power", "atan2",
		"contains", "position0", "position1",
		"array_contains", "array_position",
		"date_part_millis", // 2-arg form only; the 3-arg (timezone) form falls back
		"nullif", "missingif", "element":
		// These native harnesses are two-operand; cbq's n-ary forms fall back.
		// (ifnull/ifmissing/ifmissingornull/nvl and greatest/least are n-ary.)
		if len(operands) != 2 {
			return nil, false
		}
	case "neg",
		"abs", "ceil", "floor", "sqrt", "exp", "ln", "log", "sign",
		"degrees", "radians", "sin", "cos", "tan", "asin", "acos", "atan",
		"upper", "lower", "length", "title", "trim", "ltrim", "rtrim", "reverse",
		"to_boolean", "to_string", "to_number",
		"array_length", "array_count", "array_sum", "array_avg",
		"array_min", "array_max",
		"not", "is_null", "is_not_null",
		"is_missing", "is_not_missing", "is_valued", "is_not_valued",
		"is_array", "is_number", "is_string", "is_boolean", "is_object", "is_atom":
		if len(operands) != 1 {
			return nil, false
		}
	case "between", "replace", "date_add_millis":
		// between is exactly ternary; replace's native harness is the 3-arg form
		// (str, old, repl) -- the 4-arg count form falls back to cbq;
		// date_add_millis is always 3-arg (millis, n, part).
		if len(operands) != 3 {
			return nil, false
		}
	}

	params = append(params, name)

	for _, operand := range operands {
		child, ok := ExprTreeOptimize(labels, operand, buf, strict)
		if !ok {
			return nil, false
		}

		params = append(params, child)
	}

	return params, true
}

// fieldChainCaseInsensitive reports whether e is a field/identifier reference
// where any step is case-insensitive (`name`i) -- which our case-sensitive
// native path navigation can't honor.
func fieldChainCaseInsensitive(e expression.Expression) bool {
	switch x := e.(type) {
	case *expression.Field:
		if x.CaseInsensitive() {
			return true
		}
		return fieldChainCaseInsensitive(x.First())
	case *expression.Identifier:
		return x.CaseInsensitive()
	}
	return false
}

// compiledExprDenylist names native expr catalog kinds whose AUTO-GENERATED
// compiler emitters are known-broken (they were dead code until native-inline
// codegen landed, so never debugged). They share one root cause: a "captured
// operand" codegen mechanism (the emitter reads named operand vars -- lzChildren
// for the nary family, lzValItem/Low/High for between, lzValArr/Idx for element --
// that the generated code never declares). ExprTreesOptimize keeps any expr tree
// that CONTAINS one of these BOXED (exprStr) so the compiled code stays correct;
// each entry is removed as its emitter is fixed. Binary ops (arithmetic,
// comparison), field access (labelPath), and/or/in, and json constants emit fine.
// See DESIGN-prepare.md.
// compiledExprDenylist holds native ExprCatalog funcs that ExprTreesOptimize must
// still keep BOXED (as an exprStr island) because their compiled emitters aren't
// correct yet. Empty now: the whole nary family compiles natively -- the eager
// exprs (concat, greatest/least, ifnull/coalesce) via the eager-Vals harness, and
// the lazy CASE via a flat lzMatched-guarded short-circuit (see engine.ExprCase
// and CaptureNaryChildren). Kept as the seam for the next emitter that needs it.
var compiledExprDenylist = map[string]bool{}

// exprParamsHasDenylisted reports whether a native optimized param tree contains a
// catalog op whose compiler emitter is denylisted (so the whole expr must stay
// boxed rather than emit broken code).
func exprParamsHasDenylisted(v interface{}) bool {
	arr, ok := v.([]interface{})
	if !ok || len(arr) == 0 {
		return false
	}
	if name, _ := arr[0].(string); compiledExprDenylist[name] {
		return true
	}
	for _, e := range arr[1:] {
		if exprParamsHasDenylisted(e) {
			return true
		}
	}
	return false
}

// ExprTreesOptimize rewrites each ["exprTree", <expr>] op param, in place, to the
// NATIVE catalog form ExprTreeOptimize produces (e.g. ["labelPath",...], ["add",...])
// so the compiler emits it INLINE (cbq-free) instead of a glue.ExprStr island; an
// expr that can't be lowered -- or that lowers to a tree touching a denylisted
// (known-broken) emitter -- falls back to ["exprStr", <text>]. Each op's exprs are
// optimized against its input labels (its first child's output labels) with strict
// matching, so an outer/correlated field ref that can't resolve locally stays boxed
// rather than mis-navigating the local row. Returns false if some exprTree isn't a
// serializable expression.
//
// This is the compile-time analogue of the interpreter's per-eval ExprTree ->
// ExprTreeOptimize lowering (glue/expr.go), so native exprs compile cbq-free. It
// replaces stringifyExprTrees (which boxed everything). See DESIGN-prepare.md.
func ExprTreesOptimize(o *base.Op) (ok bool) {
	ok = true
	var walk func(op *base.Op)
	walk = func(op *base.Op) {
		if op == nil {
			return
		}
		labels := inputLabels(op)
		var rewrite func(v interface{}) interface{}
		rewrite = func(v interface{}) interface{} {
			arr, isArr := v.([]interface{})
			if !isArr {
				return v
			}
			if len(arr) >= 2 {
				if name, _ := arr[0].(string); name == "exprTree" {
					e, isExpr := arr[1].(expression.Expression)
					if !isExpr || e == nil {
						ok = false
						return v
					}
					var buf bytes.Buffer
					if params, opt := ExprTreeOptimize(labels, stripCovers(e), &buf, true); opt &&
						!exprParamsHasDenylisted(params) {
						return params // native inline
					}
					return append([]interface{}{"exprStr", e.String()}, arr[2:]...) // boxed fallback
				}
			}
			out := make([]interface{}, len(arr))
			for i, e := range arr {
				out[i] = rewrite(e)
			}
			return out
		}
		if len(op.Params) > 0 {
			if rw, isArr := rewrite(op.Params).([]interface{}); isArr {
				op.Params = rw
			}
		}
		for _, c := range op.Children {
			walk(c)
		}
	}
	walk(o)
	return ok
}
