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

package base

// The three "unknown-vs-value" byte-kinds a Val can have, used by the N1QL
// three-valued expression logic. Values are chosen so a small results slice can
// be indexed directly by ValKind (see engine ExprIsPredicate).
const (
	ValKindValue   = iota // a real value: not MISSING, not NULL
	ValKindNull           // JSON null
	ValKindMissing        // MISSING (zero-length Val)
)

// ValKind classifies a Val as VALUE, NULL, or MISSING -- the one place that
// encodes "empty bytes == MISSING, leading 'n' == null". Centralizing it keeps
// the expression harnesses (predicates, NOT, IFNULL/IFMISSING/...) consistent.
func ValKind(v Val) int {
	if len(v) == 0 {
		return ValKindMissing
	}
	if ValEqualNull(v) {
		return ValKindNull
	}
	return ValKindValue
}

// Conditional-unknown selector modes (IFNULL / IFMISSING / IFMISSINGORNULL).
const (
	CondIfNull          = iota // keep operand unless it is NULL
	CondIfMissing              // keep operand unless it is MISSING
	CondIfMissingOrNull        // keep operand only if it is a value
)

// CondUnknownKeep reports whether an operand is the "keeper" for a selector
// mode (see engine/expr_cond.go). Lives in base so the generated compiled path
// can call it, mirroring ArithApply.
func CondUnknownKeep(mode int, v Val) bool {
	kind := ValKind(v)
	switch mode {
	case CondIfNull:
		return kind != ValKindNull
	case CondIfMissing:
		return kind != ValKindMissing
	case CondIfMissingOrNull:
		return kind == ValKindValue
	}
	return false
}

// NaryFirstKept is the reducer for the variadic IFNULL/IFMISSING/IFMISSINGORNULL
// (and COALESCE) selectors: it returns the first operand "kept" under the mode,
// else NULL. Like cbq, all operands are evaluated (only the first keeper is
// captured), so error behavior matches too. Plain Go (not lz) -- the engine
// harness calls it in a single line, so intermed_build emits that call verbatim.
func NaryFirstKept(children []ExprFunc, vals Vals, yieldErr YieldErr, mode int) Val {
	rv := ValNull
	found := false
	for _, child := range children {
		cv := child(vals, yieldErr)
		if !found && CondUnknownKeep(mode, cv) {
			rv = cv
			found = true
		}
	}
	return rv
}

// NaryConcat is the reducer for the `||` string concatenation operator (cbq
// expression/concat.go): MISSING if any operand is MISSING; NULL if any operand
// is a non-string value; else the concatenation of all string operands. The
// result is built into out[:0] (reused); returns the result Val and the grown
// buffer.
//
// It concatenates each operand's *raw JSON string content* (jsonparser leaves
// escapes intact) between one pair of quotes. That is byte-identical to cbq's
// unescape-then-reescape for every ordinary escape (\n \t \" \\ etc.); it
// differs only for a redundant \uXXXX escape of a printable char (e.g. "A"
// vs "A"), which is vanishingly rare in practice. (A fully faithful version
// would unescape via jsonparser.Unescape and re-encode via
// ValComparer.EncodeAsString, at the cost of extra scratch buffers.)
func NaryConcat(children []ExprFunc, vals Vals, yieldErr YieldErr, out []byte) (Val, []byte) {
	null := false
	out = append(out[:0], '"')
	for _, child := range children {
		cv := child(vals, yieldErr)
		if len(cv) == 0 { // MISSING dominant.
			return ValMissing, out
		}
		inner, parseType := Parse(cv)
		if ParseTypeToValType[parseType] == ValTypeString {
			if !null {
				out = append(out, inner...) // raw (escaped) string content
			}
		} else {
			null = true // non-string operand.
		}
	}
	if null {
		return ValNull, out
	}
	out = append(out, '"')
	return Val(out), out
}

// CaseReduce evaluates a CASE as a flat child list [cond, then, cond, then, ...,
// else?]: it returns the first `then` whose `cond` is truthy, else the trailing
// `else` (present when the list has odd length), else NULL. It short-circuits
// like cbq (later conds/thens are not evaluated). Simple CASE is desugared to
// this searched form (each cond an eq) by the optimizer, so one reducer serves
// both. Mirrors cbq expression/case_searched.go + case_simple.go.
func CaseReduce(children []ExprFunc, vals Vals, yieldErr YieldErr) Val {
	n := len(children)
	i := 0
	for i+1 < n { // (cond, then) pairs
		if ValTruthy(children[i](vals, yieldErr)) {
			return children[i+1](vals, yieldErr)
		}
		i += 2
	}
	if i < n { // trailing else (odd length)
		return children[i](vals, yieldErr)
	}
	return ValNull
}
