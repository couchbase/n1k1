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

// ValIsString reports whether v is a JSON string Val (starts with a quote). Valid
// only on a real value (non-MISSING, non-NULL); callers classify with ValKind first.
func ValIsString(v Val) bool {
	return len(v) > 0 && v[0] == '"'
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

// NaryFirstKeptVals is the reducer for the variadic IFNULL / IFMISSING /
// IFMISSINGORNULL (and COALESCE / NVL) selectors over already-evaluated operand
// values: it returns the first childVal "kept" under the mode, else NULL. Like
// cbq, every operand is evaluated (the engine evaluates them all into childVals
// before calling), so error behavior matches too.
func NaryFirstKeptVals(childVals Vals, mode int) Val {
	for _, cv := range childVals {
		if CondUnknownKeep(mode, cv) {
			return cv
		}
	}
	return ValNull
}

// NaryConcatVals is the reducer for the `||` string concatenation operator (cbq
// expression/concat.go) over already-evaluated operand values: MISSING if any
// operand is MISSING; NULL if any operand is a non-string value; else the
// concatenation of all string operands. The result is built into out[:0]
// (reused); returns the result Val and the grown buffer.
//
// It concatenates each operand's *raw JSON string content* (jsonparser leaves
// escapes intact) between one pair of quotes. That is byte-identical to cbq's
// unescape-then-reescape for every ordinary escape (\n \t \" \\ etc.); it
// differs only for a redundant \uXXXX escape of a printable char (e.g. "A"
// vs "A"), which is vanishingly rare in practice. (A fully faithful version
// would unescape via jsonparser.Unescape and re-encode via
// ValComparer.EncodeAsString, at the cost of extra scratch buffers.)
func NaryConcatVals(childVals Vals, out []byte) (Val, []byte) {
	null := false
	out = append(out[:0], '"')
	for _, cv := range childVals {
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

// GreatestLeastVals implements GREATEST (greater=true) and LEAST (greater=false)
// over already-evaluated operand values, per cbq func_comp.go: the max/min operand
// by N1QL collation, skipping MISSING/NULL operands; NULL if every operand is
// MISSING/NULL. The winning operand's bytes are returned verbatim.
func GreatestLeastVals(vc *ValComparer, childVals Vals, greater bool) Val {
	rv := ValNull
	rvSet := false
	for _, a := range childVals {
		if ValKind(a) != ValKindValue {
			continue // skip MISSING/NULL
		}
		if !rvSet {
			rv, rvSet = a, true
		} else if cmp := vc.Compare(a, rv); (greater && cmp > 0) || (!greater && cmp < 0) {
			rv = a
		}
	}
	return rv
}

// NullMissingIf implements NULLIF (whenEqual=ValNull) and MISSINGIF
// (whenEqual=ValMissing), per cbq func_cond_unknown.go: MISSING if either operand
// is MISSING; NULL if either is NULL; whenEqual if a equals b (N1QL collation);
// else the first operand a (verbatim). vc.Compare == 0 mirrors a.Equals(b).Truth().
func NullMissingIf(vc *ValComparer, a, b, whenEqual Val) Val {
	ka, kb := ValKind(a), ValKind(b)
	if ka == ValKindMissing || kb == ValKindMissing {
		return ValMissing
	}
	if ka == ValKindNull || kb == ValKindNull {
		return ValNull
	}
	if vc.Compare(a, b) == 0 {
		return whenEqual
	}
	return a
}

