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
