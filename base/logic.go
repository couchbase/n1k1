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

// LogicAnd2 / LogicOr2 combine two already-evaluated operand values under N1QL's
// three-valued logic, mirroring cbq expression.And/Or.Evaluate for two operands.
// They are the leaf reduce called in one line from the MakeBiExprFunc harness
// (engine/expr_logic.go). The optimizer folds an n-ary AND/OR into right-nested
// binary applications of these; that is exact because SQL AND/OR are associative
// even in the presence of NULL/MISSING (a real, decided operand short-circuits
// the result regardless of grouping, and the unknown-precedence below is
// idempotent under nesting).
//
// A truthy operand is classified as MISSING (empty Val), NULL, or a real value
// whose truthiness is ValTruthy (matching cbq value.Truth(): non-empty
// string/array/object, non-zero number, the bool itself). Note the asymmetric
// unknown precedence, copied from cbq: AND yields MISSING over NULL, OR yields
// NULL over MISSING.

// LogicAnd2: a real, non-truthy operand forces FALSE; else MISSING dominates
// NULL dominates TRUE.
func LogicAnd2(a, b Val) Val {
	ka, kb := ValKind(a), ValKind(b)
	if (ka == ValKindValue && !ValTruthy(a)) || (kb == ValKindValue && !ValTruthy(b)) {
		return ValFalse
	}
	if ka == ValKindMissing || kb == ValKindMissing {
		return ValMissing
	}
	if ka == ValKindNull || kb == ValKindNull {
		return ValNull
	}
	return ValTrue
}

// LogicOr2: a real, truthy operand forces TRUE; else NULL dominates MISSING
// dominates FALSE.
func LogicOr2(a, b Val) Val {
	ka, kb := ValKind(a), ValKind(b)
	if (ka == ValKindValue && ValTruthy(a)) || (kb == ValKindValue && ValTruthy(b)) {
		return ValTrue
	}
	if ka == ValKindNull || kb == ValKindNull {
		return ValNull
	}
	if ka == ValKindMissing || kb == ValKindMissing {
		return ValMissing
	}
	return ValFalse
}
