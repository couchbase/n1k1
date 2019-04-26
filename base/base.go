// The base package holds types and definitions shared by n1k1's
// interpreter and compiler.

package base

import (
	"bytes"
	"fmt"
)

type Fields []string // Ex: "description", "address.city".

func (a Fields) IndexOf(s string) int {
	for i, v := range a {
		if v == s {
			return i
		}
	}

	return -1
}

// -----------------------------------------------------

type Vals []Val

type Val []byte // JSON encoded, usually treated as immutable.

func (a Val) String() string {
	return fmt.Sprintf("%q", []byte(a))
}

var ValMissing = Val(nil)

var ValNull = Val([]byte("null"))

var ValTrue = Val([]byte("true"))

var ValFalse = Val([]byte("false"))

// -----------------------------------------------------

func ValEqualMissing(val Val) bool {
	return len(val) == 0
}

func ValEqualNull(val Val) bool {
	return len(val) != 0 && val[0] == 'n'
}

func ValEqualTrue(val Val) bool {
	return len(val) != 0 && val[0] == 't'
}

// ValEqual follows N1QL's rules for missing & null's.
func ValEqual(valA, valB Val) (val Val) {
	if ValEqualMissing(valA) {
		val = ValMissing
	} else if ValEqualMissing(valB) {
		val = ValMissing
	} else if valA[0] == 'n' { // Avoid ValEqualNull's len() check.
		val = ValNull
	} else if valB[0] == 'n' {
		val = ValNull
	} else if bytes.Equal(valA, valB) {
		val = ValTrue
	} else {
		val = ValFalse
	}

	return val
}

// -----------------------------------------------------

// YieldVals memory ownership: the receiver implementation should
// generally copy any inputs that it wants to keep, because the
// provided slices might be reused by future invocations.
type YieldVals func(Vals)

// Ops can occasionally yield stats and progress information,
// and the receiver can return an error to abort further processing.
type YieldStats func(*Stats) error

type YieldErr func(error)

// -----------------------------------------------------

type Stats struct{} // TODO.

// -----------------------------------------------------

// An Op represents a node or operation in a query-plan tree.
type Op struct {
	// Ex: "scan", "filter", "project", etc.
	Kind string `json:"Kind,omitempty"`

	// Output fields of this operator.
	Fields Fields `json:"Fields,omitempty"`

	// Output orders of this operator.
	Orders []Order `json:"Orders,omitempty"`

	// Params based on the kind.
	Params []interface{} `json:"Params,omitempty"`

	ParentA *Op `json:"ParentA,omitempty"`
	ParentB *Op `json:"ParentB,omitempty"`
}

type Order struct {
	Expr []interface{} // Ex: ["field", "country"].
	Desc bool          // True for descending.
}

// -----------------------------------------------------

// An ExprFunc evaluates an expression against the given vals.
type ExprFunc func(vals Vals) Val

// A BiExprFunc represents a two-parameter expression.
type BiExprFunc func(a, b ExprFunc, vals Vals) Val

// A ProjectFunc projects (in relational parlance) the given vals into
// resulting vals, reusing the pre-allocated valsPre if neeeded.
type ProjectFunc func(vals, valsPre Vals) Vals

// -----------------------------------------------------

type Types []string // TODO.
