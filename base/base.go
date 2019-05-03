// The base package holds types and definitions shared by n1k1's
// interpreter and compiler.

package base

import (
	"bytes"
	"fmt"

	"github.com/buger/jsonparser"
)

type Fields []string // Ex: ".description", ".address.city".

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

// ValsDeepCopy copies vals into the optional, preallocated slices.
func ValsDeepCopy(vals Vals, preallocVals Vals, preallocVal Val) (
	Vals, Vals, Val) {
	var bytesNeeded int
	for _, val := range vals {
		bytesNeeded += len(val)
	}

	if len(preallocVal) < bytesNeeded {
		preallocVal = make(Val, bytesNeeded)
	}

	copyVal := preallocVal[:0]
	preallocVal = preallocVal[bytesNeeded:]

	if len(preallocVals) < len(vals) {
		preallocVals = make(Vals, len(vals))
	}

	copyVals := preallocVals[:0]
	preallocVals = preallocVals[len(vals):]

	for _, val := range vals {
		copyVal = append(copyVal, val...)
		copyVals = append(copyVals, copyVal)
		copyVal = copyVal[len(val):]
	}

	return copyVals, preallocVals, preallocVal
}

// -----------------------------------------------------

func ValPathGet(vIn Val, path []string) Val {
	v, _, _, err := jsonparser.Get(vIn, path...)
	if err != nil {
		return ValMissing
	}

	return v
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

	Children []*Op `json:"Children,omitempty"`
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

// -----------------------------------------------------

// Vars are used for runtime variables, config, etc.
type Vars struct {
	Fields Fields
	Vals   Vals  // Same len() as Fields.
	Next   *Vars // The root Vars has nil Next.
	Ctx    *Ctx
}

// -----------------------------------------------------

// Ctx represents the runtime context for a request.
type Ctx struct {
	YieldStats YieldStats

	// TODO: Other things that might appear here might be request ID,
	// request-specific allocators or resources, etc.
}
