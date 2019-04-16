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

type Val []byte // JSON encoded.

func (a Val) String() string {
	return fmt.Sprintf("%q", []byte(a))
}

var ValMissing = Val(nil)

var ValNull = Val([]byte("null"))

var ValTrue = Val([]byte("true"))

var ValFalse = Val([]byte("false"))

// -----------------------------------------------------

func ValEqualTrue(val Val) bool {
	return len(val) > 0 && val[0] == 't'
}

// ValEqual follows N1QL's rules for missing & null's.
func ValEqual(valA, valB Val) (val Val) {
	if bytes.Equal(valA, ValMissing) {
		val = ValMissing
	} else if bytes.Equal(valB, ValMissing) {
		val = ValMissing
	} else if bytes.Equal(valA, ValNull) {
		val = ValNull
	} else if bytes.Equal(valB, ValNull) {
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

type YieldErr func(error)

// -----------------------------------------------------

// An Operator represents a node in a hierarchical query-plan tree.
type Operator struct {
	// Ex: "scan", "filter", "project", etc.
	Kind string `json:"Kind,omitempty"`

	// Output fields of this operator.
	Fields Fields `json:"Fields,omitempty"`

	// Params based on the kind.
	Params []interface{} `json:"Params,omitempty"`

	ParentA *Operator `json:"ParentA,omitempty"`
	ParentB *Operator `json:"ParentB,omitempty"`
}

// An ExprFunc evaluates an expression against the given vals.
type ExprFunc func(vals Vals) Val

// A BiExprFunc represents a two-parameter expression.
type BiExprFunc func(a, b ExprFunc, vals Vals) Val

// A ProjectFunc projects (in relational parlance) the given vals into
// resulting vals, reusing the pre-allocated valsPre if neeeded.
type ProjectFunc func(vals, valsPre Vals) Vals

// -----------------------------------------------------

type Types []string

func SetLastType(a Types, t string) {
	if len(a) > 0 {
		a[len(a)-1] = t
	}
}

func TakeLastType(a Types) (t string) {
	if len(a) > 0 {
		t = a[len(a)-1]
		a[len(a)-1] = ""
	}

	return t
}

// -----------------------------------------------------

// JsonTypes allows 0'th byte of a json []byte to tell us the type.
var JsonTypes = map[byte]string{ // TODO: Use array instead of map?
	'"': "string",
	'{': "object",
	'[': "array",
	'n': "null",
	't': "bool", // From "true".
	'f': "bool", // From "false".
	'-': "number",
	'0': "number",
	'1': "number",
	'2': "number",
	'3': "number",
	'4': "number",
	'5': "number",
	'6': "number",
	'7': "number",
	'8': "number",
	'9': "number",
}
