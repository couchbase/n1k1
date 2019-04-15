// Base has type definitions shared by interpreter and compiler.

package base

import (
	"bytes"
	"fmt"
)

type Fields []string

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

var ValMissing = Val(nil)

var ValNull = Val([]byte("null"))

var ValTrue = Val([]byte("true"))

var ValFalse = Val([]byte("false"))

func (a Val) String() string {
	return fmt.Sprintf("%q", []byte(a))
}

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

// YieldVals memory ownership: the receiver func should generally copy
// any inputs that it wants to keep, because the provided slices might
// be reused by future invocations.
type YieldVals func(Vals)

type YieldErr func(error)

// -----------------------------------------------------

type Operator struct {
	Kind   string        // Ex: "scan", "filter", "project", etc.
	Fields Fields        // Output fields of this operator.
	Params []interface{} // Params based on the kind.

	ParentA *Operator
	ParentB *Operator
}

type ExprFunc func(vals Vals) Val

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
