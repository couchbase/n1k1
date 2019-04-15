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

const LazyScope = true // For marking varible scopes (ex: IF statement).

// -----------------------------------------------------

type LazyVals []LazyVal

type LazyVal []byte

var LazyValMissing = LazyVal(nil)

var LazyValNull = LazyVal([]byte("null"))

var LazyValTrue = LazyVal([]byte("true"))

var LazyValFalse = LazyVal([]byte("false"))

func (a LazyVal) String() string {
	return fmt.Sprintf("%q", []byte(a))
}

// -----------------------------------------------------

func LazyValEqualTrue(lazyVal LazyVal) bool {
	return len(lazyVal) > 0 && lazyVal[0] == 't'
}

// LazyValEqual follows N1QL's rules for missing & null's.
func LazyValEqual(lazyValA, lazyValB LazyVal) (lazyVal LazyVal) {
	if bytes.Equal(lazyValA, LazyValMissing) {
		lazyVal = LazyValMissing
	} else if bytes.Equal(lazyValB, LazyValMissing) {
		lazyVal = LazyValMissing
	} else if bytes.Equal(lazyValA, LazyValNull) {
		lazyVal = LazyValNull
	} else if bytes.Equal(lazyValB, LazyValNull) {
		lazyVal = LazyValNull
	} else if bytes.Equal(lazyValA, lazyValB) {
		lazyVal = LazyValTrue
	} else {
		lazyVal = LazyValFalse
	}

	return lazyVal
}

// -----------------------------------------------------

type LazyExprFunc func(lazyVals LazyVals) LazyVal

// -----------------------------------------------------

// The LazyYield memory ownership rule: the receiver func should copy
// any inputs that it wants to keep, because the provided slices might
// be reused by future invocations.
type LazyYield func(LazyVals)

type LazyYieldErr func(error)

// -----------------------------------------------------

type Operator struct {
	Kind   string        // Ex: "scan", "filter", "project", etc.
	Fields Fields        // Output fields of this operator.
	Params []interface{} // Params based on the kind.

	ParentA *Operator
	ParentB *Operator
}

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
