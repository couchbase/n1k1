package n1k1

import (
	"bytes" // <== genCompiler:hide
	"fmt"
)

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

// -----------------------------------------------------

var LazyTrue = true

type LazyVals []LazyVal

type LazyVal []byte

var LazyValMissing = LazyVal(nil)

var LazyValNull = LazyVal([]byte("null"))

var LazyValTrue = LazyVal([]byte("true"))

var LazyValFalse = LazyVal([]byte("false"))

func (a LazyVal) String() string {
	return fmt.Sprintf("%q", []byte(a))
}

func LazyValEqualTrue(lazyVal LazyVal) bool {
	return len(lazyVal) > 0 && lazyVal[0] == 't'
}

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
