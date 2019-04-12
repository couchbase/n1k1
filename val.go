package n1k1

import (
	"errors"
	"reflect"
)

var ErrMissing = errors.New("missing")

var JsonTypes = map[byte]string{ // TODO: Use byte array?
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

type LazyVal string

type LazyVals []LazyVal

const LazyValMissing = LazyVal("")

const LazyValNull = LazyVal("null")

const LazyValTrue = LazyVal("true")

const LazyValFalse = LazyVal("false")

// -----------------------------------------------------

func LazyValEquals(lazyValA, lazyValB LazyVal) (lazyVal LazyVal) {
	if lazyValA == LazyValMissing || lazyValB == LazyValMissing {
		lazyVal = LazyValMissing
	} else if lazyValA == LazyValNull || lazyValB == LazyValNull {
		lazyVal = LazyValNull
	} else if lazyValA == lazyValB {
		lazyVal = LazyValTrue
	} else {
		lazyVal = LazyValFalse
	}

	return lazyVal
}

// -----------------------------------------------------

type Types []string

func SetLastType(a Types, t string) {
	a[len(a)-1] = t
}

func TakeLastType(a Types) string {
	t := a[len(a)-1]
	a[len(a)-1] = ""
	return t
}

// -----------------------------------------------------

func DeepEqual(a, b interface{}) bool {
	return reflect.DeepEqual(a, b)
}
