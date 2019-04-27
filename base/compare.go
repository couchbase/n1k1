package base

import (
	"bytes"
	"encoding/json"
)

const (
	TYPE_MISSING = int(iota)
	TYPE_NULL
	TYPE_BOOL
	TYPE_NUMBER
	TYPE_STRING
	TYPE_ARRAY
	TYPE_OBJECT
)

var TYPE_NAMES = []string{
	TYPE_MISSING: "missing",
	TYPE_NULL:    "null",
	TYPE_BOOL:    "bool",
	TYPE_NUMBER:  "number",
	TYPE_STRING:  "string",
	TYPE_ARRAY:   "array",
	TYPE_OBJECT:  "object",
}

// Determine type by the first (0'th) byte of a JSON value.
var JsonByteToType = []int{
	'"': TYPE_STRING,
	'{': TYPE_OBJECT,
	'[': TYPE_ARRAY,
	'n': TYPE_NULL,
	't': TYPE_BOOL,
	'f': TYPE_BOOL,
	'-': TYPE_NUMBER,
	'0': TYPE_NUMBER,
	'1': TYPE_NUMBER,
	'2': TYPE_NUMBER,
	'3': TYPE_NUMBER,
	'4': TYPE_NUMBER,
	'5': TYPE_NUMBER,
	'6': TYPE_NUMBER,
	'7': TYPE_NUMBER,
	'8': TYPE_NUMBER,
	'9': TYPE_NUMBER,
}

// ---------------------------------------------

type SameTypeCompareFunc func(a, b Val) int

var SameTypeCompareFuncs = []SameTypeCompareFunc{
	TYPE_MISSING: SameTypeCompareMissing,
	TYPE_NULL:    SameTypeCompareNull,
	TYPE_BOOL:    SameTypeCompareBool,
	TYPE_NUMBER:  CompareJson,
	TYPE_STRING:  SameTypeCompareString,
	TYPE_ARRAY:   CompareJson,
	TYPE_OBJECT:  CompareJson,
}

// ---------------------------------------------

func SameTypeCompareMissing(a, b Val) int {
	return 0
}

func SameTypeCompareNull(a, b Val) int {
	return 0
}

func SameTypeCompareBool(a, b Val) int {
	return int(a[0]) - int(b[0]) // E.g., 't' - 'f'.
}

func SameTypeCompareString(a, b Val) int {
	return bytes.Compare(a[1:], b[1:]) // Skip '"' prefix.
}

// ---------------------------------------------

func CompareJson(a, b Val) int {
	var av, bv interface{}

	err := json.Unmarshal(a, &av)
	if err != nil {
		return -1
	}

	err = json.Unmarshal(b, &bv)
	if err != nil {
		return 1
	}

	return CompareGeneral(av, bv)
}

func CompareGeneral(a, b interface{}) int {
	return 0
}

// ---------------------------------------------

func ValCompare(a, b Val) int {
	var ta int
	if len(a) > 0 {
		ta = JsonByteToType[a[0]]
	}

	var tb int
	if len(b) > 0 {
		tb = JsonByteToType[b[0]]
	}

	if ta != tb {
		return ta - tb
	}

	return SameTypeCompareFuncs[ta](a, b)
}
