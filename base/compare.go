package base

import (
	"bytes"
	"encoding/json"
	"sort"
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

var SameTypeCompareFuncs = []func(a, b Val) int{
	TYPE_MISSING: CompareUnmarshalJson,
	TYPE_NULL:    func(a, b Val) int { return 0 },
	TYPE_BOOL:    CompareBothBool,
	TYPE_NUMBER:  CompareUnmarshalJson,
	TYPE_STRING:  CompareBothString,
	TYPE_ARRAY:   CompareUnmarshalJson,
	TYPE_OBJECT:  CompareUnmarshalJson,
}

// ---------------------------------------------

// Both a & b are JSON encoded bool's.
func CompareBothBool(a, b Val) int {
	return int(a[0]) - int(b[0]) // Ex: 't' - 'f'.
}

// Both a & b are JSON encoded strings.
func CompareBothString(a, b Val) int {
	return bytes.Compare(a[1:], b[1:]) // Skip '"' prefix.
}

// ---------------------------------------------

// Compares a & b via UnmarshalJSON().
func CompareUnmarshalJson(a, b Val) int {
	var av, bv interface{}

	err := json.Unmarshal(a, &av)
	if err != nil {
		return -1
	}

	err = json.Unmarshal(b, &bv)
	if err != nil {
		return 1
	}

	return CompareInterfaces(av, bv)
}

// ---------------------------------------------

// Compares the interface{} output's of UnmarshalJSON().
func CompareInterfaces(a, b interface{}) int {
	ta := InterfaceToType(a)
	tb := InterfaceToType(b)

	if ta != tb {
		return ta - tb
	}

	switch ta {
	case TYPE_STRING:
		sa := a.(string)
		sb := b.(string)
		if sa == sb {
			return 0
		}
		if sa < sb {
			return -1
		}
		return 1

	case TYPE_NUMBER:
		sa := a.(float64)
		sb := b.(float64)
		if sa == sb {
			return 0
		}
		if sa < sb {
			return -1
		}
		return 1

	case TYPE_OBJECT:
		oa := a.(map[string]interface{})
		ob := b.(map[string]interface{})

		delta := len(oa) - len(ob) // Larger object wins.
		if delta != 0 {
			return delta
		}

		// Sort keys.
		keys := make([]string, 0, len(oa)+len(ob))

		for key := range oa {
			keys = append(keys, key)
		}
		for key := range ob {
			keys = append(keys, key)
		}

		sort.Strings(keys)

		uniq := keys[:0] // Dedupe keys.

		for i, key := range keys {
			if i == 0 || key != keys[i-1] {
				uniq = append(uniq, key)
			}
		}

		// Compare by sorted, uniq keys.
		for _, key := range uniq {
			va, ok := oa[key]
			if !ok {
				return 1
			}

			vb, ok := ob[key]
			if !ok {
				return -1
			}

			cmp := CompareInterfaces(va, vb)
			if cmp != 0 {
				return cmp
			}
		}

		return 0

	case TYPE_ARRAY:
		sa := a.([]interface{})
		sb := a.([]interface{})

		for i, x := range sa {
			if i >= len(sb) {
				return 1
			}

			cmp := CompareInterfaces(x, sb[i])
			if cmp != 0 {
				return cmp
			}
		}

		return 0

	case TYPE_BOOL:
		sa := a.(bool)
		sb := b.(bool)
		if sa == sb {
			return 0
		}
		if !sa {
			return -1
		}
		return 1

	case TYPE_NULL:
		return 0

	case TYPE_MISSING:
		return 0

	default:
		return 0
	}

	return 0
}

// ---------------------------------------------

func InterfaceToType(val interface{}) int {
	if val == nil {
		return TYPE_NULL
	}

	switch val.(type) {
	case string:
		return TYPE_STRING
	case float64:
		return TYPE_NUMBER
	case map[string]interface{}:
		return TYPE_OBJECT
	case []interface{}:
		return TYPE_ARRAY
	case nil:
		return TYPE_NULL
	case bool:
		return TYPE_BOOL
	default:
		return TYPE_MISSING
	}

	return TYPE_MISSING
}

// ---------------------------------------------

func ValCompare(a, b Val) int {
	if len(a) == 0 || len(b) == 0 {
		return CompareUnmarshalJson(a, b)
	}

	ta := JsonByteToType[a[0]]
	tb := JsonByteToType[b[0]]

	if ta != tb {
		return ta - tb
	}

	return SameTypeCompareFuncs[ta](a, b)
}
