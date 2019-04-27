package base

import (
	"bytes"
	"encoding/json"
	"sort"
)

const (
	TYPE_UNKNOWN = int(iota)
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

type ValComparer struct {
	Preallocs [][]byte
}

func (c *ValComparer) Compare(a, b Val) int {
	if len(a) == 0 || len(b) == 0 {
		return CompareUnmarshal(c, a, b)
	}

	// Guess types as optimization to avoid CompareUnmarshal.
	ta := JsonByteToType[a[0]]
	tb := JsonByteToType[b[0]]

	if ta != tb && ta != TYPE_UNKNOWN && tb != TYPE_UNKNOWN {
		return ta - tb
	}

	return SameTypeCompareFuncs[ta](c, a, b)
}

// ---------------------------------------------

var SameTypeCompareFuncs = []func(*ValComparer, Val, Val) int{
	TYPE_UNKNOWN: CompareUnmarshal,
	TYPE_NULL:    func(c *ValComparer, a, b Val) int { return 0 },
	TYPE_BOOL:    CompareBothBool,
	TYPE_NUMBER:  CompareUnmarshal,
	TYPE_STRING:  CompareBothString,
	TYPE_ARRAY:   CompareUnmarshal,
	TYPE_OBJECT:  CompareUnmarshal,
}

// ---------------------------------------------

// Both a & b are JSON encoded bool's.
func CompareBothBool(c *ValComparer, a, b Val) int {
	return int(a[0]) - int(b[0]) // Ex: 't' - 'f'.
}

// Both a & b are JSON encoded strings.
func CompareBothString(c *ValComparer, a, b Val) int {
	return bytes.Compare(a[1:], b[1:]) // Skip '"' prefix.
}

// ---------------------------------------------

// Compares a & b by first invoking json.Unmarshal().
func CompareUnmarshal(c *ValComparer, a, b Val) int {
	var av, bv interface{}

	errA := json.Unmarshal(a, &av)
	errB := json.Unmarshal(b, &bv)

	if errA != nil || errB != nil {
		if errA != nil && errB != nil {
			return 0
		}
		if errA != nil {
			return -1
		}
		return 1
	}

	return c.CompareInterfaces(av, bv, 0)
}

// ---------------------------------------------

// Compares the interface{} output of json.Unmarshal().
func (c *ValComparer) CompareInterfaces(a, b interface{}, depth int) int {
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

			cmp := c.CompareInterfaces(va, vb, depth+1)
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

			cmp := c.CompareInterfaces(x, sb[i], depth+1)
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

	case TYPE_UNKNOWN:
		return 0

	default:
		return 0
	}

	return 0
}

// ---------------------------------------------

// InterfaceToType takes as input the result of json.Unmarshal().
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
		return TYPE_UNKNOWN
	}

	return TYPE_UNKNOWN
}
