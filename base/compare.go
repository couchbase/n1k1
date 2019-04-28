package base

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/buger/jsonparser"
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

// ---------------------------------------------

type ValComparer struct {
	Preallocs [][]string // Slices reused across Compare()'s.
}

// ---------------------------------------------

func (c *ValComparer) Alloc(depth, size int) []string {
	for len(c.Preallocs) < depth+1 {
		c.Preallocs = append(c.Preallocs, nil)
	}

	a := c.Preallocs[depth]
	if len(a) < size {
		a = make([]string, size)
		c.Preallocs[depth] = a
	}

	return a[:0]
}

// ---------------------------------------------

func (c *ValComparer) CompareOrig(a, b Val, av, bv *interface{}) int {
	var errA, errB error

	if *av == nil {
		errA = json.Unmarshal(a, av)
	}

	if *bv == nil {
		errB = json.Unmarshal(b, bv)
	}

	if errA != nil || errB != nil {
		if errA != nil && errB != nil {
			return 0
		}
		if errA != nil {
			return -1
		}
		return 1
	}

	return c.CompareInterfaces(*av, *bv, 0)
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
		keys := c.Alloc(depth, len(oa)+len(ob))

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

// ---------------------------------------------

type LessFunc func(
	valsA, valsB Vals, iA, iB []interface{}) bool

// ---------------------------------------------

func OrderByItems(items, projected []Vals, interfaces [][]interface{},
	lessFunc LessFunc) {
	sort.Sort(&OrderBySorter{items, projected, interfaces, lessFunc})
}

type OrderBySorter struct {
	Items      []Vals
	Projected  []Vals          // Same len() as Items.
	Interfaces [][]interface{} // Same len() as Items.
	LessFunc   LessFunc
}

func (a *OrderBySorter) Len() int {
	return len(a.Items)
}

func (a *OrderBySorter) Swap(i, j int) {
	a.Items[i], a.Items[j] = a.Items[j], a.Items[i]
	a.Projected[i], a.Projected[j] = a.Projected[j], a.Projected[i]
	a.Interfaces[i], a.Interfaces[j] = a.Interfaces[j], a.Interfaces[i]
}

func (a *OrderBySorter) Less(i, j int) bool {
	return a.LessFunc(
		a.Projected[i], a.Projected[j], a.Interfaces[i], a.Interfaces[j])
}

// ---------------------------------------------

var ValueTypePriority = []int{
	jsonparser.NotExist: 0,
	jsonparser.Unknown:  1,
	jsonparser.Null:     2,
	jsonparser.Boolean:  3,
	jsonparser.Number:   4,
	jsonparser.String:   5,
	jsonparser.Array:    6,
	jsonparser.Object:   7,
}

func CompareErr(aErr, bErr error) int {
	if aErr != nil && bErr != nil {
		return 0
	}
	if aErr != nil {
		return -1
	}
	return 1
}

func (c *ValComparer) Compare(a, b Val, av, bv *interface{}) int {
	return c.CompareDeep(a, b, 0)
}

func (c *ValComparer) CompareDeep(a, b []byte, depth int) int {
	aValue, aValueType, _, aErr := jsonparser.Get(a)
	bValue, bValueType, _, bErr := jsonparser.Get(b)

	if aErr != nil || bErr != nil {
		return CompareErr(aErr, bErr)
	}

	if aValueType != bValueType {
		return ValueTypePriority[aValueType] - ValueTypePriority[bValueType]
	}

	switch aValueType {
	case jsonparser.String:
		av, aErr := jsonparser.ParseString(aValue) // TODO: Mem-management.
		bv, bErr := jsonparser.ParseString(bValue)

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		if av == bv {
			return 0
		}
		if av < bv {
			return -1
		}
		return 1

	case jsonparser.Number:
		av, aErr := jsonparser.ParseFloat(aValue)
		bv, bErr := jsonparser.ParseFloat(bValue)

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		if av == bv {
			return 0
		}
		if av < bv {
			return -1
		}
		return 1

	case jsonparser.Boolean:
		return int(aValue[0]) - int(bValue[0]) // Ex: 't' - 'f'.

	case jsonparser.Array:
		var bItems [][]byte
		_, bErr := jsonparser.ArrayEach(bValue,
			func(v []byte, vT jsonparser.ValueType, vOffset int, vErr error) {
				bItems = append(bItems, v)
			})

		depthPlus1 := depth + 1

		var i int
		var cmp int

		_, aErr := jsonparser.ArrayEach(aValue,
			func(v []byte, vT jsonparser.ValueType, vOffset int, vErr error) {
				if cmp != 0 {
					return
				}

				if i >= len(bItems) {
					cmp = 1
					return
				}

				cmp = c.CompareDeep(v, bItems[i], depthPlus1)

				i++
			})

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		return cmp

	case jsonparser.Object:
		var kvs KeyVals

		var aLen int
		aErr := jsonparser.ObjectEach(aValue,
			func(k []byte, v []byte, vT jsonparser.ValueType, offset int) error {
				kvs = append(kvs, KeyVal{k, v, 1})
				aLen++
				return nil
			})

		var bLen int
		bErr := jsonparser.ObjectEach(aValue,
			func(k []byte, v []byte, vT jsonparser.ValueType, offset int) error {
				kvs = append(kvs, KeyVal{k, v, -1})
				bLen++
				return nil
			})

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		delta := aLen - bLen // Larger object wins.
		if delta != 0 {
			return delta
		}

		sort.Sort(kvs)

		// With closely matching objects, the sorted kvs should will
		// like a sequence of pairs, like...
		//
		// [{"city", "sf", 1}, {"city", "sf", -1}, {"state", ...} ...]
		//
		// A KeyVal entry from aValue has Pos 1.
		// A KeyVal entry from bValue has Pos -1.
		//
		// The following loop looks for a non-matching pair, kvX & kvY.
		//
		depthPlus1 := depth + 1

		i := 0
		for i < len(kvs) {
			kvX := kvs[i]
			i++

			if i >= len(kvs) {
				return kvX.Pos
			}

			kvY := kvs[i]
			i++

			if kvX.Pos == kvY.Pos {
				return kvX.Pos
			}

			if !bytes.Equal(kvX.Key, kvY.Key) {
				return kvX.Pos
			}

			cmp := c.CompareDeep(kvX.Val, kvY.Val, depthPlus1)
			if cmp != 0 {
				return cmp
			}
		}

		return 0

	default: // Null, NotExist, Unknown.
		return 0
	}
}

// ---------------------------------------------

type KeyVal struct {
	Key []byte
	Val []byte
	Pos int
}

type KeyVals []KeyVal

func (a KeyVals) Len() int { return len(a) }

func (a KeyVals) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a KeyVals) Less(i, j int) bool {
	cmp := bytes.Compare(a[i].Key, a[j].Key)
	if cmp < 0 {
		return true
	}

	if cmp > 0 {
		return false
	}

	return a[i].Pos > a[j].Pos // Reverse ordering on Pos.
}
