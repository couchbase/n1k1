package base

import (
	"bytes"
	"sort"

	"github.com/buger/jsonparser"
)

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

func (c *ValComparer) Compare(a, b Val) int {
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

	// Both types are the same, so need type-based cases...
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
		// look like a sequence of pairs, like...
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

func CompareErr(aErr, bErr error) int {
	if aErr != nil && bErr != nil {
		return 0
	}

	if aErr != nil {
		return -1
	}

	return 1
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

// ---------------------------------------------

type LessFunc func(valsA, valsB Vals) bool

// ---------------------------------------------

func OrderByItems(items, projected []Vals, lessFunc LessFunc) {
	sort.Sort(&OrderBySorter{items, projected, lessFunc})
}

type OrderBySorter struct {
	Items     []Vals
	Projected []Vals // Same len() as Items.
	LessFunc  LessFunc
}

func (a *OrderBySorter) Len() int {
	return len(a.Items)
}

func (a *OrderBySorter) Swap(i, j int) {
	a.Items[i], a.Items[j] = a.Items[j], a.Items[i]
	a.Projected[i], a.Projected[j] = a.Projected[j], a.Projected[i]
}

func (a *OrderBySorter) Less(i, j int) bool {
	return a.LessFunc(a.Projected[i], a.Projected[j])
}
