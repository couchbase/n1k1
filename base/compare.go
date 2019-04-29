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
	// Reused across Compare()'s, indexed by: pos, depth.
	Bytes [][][]byte

	// Reused across Compare()'s, indexed by: depth.
	BytesSlice [][][]byte

	// Reused across Compare()'s, indexed by: depth.
	KeyVals []KeyVals
}

func NewValComparer() *ValComparer {
	return &ValComparer{Bytes: make([][][]byte, 2)}
}

// ---------------------------------------------

func (c *ValComparer) BytesGet(pos, depth int) []byte {
	byDepth := c.Bytes[pos]

	for len(byDepth) < depth+1 {
		byDepth = append(byDepth, nil)
		c.Bytes[pos] = byDepth
	}

	return byDepth[depth]
}

func (c *ValComparer) BytesSet(pos, depth int, s []byte) {
	c.Bytes[pos][depth] = s[0:cap(s)]
}

// ---------------------------------------------

func (c *ValComparer) BytesSliceGet(depth int) [][]byte {
	for len(c.BytesSlice) < depth+1 {
		c.BytesSlice = append(c.BytesSlice, nil)
	}

	return c.BytesSlice[depth]
}

func (c *ValComparer) BytesSliceSet(depth int, s [][]byte) {
	c.BytesSlice[depth] = s[:0]
}

// ---------------------------------------------

func (c *ValComparer) KeyValsGet(depth int) KeyVals {
	for len(c.KeyVals) < depth+1 {
		c.KeyVals = append(c.KeyVals, nil)
	}

	return c.KeyVals[depth]
}

func (c *ValComparer) KeyValsSet(depth int, s KeyVals) {
	c.KeyVals[depth] = s[:0]
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
		aBuf := c.BytesGet(0, depth)
		bBuf := c.BytesGet(1, depth)

		av, aErr := jsonparser.Unescape(aValue, aBuf)
		bv, bErr := jsonparser.Unescape(bValue, bBuf)

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		c.BytesSet(0, depth, av)
		c.BytesSet(1, depth, bv)

		return bytes.Compare(av, bv)

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
		bItems := c.BytesSliceGet(depth)

		_, bErr := jsonparser.ArrayEach(bValue,
			func(v []byte, vT jsonparser.ValueType, vOffset int, vErr error) {
				bItems = append(bItems, v)
			})

		bLen := len(bItems)

		depthPlus1 := depth + 1

		var i int
		var cmp int

		_, aErr := jsonparser.ArrayEach(aValue,
			func(v []byte, vT jsonparser.ValueType, vOffset int, vErr error) {
				if cmp != 0 {
					return
				}

				if i >= bLen {
					cmp = 1
					return
				}

				cmp = c.CompareDeep(v, bItems[i], depthPlus1)

				i++
			})

		c.BytesSliceSet(depth, bItems)

		if i < bLen {
			return -1
		}

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		return cmp

	case jsonparser.Object:
		kvs := c.KeyValsGet(depth)

		var aLen int
		aErr := jsonparser.ObjectEach(aValue,
			func(k []byte, v []byte, vT jsonparser.ValueType, offset int) error {
				kvs = append(kvs, KeyVal{k, v, 1})
				aLen++
				return nil
			})

		var bLen int
		bErr := jsonparser.ObjectEach(bValue,
			func(k []byte, v []byte, vT jsonparser.ValueType, offset int) error {
				kvs = append(kvs, KeyVal{k, v, -1})
				bLen++
				return nil
			})

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		c.KeyValsSet(depth, kvs)

		if aLen != bLen {
			return aLen - bLen // Larger object wins.
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

type ValsProjected struct {
	Vals      Vals
	Projected Vals
}

func ValsProjectedVals(x *ValsProjected) Vals { return x.Vals }

// ---------------------------------------------

type HeapValsProjected struct {
	ValsProjected []ValsProjected
	LessFunc      LessFunc
}

func (a *HeapValsProjected) GetVals(i int) Vals {
	return a.ValsProjected[i].Vals
}

func (a *HeapValsProjected) Len() int { return len(a.ValsProjected) }

func (a *HeapValsProjected) Swap(i, j int) {
	a.ValsProjected[i], a.ValsProjected[j] = a.ValsProjected[j], a.ValsProjected[i]
}

func (a *HeapValsProjected) Less(i, j int) bool {
	// Reverse of normal LessFunc() so that we have a max-heap.
	return a.LessFunc(a.ValsProjected[j].Projected, a.ValsProjected[i].Projected)
}

func (a *HeapValsProjected) Push(x interface{}) {
	a.ValsProjected = append(a.ValsProjected, x.(ValsProjected))
}

func (a *HeapValsProjected) Pop() interface{} {
	end := len(a.ValsProjected) - 1
	rv := a.ValsProjected[end]
	a.ValsProjected = a.ValsProjected[0:end]
	return rv
}
