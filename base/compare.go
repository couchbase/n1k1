package base

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/buger/jsonparser"
)

var ValueTypePriority = []int{
	jsonparser.NotExist: 0,
	jsonparser.Null:     1,
	jsonparser.Boolean:  2,
	jsonparser.Number:   3,
	jsonparser.String:   4,
	jsonparser.Array:    5,
	jsonparser.Object:   6,
	jsonparser.Unknown:  7, // Ex: BINARY.
}

// ---------------------------------------------

type ValComparer struct {
	// Reused across Compare()'s, indexed by: depth.
	KeyVals []KeyVals

	Buffer bytes.Buffer // Recycled io.Writer.
}

func NewValComparer() *ValComparer { return &ValComparer{} }

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

	return c.CompareDeepType(aValue, bValue, aValueType, bValueType, depth)
}

func (c *ValComparer) CompareDeepType(aValue, bValue []byte,
	aValueType, bValueType jsonparser.ValueType, depth int) int {
	if aValueType != bValueType {
		return ValueTypePriority[aValueType] - ValueTypePriority[bValueType]
	}

	// Both types are the same, so need type-based cases...
	switch aValueType {
	case jsonparser.String:
		kvs := c.KeyValsAcquire(depth)

		kvs = append(kvs, KeyVal{ReuseNextKey(kvs), nil, 0, 0})
		aBuf := kvs[len(kvs)-1].Key

		kvs = append(kvs, KeyVal{ReuseNextKey(kvs), nil, 0, 0})
		bBuf := kvs[len(kvs)-1].Key

		av, aErr := jsonparser.Unescape(aValue, aBuf[:cap(aBuf)])
		bv, bErr := jsonparser.Unescape(bValue, bBuf[:cap(bBuf)])

		kvs[0].Key = av
		kvs[1].Key = bv

		c.KeyValsRelease(depth, kvs)

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

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
		kvs := c.KeyValsAcquire(depth)

		_, bErr := jsonparser.ArrayEach(bValue,
			func(v []byte, vT jsonparser.ValueType, o int, vErr error) {
				kvs = append(kvs, KeyVal{ReuseNextKey(kvs), v, vT, 0})
			})

		bLen := len(kvs)

		depthPlus1 := depth + 1

		var i int
		var cmp int

		_, aErr := jsonparser.ArrayEach(aValue,
			func(v []byte, vT jsonparser.ValueType, o int, vErr error) {
				if cmp != 0 {
					return
				}

				if i >= bLen {
					cmp = 1
					return
				}

				cmp = c.CompareDeepType(
					v, kvs[i].Val, vT, kvs[i].ValType, depthPlus1)

				i++
			})

		c.KeyValsRelease(depth, kvs)

		if i < bLen {
			return -1
		}

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		return cmp

	case jsonparser.Object:
		kvs := c.KeyValsAcquire(depth)

		var aLen int
		aErr := jsonparser.ObjectEach(aValue,
			func(k []byte, v []byte, vT jsonparser.ValueType, o int) error {
				kCopy := append(ReuseNextKey(kvs), k...)
				kvs = append(kvs, KeyVal{kCopy, v, vT, 1})
				aLen++
				return nil
			})

		var bLen int
		bErr := jsonparser.ObjectEach(bValue,
			func(k []byte, v []byte, vT jsonparser.ValueType, o int) error {
				kCopy := append(ReuseNextKey(kvs), k...)
				kvs = append(kvs, KeyVal{kCopy, v, vT, -1})
				bLen++
				return nil
			})

		c.KeyValsRelease(depth, kvs)

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

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

			cmp := c.CompareDeepType(kvX.Val, kvY.Val,
				kvX.ValType, kvY.ValType, depthPlus1)
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

// EncodeAsString appends the JSON encoded string to the optional out
// slice and returns the extended out.
func (c *ValComparer) EncodeAsString(s []byte, out []byte) ([]byte, error) {
	c.Buffer.Reset()

	fmt.Fprintf(&c.Buffer, "%q", s)

	written := c.Buffer.Len()

	lenOld := len(out)
	needed := lenOld + written

	if cap(out) >= needed {
		out = out[:needed]
	} else {
		out = append(make([]byte, 0, needed), out...)[:needed]
	}

	c.Buffer.Read(out[lenOld:])

	return out, nil
}

// ---------------------------------------------

func (c *ValComparer) KeyValsAcquire(depth int) KeyVals {
	for len(c.KeyVals) < depth+1 {
		c.KeyVals = append(c.KeyVals, nil)
	}

	return c.KeyVals[depth]
}

func (c *ValComparer) KeyValsRelease(depth int, s KeyVals) {
	c.KeyVals[depth] = s[:0]
}

// ---------------------------------------------

type KeyVal struct {
	Key     []byte
	Val     []byte
	ValType jsonparser.ValueType
	Pos     int
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

// When append()'ing to the kvs, the entry that we're going to
// overwrite might have a Key []byte that we can reuse.
func ReuseNextKey(kvs KeyVals) []byte {
	if cap(kvs) > len(kvs) {
		return kvs[0 : len(kvs)+1][len(kvs)].Key[:0]
	}

	return nil
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

type LessFunc func(valsA, valsB Vals) bool

// ---------------------------------------------

type ValsProjected struct {
	Vals      Vals
	Projected Vals
}

// ---------------------------------------------

type HeapValsProjected struct {
	ValsProjected []ValsProjected
	LessFunc      LessFunc
}

func (a *HeapValsProjected) GetVals(i int) Vals {
	return a.ValsProjected[i].Vals
}

func (a *HeapValsProjected) GetProjected(i int) Vals {
	return a.ValsProjected[i].Projected
}

func (a *HeapValsProjected) Len() int { return len(a.ValsProjected) }

func (a *HeapValsProjected) Swap(i, j int) {
	a.ValsProjected[i], a.ValsProjected[j] =
		a.ValsProjected[j], a.ValsProjected[i]
}

func (a *HeapValsProjected) Less(i, j int) bool {
	// Reverse of normal LessFunc() so that we have a max-heap.
	return a.LessFunc(
		a.ValsProjected[j].Projected, a.ValsProjected[i].Projected)
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
