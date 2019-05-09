package base

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/buger/jsonparser"
)

const (
	ValTypeMissing = iota
	ValTypeNull
	ValTypeBoolean
	ValTypeNumber
	ValTypeString
	ValTypeArray
	ValTypeObject
	ValTypeUnknown // Ex: BINARY.
)

var ParseTypeToValType = []int{
	jsonparser.NotExist: ValTypeMissing,
	jsonparser.Null:     ValTypeNull,
	jsonparser.Boolean:  ValTypeBoolean,
	jsonparser.Number:   ValTypeNumber,
	jsonparser.String:   ValTypeString,
	jsonparser.Array:    ValTypeArray,
	jsonparser.Object:   ValTypeObject,
	jsonparser.Unknown:  ValTypeUnknown, // Ex: BINARY.
}

// ---------------------------------------------

func Parse(b []byte) (parseVal []byte, parseType int) {
	if len(b) == 0 {
		return nil, int(jsonparser.NotExist) // ValTypeMissing.
	}

	v, vt, _, err := jsonparser.Get(b)
	if err != nil {
		return b, int(jsonparser.Unknown)
	}

	return v, int(vt)
}

func ParseTypeHasValue(parseType int) bool {
	return ParseTypeToValType[parseType] > ValTypeNull
}

func ParseFloat64(v []byte) (float64, error) {
	return jsonparser.ParseFloat(v)
}

// ---------------------------------------------

type ValComparer struct {
	// Reused across Compare()'s, indexed by: depth.
	KeyVals []KeyVals

	Buffer bytes.Buffer

	Bytes []byte

	Encoder *json.Encoder
}

func NewValComparer() *ValComparer {
	rv := &ValComparer{}
	rv.Encoder = json.NewEncoder(&rv.Buffer)
	return rv
}

// ---------------------------------------------

func (c *ValComparer) Compare(a, b Val) int {
	aValue, aValueType, _, aErr := jsonparser.Get(a)
	bValue, bValueType, _, bErr := jsonparser.Get(b)

	if aErr != nil || bErr != nil {
		return CompareErr(aErr, bErr)
	}

	return c.CompareWithType(aValue, bValue, int(aValueType), int(bValueType), 0)
}

func (c *ValComparer) CompareWithType(aValue, bValue []byte,
	aValueType, bValueType int, depth int) int {
	if aValueType != bValueType {
		return ParseTypeToValType[aValueType] - ParseTypeToValType[bValueType]
	}

	// Both types are the same, so need type-based cases...
	switch jsonparser.ValueType(aValueType) {
	case jsonparser.String:
		kvs := c.KeyValsAcquire(depth)

		aBuf := ReuseNextKey(kvs)
		kvs = append(kvs, KeyVal{Key: aBuf})

		bBuf := ReuseNextKey(kvs)
		kvs = append(kvs, KeyVal{Key: bBuf})

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
				kvs = append(kvs, KeyVal{ReuseNextKey(kvs), v, int(vT), 0})
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

				cmp = c.CompareWithType(
					v, kvs[i].Val, int(vT), kvs[i].ValType, depthPlus1)

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
				kvs = append(kvs, KeyVal{kCopy, v, int(vT), 1})
				aLen++
				return nil
			})

		var bLen int
		bErr := jsonparser.ObjectEach(bValue,
			func(k []byte, v []byte, vT jsonparser.ValueType, o int) error {
				kCopy := append(ReuseNextKey(kvs), k...)
				kvs = append(kvs, KeyVal{kCopy, v, int(vT), -1})
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

			cmp := c.CompareWithType(kvX.Val, kvY.Val,
				int(kvX.ValType), int(kvY.ValType), depthPlus1)
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

	c.Bytes = s

	c.Encoder.Encode(c)

	written := c.Buffer.Len() - 1 // Strip off newline from encoder.

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

// MarshalText() allows the ValComparer to implements the
// encoding.TextMarshaler interface with no extra allocations.
func (c *ValComparer) MarshalText() ([]byte, error) { return c.Bytes, nil }

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
	ValType int
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
