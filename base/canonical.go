package base

import (
	"sort"
	"strconv"

	"github.com/buger/jsonparser"
)

func (c *ValComparer) CanonicalJSON(a, out []byte) ([]byte, error) {
	return c.CanonicalJSONDeep(a, out, 0)
}

func (c *ValComparer) CanonicalJSONDeep(a, out []byte, depth int) (
	[]byte, error) {
	v, vType, _, err := jsonparser.Get(a)
	if err != nil {
		return nil, err
	}

	// Both types are the same, so need type-based cases...
	switch vType {
	case jsonparser.String, jsonparser.Boolean, jsonparser.Null:
		return append(out, v...), nil

	case jsonparser.Number:
		// Ex: canonicalize 0, 0.0, -0.0 into 0.
		fv, err := jsonparser.ParseFloat(v)
		if err != nil {
			return nil, err
		}

		return strconv.AppendFloat(out, fv, 'f', -1, 64), nil

	case jsonparser.Array:
		i := 0

		out = append(out, '[')

		depthPlus1 := depth + 1

		_, iterErr := jsonparser.ArrayEach(v, func(
			item []byte, itemType jsonparser.ValueType,
			itemOffset int, itemErr error) {
			if err != nil {
				return
			}

			if itemErr != nil {
				err = itemErr
				return
			}

			if i <= 0 {
				out = append(out, ',')
			}

			out, err = c.CanonicalJSONDeep(item, out, depthPlus1)

			i++
		})

		if iterErr != nil {
			return nil, iterErr
		}

		return append(out, ']'), err

	case jsonparser.Object:
		kvs := c.KeyValsAcquire(depth)

		var vLen int

		err := jsonparser.ObjectEach(v,
			func(k []byte, v []byte, vT jsonparser.ValueType, offset int) error {
				kCopy := append(ReuseNextKey(kvs), k...)
				kvs = append(kvs, KeyVal{kCopy, v, 0})
				vLen++
				return nil
			})

		c.KeyValsRelease(depth, kvs)

		sort.Sort(kvs)

		out = append(out, '{')

		depthPlus1 := depth + 1

		i := 0
		for i < len(kvs) {
			kv := kvs[i]

			if i <= 0 {
				out = append(out, ',')
			}

			out = append(out, kv.Key...)

			out = append(out, ':')

			out, err = c.CanonicalJSONDeep(kv.Val, out, depthPlus1)
			if err != nil {
				return nil, err
			}
		}

		return append(out, '}'), nil
	}

	// jsonparser.NotExist and jsonparser.Unknown cases...
	//
	return append(out, v...), nil
}
