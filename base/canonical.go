package base

import (
	"sort"
	"strconv"

	"github.com/buger/jsonparser"
)

// CanonicalJSON returns a JSON encoded representation of the Val,
// e.g., sorting object field names, etc. The optional out slice is
// reused and extended via append().
func (c *ValComparer) CanonicalJSON(a Val, out []byte) ([]byte, error) {
	v, vType, _, err := jsonparser.Get(a)
	if err != nil {
		return out, err
	}

	return c.CanonicalJSONWithType(v, int(vType), out, 0)
}

func (c *ValComparer) CanonicalJSONWithType(v []byte, vType int,
	out []byte, depth int) (rv []byte, err error) {
	switch jsonparser.ValueType(vType) {
	case jsonparser.String:
		out = append(append(append(out, '"'), v...), '"')

		return out, nil

	case jsonparser.Boolean, jsonparser.Null:
		return append(out, v...), nil

	case jsonparser.Number:
		// Ex: canonicalize 0, 0.0, -0.0 into 0.
		fv, err := jsonparser.ParseFloat(v)
		if err != nil {
			return out, err
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

			if i > 0 {
				out = append(out, ',')
			}

			out, err = c.CanonicalJSONWithType(
				item, int(itemType), out, depthPlus1)

			i++
		})

		if iterErr != nil {
			return out, iterErr
		}

		return append(out, ']'), err

	case jsonparser.Object:
		kvs := c.KeyValsAcquire(depth)

		err := jsonparser.ObjectEach(v,
			func(k []byte, v []byte, vT jsonparser.ValueType, o int) error {
				kCopy := append(ReuseNextKey(kvs), k...)
				kvs = append(kvs, KeyVal{kCopy, v, int(vT), 0})
				return nil
			})

		c.KeyValsRelease(depth, kvs)

		sort.Sort(kvs)

		out = append(out, '{')

		depthPlus1 := depth + 1

		for i := 0; i < len(kvs); i++ {
			kv := kvs[i]

			if i > 0 {
				out = append(out, ',')
			}

			out, err = c.EncodeAsString(kv.Key, out)
			if err != nil {
				return out, err
			}

			out = append(out, ':')

			out, err = c.CanonicalJSONWithType(kv.Val, kv.ValType,
				out, depthPlus1)
			if err != nil {
				return out, err
			}
		}

		return append(out, '}'), nil

	default: // jsonparser.NotExist & jsonparser.Unknown cases...
		return append(out, v...), nil
	}
}
