package base

import (
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

// ---------------------------------------------

type ValComparer struct {
	Preallocs [][]string
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

type OrderBySorter struct {
	Items         []Vals
	Projected     []Vals // Same len() as Items.
	ProjectedLess func(projectedA, projectedB Vals) bool
}

func (a *OrderBySorter) Len() int {
	return len(a.Items)
}

func (a *OrderBySorter) Swap(i, j int) {
	a.Items[i], a.Items[j] = a.Items[j], a.Items[i]
	a.Projected[i], a.Projected[j] = a.Projected[j], a.Projected[i]
}

func (a *OrderBySorter) Less(i, j int) bool {
	return a.ProjectedLess(a.Projected[i], a.Projected[j])
}
