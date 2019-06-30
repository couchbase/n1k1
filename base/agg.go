//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package base

import (
	"encoding/binary"
	"math"
	"strconv"
)

var Zero8 [8]byte // 64-bits of zeros.

// BufUnused returns buf[n:] if there's enough len() or returns nil.
func BufUnused(buf []byte, n int) []byte {
	if len(buf) >= n {
		return buf[n:]
	}

	return nil
}

// -----------------------------------------------------

// AggCatalog is a registry of named aggregation handlers related to
// GROUP BY, such as "count", "sum", etc.
var AggCatalog = map[string]int{}

// Aggs provides for 0-index-based lookups of aggregation handlers
// that were registered into the AggCatalog.
var Aggs []*Agg

// Agg defines the interface for an aggregation handler.
type Agg struct {
	// Init extends agg bytes with initial data for the aggregation.
	Init func(vars *Vars, agg []byte) (aggOut []byte)

	// Update incorporates the incoming val with the existing agg
	// data, by extending and returning the given aggNew.  Also
	// returns aggRest which is the agg bytes that were unread.
	Update func(vars *Vars, val Val, aggNew, agg []byte, vc *ValComparer) (
		aggNewOut, aggRest []byte, changed bool)

	// Result returns the final result of the aggregation.
	// Also returns aggRest or the agg bytes that were unread.
	Result func(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte)
}

// -----------------------------------------------------

func init() {
	AggCatalog["count"] = len(Aggs)
	Aggs = append(Aggs, AggCount)

	AggCatalog["sum"] = len(Aggs)
	Aggs = append(Aggs, AggSum)

	AggCatalog["avg"] = len(Aggs)
	Aggs = append(Aggs, AggAvg)

	AggCatalog["min"] = len(Aggs)
	Aggs = append(Aggs, AggMin)

	AggCatalog["max"] = len(Aggs)
	Aggs = append(Aggs, AggMax)
}

// -----------------------------------------------------

var AggCount = &Agg{
	Init: func(vars *Vars, agg []byte) []byte { return append(agg, Zero8[:8]...) },

	Update: func(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
		[]byte, []byte, bool) {
		c := binary.LittleEndian.Uint64(agg[:8])
		return BinaryAppendUint64(aggNew, c+1), agg[8:], true
	},

	Result: func(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
		c := binary.LittleEndian.Uint64(agg[:8])

		vBuf := strconv.AppendUint(buf[:0], c, 10)

		return Val(vBuf), agg[8:], BufUnused(buf, len(vBuf))
	},
}

// -----------------------------------------------------

var AggSum = &Agg{
	Init: func(vars *Vars, agg []byte) []byte { return append(agg, Zero8[:8]...) },

	Update: func(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
		[]byte, []byte, bool) {
		parsedVal, parsedType := Parse(v)
		if ParseTypeToValType[parsedType] == ValTypeNumber {
			f, err := ParseFloat64(parsedVal)
			if err == nil {
				s := math.Float64frombits(binary.LittleEndian.Uint64(agg[:8])) + f
				return BinaryAppendUint64(aggNew, math.Float64bits(s)), agg[8:], true
			}
		}

		return append(aggNew, agg[:8]...), agg[8:], false
	},

	Result: func(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
		s := math.Float64frombits(binary.LittleEndian.Uint64(agg[:8]))

		vBuf := strconv.AppendFloat(buf[:0], s, 'f', -1, 64)

		return Val(vBuf), agg[8:], BufUnused(buf, len(vBuf))
	},
}

// -----------------------------------------------------

var AggAvg = &Agg{
	Init: func(vars *Vars, agg []byte) []byte {
		return AggSum.Init(vars, AggCount.Init(vars, agg))
	},

	Update: func(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
		[]byte, []byte, bool) {
		aggNew, agg, _ = AggCount.Update(vars, v, aggNew, agg, vc)
		aggNew, agg, _ = AggSum.Update(vars, v, aggNew, agg, vc)
		return aggNew, agg, true
	},

	Result: func(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
		c := binary.LittleEndian.Uint64(agg[:8])
		if c == 0 {
			return Val(nil), agg[16:], buf
		}

		s := math.Float64frombits(binary.LittleEndian.Uint64(agg[8:16]))

		vBuf := strconv.AppendFloat(buf[:0], s/float64(c), 'f', -1, 64)

		return Val(vBuf), agg[16:], BufUnused(buf, len(vBuf))
	},
}

// -----------------------------------------------------

var AggMin = &Agg{
	Init:   func(vars *Vars, agg []byte) []byte { return append(agg, Zero8[:8]...) },
	Update: AggCompareUpdate(func(cmp int) bool { return cmp < 0 }),
	Result: AggCompareResult,
}

var AggMax = &Agg{
	Init:   func(vars *Vars, agg []byte) []byte { return append(agg, Zero8[:8]...) },
	Update: AggCompareUpdate(func(cmp int) bool { return cmp > 0 }),
	Result: AggCompareResult,
}

// -----------------------------------------------------

func AggCompareUpdate(comparer func(int) bool) func(
	vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) ([]byte, []byte, bool) {
	return func(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) ([]byte, []byte, bool) {
		n := binary.LittleEndian.Uint64(agg[:8])
		if n <= 0 || comparer(vc.Compare(v, agg[8:8+n])) {
			aggNew = BinaryAppendUint64(aggNew, uint64(len(v)))
			aggNew = append(aggNew, v...)
			return aggNew, agg[8+n:], true
		}

		return append(aggNew, agg[:8+n]...), agg[8+n:], false
	}
}

func AggCompareResult(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
	n := binary.LittleEndian.Uint64(agg[:8])

	vBuf := append(buf[:0], agg[8:8+n]...)

	return Val(vBuf), agg[8+n:], BufUnused(buf, len(vBuf))
}
