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
	"bytes"
	"encoding/binary"
	"math"
	"sort"
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

	// Vectorized SUM variants (DESIGN-col.md Step 5); chosen at plan-rewrite time
	// by the source column's type. Reuse AggSum's accumulator + Result.
	AggCatalog["sum_v_float64"] = len(Aggs)
	Aggs = append(Aggs, AggSumVFloat64)

	AggCatalog["sum_v_int64"] = len(Aggs)
	Aggs = append(Aggs, AggSumVInt64)

	AggCatalog["count_v"] = len(Aggs)
	Aggs = append(Aggs, AggCountV)

	AggCatalog["avg_v_float64"] = len(Aggs)
	Aggs = append(Aggs, AggAvgVFloat64)

	AggCatalog["avg_v_int64"] = len(Aggs)
	Aggs = append(Aggs, AggAvgVInt64)

	AggCatalog["avg"] = len(Aggs)
	Aggs = append(Aggs, AggAvg)

	AggCatalog["min"] = len(Aggs)
	Aggs = append(Aggs, AggMin)

	AggCatalog["max"] = len(Aggs)
	Aggs = append(Aggs, AggMax)

	AggCatalog["array_agg"] = len(Aggs)
	Aggs = append(Aggs, AggArrayAgg)

	// DISTINCT variants: e.g. COUNT(DISTINCT x), ARRAY_AGG(DISTINCT x). They
	// share aggDistinctUpdate (accumulate unique canonical values) and differ
	// only in Result.
	AggCatalog["count_distinct"] = len(Aggs)
	Aggs = append(Aggs, AggCountDistinct)

	AggCatalog["array_agg_distinct"] = len(Aggs)
	Aggs = append(Aggs, AggArrayAggDistinct)

	// COUNTN counts only NUMBER-typed values (COUNT counts all non-MISSING).
	registerAgg("countn", &Agg{Init: aggU64Init, Update: aggCountNUpdate, Result: aggU64Result})
	registerAgg("countn_distinct", &Agg{
		Init: aggU64Init, Update: aggNumDistinctUpdate, Result: aggDistinctCountResult})

	// MEDIAN / STDDEV / VARIANCE family. Each accumulates the group's NUMBER
	// values (a numeric list, or a distinct canonical set for DISTINCT) and
	// computes the statistic in Result, mirroring couchbase/query's two-pass
	// algorithm (mean = sum/count; variance = sum of squared deviations /
	// (count - delta), delta=1 for sample and 0 for population) so the float
	// results match exactly. "variance"/"stddev" are the sample forms.
	for _, v := range []struct {
		name         string
		samp, sqrtIt bool
	}{
		{"variance", true, false}, {"var_samp", true, false}, {"var_pop", false, false},
		{"stddev", true, true}, {"stddev_samp", true, true}, {"stddev_pop", false, true},
	} {
		registerAgg(v.name, makeVarianceAgg(false, v.samp, v.sqrtIt))
		registerAgg(v.name+"_distinct", makeVarianceAgg(true, v.samp, v.sqrtIt))
	}
	registerAgg("median", makeMedianAgg(false))
	registerAgg("median_distinct", makeMedianAgg(true))
}

func registerAgg(name string, agg *Agg) {
	AggCatalog[name] = len(Aggs)
	Aggs = append(Aggs, agg)
}

// -----------------------------------------------------

// aggDistinctWalk returns the byte length of the n length-prefixed elements
// that begin at agg[8:] (i.e. this distinct-agg's portion, excluding the count).
func aggDistinctWalk(n uint64, agg []byte) (total int) {
	for i := uint64(0); i < n; i++ {
		l := binary.LittleEndian.Uint64(agg[8+total : 8+total+8])
		total += 8 + int(l)
	}
	return total
}

// aggDistinctUpdate adds v's canonical form to the distinct set if not already
// present. State: count(uint64) + count*(len-prefixed canonical val).
func aggDistinctUpdate(v Val, aggNew, agg []byte, vc *ValComparer) (
	[]byte, []byte, bool) {
	n := binary.LittleEndian.Uint64(agg[:8])
	total := aggDistinctWalk(n, agg)

	if len(v) <= 0 { // DISTINCT aggregates ignore MISSING.
		return append(aggNew, agg[:8+total]...), agg[8+total:], false
	}

	cv, err := vc.CanonicalJSON(v, nil)
	if err != nil {
		cv = v
	}

	// Already present?
	off := 0
	for i := uint64(0); i < n; i++ {
		l := int(binary.LittleEndian.Uint64(agg[8+off : 8+off+8]))
		if bytes.Equal(agg[8+off+8:8+off+8+l], cv) {
			return append(aggNew, agg[:8+total]...), agg[8+total:], false
		}
		off += 8 + l
	}

	aggNew = BinaryAppendUint64(aggNew, n+1)
	aggNew = append(aggNew, agg[8:8+total]...)
	aggNew = BinaryAppendUint64(aggNew, uint64(len(cv)))
	aggNew = append(aggNew, cv...)

	return aggNew, agg[8+total:], true
}

var AggCountDistinct = &Agg{
	Init: func(vars *Vars, agg []byte) []byte { return append(agg, Zero8[:8]...) },
	Update: func(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) ([]byte, []byte, bool) {
		return aggDistinctUpdate(v, aggNew, agg, vc)
	},
	Result: func(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
		n := binary.LittleEndian.Uint64(agg[:8])
		total := aggDistinctWalk(n, agg)
		vBuf := strconv.AppendUint(buf[:0], n, 10)
		return Val(vBuf), agg[8+total:], BufUnused(buf, len(vBuf))
	},
}

var AggArrayAggDistinct = &Agg{
	Init: func(vars *Vars, agg []byte) []byte { return append(agg, Zero8[:8]...) },
	Update: func(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) ([]byte, []byte, bool) {
		return aggDistinctUpdate(v, aggNew, agg, vc)
	},
	Result: func(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
		n := binary.LittleEndian.Uint64(agg[:8])
		// ARRAY_AGG(DISTINCT ...) over an all-MISSING group is NULL, not [] (N1QL).
		if n == 0 {
			return ValNull, agg[8:], buf
		}
		vBuf := append(buf[:0], '[')
		total := 0
		for i := uint64(0); i < n; i++ {
			l := int(binary.LittleEndian.Uint64(agg[8+total : 8+total+8]))
			if i > 0 {
				vBuf = append(vBuf, ',')
			}
			vBuf = append(vBuf, agg[8+total+8:8+total+8+l]...)
			total += 8 + l
		}
		vBuf = append(vBuf, ']')
		return Val(vBuf), agg[8+total:], BufUnused(buf, len(vBuf))
	},
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

// Vectorized SUM over a packed, fixed-width column (DESIGN-col.md Step 5). Unlike
// the scalar AggSum, whose Update folds one JSON Val, these fold a whole COLUMN in
// one Update call: the incoming Val is the raw little-endian value buffer of a
// numeric column -- e.g. an Arrow Float64/Int64 buffer, borrowed -- holding
// len(v)/8 values, and there is no JSON parse. They reuse AggSum's 8-byte float64
// accumulator and Result verbatim, so their output is byte-identical to "sum";
// only the Update differs. The type lives in the catalog key (sum_v_float64 vs
// sum_v_int64), chosen at plan-rewrite time from the source column's type.
//
// Contract: the caller guarantees a numeric, non-null column (null_count==0); a
// column with nulls needs a companion validity mask (a later step). Values are
// summed left-to-right in buffer order, matching the scalar row fold bit-for-bit
// (every slot is added as a float64, as scalar SUM does via ParseFloat64).

var AggSumVFloat64 = &Agg{
	Init:   AggSum.Init,
	Update: aggSumVFloat64Update,
	Result: AggSum.Result,
}

var AggSumVInt64 = &Agg{
	Init:   AggSum.Init,
	Update: aggSumVInt64Update,
	Result: AggSum.Result,
}

// Two dedicated, branchless loops rather than one loop with a per-element type
// switch -- these are the hot kernels, and the type is fixed for the whole call.

func aggSumVFloat64Update(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
	[]byte, []byte, bool) {
	s := math.Float64frombits(binary.LittleEndian.Uint64(agg[:8]))
	n := len(v) / 8
	for i := 0; i < n; i++ {
		s += math.Float64frombits(binary.LittleEndian.Uint64(v[i*8:]))
	}
	return BinaryAppendUint64(aggNew, math.Float64bits(s)), agg[8:], n > 0
}

func aggSumVInt64Update(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
	[]byte, []byte, bool) {
	s := math.Float64frombits(binary.LittleEndian.Uint64(agg[:8]))
	n := len(v) / 8
	for i := 0; i < n; i++ {
		s += float64(int64(binary.LittleEndian.Uint64(v[i*8:])))
	}
	return BinaryAppendUint64(aggNew, math.Float64bits(s)), agg[8:], n > 0
}

// count_v: vectorized COUNT over a packed 8-byte column -- shorts to len(v)/8
// (the element count), no fold loop. Reuses AggCount's counter + Result. Sound
// only for null_count==0 columns (then COUNT(x) == row count); nulls need a
// validity mask later.
var AggCountV = &Agg{
	Init: AggCount.Init,
	Update: func(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
		[]byte, []byte, bool) {
		n := len(v) / 8
		c := binary.LittleEndian.Uint64(agg[:8])
		return BinaryAppendUint64(aggNew, c+uint64(n)), agg[8:], n > 0
	},
	Result: AggCount.Result,
}

// avg_v: vectorized AVG, reusing AggAvg's [count][sum] accumulator + Result. The
// count adds len(v)/8; the sum folds the column in buffer order -- same as scalar
// AVG, so bit-exact. Branchless per-type, like the sum kernels.
var AggAvgVFloat64 = &Agg{Init: AggAvg.Init, Update: aggAvgVFloat64Update, Result: AggAvg.Result}
var AggAvgVInt64 = &Agg{Init: AggAvg.Init, Update: aggAvgVInt64Update, Result: AggAvg.Result}

func aggAvgVFloat64Update(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
	[]byte, []byte, bool) {
	n := len(v) / 8
	c := binary.LittleEndian.Uint64(agg[:8])
	aggNew = BinaryAppendUint64(aggNew, c+uint64(n))
	s := math.Float64frombits(binary.LittleEndian.Uint64(agg[8:16]))
	for i := 0; i < n; i++ {
		s += math.Float64frombits(binary.LittleEndian.Uint64(v[i*8:]))
	}
	return BinaryAppendUint64(aggNew, math.Float64bits(s)), agg[16:], n > 0
}

func aggAvgVInt64Update(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
	[]byte, []byte, bool) {
	n := len(v) / 8
	c := binary.LittleEndian.Uint64(agg[:8])
	aggNew = BinaryAppendUint64(aggNew, c+uint64(n))
	s := math.Float64frombits(binary.LittleEndian.Uint64(agg[8:16]))
	for i := 0; i < n; i++ {
		s += float64(int64(binary.LittleEndian.Uint64(v[i*8:])))
	}
	return BinaryAppendUint64(aggNew, math.Float64bits(s)), agg[16:], n > 0
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

// -----------------------------------------------------

// AggArrayAgg implements ARRAY_AGG: it accumulates the group's (non-MISSING)
// values, and its Result is their JSON array. NULL is included; MISSING is not.
//
// State layout: an 8-byte element count, followed by that many length-prefixed
// (8-byte len + JSON bytes) elements.
var AggArrayAgg = &Agg{
	Init: func(vars *Vars, agg []byte) []byte { return append(agg, Zero8[:8]...) },

	Update: func(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
		[]byte, []byte, bool) {
		n := binary.LittleEndian.Uint64(agg[:8])

		// Walk the existing elements to find the end of this agg's portion.
		total := 0
		for i := uint64(0); i < n; i++ {
			l := binary.LittleEndian.Uint64(agg[8+total : 8+total+8])
			total += 8 + int(l)
		}

		if len(v) <= 0 { // ARRAY_AGG ignores MISSING.
			return append(aggNew, agg[:8+total]...), agg[8+total:], false
		}

		aggNew = BinaryAppendUint64(aggNew, n+1)
		aggNew = append(aggNew, agg[8:8+total]...)
		aggNew = BinaryAppendUint64(aggNew, uint64(len(v)))
		aggNew = append(aggNew, v...)

		return aggNew, agg[8+total:], true
	},

	Result: func(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
		n := binary.LittleEndian.Uint64(agg[:8])

		// ARRAY_AGG over a group with no non-MISSING values is NULL, not [] (N1QL;
		// n counts non-MISSING accumulated values, so n==0 means all were MISSING).
		if n == 0 {
			return ValNull, agg[8:], buf
		}

		vBuf := append(buf[:0], '[')

		total := 0
		for i := uint64(0); i < n; i++ {
			l := int(binary.LittleEndian.Uint64(agg[8+total : 8+total+8]))
			if i > 0 {
				vBuf = append(vBuf, ',')
			}
			vBuf = append(vBuf, agg[8+total+8:8+total+8+l]...)
			total += 8 + l
		}

		vBuf = append(vBuf, ']')

		return Val(vBuf), agg[8+total:], BufUnused(buf, len(vBuf))
	},
}

// -----------------------------------------------------
// COUNTN / MEDIAN / STDDEV / VARIANCE support.

// aggU64Init / aggU64Result handle a bare 8-byte uint64 counter state.
func aggU64Init(vars *Vars, agg []byte) []byte { return append(agg, Zero8[:8]...) }

func aggU64Result(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
	c := binary.LittleEndian.Uint64(agg[:8])
	vBuf := strconv.AppendUint(buf[:0], c, 10)
	return Val(vBuf), agg[8:], BufUnused(buf, len(vBuf))
}

// isNumberVal reports whether v parses as a JSON number.
func isNumberVal(v Val) bool {
	_, pt := Parse(v)
	return ParseTypeToValType[pt] == ValTypeNumber
}

// aggCountNUpdate increments the counter only for NUMBER-typed values.
func aggCountNUpdate(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
	[]byte, []byte, bool) {
	c := binary.LittleEndian.Uint64(agg[:8])
	if isNumberVal(v) {
		return BinaryAppendUint64(aggNew, c+1), agg[8:], true
	}
	return append(aggNew, agg[:8]...), agg[8:], false
}

// aggNumDistinctUpdate is aggDistinctUpdate restricted to NUMBER values (others
// are ignored), used by COUNTN(DISTINCT ...) and the DISTINCT statistical aggs.
func aggNumDistinctUpdate(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
	[]byte, []byte, bool) {
	if isNumberVal(v) {
		return aggDistinctUpdate(v, aggNew, agg, vc)
	}
	n := binary.LittleEndian.Uint64(agg[:8])
	total := aggDistinctWalk(n, agg)
	return append(aggNew, agg[:8+total]...), agg[8+total:], false
}

// aggDistinctCountResult returns the size of a distinct set (for COUNTN DISTINCT).
func aggDistinctCountResult(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
	n := binary.LittleEndian.Uint64(agg[:8])
	total := aggDistinctWalk(n, agg)
	vBuf := strconv.AppendUint(buf[:0], n, 10)
	return Val(vBuf), agg[8+total:], BufUnused(buf, len(vBuf))
}

// aggNumListUpdate appends v's float64 to a numeric-list state (an 8-byte count
// followed by that many 8-byte float64s). Non-numbers are ignored.
func aggNumListUpdate(vars *Vars, v Val, aggNew, agg []byte, vc *ValComparer) (
	[]byte, []byte, bool) {
	n := binary.LittleEndian.Uint64(agg[:8])
	end := 8 + int(n)*8
	pv, pt := Parse(v)
	if ParseTypeToValType[pt] == ValTypeNumber {
		if f, err := ParseFloat64(pv); err == nil {
			aggNew = BinaryAppendUint64(aggNew, n+1)
			aggNew = append(aggNew, agg[8:end]...)
			aggNew = BinaryAppendUint64(aggNew, math.Float64bits(f))
			return aggNew, agg[end:], true
		}
	}
	return append(aggNew, agg[:end]...), agg[end:], false
}

// aggFloats extracts the accumulated float64 values (and the trailing aggRest)
// from either a numeric-list state (distinct==false) or a distinct canonical
// set (distinct==true).
func aggFloats(distinct bool, agg []byte) (vals []float64, aggRest []byte) {
	n := binary.LittleEndian.Uint64(agg[:8])
	off := 8
	if !distinct {
		// Same numeric-list layout the zero-garbage aggNumListAt walks (agg_ext.go);
		// keep the two decoders in step if the layout ever changes.
		vals = make([]float64, 0, n)
		for i := uint64(0); i < n; i++ {
			vals = append(vals, aggNumListAt(agg, i))
		}
		return vals, agg[8+int(n)*8:]
	}
	total := aggDistinctWalk(n, agg)
	vals = make([]float64, 0, n)
	for i := uint64(0); i < n; i++ {
		l := int(binary.LittleEndian.Uint64(agg[off : off+8]))
		off += 8
		if f, err := ParseFloat64(agg[off : off+l]); err == nil {
			vals = append(vals, f)
		}
		off += l
	}
	return vals, agg[8+total:]
}

// makeVarianceAgg builds a VARIANCE/STDDEV handler. samp selects sample (delta=1)
// vs population (delta=0); sqrtIt takes the square root (STDDEV vs VARIANCE).
func makeVarianceAgg(distinct, samp, sqrtIt bool) *Agg {
	update := aggNumListUpdate
	if distinct {
		update = aggNumDistinctUpdate
	}
	delta := 0.0
	if samp {
		delta = 1.0
	}
	return &Agg{
		Init:   aggU64Init,
		Update: update,
		Result: func(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
			vals, rest := aggFloats(distinct, agg)
			count := len(vals)
			if count == 0 { // Empty group -> NULL.
				return ValNull, rest, buf
			}
			if count == 1 { // Sample stat of one value is undefined (NULL); pop is 0.
				if samp {
					return ValNull, rest, buf
				}
				vBuf := strconv.AppendFloat(buf[:0], 0, 'f', -1, 64)
				return Val(vBuf), rest, BufUnused(buf, len(vBuf))
			}
			sum := 0.0
			for _, f := range vals {
				sum += f
			}
			mean := sum / float64(count)
			variance := 0.0
			for _, f := range vals {
				d := f - mean
				variance += d * d
			}
			r := variance / (float64(count) - delta)
			if sqrtIt {
				r = math.Sqrt(r)
			}
			vBuf := strconv.AppendFloat(buf[:0], r, 'f', -1, 64)
			return Val(vBuf), rest, BufUnused(buf, len(vBuf))
		},
	}
}

// makeMedianAgg builds a MEDIAN handler (sort the values; for an even count,
// average the two middle values), matching couchbase/query.
func makeMedianAgg(distinct bool) *Agg {
	update := aggNumListUpdate
	if distinct {
		update = aggNumDistinctUpdate
	}
	return &Agg{
		Init:   aggU64Init,
		Update: update,
		Result: func(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
			vals, rest := aggFloats(distinct, agg)
			if len(vals) == 0 {
				return ValNull, rest, buf
			}
			sort.Float64s(vals)
			n := len(vals)
			var m float64
			if n&1 == 1 {
				m = vals[n/2]
			} else {
				m = (vals[n/2-1] + vals[n/2]) / 2
			}
			vBuf := strconv.AppendFloat(buf[:0], m, 'f', -1, 64)
			return Val(vBuf), rest, BufUnused(buf, len(vBuf))
		},
	}
}
