package base

import (
	"encoding/binary"
	"math"
	"strconv"
)

var Zero8 [8]byte // 64-bits of zeros.

// -----------------------------------------------------

// AggCatalog is a registry of named aggregation handlers, which
// supports GROUP BY "count", etc.
var AggCatalog = map[string]int{}

var Aggs []*Agg

type Agg struct {
	// Init extends the agg bytes with space for the aggregation.
	Init func(agg []byte) (aggOut []byte)

	// Update incorporates the incoming val with the existing agg
	// bytes, by extending and returning the given aggNew.
	// Also returns agg sliced to the bytes that remain unread.
	Update func(val Val, aggNew, agg []byte) (aggNewOut, aggRest []byte)

	// Result returns the final result of the aggregation.
	// Also returns agg sliced to the bytes that remain unread.
	Result func(agg, buf []byte) (v Val, aggRest, bufOut []byte)
}

// -----------------------------------------------------

func init() {
	AggCatalog["count"] = len(Aggs)
	Aggs = append(Aggs, AggCount)

	AggCatalog["sum"] = len(Aggs)
	Aggs = append(Aggs, AggSum)
}

// -----------------------------------------------------

var AggCount = &Agg{
	Init: func(agg []byte) []byte { return append(agg, Zero8[:8]...) },

	Update: func(v Val, aggNew, agg []byte) (aggNewOut, aggRest []byte) {
		c := binary.LittleEndian.Uint64(agg[:8])
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:8], c+1)
		return append(aggNew, b[:8]...), agg[8:]
	},

	Result: func(agg, buf []byte) (v Val, aggRest, bufOut []byte) {
		c := binary.LittleEndian.Uint64(agg[:8])
		vBuf := strconv.AppendUint(buf[:0], c, 10)
		return Val(vBuf), agg[8:], buf[len(vBuf):]
	},
}

// -----------------------------------------------------

var AggSum = &Agg{
	Init: func(agg []byte) []byte { return append(agg, Zero8[:8]...) },

	Update: func(v Val, aggNew, agg []byte) (aggNewOut, aggRest []byte) {
		parsedVal, parsedType := Parse(v)
		if ParseTypeToValType[parsedType] == ValTypeNumber {
			f, err := ParseFloat64(parsedVal)
			if err == nil {
				s := math.Float64frombits(binary.LittleEndian.Uint64(agg[:8]))
				var b [8]byte
				binary.LittleEndian.PutUint64(b[:8], math.Float64bits(s+f))
				return append(aggNew, b[:8]...), agg[8:]
			}
		}

		return append(aggNew, agg[:8]...), agg[8:]
	},

	Result: func(agg, buf []byte) (v Val, aggRest, bufOut []byte) {
		s := math.Float64frombits(binary.LittleEndian.Uint64(agg[:8]))
		vBuf := strconv.AppendFloat(buf[:0], s, 'f', -1, 64)
		return Val(vBuf), agg[8:], buf[len(vBuf):]
	},
}
