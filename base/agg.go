package base

import (
	"encoding/binary"
	"strconv"
)

// AggCatalog is a registry of named aggregation handlers, which
// supports GROUP BY "count", etc.
var AggCatalog = map[string]*Agg{}

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
	AggCatalog["count"] = &Agg{
		Init: func(agg []byte) []byte {
			var b [8]byte
			return append(agg, b[:8]...) // For uint64 count.
		},

		Update: func(val Val, aggNew, agg []byte) (
			aggNewOut, aggRest []byte) {
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
}
