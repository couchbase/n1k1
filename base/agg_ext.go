//  Copyright (c) 2026 Couchbase, Inc.
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
)

// This file holds n1k1's "extension" aggregates -- new aggregate functions
// beyond the SQL++ standard set, meant to demonstrate that the byte-slice
// Init/Update/Result Agg protocol (see agg.go) is a stable public seam for
// third parties. sparkline() and histogram() render an inline unicode chart of
// a group's numeric values, in the spirit of DuckDB's/ClickHouse's bar/spark
// helpers. See DESIGN-extensions.md.
//
// Both accumulate the group's NUMBER values into the same numeric-list state
// used by MEDIAN/VARIANCE (aggNumListUpdate: an 8-byte count followed by that
// many little-endian float64 bits). Update stays allocation-free -- it only
// appends bytes. Result renders the chart by walking the byte state directly
// (no intermediate []float64), into the caller-provided reusable buf, honoring
// n1k1's zero-garbage discipline (DESIGN.md).

func init() {
	AggRegister("sparkline", &Agg{Init: AggU64Init, Update: AggNumListUpdate, Result: AggSparkLineResult})
	AggRegister("histogram", &Agg{Init: AggU64Init, Update: AggNumListUpdate, Result: AggHistogramResult})
}

// The eight vertical "block" runes ▁▂▃▄▅▆▇█ (U+2581..U+2588), used to draw the
// bars. Each is 3 UTF-8 bytes: 0xE2 0x96 (0x81+level), for level 0..7. They are
// plain UTF-8 (no JSON string escaping needed), so we emit them between quotes.
const AggSparkLineLevels = 8

// AggSparkLineBlockAppend appends the UTF-8 bytes for the block at level (clamped to
// 0..7) to buf, allocation-free.
func AggSparkLineBlockAppend(buf []byte, level int) []byte {
	if level < 0 {
		level = 0
	}
	if level >= AggSparkLineLevels {
		level = AggSparkLineLevels - 1
	}
	return append(buf, 0xE2, 0x96, byte(0x81+level))
}

// AggSparkLineScaleLevel maps v within [min,max] to a block level in 0..(sparkLevels-1).
// A degenerate (zero-width) range maps everything to the baseline level 0.
func AggSparkLineScaleLevel(v, min, max float64) int {
	if max <= min {
		return 0
	}
	return int(math.Round((v - min) / (max - min) * float64(AggSparkLineLevels-1)))
}

// -----------------------------------------------------
// sparkline(x): a unicode sparkline of the group's numeric values, in
// accumulation (input) order. For long series the sequence is downsampled to
// sparklineMaxWidth bars, each the mean of a contiguous segment -- so the shape
// of a big series is still legible as a short inline chart. Empty group -> NULL.

const AggSparkLineMaxWidth = 100

func AggSparkLineResult(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
	n := AggNumListCount(agg)
	end := 8 + int(n)*8
	rest := agg[end:]
	if n == 0 {
		return ValNull, rest, buf
	}

	// Downsample into up to sparklineMaxWidth segment means. A fixed stack array
	// keeps this allocation-free regardless of group size.
	var segs [AggSparkLineMaxWidth]float64
	segCount := int(n)
	if segCount > AggSparkLineMaxWidth {
		segCount = AggSparkLineMaxWidth
	}
	for s := 0; s < segCount; s++ {
		lo := uint64(s) * n / uint64(segCount)
		hi := uint64(s+1) * n / uint64(segCount)
		if hi <= lo {
			hi = lo + 1
		}
		sum := 0.0
		for i := lo; i < hi; i++ {
			sum += AggNumListAt(agg, i)
		}
		segs[s] = sum / float64(hi-lo)
	}

	min, max := segs[0], segs[0]
	for s := 1; s < segCount; s++ {
		if segs[s] < min {
			min = segs[s]
		}
		if segs[s] > max {
			max = segs[s]
		}
	}

	vBuf := append(buf[:0], '"')
	for s := 0; s < segCount; s++ {
		vBuf = AggSparkLineBlockAppend(vBuf, AggSparkLineScaleLevel(segs[s], min, max))
	}
	vBuf = append(vBuf, '"')

	return Val(vBuf), rest, BufUnused(buf, len(vBuf))
}

// -----------------------------------------------------

// AggNumListCount / AggNumListAt read the count and i-th float64 of the
// numeric-list state written by AggNumListUpdate, without allocating.
func AggNumListCount(agg []byte) uint64 {
	return binary.LittleEndian.Uint64(agg[:8])
}

func AggNumListAt(agg []byte, i uint64) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(agg[8+i*8 : 8+i*8+8]))
}

// -----------------------------------------------------
// histogram(x): a unicode bar chart of the value distribution -- the numeric
// range [min,max] is split into histogramBuckets equal-width bins and each
// bin's bar height is scaled to the fullest bin. Empty group -> NULL.

const AggHistogramBuckets = 20

func AggHistogramResult(vars *Vars, agg, buf []byte) (v Val, aggRest, bufOut []byte) {
	n := AggNumListCount(agg)
	end := 8 + int(n)*8
	rest := agg[end:]
	if n == 0 {
		return ValNull, rest, buf
	}

	min, max := AggNumListAt(agg, 0), AggNumListAt(agg, 0)
	for i := uint64(1); i < n; i++ {
		f := AggNumListAt(agg, i)
		if f < min {
			min = f
		}
		if f > max {
			max = f
		}
	}

	// Tally into fixed buckets (stack array -> allocation-free).
	var counts [AggHistogramBuckets]uint64
	for i := uint64(0); i < n; i++ {
		f := AggNumListAt(agg, i)
		b := 0
		if max > min {
			b = int((f - min) / (max - min) * float64(AggHistogramBuckets))
			if b >= AggHistogramBuckets {
				b = AggHistogramBuckets - 1 // the max value lands in the last bucket
			}
		}
		counts[b]++
	}

	maxCount := counts[0]
	for b := 1; b < AggHistogramBuckets; b++ {
		if counts[b] > maxCount {
			maxCount = counts[b]
		}
	}

	vBuf := append(buf[:0], '"')
	for b := 0; b < AggHistogramBuckets; b++ {
		level := 0
		if maxCount > 0 {
			level = int(math.Round(float64(counts[b]) / float64(maxCount) * float64(AggSparkLineLevels-1)))
		}
		vBuf = AggSparkLineBlockAppend(vBuf, level)
	}
	vBuf = append(vBuf, '"')

	return Val(vBuf), rest, BufUnused(buf, len(vBuf))
}
