//go:build n1ql

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

package glue

// Columnar VECTOR_DISTANCE (DESIGN-vectors.md), the "compute columnar, then transpose
// to rows" design: a top-K vector query
//
//   SELECT id, VECTOR_DISTANCE(vec, $q, "cosine") AS d FROM ks ORDER BY d ASC LIMIT k
//
// lowers to project(VECTOR_DISTANCE) -> scan-records. The ONLY step worth vectorizing
// is the distance MAP -- the sort is over one float per row (trivial). So
// VectorColumnarScan reads the vec column as borrowed float32 (records.VectorBatchSource,
// no JSON parse), computes each row's distance on the byte lane
// (base.VectorDistanceVFloat32), and TRANSPOSES each page into ordinary rows
// {scalars..., distance}. Those rows feed the EXISTING row-lane order-offset-limit +
// project machinery, so ORDER/LIMIT/OFFSET need no new columnar operator -- the risky
// top-K executor never has to exist.

import (
	"math"
	"strconv"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"
)

// VectorColumnarScan streams src's vec column, computes each row's distance to q (the
// static query vector, already parsed to float64), and calls yield once per row with a
// fresh base.Vals = [scalarFields[0], .., scalarFields[n-1], distance]. Distances are
// formatted to match cbq's boxed / n1k1's native float64 VECTOR_DISTANCE bit-for-bit
// (base.VectorDistanceVFloat32 promotes the float32 storage to float64); a NULL distance
// (zero-norm cosine, or a NULL/absent vec row) is base.ValNull. The yielded Vals own
// their bytes, so they outlive the borrowed batch. yield may return an error to stop.
func VectorColumnarScan(src records.VectorBatchSource, vecField string, scalarFields []string,
	q []float64, metric string, yield func(base.Vals) error) error {
	var dist []float64
	for {
		vb, ok, err := src.NextVectorBatch(vecField, scalarFields)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if cap(dist) < vb.Rows {
			dist = make([]float64, vb.Rows)
		}
		dist = dist[:vb.Rows]

		if vb.Regular {
			// No nulls, fixed stride: the tight contiguous kernel over the whole page.
			base.VectorDistanceVFloat32(dist, vb.Vec, q, vb.Rows, vb.Dim, metric)
		} else {
			// Nulls present -> per-row via offsets; a zero-length (NULL) row is NaN.
			for r := 0; r < vb.Rows; r++ {
				rv := vb.RowVec(r)
				if len(rv) == 0 {
					dist[r] = math.NaN()
					continue
				}
				base.VectorDistanceVFloat32(dist[r:r+1], rv, q, 1, len(rv), metric)
			}
		}

		for r := 0; r < vb.Rows; r++ {
			row := make(base.Vals, len(scalarFields)+1)
			for c := range scalarFields {
				row[c] = scalarValAt(vb.Scalars[c], vb.ScalarTypes[c], vb.ScalarValids[c], r)
			}
			if math.IsNaN(dist[r]) {
				row[len(scalarFields)] = base.ValNull
			} else {
				row[len(scalarFields)] = base.Val(strconv.AppendFloat(nil, dist[r], 'f', -1, 64))
			}
			if err := yield(row); err != nil {
				return err
			}
		}
	}
	return nil
}

// scalarValAt formats a scalar column's row-r value as a JSON Val (owning its bytes),
// honoring the validity bitmap (a null lane -> ValNull). Reuses formatStat (columnar.go)
// so an INT64 id renders as an integer and a DOUBLE as cbq's 'g' float, matching the
// row lane.
func scalarValAt(buf []byte, typ string, valid []byte, r int) base.Val {
	if valid != nil && buf != nil && valid[r>>3]&(1<<(uint(r)&7)) == 0 {
		return base.ValNull
	}
	return formatStat(typ, buf[r*8:r*8+8])
}
