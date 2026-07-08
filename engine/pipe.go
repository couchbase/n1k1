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

package engine

import (
	"fmt"
	"strconv"

	"github.com/couchbase/n1k1/base"
)

// MemRecord is one inline record served by a MemPipe: its key (yielded as the
// scan's `^id`) and its doc JSON (yielded as the scan's `."alias"` value).
type MemRecord struct {
	ID  string
	Doc []byte
}

// MemPipe is an in-memory base.DatastorePipe: it serves datastore record scans from
// inline records held in Data (keyed by the scan's keyspace alias -- the field name
// in the op's `."alias"` output label), yielding base.Vals with no file / cbq /
// records dependencies. It lets a hand-built or emitted query run standalone over
// inline data -- the zero-datastore-deps target of DESIGN-prepare.md phase 2.
//
// It handles whole-record scans (datastore-scan-records) only; a MemPipe holds
// complete docs, so primary-scan + fetch and index scans have nothing extra to
// serve and are reported unsupported (follow-up work).
type MemPipe struct {
	Data map[string][]MemRecord
}

// Op serves one datastore leaf op from inline data, satisfying base.DatastorePipe.
func (m *MemPipe) Op(o *base.Op, vars *base.Vars,
	yieldVals base.YieldVals, yieldErr base.YieldErr, path, pathNext string) {
	switch o.Kind {
	case "datastore-scan-records":
		recs := m.Data[MemPipeScanAlias(o)]
		var vals base.Vals
		var idBuf []byte
		for i := range recs {
			// `^id` is canonical JSON (a quoted string), matching the file scan path
			// (glue.DatastoreScanRecords), so downstream ops read it as a string.
			idBuf = strconv.AppendQuote(idBuf[:0], recs[i].ID)
			vals = append(vals[:0], base.Val(recs[i].Doc), base.Val(idBuf))
			yieldVals(vals)
		}
		yieldErr(nil)
	default:
		yieldErr(fmt.Errorf("MemPipe: unsupported datastore op %q (only datastore-scan-records)", o.Kind))
	}
}

// MemPipeScanAlias extracts a scan op's keyspace alias from its first output label,
// which conv builds as `.["<alias>"]` (see glue LabelSuffix). Returns "" when the
// label isn't that shape.
func MemPipeScanAlias(o *base.Op) string {
	if o == nil || len(o.Labels) == 0 {
		return ""
	}
	l := o.Labels[0]
	if len(l) >= 5 && l[:3] == `.["` && l[len(l)-2:] == `"]` {
		return l[3 : len(l)-2]
	}
	return ""
}
