//go:build !js

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

package records

// Apache Iceberg table source (DESIGN-data.md §7) -- a read-only SPIKE. An Iceberg table
// is a metadata layer over Parquet data files; `apache/iceberg-go` (a cgo-free dependency
// n1k1 already carries) resolves the current snapshot -> data files (with file pruning,
// merge-on-read deletes, and field-ID schema evolution all handled) and yields Arrow
// record batches. We drive that scan and transpose each batch to NDJSON rows via the SAME
// renderer as the Parquet source (arrowBatchToNDJSON), so an Iceberg table reuses the
// existing byte-lane machinery -- the only new code is "drive the scan, feed its batches
// to the shared renderer."
//
// Scope of this spike: load a table by its filesystem metadata location (no catalog
// server), scan the current snapshot, project *, no pushdown yet. Deferred: predicate/
// projection pushdown into the scan, time-travel (snapshot selection), catalogs
// (REST/Glue/SQL), S3, and wiring an Iceberg table as a queryable keyspace (FROM).
// Guarded !js like parquet.go (arrow-go's Parquet reader doesn't build for wasm).

import (
	"context"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	iceio "github.com/apache/iceberg-go/io"
	itable "github.com/apache/iceberg-go/table"
)

// icebergSource streams an Iceberg table's current snapshot as JSON records, pulling one
// Arrow batch at a time from iceberg-go's scan and transposing it to NDJSON lines. Doc
// borrows the render buffer, valid until the next batch loads (same contract as
// parquetSource).
type icebergSource struct {
	next func() (arrow.RecordBatch, error, bool) // pulled from the scan's iter.Seq2
	stop func()

	cur      arrow.RecordBatch // current batch, released on the next pull / Close
	buf      []byte            // current batch rendered as NDJSON (reused)
	lines    [][]byte          // per-row slices into buf
	li       int               // next line index
	idPrefix string
	idBuf    []byte
	row      int
	done     bool
}

// OpenIcebergTable opens an Iceberg table for read by its metadata-file location (e.g.
// ".../metadata/00003.metadata.json"), via iceberg-go with a filesystem FileIO and NO
// catalog. Records are yielded as the table's current snapshot, projected *.
func OpenIcebergTable(metadataLocation, idPrefix string) (Source, error) {
	ctx := context.Background()
	fsF := iceio.LoadFSFunc(nil, metadataLocation) // LocalFS for a filesystem path
	tbl, err := itable.NewFromLocation(ctx, itable.Identifier{"iceberg", "table"}, metadataLocation, fsF, nil)
	if err != nil {
		return nil, err
	}
	_, recs, err := tbl.Scan().ToArrowRecords(ctx)
	if err != nil {
		return nil, err
	}
	next, stop := iter.Pull2(recs)
	return &icebergSource{next: next, stop: stop, idPrefix: idPrefix}, nil
}

func (s *icebergSource) Next(rec *Record) (bool, error) {
	for s.li >= len(s.lines) {
		if s.done {
			return false, nil
		}
		if s.cur != nil {
			s.cur.Release()
			s.cur = nil
		}
		batch, err, ok := s.next()
		if !ok {
			s.done = true
			return false, nil
		}
		if err != nil {
			s.done = true
			return false, err
		}
		s.cur = batch
		s.buf, err = arrowBatchToNDJSON(s.buf, batch)
		if err != nil {
			return false, err
		}
		s.lines = splitNDJSON(s.buf, s.lines[:0])
		s.li = 0
	}
	rec.Doc = s.lines[s.li]
	s.idBuf = appendRecordID(s.idBuf[:0], s.idPrefix, s.row)
	rec.ID = s.idBuf
	s.li++
	s.row++
	return true, nil
}

func (s *icebergSource) Close() error {
	if s.cur != nil {
		s.cur.Release()
		s.cur = nil
	}
	if s.stop != nil {
		s.stop()
	}
	return nil
}
