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

// Apache Iceberg table source (DESIGN-data.md §7). An Iceberg table is a metadata layer
// over Parquet data files; `apache/iceberg-go` (a cgo-free dependency n1k1 already carries)
// resolves the current snapshot -> data files (with file pruning, merge-on-read deletes,
// and field-ID schema evolution all handled) and yields Arrow record batches. We drive that
// scan and transpose each batch to NDJSON rows via the SAME renderer as the Parquet source
// (arrowBatchToNDJSON), so an Iceberg table reuses the existing byte-lane machinery.
//
// PUSHDOWN: the scan is built LAZILY (on the first Next) so an optional projection
// (ColumnsProjector) and predicate (RowFilterer) can be applied first, feeding iceberg-go's
// WithSelectedFields (read only the needed columns) and WithRowFilter (prune whole data
// files by manifest column stats before opening them). Both are best-effort hints: the query
// engine still projects and filters downstream, so a partial/absent pushdown is always
// correct. Deferred: time-travel (snapshot selection), catalogs (REST/Glue/SQL), S3.
// Guarded !js like parquet.go (arrow-go's Parquet reader doesn't build for wasm).

import (
	"context"
	"iter"
	"math"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	itable "github.com/apache/iceberg-go/table"
)

// IcebergProjectionApplied / IcebergRowFilterApplied count how often a projection /
// predicate pushdown was actually handed to iceberg-go's scan (test + observability
// hooks). A dropped-because-unconvertible predicate does NOT increment the filter counter.
var (
	IcebergProjectionApplied int64
	IcebergRowFilterApplied  int64
)

// icebergSource streams an Iceberg table's current snapshot as JSON records, pulling one
// Arrow batch at a time from iceberg-go's scan and transposing it to NDJSON lines. Doc
// borrows the render buffer, valid until the next batch loads (same contract as
// parquetSource). The scan is built on the first Next so ProjectColumns / SetRowFilter
// (which must precede iteration) can restrict it.
type icebergSource struct {
	ctx context.Context
	tbl *itable.Table

	selected  []string                  // WithSelectedFields; nil => all columns
	rowFilter iceberg.BooleanExpression // WithRowFilter; nil => no pruning hint

	started bool
	next    func() (arrow.RecordBatch, error, bool) // pulled from the scan's iter.Seq2
	stop    func()

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
// catalog. Records are the table's current snapshot; the scan is deferred to the first
// Next so an optional projection/predicate pushdown can be applied first.
func OpenIcebergTable(metadataLocation, idPrefix string) (Source, error) {
	ctx := context.Background()
	fsF := iceio.LoadFSFunc(nil, metadataLocation) // LocalFS for a filesystem path
	tbl, err := itable.NewFromLocation(ctx, itable.Identifier{"iceberg", "table"}, metadataLocation, fsF, nil)
	if err != nil {
		return nil, err
	}
	return &icebergSource{ctx: ctx, tbl: tbl, idPrefix: idPrefix}, nil
}

// ProjectColumns implements ColumnsProjector: read only the named top-level columns via
// iceberg-go's WithSelectedFields. Must be called before the first Next. Unknown names are
// harmless (iceberg-go ignores a selected field the schema lacks; it reads as MISSING
// downstream regardless).
func (s *icebergSource) ProjectColumns(names []string) error {
	if s.started {
		return nil
	}
	if len(names) > 0 {
		s.selected = names
	}
	return nil
}

// SetRowFilter implements RowFilterer: convert the neutral ScanPredicate to an iceberg-go
// BooleanExpression for file pruning. Best-effort -- an unconvertible predicate (a field the
// schema lacks, an unsupported type, or a partial OR we can't push soundly) is DROPPED, not
// an error, since the engine still applies the real filter. Never fails a query.
func (s *icebergSource) SetRowFilter(pred ScanPredicate) error {
	if s.started {
		return nil
	}
	expr, ok := s.predicateToIceberg(pred)
	if !ok {
		return nil // drop silently; the engine's filter still runs.
	}
	// Validate it binds to the current schema before committing -- a bad filter would make
	// the scan itself error, which must never happen for a mere optimization.
	if _, err := iceberg.BindExpr(s.tbl.Schema(), expr, false); err != nil {
		return nil
	}
	s.rowFilter = expr
	return nil
}

// predicateToIceberg converts a neutral ScanPredicate to an iceberg BooleanExpression.
// Correctness of DROPPING an unconvertible clause depends on the mode: in an AND, dropping
// a clause WEAKENS the filter (a superset survives -- safe); in an OR, dropping a clause
// STRENGTHENS it (matching rows could be pruned -- UNSAFE), so any unconvertible OR clause
// drops the WHOLE predicate. Returns ok=false to push nothing.
func (s *icebergSource) predicateToIceberg(pred ScanPredicate) (iceberg.BooleanExpression, bool) {
	if len(pred.Clauses) == 0 {
		return nil, false
	}
	var built []iceberg.BooleanExpression
	for _, cl := range pred.Clauses {
		e, ok := s.clauseToIceberg(cl)
		if !ok {
			if pred.Mode == "or" {
				return nil, false // can't push a partial OR soundly.
			}
			continue // AND: dropping a clause only widens the result.
		}
		built = append(built, e)
	}
	if len(built) == 0 {
		return nil, false
	}
	if len(built) == 1 {
		return built[0], true
	}
	if pred.Mode == "or" {
		return iceberg.NewOr(built[0], built[1], built[2:]...), true
	}
	return iceberg.NewAnd(built[0], built[1], built[2:]...), true
}

// clauseToIceberg builds one typed comparison predicate, matching the literal's Go type to
// the column's Iceberg type so the expression binds cleanly. Unknown field / unsupported
// type / non-integral constant on an integer column -> ok=false (that clause isn't pushed).
func (s *icebergSource) clauseToIceberg(cl ScanClause) (iceberg.BooleanExpression, bool) {
	nf, ok := s.tbl.Schema().FindFieldByName(cl.Field)
	if !ok || nf.Type == nil {
		return nil, false
	}
	ref := iceberg.Reference(cl.Field)
	switch nf.Type.String() {
	case "int", "long":
		f, ok := cl.Const.(float64)
		if !ok || f != math.Trunc(f) || math.IsInf(f, 0) {
			return nil, false
		}
		if nf.Type.String() == "int" {
			return icebergCmp(ref, cl.Op, int32(f))
		}
		return icebergCmp(ref, cl.Op, int64(f))
	case "float":
		f, ok := cl.Const.(float64)
		if !ok {
			return nil, false
		}
		return icebergCmp(ref, cl.Op, float32(f))
	case "double":
		f, ok := cl.Const.(float64)
		if !ok {
			return nil, false
		}
		return icebergCmp(ref, cl.Op, f)
	case "string":
		v, ok := cl.Const.(string)
		if !ok {
			return nil, false
		}
		return icebergCmp(ref, cl.Op, v)
	case "boolean":
		v, ok := cl.Const.(bool)
		if !ok || (cl.Op != "eq" && cl.Op != "ne") {
			return nil, false
		}
		return icebergCmp(ref, cl.Op, v)
	}
	return nil, false
}

// icebergCmp maps a neutral op to the iceberg-go typed predicate constructor.
func icebergCmp[T iceberg.LiteralType](ref iceberg.UnboundTerm, op string, v T) (iceberg.BooleanExpression, bool) {
	switch op {
	case "eq":
		return iceberg.EqualTo(ref, v), true
	case "ne":
		return iceberg.NotEqualTo(ref, v), true
	case "lt":
		return iceberg.LessThan(ref, v), true
	case "le":
		return iceberg.LessThanEqual(ref, v), true
	case "gt":
		return iceberg.GreaterThan(ref, v), true
	case "ge":
		return iceberg.GreaterThanEqual(ref, v), true
	}
	return nil, false
}

// start builds the scan (once) with the accumulated projection + predicate pushdowns.
func (s *icebergSource) start() error {
	s.started = true
	var opts []itable.ScanOption
	if len(s.selected) > 0 {
		opts = append(opts, itable.WithSelectedFields(s.selected...))
		atomic.AddInt64(&IcebergProjectionApplied, 1)
	}
	if s.rowFilter != nil {
		opts = append(opts, itable.WithRowFilter(s.rowFilter))
		atomic.AddInt64(&IcebergRowFilterApplied, 1)
	}
	_, recs, err := s.tbl.Scan(opts...).ToArrowRecords(s.ctx)
	if err != nil {
		return err
	}
	s.next, s.stop = iter.Pull2(recs)
	return nil
}

func (s *icebergSource) Next(rec *Record) (bool, error) {
	if !s.started {
		if err := s.start(); err != nil {
			s.done = true
			return false, err
		}
	}
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
