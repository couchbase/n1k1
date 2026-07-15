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
	"fmt"
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
	IcebergSnapshotApplied   int64
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
	snapshot  ScanSnapshot              // time-travel selector; zero Mode => current

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

// SetSnapshot implements Snapshotter: read a past snapshot (by id or as-of timestamp) rather
// than the current one. A zero-Mode selector is a no-op. Must precede the first Next.
func (s *icebergSource) SetSnapshot(sel ScanSnapshot) error {
	if s.started {
		return nil
	}
	s.snapshot = sel
	return nil
}

// predicateToIceberg converts a neutral ScanPredicate TREE to an iceberg BooleanExpression,
// recursing over its AND/OR nodes. The tree is negation-normal (no NOT nodes), so it's
// monotone: an AND DROPS any unconvertible child (dropping only widens the result -- safe),
// while an OR is all-or-nothing (dropping a disjunct could prune matching rows). A child is
// unconvertible when a leaf's type doesn't map or a nested node itself drops out entirely.
func (s *icebergSource) predicateToIceberg(p ScanPredicate) (iceberg.BooleanExpression, bool) {
	switch p.Bool {
	case "":
		return s.clauseToIceberg(p.Clause)
	case "and":
		var built []iceberg.BooleanExpression
		for _, c := range p.Children {
			if e, ok := s.predicateToIceberg(c); ok {
				built = append(built, e) // else drop this conjunct (widen -- safe).
			}
		}
		return combineIceberg(built, false)
	case "or":
		var built []iceberg.BooleanExpression
		for _, c := range p.Children {
			e, ok := s.predicateToIceberg(c)
			if !ok {
				return nil, false // can't push a partial OR soundly.
			}
			built = append(built, e)
		}
		return combineIceberg(built, true)
	}
	return nil, false
}

// combineIceberg AND/ORs a slice of expressions: 0 -> not pushable, 1 -> that expression,
// >=2 -> NewAnd/NewOr.
func combineIceberg(built []iceberg.BooleanExpression, or bool) (iceberg.BooleanExpression, bool) {
	switch len(built) {
	case 0:
		return nil, false
	case 1:
		return built[0], true
	}
	if or {
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

	// Null tests are type-independent.
	switch cl.Op {
	case "isnull":
		return iceberg.IsNull(ref), true
	case "notnull":
		return iceberg.NotNull(ref), true
	}

	// Comparisons + membership are typed: match the literal's Go type to the column's
	// Iceberg type so the expression binds cleanly.
	switch nf.Type.String() {
	case "int":
		return icebergClause[int32](ref, cl, toIntN[int32])
	case "long":
		return icebergClause[int64](ref, cl, toIntN[int64])
	case "float":
		return icebergClause[float32](ref, cl, func(x interface{}) (float32, bool) {
			f, ok := x.(float64)
			return float32(f), ok
		})
	case "double":
		return icebergClause[float64](ref, cl, func(x interface{}) (float64, bool) {
			f, ok := x.(float64)
			return f, ok
		})
	case "string", "date", "time", "timestamp", "timestamptz", "uuid":
		// Temporal / uuid columns are the common PARTITION keys (e.g. day(event_ts)).
		// SQL++ has no native date literal, so the constant arrives as a string; build a
		// string-literal predicate and let iceberg-go coerce it to the column type during
		// Bind (StringLiteral.To parses "2006-01-02" dates, "...T15:04:05" timestamps,
		// RFC3339 for tz, and uuids). A malformed string fails BindExpr in SetRowFilter and
		// is dropped -- so a range filter on the partition source column still prunes
		// partitions (iceberg-go's inclusive projection), while a bad one is simply ignored.
		return icebergClause[string](ref, cl, func(x interface{}) (string, bool) {
			s, ok := x.(string)
			return s, ok
		})
	case "boolean":
		if cl.Op != "eq" && cl.Op != "ne" {
			return nil, false // ordering / membership on a bool isn't pushed.
		}
		return icebergClause[bool](ref, cl, func(x interface{}) (bool, bool) {
			b, ok := x.(bool)
			return b, ok
		})
	}
	return nil, false
}

// toIntN converts a JSON number (float64) to an integer type, requiring it be integral so
// the integer kernel is exact (a fractional constant on an int column isn't pushed).
func toIntN[T int32 | int64](x interface{}) (T, bool) {
	f, ok := x.(float64)
	if !ok || f != math.Trunc(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return T(f), true
}

// icebergClause builds a typed comparison or membership predicate, converting the clause's
// constant(s) to T via conv. A conversion failure (wrong Go type, non-integral int) -> false.
func icebergClause[T iceberg.LiteralType](ref iceberg.UnboundTerm, cl ScanClause,
	conv func(interface{}) (T, bool)) (iceberg.BooleanExpression, bool) {
	switch cl.Op {
	case "in", "notin":
		if len(cl.Consts) == 0 {
			return nil, false
		}
		vs := make([]T, 0, len(cl.Consts))
		for _, c := range cl.Consts {
			v, ok := conv(c)
			if !ok {
				return nil, false
			}
			vs = append(vs, v)
		}
		if cl.Op == "in" {
			return iceberg.IsIn(ref, vs...), true
		}
		return iceberg.NotIn(ref, vs...), true
	}
	v, ok := conv(cl.Const)
	if !ok {
		return nil, false
	}
	switch cl.Op {
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
	switch s.snapshot.Mode {
	case "id":
		opts = append(opts, itable.WithSnapshotID(s.snapshot.ID))
		atomic.AddInt64(&IcebergSnapshotApplied, 1)
	case "asof":
		opts = append(opts, itable.WithSnapshotAsOf(s.snapshot.AsOfMs))
		atomic.AddInt64(&IcebergSnapshotApplied, 1)
	}
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

// pullBatch lazily starts the scan, releases the previous batch (its buffers were borrowed),
// and pulls the next one into s.cur. ok=false marks end-of-stream. Shared by the row path
// (Next) and the columnar path (NextColumns) -- a source is used in one mode, not both.
func (s *icebergSource) pullBatch() (arrow.RecordBatch, bool, error) {
	if !s.started {
		if err := s.start(); err != nil {
			s.done = true
			return nil, false, err
		}
	}
	if s.done {
		return nil, false, nil
	}
	if s.cur != nil {
		s.cur.Release()
		s.cur = nil
	}
	batch, err, ok := s.next()
	if !ok {
		s.done = true
		return nil, false, nil
	}
	if err != nil {
		s.done = true
		return nil, false, err
	}
	s.cur = batch
	return batch, true, nil
}

func (s *icebergSource) Next(rec *Record) (bool, error) {
	for s.li >= len(s.lines) {
		batch, ok, err := s.pullBatch()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		if s.buf, err = arrowBatchToNDJSON(s.buf, batch); err != nil {
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

// NextColumns implements ColumnBatchSource: pull one Arrow batch and hand back each PROJECTED
// column's raw little-endian value buffer + validity bitmap (borrowed from the batch, valid
// until the next call / Close), in the ProjectColumns order -- so the vectorized aggregates
// consume the Iceberg data with no JSON transpose, exactly like the Parquet source. Only
// fixed-width 8-byte numeric columns are supported; anything else errors and the caller
// falls back to the row path.
func (s *icebergSource) NextColumns() (cols, valids [][]byte, rows int, ok bool, err error) {
	batch, ok, err := s.pullBatch()
	if err != nil || !ok {
		return nil, nil, 0, false, err
	}
	order, err := s.columnOrder(batch)
	if err != nil {
		return nil, nil, 0, false, err
	}
	rows = int(batch.NumRows())
	for _, ci := range order {
		c := batch.Column(ci)
		b, e := arrowValueBytes(c)
		if e != nil {
			return nil, nil, 0, false, e
		}
		cols = append(cols, b)
		valids = append(valids, arrowValidityBytes(c))
	}
	return cols, valids, rows, true, nil
}

// columnOrder maps the projected field names (ProjectColumns order) to this batch's column
// indices -- iceberg-go may return WithSelectedFields columns in schema order, but the
// vectorized aggregates map columns positionally to the projected-names order, so we realign
// by name. No projection => all columns in batch order.
func (s *icebergSource) columnOrder(batch arrow.RecordBatch) ([]int, error) {
	n := int(batch.NumCols())
	if len(s.selected) == 0 {
		order := make([]int, n)
		for i := range order {
			order[i] = i
		}
		return order, nil
	}
	sch := batch.Schema()
	byName := make(map[string]int, n)
	for i := 0; i < n; i++ {
		byName[sch.Field(i).Name] = i
	}
	order := make([]int, 0, len(s.selected))
	for _, name := range s.selected {
		ci, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("records: projected column %q missing from Iceberg batch", name)
		}
		order = append(order, ci)
	}
	return order, nil
}

// Columns implements ColumnsSource: describe the table's top-level columns (name + a
// Parquet-style physical type) from the schema, no data read -- enough for the planner to
// decide the vectorized-aggregate path (which only fires on INT64/DOUBLE columns). Per-column
// stats aren't surfaced (Count/NullCount -1, Min/Max nil), so the metadata-only MIN/MAX/COUNT
// path stays off; the vectorized scan path handles the aggregation.
func (s *icebergSource) Columns() []ColumnMeta {
	fields := s.tbl.Schema().Fields()
	out := make([]ColumnMeta, 0, len(fields))
	for _, f := range fields {
		out = append(out, ColumnMeta{
			Name: f.Name, Type: icebergPhysType(f.Type), Count: -1, NullCount: -1,
		})
	}
	return out
}

// icebergPhysType maps an Iceberg primitive type to the Parquet-style physical-type name the
// columnar planner keys on. Only "INT64"/"DOUBLE" enable the vectorized path; everything else
// (incl. nested types) maps to a distinct name so it stays on the row path.
func icebergPhysType(t iceberg.Type) string {
	switch t.String() {
	case "long":
		return "INT64"
	case "double":
		return "DOUBLE"
	case "int":
		return "INT32"
	case "float":
		return "FLOAT"
	case "boolean":
		return "BOOLEAN"
	case "string":
		return "BYTE_ARRAY"
	}
	return t.String()
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
