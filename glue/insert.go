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

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// insertWriterQueue bounds how many evaluated docs may be in flight between the
// source-query producer and the writer goroutine (see InsertRun). Small: enough to
// keep the writer fed across a flush syscall without materializing the result set.
const insertWriterQueue = 256

// INSERT write modes, chosen via the SQL++ OPTIONS clause -- e.g.
// `INSERT INTO ks (KEY ..., VALUE ..., OPTIONS {"mode":"append"}) SELECT ...`.
const (
	insertModeNew       = "new"       // default: error if the target file exists
	insertModeAppend    = "append"    // create-or-append to the target file
	insertModeOverwrite = "overwrite" // atomically replace the target file
)

// InsertRun implements phase-1 INSERT INTO as a MATERIALIZE: it runs the INSERT's
// source (a SELECT or a VALUES list), evaluates each row's VALUE expression, and
// writes the results as JSON Lines to a brand-new keyspace file. It is intercepted
// at the statement level (Session.Run), before the cbq planner, so the target need
// not pre-exist -- see the dispatch comment in session.go.
//
// Layout: n1k1's file datastore makes a *directory* under the namespace a keyspace
// (its record files are unioned); a loose file directly under the namespace is not.
// So `INSERT INTO `analysis/2026-06-09.jsonl“ writes <root>/<ns>/analysis/
// 2026-06-09.jsonl and the queryable keyspace is `analysis` -- `SELECT * FROM
// analysis` reads it, and later dated files accumulate into the same keyspace.
//
// The source SELECT streams straight into the output file (each row's VALUE is
// evaluated and written as it is produced) -- no full-result buffering -- by
// temporarily routing the session's row callback to the writer. A stage breaker
// hands each evaluated doc to a dedicated writer goroutine over a bounded channel,
// so file encoding + I/O overlap with query compute (see the Select branch).
//
// Write mode is chosen by the SQL++ OPTIONS clause (see insertWriteMode): "new"
// (default) writes a brand-new file and errors if it already exists; "append"
// creates-or-appends; "overwrite" atomically replaces. The VALUE of each source
// row is written verbatim, one JSON object per line (KEY is not used -- record ids
// are positional, as for every flat keyspace).
//
// A RETURNING projection (see insertReturner) makes the statement return a row per
// inserted doc -- streamed through the caller's OnRow as each doc is written, or in
// Result.Rows otherwise -- instead of the mutation summary. Still unsupported: the
// faithful cbq SendInsert path (a later phase).
func (s *Session) InsertRun(ins *algebra.Insert) (res *Result, err error) {
	valueExpr := ins.Value()
	if valueExpr == nil && ins.Select() != nil {
		return nil, fmt.Errorf("INSERT ... SELECT requires a VALUE expression")
	}

	path, ks, err := s.insertTargetPath(ins.KeyspaceRef())
	if err != nil {
		return nil, err
	}

	ctx := NewExprGlueContext(time.Now())

	// RETURNING: project each inserted doc to a result row. sink is the caller's row
	// callback captured up front (the Select branch reroutes s.OnRow to the writer),
	// so RETURNING rows stream to the real output even while the source query runs.
	var ret *insertReturner
	if proj := ins.Returning(); proj != nil {
		ret = &insertReturner{proj: proj, alias: ins.KeyspaceRef().Alias(), ctx: ctx, sink: s.OnRow}
	}

	mode, err := insertWriteMode(ins.Options(), ctx)
	if err != nil {
		return nil, err
	}
	_, statErr := os.Stat(path)
	exists := statErr == nil
	if exists && mode == insertModeNew {
		return nil, fmt.Errorf("INSERT INTO %q: target file %q already exists "+
			`(mode "new"; use OPTIONS {"mode":"append"} or {"mode":"overwrite"})`, ks, path)
	}

	// "append" seeds the temp file with the existing file's bytes first, so the
	// copy-then-rename stays crash-safe; "new"/"overwrite" start empty (at finish()
	// the rename replaces any existing file atomically). Appending to a missing file
	// is just a create -- same as "new".
	seedFrom := ""
	if mode == insertModeAppend && exists {
		seedFrom = path
	}

	w, err := newJSONLWriter(path, seedFrom)
	if err != nil {
		return nil, err
	}
	// On any error, discard the partial temp file. On success, w.finish() renames.
	defer func() {
		if err != nil {
			w.abort()
		}
	}()

	if sel := ins.Select(); sel != nil {
		// Stage breaker: the source query (producer) evaluates each row's VALUE expr
		// and hands the resulting doc to a dedicated writer goroutine over a bounded
		// channel, so JSON encoding + file I/O overlap with query compute rather than
		// blocking the producer on every flush syscall. The channel's small capacity
		// bounds memory to a few in-flight rows -- still streaming, not materializing.
		//
		// Error split (avoids sharing mutable state across the two goroutines): the
		// producer latches VALUE-eval failures in evalErr; the writer owns w.err. They
		// are combined only after the writer goroutine has joined (channel close ->
		// range exit -> <-done establishes happens-before), so neither field is touched
		// concurrently.
		docs := make(chan value.Value, insertWriterQueue)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for doc := range docs {
				w.write(doc) // sole writer of w.err / w.n
				if ret != nil {
					ret.emit(doc) // sole writer of ret.* (this goroutine only)
				}
			}
		}()

		var evalErr error
		origOnRow := s.OnRow
		s.OnRow = func(row []byte) {
			if evalErr != nil {
				return // stop feeding once a VALUE eval has failed (query still drains)
			}
			// Copy: OnRow's row buffer is reused by the producer for the next row, and
			// the doc we build outlives this call (it crosses the channel to the writer).
			item := value.NewValue(append([]byte(nil), row...))
			doc, ev := valueExpr.Evaluate(item, ctx)
			if ev != nil {
				evalErr = fmt.Errorf("INSERT VALUE evaluation failed: %w", ev)
				return
			}
			docs <- doc
		}
		_, runErr := s.StatementRun(sel, s.NamedArgs, s.PositionalArgs)
		s.OnRow = origOnRow
		close(docs)
		<-done // writer has drained and finished touching w.*

		if runErr != nil {
			err = fmt.Errorf("INSERT source query failed: %w", runErr)
			return nil, err
		}
		if evalErr != nil {
			err = evalErr
			return nil, err
		}
		if err = w.err; err != nil {
			return nil, err
		}
	} else {
		// VALUES (key, value), ... -- evaluate each pair's constant VALUE expression.
		for _, pair := range ins.Values() {
			if pair.Value() == nil {
				continue
			}
			doc, evErr := pair.Value().Evaluate(value.NULL_VALUE, ctx)
			if evErr != nil {
				err = fmt.Errorf("INSERT VALUES evaluation failed: %w", evErr)
				return nil, err
			}
			w.write(doc)
			if ret != nil {
				ret.emit(doc)
			}
		}
		if err = w.err; err != nil {
			return nil, err
		}
	}
	if ret != nil && ret.err != nil {
		err = fmt.Errorf("INSERT RETURNING failed: %w", ret.err)
		return nil, err
	}

	if err = w.finish(); err != nil {
		return nil, err
	}

	// RETURNING: the projected rows are the result -- already streamed via the
	// caller's OnRow when set, else handed back in Result.Rows. No summary.
	if ret != nil {
		if ret.sink != nil {
			return &Result{Count: ret.n}, nil
		}
		return &Result{Rows: ret.rows, Count: ret.n}, nil
	}

	// Otherwise report the mutation like a small result the CLI can render (or stream).
	summary, _ := json.Marshal(map[string]interface{}{"inserted": w.n, "keyspace": ks, "mode": mode})
	if s.OnRow != nil {
		s.OnRow(summary)
		return &Result{Count: w.n}, nil
	}
	return &Result{Rows: []json.RawMessage{summary}, Count: w.n}, nil
}

// insertTargetPath resolves the INSERT target keyspace name to an on-disk file path
// under the file datastore's <root>/<namespace>/, mirroring the read-side resolver
// (KeyspaceDir). The keyspace name may contain '/' (e.g. `analysis/2026-06-09.jsonl`),
// which becomes a subpath; the first segment is the queryable keyspace directory.
func (s *Session) insertTargetPath(ref *algebra.KeyspaceRef) (path, ks string, err error) {
	if s.Store == nil || s.Store.Datastore == nil {
		return "", "", fmt.Errorf("INSERT: no datastore")
	}
	url := s.Store.Datastore.URL()
	root := strings.TrimPrefix(url, "file://")
	if root == url {
		return "", "", fmt.Errorf("INSERT: target is not a file datastore (%q)", url)
	}
	ns := ref.Namespace()
	if ns == "" {
		ns = s.Namespace
	}
	ks = ref.Keyspace()
	if ks == "" {
		return "", "", fmt.Errorf("INSERT: empty target keyspace")
	}
	// The keyspace name is a datastore-relative path; keep it within <root>/<ns>.
	clean := filepath.Clean(ks)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.IsAbs(clean) {
		return "", "", fmt.Errorf("INSERT: target keyspace %q escapes the datastore", ks)
	}
	return filepath.Join(root, ns, clean), ks, nil
}

// insertWriteMode reads the INSERT OPTIONS object's optional "mode" field, which
// selects how the target file is written: "new" (default -- error if the file
// already exists), "append" (create-or-append), or "overwrite" (atomic replace).
// "replace" is accepted as a synonym for "overwrite". A nil/absent/NULL mode means
// "new". OPTIONS is a normal expression; it is evaluated as a constant (against a
// NULL row) -- the same clause cbq uses for e.g. {"expiration": ...} on Server.
func insertWriteMode(opts expression.Expression, ctx expression.Context) (string, error) {
	if opts == nil {
		return insertModeNew, nil
	}
	ov, e := opts.Evaluate(value.NULL_VALUE, ctx)
	if e != nil {
		return "", fmt.Errorf("INSERT OPTIONS evaluation failed: %w", e)
	}
	m, ok := ov.Field("mode")
	if !ok || m.Type() == value.MISSING || m.Type() == value.NULL {
		return insertModeNew, nil
	}
	if m.Type() != value.STRING {
		return "", fmt.Errorf(`INSERT OPTIONS: "mode" must be a string, got %v`, m.Type())
	}
	switch strings.ToLower(m.Actual().(string)) {
	case "", insertModeNew:
		return insertModeNew, nil
	case insertModeAppend:
		return insertModeAppend, nil
	case insertModeOverwrite, "replace":
		return insertModeOverwrite, nil
	default:
		return "", fmt.Errorf(`INSERT OPTIONS: unknown mode %q (want "new", "append", or "overwrite")`, m.Actual())
	}
}

// insertReturner projects a RETURNING clause over each inserted doc. INSERT is run
// at the statement level (outside the planner), so there is no projection operator
// -- this evaluates the *algebra.Projection directly, mirroring cbq's formalized
// shape: a bare RETURNING field becomes `(alias.field)`, so exprs are evaluated
// against a one-key wrapper {alias: doc}; RETURNING * is a star+self term whose
// value is the whole doc; RETURNING RAW <expr> yields the bare value, not an object.
//
// emit is called from a single goroutine (the writer, in the Select branch, or the
// main goroutine for VALUES), so no field needs synchronization; the first error is
// latched in err. Each row is streamed to sink (the caller's OnRow) when set, else
// accumulated in rows for Result.Rows.
type insertReturner struct {
	proj  *algebra.Projection
	alias string
	ctx   expression.Context
	sink  func([]byte)
	rows  []json.RawMessage
	buf   bytes.Buffer
	n     int
	err   error
}

func (r *insertReturner) emit(doc value.Value) {
	if r.err != nil {
		return
	}
	b, ok, e := r.project(doc)
	if e != nil {
		r.err = e
		return
	}
	if !ok { // e.g. RETURNING RAW <expr> that evaluated to MISSING
		return
	}
	r.n++
	if r.sink != nil {
		r.sink(b) // reused buffer; OnRow must copy to retain (same contract as Session)
		return
	}
	r.rows = append(r.rows, append(json.RawMessage(nil), b...))
}

// project builds one RETURNING row for doc, returning its compact JSON bytes (into
// the reused buf), whether a row is present, and any evaluation error.
func (r *insertReturner) project(doc value.Value) ([]byte, bool, error) {
	wrapped := value.NewValue(map[string]interface{}{r.alias: doc})

	if r.proj.Raw() { // RETURNING RAW <expr> -- a single bare value per row
		v, e := r.proj.Terms()[0].Expression().Evaluate(wrapped, r.ctx)
		if e != nil {
			return nil, false, e
		}
		if v == nil || v.Type() == value.MISSING {
			return nil, false, nil
		}
		return r.marshal(v)
	}

	out := value.NewValue(map[string]interface{}{})
	for _, term := range r.proj.Terms() {
		if term.Star() {
			// `*` (self) projects the whole doc; `<expr>.*` projects that object's fields.
			obj := doc
			if !term.Self() && term.Expression() != nil {
				o, e := term.Expression().Evaluate(wrapped, r.ctx)
				if e != nil {
					return nil, false, e
				}
				obj = o
			}
			if obj != nil && obj.Type() == value.OBJECT {
				for k, fv := range obj.Fields() {
					out.SetField(k, fv)
				}
			}
			continue
		}
		v, e := term.Expression().Evaluate(wrapped, r.ctx)
		if e != nil {
			return nil, false, e
		}
		if v != nil && v.Type() != value.MISSING {
			out.SetField(term.Alias(), v)
		}
	}
	return r.marshal(out)
}

func (r *insertReturner) marshal(v value.Value) ([]byte, bool, error) {
	r.buf.Reset()
	if e := v.WriteJSON(nil, &r.buf, "", "", true); e != nil {
		return nil, false, e
	}
	return r.buf.Bytes(), true, nil
}

// jsonlWriter streams JSON documents, one compact line each, into a temp sibling
// that is renamed over the target on finish() -- so a mid-stream failure never
// leaves a partial keyspace file, and an "overwrite"/"append" rename replaces the
// old file atomically. For "append" the temp is first seeded with the existing
// file's bytes (copy-then-rename). A MISSING doc is skipped ("no document"). The
// first write/encode error is latched in err and short-circuits later writes.
type jsonlWriter struct {
	path string
	tmp  string
	f    *os.File
	w    *bufio.Writer
	buf  bytes.Buffer
	n    int
	err  error
}

// newJSONLWriter opens the temp sibling. If seedFrom is non-empty, the temp is
// pre-filled with that file's bytes (the append mode's copy) before any new rows.
func newJSONLWriter(path, seedFrom string) (*jsonlWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("INSERT: creating %q: %w", filepath.Dir(path), err)
	}
	tmp := path + ".n1k1-insert.tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("INSERT: creating temp file: %w", err)
	}
	jw := &jsonlWriter{path: path, tmp: tmp, f: f, w: bufio.NewWriter(f)}
	if seedFrom != "" {
		if err := jw.seed(seedFrom); err != nil {
			jw.abort()
			return nil, err
		}
	}
	return jw, nil
}

// seed pre-fills the temp file with an existing file's bytes (append mode), then
// guarantees a trailing newline so the first appended row starts on its own JSONL
// line even if the seeded file did not end in one.
func (jw *jsonlWriter) seed(src string) error {
	in, e := os.Open(src)
	if e != nil {
		return fmt.Errorf("INSERT: opening %q to append: %w", src, e)
	}
	defer in.Close()
	t := &lastByteWriter{w: jw.w}
	if _, e := io.Copy(t, in); e != nil {
		return fmt.Errorf("INSERT: copying %q for append: %w", src, e)
	}
	if t.n > 0 && t.last != '\n' {
		if e := jw.w.WriteByte('\n'); e != nil {
			return e
		}
	}
	return nil
}

// lastByteWriter forwards writes and remembers the last byte written, so the append
// seeder can tell whether the copied file already ended in a newline.
type lastByteWriter struct {
	w    io.Writer
	last byte
	n    int64
}

func (t *lastByteWriter) Write(p []byte) (int, error) {
	nn, err := t.w.Write(p)
	if nn > 0 {
		t.last = p[nn-1]
		t.n += int64(nn)
	}
	return nn, err
}

func (jw *jsonlWriter) setErr(e error) {
	if jw.err == nil {
		jw.err = e
	}
}

func (jw *jsonlWriter) write(doc value.Value) {
	if jw.err != nil || doc == nil || doc.Type() == value.MISSING {
		return
	}
	jw.buf.Reset()
	// Compact, sorted-key JSON -- the same serialization a subsequent SELECT reads
	// back (value.WriteJSON).
	if e := doc.WriteJSON(nil, &jw.buf, "", "", true); e != nil {
		jw.setErr(fmt.Errorf("INSERT: encoding row %d: %w", jw.n, e))
		return
	}
	if _, e := jw.w.Write(jw.buf.Bytes()); e != nil {
		jw.setErr(e)
		return
	}
	if e := jw.w.WriteByte('\n'); e != nil {
		jw.setErr(e)
		return
	}
	jw.n++
}

// finish flushes, closes, and atomically renames the temp file into place.
func (jw *jsonlWriter) finish() error {
	if jw.err != nil {
		return jw.err
	}
	if e := jw.w.Flush(); e != nil {
		jw.f.Close()
		return e
	}
	if e := jw.f.Close(); e != nil {
		return e
	}
	if e := os.Rename(jw.tmp, jw.path); e != nil {
		return fmt.Errorf("INSERT: finalizing %q: %w", jw.path, e)
	}
	return nil
}

// abort discards the temp file (best effort) after a failure.
func (jw *jsonlWriter) abort() {
	jw.f.Close()
	os.Remove(jw.tmp)
}
