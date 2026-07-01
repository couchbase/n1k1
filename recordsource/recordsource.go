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

// Package recordsource decodes on-disk data files into a stream of JSON
// records for n1k1's FROM path. It is deliberately pure-Go (no couchbase/query
// dependency and no build tag) so it can be unit-tested standalone and reused
// by both the glue datastore path and the engine's direct-file scan path.
//
// Allocation model (see DESIGN-data.md "Allocation model"): the API is
// read-into / borrowed-slice, not per-value boxing. Next(*Record) fills a
// caller-owned Record whose ID/Doc byte slices are borrowed from an internal
// buffer and are only valid until the next Next call — callers copy to retain.
// This mirrors n1k1's base.Val = []byte engine, so a decoded record hands
// straight to a base.Val with no interface{} boxing.
package recordsource

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Record is one decoded record. ID is a synthetic document key (META().id) and
// Doc is the record's JSON bytes. Both slices are borrowed from the Source's
// internal buffer and are only valid until the next Next call.
type Record struct {
	ID  []byte
	Doc []byte
}

// Source yields a stream of records. Next reports false at end-of-stream (with
// nil error). A non-nil error aborts the stream.
type Source interface {
	Next(rec *Record) (bool, error)
	Close() error
}

// Supported record-file extensions (after any compression suffix is stripped).
// jsonl/ndjson are line-delimited (streaming); json/jsons are a value stream or
// a top-level array.
var recordExts = map[string]bool{
	".json": true, ".jsons": true, ".jsonl": true, ".ndjson": true,
}

// IsRecordFile reports whether path (by extension, ignoring a .gz/.zst suffix)
// is a data file this package can decode.
func IsRecordFile(path string) bool {
	return recordExts[innerExt(path)]
}

// innerExt returns the format-determining extension, seeing through a single
// compression suffix: "a/b.jsonl.gz" -> ".jsonl", "x.json" -> ".json".
func innerExt(path string) string {
	base := filepath.Base(path)
	ext := strings.ToLower(filepath.Ext(base))
	if ext == ".gz" || ext == ".zst" {
		ext = strings.ToLower(filepath.Ext(base[:len(base)-len(ext)]))
	}
	return ext
}

// stem returns the file's base name with its format extension (and any
// compression suffix) removed: "orders/order-1001.json" -> "order-1001",
// "2025.jsonl.gz" -> "2025".
func stem(path string) string {
	base := filepath.Base(path)
	if ext := strings.ToLower(filepath.Ext(base)); ext == ".gz" || ext == ".zst" {
		base = base[:len(base)-len(ext)]
	}
	if ext := filepath.Ext(base); ext != "" {
		base = base[:len(base)-len(ext)]
	}
	return base
}

// openDecompressed opens path and, if it carries a .gz suffix, wraps it in a
// transparent gzip reader (keyed off the outer extension). Returns a reader and
// the underlying closers to release.
func openDecompressed(path string) (io.Reader, []io.Closer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	closers := []io.Closer{f}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".gz":
		gz, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, nil, err
		}
		closers = append([]io.Closer{gz}, closers...) // close gz before f
		return gz, closers, nil
	case ".zst":
		f.Close()
		return nil, nil, fmt.Errorf("recordsource: .zst not yet supported: %s", path)
	default:
		return f, closers, nil
	}
}

// OpenFile returns a Source over one file, choosing the decoder by the inner
// extension and transparently decompressing. idPrefix is prepended to synthetic
// record IDs (typically the file's keyspace-relative path); for a single-record
// file the ID is instead the file stem, matching today's one-doc-per-file
// META().id convention.
func OpenFile(path, idPrefix string) (Source, error) {
	if !IsRecordFile(path) {
		return nil, fmt.Errorf("recordsource: unsupported file: %s", path)
	}
	r, closers, err := openDecompressed(path)
	if err != nil {
		return nil, err
	}
	switch innerExt(path) {
	case ".jsonl", ".ndjson":
		return newJSONLSource(r, closers, idPrefix), nil
	default: // .json, .jsons
		s, err := newJSONSource(r, closers, idPrefix, stem(path))
		if err != nil {
			closeAll(closers)
			return nil, err
		}
		return s, nil
	}
}

func closeAll(closers []io.Closer) error {
	var first error
	for _, c := range closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// -------------------------------------------------------------- JSONL source

// jsonlSource streams a line-delimited JSON file. Each non-blank line is one
// record; Doc borrows the bufio.Scanner buffer (valid until the next Next).
type jsonlSource struct {
	sc       *bufio.Scanner
	closers  []io.Closer
	idPrefix string
	line     int    // 1-based line counter over input lines
	idBuf    []byte // reused ID scratch
}

func newJSONLSource(r io.Reader, closers []io.Closer, idPrefix string) *jsonlSource {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // allow large records
	return &jsonlSource{sc: sc, closers: closers, idPrefix: idPrefix}
}

func (s *jsonlSource) Next(rec *Record) (bool, error) {
	for s.sc.Scan() {
		s.line++
		b := bytes.TrimSpace(s.sc.Bytes())
		if len(b) == 0 {
			continue // skip blank lines
		}
		rec.Doc = b // borrowed from scanner
		s.idBuf = appendRecordID(s.idBuf[:0], s.idPrefix, s.line-1)
		rec.ID = s.idBuf
		return true, nil
	}
	if err := s.sc.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func (s *jsonlSource) Close() error { return closeAll(s.closers) }

// -------------------------------------------------------------- JSON source

// jsonSource handles .json/.jsons: a top-level array (each element a record) or
// a stream of one-or-more whitespace-separated JSON values. Values are buffered
// so IDs can follow the single-doc convention (stem) when the file holds exactly
// one record; JSONL remains the streaming path for large data.
type jsonSource struct {
	docs     [][]byte
	i        int
	closers  []io.Closer
	idPrefix string
	stem     string
	single   bool
	idBuf    []byte
}

func newJSONSource(r io.Reader, closers []io.Closer, idPrefix, stem string) (*jsonSource, error) {
	br := bufio.NewReader(r)
	// Peek the first non-whitespace byte to distinguish a top-level array.
	first, err := firstNonSpace(br)
	if err != nil {
		return nil, err
	}
	var docs [][]byte
	dec := json.NewDecoder(br)
	if first == '[' {
		if _, err := dec.Token(); err != nil { // consume '['
			return nil, err
		}
		for dec.More() {
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, err
			}
			docs = append(docs, []byte(raw))
		}
	} else {
		for {
			var raw json.RawMessage
			err := dec.Decode(&raw)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			docs = append(docs, []byte(raw))
		}
	}
	return &jsonSource{
		docs: docs, closers: closers, idPrefix: idPrefix,
		stem: stem, single: len(docs) == 1,
	}, nil
}

func (s *jsonSource) Next(rec *Record) (bool, error) {
	if s.i >= len(s.docs) {
		return false, nil
	}
	rec.Doc = s.docs[s.i]
	if s.single {
		s.idBuf = append(s.idBuf[:0], s.stem...)
	} else {
		s.idBuf = appendRecordID(s.idBuf[:0], s.idPrefix, s.i)
	}
	rec.ID = s.idBuf
	s.i++
	return true, nil
}

func (s *jsonSource) Close() error { return closeAll(s.closers) }

// firstNonSpace reads and un-reads (via Peek) the first non-whitespace byte.
func firstNonSpace(br *bufio.Reader) (byte, error) {
	for {
		b, err := br.Peek(1)
		if err != nil {
			return 0, err
		}
		switch b[0] {
		case ' ', '\t', '\r', '\n':
			br.Discard(1)
			continue
		default:
			return b[0], nil
		}
	}
}

// appendRecordID builds "<prefix>#<n>" into dst (reused).
func appendRecordID(dst []byte, prefix string, n int) []byte {
	dst = append(dst, prefix...)
	dst = append(dst, '#')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return dst
}

// -------------------------------------------------------------- directory walk

// WalkOptions configures directory discovery.
type WalkOptions struct {
	Recurse bool // descend into subdirectories (default true via Walk)
}

// Walk returns a Source over the union of all supported record files under dir,
// concatenating their record streams. Files are visited in sorted (stable)
// order for deterministic output. Synthetic IDs are prefixed with each file's
// dir-relative path, e.g. "events/2026-01-01.jsonl#3".
func Walk(dir string, opts WalkOptions) (Source, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if !opts.Recurse && path != dir {
				return filepath.SkipDir
			}
			return nil
		}
		if IsRecordFile(path) {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return &walkSource{dir: dir, files: files}, nil
}

// walkSource streams records across a sorted list of files, opening each lazily.
type walkSource struct {
	dir   string
	files []string
	i     int
	cur   Source
}

func (w *walkSource) Next(rec *Record) (bool, error) {
	for {
		if w.cur == nil {
			if w.i >= len(w.files) {
				return false, nil
			}
			path := w.files[w.i]
			rel, err := filepath.Rel(w.dir, path)
			if err != nil {
				rel = filepath.Base(path)
			}
			s, err := OpenFile(path, filepath.ToSlash(rel))
			if err != nil {
				return false, err
			}
			w.cur = s
		}
		ok, err := w.cur.Next(rec)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		w.cur.Close()
		w.cur = nil
		w.i++
	}
}

func (w *walkSource) Close() error {
	if w.cur != nil {
		err := w.cur.Close()
		w.cur = nil
		return err
	}
	return nil
}
