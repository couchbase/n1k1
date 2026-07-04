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

// Package records decodes on-disk data files into a stream of JSON
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
package records

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
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
// a top-level array; csv/tsv are header + delimited rows decoded into JSON
// objects; yaml/yml are one-or-more (`---`-separated) documents, each converted
// to a JSON value.
var recordExts = map[string]bool{
	".json": true, ".jsons": true, ".jsonl": true, ".ndjson": true,
	".csv": true, ".tsv": true,
	".yaml": true, ".yml": true,
}

// IsRecordFile reports whether path (by extension, ignoring a .gz/.zst suffix)
// is a data file this package can decode (structured JSON/CSV, or a document/
// media file the extract provider handles -- see the extractors table).
func IsRecordFile(path string) bool {
	ext := innerExt(path)
	return recordExts[ext] || isExtractExt(ext)
}

// IsStructuredFile reports whether path is a *structured* data file (JSON family
// or CSV/TSV) -- i.e. a record file that is NOT an extracted document (PDF/DOCX/
// XLSX). Directory discovery uses this to auto-expose data files as keyspaces
// without flooding the list with every document in a folder.
func IsStructuredFile(path string) bool {
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

// isCompressed reports whether path carries a compression suffix (.gz/.zst),
// i.e. its bytes are decompressed on read -- so a byte offset into the record
// stream doesn't address the file's raw bytes and can't be sought.
func isCompressed(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".gz" || ext == ".zst"
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

// Stem returns the keyspace-style base name for a single record file: the base
// name with its format extension and any compression suffix removed
// ("orders.jsonl.gz" -> "orders", "dump.ndjson" -> "dump"). Used for the
// single-file-as-keyspace naming of DESIGN-data.md scenario B2.
func Stem(path string) string { return stem(path) }

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
		return nil, nil, fmt.Errorf("records: .zst not yet supported: %s", path)
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
		return nil, fmt.Errorf("records: unsupported file: %s", path)
	}
	// PDF/DOCX/XLSX documents are opened by path (zip / whole-file readers),
	// not through the streaming decompression layer.
	if isExtractExt(innerExt(path)) {
		return newExtractSource(path)
	}
	r, closers, err := openDecompressed(path)
	if err != nil {
		return nil, err
	}
	// A byte offset into the stream is seekable only for an uncompressed file (a
	// .gz/.zst offset is into the decompressed stream, which can't seek the
	// compressed bytes). Gates the "@<offset>" id suffix (see newJSONLSource).
	seekable := !isCompressed(path)

	switch innerExt(path) {
	case ".jsonl", ".ndjson":
		return newJSONLSource(r, closers, idPrefix, seekable), nil
	case ".csv", ".tsv":
		comma := ','
		if innerExt(path) == ".tsv" {
			comma = '\t'
		}
		s, err := newCSVSource(r, closers, idPrefix, comma)
		if err != nil {
			closeAll(closers)
			return nil, err
		}
		return s, nil
	case ".yaml", ".yml":
		s, err := newYAMLSource(r, closers, idPrefix, stem(path), seekable)
		if err != nil {
			closeAll(closers)
			return nil, err
		}
		return s, nil
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
	seekable bool   // append a byte-offset suffix to ids (uncompressed files only)
	line     int    // 1-based line counter over input lines
	off      int64  // cumulative bytes consumed (absolute file position)
	tokOff   int64  // byte offset of the token from the most recent split call
	idBuf    []byte // reused ID scratch
}

// newJSONLSource streams r as line-delimited JSON. seekable reports whether r is
// the file's raw bytes (not a decompressing wrapper), i.e. whether a byte offset
// into it is meaningful for random access -- when true, each record id carries an
// "@<offset>" suffix so a key-based fetch can seek straight to the line (see
// jsonlSource.Next and glue's container fetch). For a compressed file it is false
// (an uncompressed-stream offset can't seek the compressed bytes).
func newJSONLSource(r io.Reader, closers []io.Closer, idPrefix string, seekable bool) *jsonlSource {
	s := &jsonlSource{closers: closers, idPrefix: idPrefix, seekable: seekable}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // allow large records
	// Wrap ScanLines to track each line's absolute byte offset without giving up
	// the alloc-free borrow-the-buffer reads: the running sum of advance counts is
	// the file position, and a line starts at that position before its own advance
	// is applied (0-advance "need more data" calls leave tokOff untouched).
	sc.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		advance, token, err = bufio.ScanLines(data, atEOF)
		s.tokOff = s.off
		s.off += int64(advance)
		return advance, token, err
	})
	s.sc = sc
	return s
}

func (s *jsonlSource) Next(rec *Record) (bool, error) {
	for s.sc.Scan() {
		s.line++
		lineOff := s.tokOff // start-of-line offset for the token Scan just produced
		b := bytes.TrimSpace(s.sc.Bytes())
		if len(b) == 0 {
			continue // skip blank lines
		}
		rec.Doc = b // borrowed from scanner
		s.idBuf = appendRecordID(s.idBuf[:0], s.idPrefix, s.line-1)
		if s.seekable {
			// "@<offset>" of the line's first byte; fetch seeks here then TrimSpaces
			// the line, matching Doc exactly even with leading/trailing whitespace.
			s.idBuf = append(s.idBuf, '@')
			s.idBuf = strconv.AppendInt(s.idBuf, lineOff, 10)
		}
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

// jsonBufPool recycles the read buffer used to slurp a whole .json/.jsons file
// before splitting it into top-level values. A nested-loop join re-opens each
// inner file O(N) times, so allocating a fresh bufio.Reader (4 KiB) + json.Decoder
// per open dominated both CPU and allocations (see DESIGN-data.md); reading into a
// pooled buffer and splitting the values in-memory removes that per-open churn.
// (json.Decoder itself can't be pooled -- it has no Reset and latches io.EOF.)
var jsonBufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

// jsonBufPoolMaxCap caps which buffers return to the pool, so one huge file can't
// pin a large buffer in the pool forever.
const jsonBufPoolMaxCap = 1 << 20 // 1 MiB.

func newJSONSource(r io.Reader, closers []io.Closer, idPrefix, stem string) (*jsonSource, error) {
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	if _, err := buf.ReadFrom(r); err != nil {
		if buf.Cap() <= jsonBufPoolMaxCap {
			jsonBufPool.Put(buf)
		}
		return nil, err
	}
	docs, err := splitJSONValues(buf.Bytes())
	if buf.Cap() <= jsonBufPoolMaxCap {
		jsonBufPool.Put(buf) // docs own copies of their bytes, so the buffer is free to reuse.
	}
	if err != nil {
		return nil, err
	}
	return &jsonSource{
		docs: docs, closers: closers, idPrefix: idPrefix,
		stem: stem, single: len(docs) == 1,
	}, nil
}

// splitJSONValues splits a complete .json/.jsons buffer into owned per-record byte
// slices, matching the historical json.Decoder behavior: a leading '[' is a
// top-level array whose elements are the records, otherwise the buffer is a stream
// of one-or-more whitespace-separated JSON values. Structure isn't fully validated
// here (that happens downstream at evaluation); only value boundaries are found.
func splitJSONValues(data []byte) ([][]byte, error) {
	i, n := 0, len(data)
	for i < n && isJSONSpace(data[i]) {
		i++
	}
	if i >= n {
		return nil, nil
	}
	var docs [][]byte
	if data[i] == '[' { // top-level array: each element is a record.
		i++
		for {
			for i < n && isJSONSpace(data[i]) {
				i++
			}
			if i >= n {
				return nil, fmt.Errorf("records: unterminated JSON array")
			}
			if data[i] == ']' {
				break
			}
			if data[i] == ',' { // element separator.
				i++
				continue
			}
			start, end, err := scanJSONValue(data, i)
			if err != nil {
				return nil, err
			}
			docs = append(docs, append([]byte(nil), data[start:end]...))
			i = end
		}
		return docs, nil
	}
	for i < n { // whitespace-separated value stream.
		for i < n && isJSONSpace(data[i]) {
			i++
		}
		if i >= n {
			break
		}
		start, end, err := scanJSONValue(data, i)
		if err != nil {
			return nil, err
		}
		docs = append(docs, append([]byte(nil), data[start:end]...))
		i = end
	}
	return docs, nil
}

// scanJSONValue returns [start,end) of the single JSON value beginning at data[i]
// (which must be a non-whitespace byte). Objects/arrays are matched by nesting
// depth while respecting string literals and escapes; strings by their closing
// quote; primitives (number/true/false/null) run until whitespace or a structural
// byte (, ] }).
func scanJSONValue(data []byte, i int) (start, end int, err error) {
	start = i
	n := len(data)
	switch data[i] {
	case '{', '[':
		depth, inStr, esc := 0, false, false
		for ; i < n; i++ {
			c := data[i]
			if inStr {
				switch {
				case esc:
					esc = false
				case c == '\\':
					esc = true
				case c == '"':
					inStr = false
				}
				continue
			}
			switch c {
			case '"':
				inStr = true
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return start, i + 1, nil
				}
			}
		}
		return 0, 0, fmt.Errorf("records: unterminated JSON value")
	case '"':
		esc := false
		for i++; i < n; i++ {
			c := data[i]
			switch {
			case esc:
				esc = false
			case c == '\\':
				esc = true
			case c == '"':
				return start, i + 1, nil
			}
		}
		return 0, 0, fmt.Errorf("records: unterminated JSON string")
	default: // primitive.
		for i < n {
			c := data[i]
			if isJSONSpace(c) || c == ',' || c == ']' || c == '}' {
				break
			}
			i++
		}
		return start, i, nil
	}
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

// -------------------------------------------------------------- YAML source

// yamlSource yields records precomputed from a YAML file: each holds an id and
// the record's JSON bytes (both borrowed until the next Next, per the Source
// contract). Built by newYAMLSource.
type yamlSource struct {
	recs    []yamlRec
	i       int
	closers []io.Closer
}

type yamlRec struct {
	id  []byte
	doc []byte
}

func (s *yamlSource) Next(rec *Record) (bool, error) {
	if s.i >= len(s.recs) {
		return false, nil
	}
	rec.ID, rec.Doc = s.recs[s.i].id, s.recs[s.i].doc
	s.i++
	return true, nil
}

func (s *yamlSource) Close() error { return closeAll(s.closers) }

// newYAMLSource handles .yaml/.yml, mirroring the JSON record model. A file is
// one of:
//   - a multi-document (`---`-separated) stream -- one record per document; this
//     is YAML's native equivalent of JSON Lines. When seekable (an uncompressed
//     file), each document's byte offset is baked into its id
//     (<prefix>#<i>@<offset>), so a key-based fetch can seek straight to it and
//     decode that one document (see DecodeYAMLDoc);
//   - a single document that is a top-level sequence (list) -- one record per
//     element, like a top-level array in a `.json` file. A sequence element is
//     NOT a standalone document, so these carry NO offset (ids <prefix>#<i>);
//   - a single document (map/scalar) -- one record, id = the file stem.
//
// Each record is decoded and re-marshaled to a JSON value, so the rest of the
// pipeline sees the same JSON bytes it gets for a .json record. The whole file is
// read up front: document boundaries (byte offsets) are found by scanning for
// `---` markers, which YAML reserves at column 0 (see yamlDocOffsets), then each
// document's byte range is yaml.Unmarshal'd.
func newYAMLSource(r io.Reader, closers []io.Closer, idPrefix, stem string, seekable bool) (Source, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	// Decode each `---`-delimited document, remembering its start offset.
	type docAt struct {
		off int
		v   interface{}
	}
	var docs []docAt
	offs := yamlDocOffsets(data)
	for i, off := range offs {
		end := len(data)
		if i+1 < len(offs) {
			end = offs[i+1]
		}
		var v interface{}
		if err := yaml.Unmarshal(data[off:end], &v); err != nil {
			return nil, err
		}
		if v == nil {
			continue // empty document (e.g. a bare or trailing `---`)
		}
		docs = append(docs, docAt{off, v})
	}

	src := &yamlSource{closers: closers}
	marshal := func(v interface{}) ([]byte, error) { return json.Marshal(yamlToJSONValue(v)) }

	// A single top-level sequence expands to one record per element (like a
	// top-level JSON array). Elements aren't standalone documents, so no offset.
	if len(docs) == 1 {
		if seq, ok := docs[0].v.([]interface{}); ok {
			for i, el := range seq {
				jb, err := marshal(el)
				if err != nil {
					return nil, err
				}
				src.recs = append(src.recs, yamlRec{id: appendRecordID(nil, idPrefix, i), doc: jb})
			}
			return src, nil
		}
		// A single non-sequence document -> the file stem (one-doc-per-file).
		jb, err := marshal(docs[0].v)
		if err != nil {
			return nil, err
		}
		src.recs = append(src.recs, yamlRec{id: []byte(stem), doc: jb})
		return src, nil
	}

	// A multi-document stream -> one record per document, byte-seekable (when the
	// file's bytes are the raw file, i.e. uncompressed): id <prefix>#<i>@<offset>.
	for i, d := range docs {
		jb, err := marshal(d.v)
		if err != nil {
			return nil, err
		}
		id := appendRecordID(nil, idPrefix, i)
		if seekable {
			id = append(id, '@')
			id = strconv.AppendInt(id, int64(d.off), 10)
		}
		src.recs = append(src.recs, yamlRec{id: id, doc: jb})
	}
	return src, nil
}

// yamlDocOffsets returns the byte offset of each YAML document in data. The first
// document starts at 0; each later one starts at a `---` document-start marker.
// YAML reserves `---` at column 0 as that marker (block-scalar bodies are
// indented and a plain scalar can't begin a continuation line with a column-0
// `---`), so a line beginning `---` (then end-of-line, space/tab, or a `#`
// comment) reliably delimits a document -- letting a fetch seek to one.
func yamlDocOffsets(data []byte) []int {
	offs := []int{0}
	i, lineNo := 0, 0
	for i < len(data) {
		lineStart := i
		nl := bytes.IndexByte(data[i:], '\n')
		var line []byte
		if nl < 0 {
			line, i = data[i:], len(data)
		} else {
			line, i = data[i:i+nl], i+nl+1
		}
		// Line 0's start (offset 0) is already recorded; a `---` there is the first
		// document's own marker, not a separator.
		if lineNo > 0 && isYAMLDocMarker(line) {
			offs = append(offs, lineStart)
		}
		lineNo++
	}
	return offs
}

func isYAMLDocMarker(line []byte) bool {
	if !bytes.HasPrefix(line, []byte("---")) {
		return false
	}
	rest := line[3:]
	return len(rest) == 0 || rest[0] == ' ' || rest[0] == '\t' || rest[0] == '\r' || rest[0] == '#'
}

// IsYAMLFile reports whether path is a YAML file (by inner extension, ignoring a
// compression suffix). Used by the fetch path to decode a container record as one
// YAML document rather than one line.
func IsYAMLFile(path string) bool {
	ext := innerExt(path)
	return ext == ".yaml" || ext == ".yml"
}

// DecodeYAMLDoc reads exactly one YAML document from r and returns it as JSON
// bytes (the same shape newYAMLSource produces). For a key-based fetch that has
// seeked r to a document's byte offset (the `@<offset>` in a multi-doc YAML id).
// ok is false (nil error) at EOF or for an empty document.
func DecodeYAMLDoc(r io.Reader) (doc []byte, ok bool, err error) {
	var v interface{}
	e := yaml.NewDecoder(r).Decode(&v)
	if e == io.EOF {
		return nil, false, nil
	}
	if e != nil {
		return nil, false, e
	}
	if v == nil {
		return nil, false, nil
	}
	jb, e := json.Marshal(yamlToJSONValue(v))
	if e != nil {
		return nil, false, e
	}
	return jb, true, nil
}

// yamlToJSONValue makes a YAML-decoded value json.Marshal-able: it rewrites any
// map with non-string keys (YAML allows them; JSON doesn't) to a string-keyed
// map, recursing through nested maps/slices. yaml.v3 already decodes string-keyed
// mappings as map[string]interface{}, so the common case is a cheap walk.
func yamlToJSONValue(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		for k, val := range x {
			x[k] = yamlToJSONValue(val)
		}
		return x
	case map[interface{}]interface{}:
		m := make(map[string]interface{}, len(x))
		for k, val := range x {
			m[fmt.Sprint(k)] = yamlToJSONValue(val)
		}
		return m
	case []interface{}:
		for i, val := range x {
			x[i] = yamlToJSONValue(val)
		}
		return x
	default:
		return v
	}
}

// appendRecordID builds "<prefix>#<n>" into dst (reused).
func appendRecordID(dst []byte, prefix string, n int) []byte {
	dst = append(dst, prefix...)
	dst = append(dst, '#')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return dst
}

// -------------------------------------------------------------- CSV/TSV source

// csvSource decodes a delimited file (CSV/TSV) into one JSON object per data
// row, keyed by the header row. Values get light type inference (int/float/bool/
// null) with a string fallback -- see csvAppendValue. The doc JSON is built into
// a reused buffer (borrowed until the next Next); encoding/csv handles quoting/
// escaping/embedded newlines correctly (unlike the old op_scan.go splitter).
//
// Allocation note: encoding/csv with ReuseRecord reuses its []string slice, but
// the field strings themselves are allocated per row -- acceptable for a first
// cut (they're consumed immediately into the doc buffer). A fully []byte-native
// CSV reader is a later optimization (DESIGN-data.md "Allocation model").
type csvSource struct {
	r        *csv.Reader
	header   []string
	closers  []io.Closer
	idPrefix string
	row      int
	docBuf   []byte
	idBuf    []byte
	done     bool
}

func newCSVSource(r io.Reader, closers []io.Closer, idPrefix string, comma rune) (*csvSource, error) {
	cr := csv.NewReader(r)
	cr.Comma = comma
	cr.ReuseRecord = true   // reuse the []string across rows
	cr.FieldsPerRecord = -1 // tolerate ragged rows (map by position)
	cr.TrimLeadingSpace = false

	s := &csvSource{r: cr, closers: closers, idPrefix: idPrefix}

	hdr, err := cr.Read()
	if err == io.EOF {
		s.done = true // empty file: no header, no rows
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	s.header = append([]string(nil), hdr...) // copy (ReuseRecord will overwrite)
	return s, nil
}

func (s *csvSource) Next(rec *Record) (bool, error) {
	if s.done {
		return false, nil
	}
	fields, err := s.r.Read()
	if err == io.EOF {
		s.done = true
		return false, nil
	}
	if err != nil {
		return false, err
	}
	s.docBuf = csvRowToJSON(s.docBuf[:0], s.header, fields)
	rec.Doc = s.docBuf
	s.idBuf = appendRecordID(s.idBuf[:0], s.idPrefix, s.row)
	rec.ID = s.idBuf
	s.row++
	return true, nil
}

func (s *csvSource) Close() error { return closeAll(s.closers) }

// csvRowToJSON builds {"<hdr0>":<v0>,...} into dst. A field missing for a header
// column (short row) becomes null; extra fields beyond the header are dropped.
func csvRowToJSON(dst []byte, header, fields []string) []byte {
	dst = append(dst, '{')
	for i, key := range header {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = strconv.AppendQuote(dst, key)
		dst = append(dst, ':')
		if i < len(fields) {
			dst = csvAppendValue(dst, fields[i])
		} else {
			dst = append(dst, "null"...)
		}
	}
	dst = append(dst, '}')
	return dst
}

// csvAppendValue appends a CSV cell as a JSON value with light per-cell type
// inference: empty -> null; true/false -> bool; numbers -> int/float; else a
// JSON string. Two guards keep identifier-ish cells as strings rather than
// lossily coercing them: a leading-zero integer part ("007", zip codes) and an
// integer that overflows int64 (long account/id numbers) stay strings --
// while ordinary decimals like "129.50" become numbers (so SUM/AVG work).
// (Per-cell inference is inherently limited; column-level sniffing + per-column
// overrides are a later refinement -- DESIGN-data.md §1.)
func csvAppendValue(dst []byte, s string) []byte {
	switch {
	case s == "":
		return append(dst, "null"...)
	case s == "true" || s == "false":
		return append(dst, s...)
	case hasLeadingZeroIntPart(s):
		// "007", "00.5": preserve as string.
	case isIntegerShaped(s):
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return strconv.AppendInt(dst, i, 10)
		}
		// integer-shaped but overflows int64: keep full precision as a string.
	default:
		if f, err := strconv.ParseFloat(s, 64); err == nil &&
			!math.IsInf(f, 0) && !math.IsNaN(f) {
			return strconv.AppendFloat(dst, f, 'g', -1, 64)
		}
	}
	return strconv.AppendQuote(dst, s)
}

// isIntegerShaped reports whether s is an optional sign followed by all digits.
func isIntegerShaped(s string) bool {
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// hasLeadingZeroIntPart reports whether the integer part of s (before any '.'/'e')
// is a multi-digit run starting with '0' -- the "007" / "00.5" identifier case.
func hasLeadingZeroIntPart(s string) bool {
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		s = s[1:]
	}
	end := len(s)
	for i := 0; i < len(s); i++ {
		if s[i] == '.' || s[i] == 'e' || s[i] == 'E' {
			end = i
			break
		}
	}
	ip := s[:end]
	return len(ip) > 1 && ip[0] == '0'
}

// -------------------------------------------------------------- directory walk

// WalkOptions configures directory discovery and which files are eligible.
// The zero value is restrictive (no formats, no recurse); use AllModes() for
// the flexible default, or ParseModes() to honor a user's --modes restriction.
type WalkOptions struct {
	Recurse   bool            // descend into subdirectories
	Formats   map[string]bool // allowed inner extensions (".json", ".jsonl", …); nil = all supported
	AllowGzip bool            // permit a .gz compression suffix
	AllowZstd bool            // permit a .zst compression suffix (not yet decodable)

	// Spec is the canonical, reusable -formats/.formats token string that produced
	// this WalkOptions (e.g. "json,csv,gzip", or "all" for the default). Set by
	// AllModes/ParseModes; it round-trips (unlike a raw extension list, since exts
	// like .jsons/.ndjson aren't themselves valid tokens). Shown by `.formats`.
	Spec string

	// Meta controls whether a _meta sub-object (path/name/ext/size/mtime) is
	// added to records (see meta.go). PathPrefix is prepended to each record's
	// _meta.path (e.g. "<namespace>/<keyspace>") so it's dir-relative.
	Meta       MetaMode
	PathPrefix string
}

// AllModes returns the flexible default: recurse, all supported formats, gzip on.
// n1k1 uses this unless the user restricts scanning via --modes (see ParseModes).
func AllModes() WalkOptions {
	return WalkOptions{Recurse: true, Formats: nil, AllowGzip: true, AllowZstd: false, Spec: "all"}
}

// ParseModes builds a restrictive WalkOptions from a comma-separated mode list
// (the CLI's -scan flag), so a user with subdirs/formats they don't want
// scanned can lock n1k1 down. Recognized tokens:
//
//	all       → everything (flexible, the default)
//	json      → .json/.jsons        jsonl → .jsonl/.ndjson
//	csv       → .csv                 tsv   → .tsv
//	extract   → every extract format (all of the below groups)
//	doc       → .pdf/.docx/.xlsx/.pptx    text → .txt/.log/.md/.markdown/.rtf
//	image     → .png/.jpg/.jpeg           video → .mp4/.mov
//	pdf|docx|xlsx|pptx|txt|md|rtf|png|jpg|mp4|… → that one extension
//	gzip      → allow .gz            recurse → descend subdirs
//
// An empty string (or "all") means "unrestricted" (AllModes). Unknown tokens
// are an error.
func ParseModes(csv string) (WalkOptions, error) {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return AllModes(), nil
	}
	opts := WalkOptions{Formats: map[string]bool{}}
	// Record the canonical primary token per recognized input, deduped and in the
	// order given, so opts.Spec round-trips (e.g. "ndjson,gz" -> "jsonl,gzip").
	var spec []string
	seen := map[string]bool{}
	add := func(canon string) {
		if !seen[canon] {
			seen[canon] = true
			spec = append(spec, canon)
		}
	}
	for _, tok := range strings.Split(csv, ",") {
		switch strings.ToLower(strings.TrimSpace(tok)) {
		case "":
			// tolerate empty items from trailing commas
		case "all":
			return AllModes(), nil // everything (flexible), same as empty
		case "json":
			opts.Formats[".json"], opts.Formats[".jsons"] = true, true
			add("json")
		case "jsonl", "ndjson":
			opts.Formats[".jsonl"], opts.Formats[".ndjson"] = true, true
			add("jsonl")
		case "csv":
			opts.Formats[".csv"] = true
			add("csv")
		case "tsv":
			opts.Formats[".tsv"] = true
			add("tsv")
		case "yaml", "yml":
			opts.Formats[".yaml"], opts.Formats[".yml"] = true, true
			add("yaml")
		case "extract":
			for ext := range extractors { // every registered extract format
				opts.Formats[ext] = true
			}
			add("extract")
		case "doc", "docs", "office":
			opts.Formats[".pdf"], opts.Formats[".docx"] = true, true
			opts.Formats[".xlsx"], opts.Formats[".pptx"] = true, true
			add("doc")
		case "text":
			opts.Formats[".txt"], opts.Formats[".log"] = true, true
			opts.Formats[".md"], opts.Formats[".markdown"], opts.Formats[".rtf"] = true, true, true
			add("text")
		case "image", "img":
			opts.Formats[".png"], opts.Formats[".jpg"], opts.Formats[".jpeg"] = true, true, true
			add("image")
		case "video":
			opts.Formats[".mp4"], opts.Formats[".mov"] = true, true
			add("video")
		case "pdf":
			opts.Formats[".pdf"] = true
			add("pdf")
		case "docx":
			opts.Formats[".docx"] = true
			add("docx")
		case "xlsx":
			opts.Formats[".xlsx"] = true
			add("xlsx")
		case "pptx":
			opts.Formats[".pptx"] = true
			add("pptx")
		case "txt":
			opts.Formats[".txt"], opts.Formats[".log"] = true, true
			add("txt")
		case "md", "markdown":
			opts.Formats[".md"], opts.Formats[".markdown"] = true, true
			add("md")
		case "rtf":
			opts.Formats[".rtf"] = true
			add("rtf")
		case "png":
			opts.Formats[".png"] = true
			add("png")
		case "jpg", "jpeg":
			opts.Formats[".jpg"], opts.Formats[".jpeg"] = true, true
			add("jpg")
		case "mp4":
			opts.Formats[".mp4"] = true
			add("mp4")
		case "mov":
			opts.Formats[".mov"] = true
			add("mov")
		case "gzip", "gz":
			opts.AllowGzip = true
			add("gzip")
		case "zstd", "zst":
			opts.AllowZstd = true
			add("zstd")
		case "recurse", "recursive":
			opts.Recurse = true
			add("recurse")
		default:
			return WalkOptions{}, fmt.Errorf("records: unknown scan mode %q", tok)
		}
	}
	opts.Spec = strings.Join(spec, ",")
	return opts, nil
}

// ModeInfo documents one token accepted by ParseModes, for help output (the
// CLI's `.formats` listing). Kind groups the token for display.
type ModeInfo struct {
	Token   string   // primary token
	Aliases []string // other tokens that mean the same
	Exts    []string // file extensions it admits (nil for modifiers / "all")
	Kind    string   // "structured" | "extract" | "modifier" | "meta"
	Desc    string   // short one-line explanation
}

// Modes returns the supported scan modes/formats in display order, for help
// output. Keep this in sync with ParseModes -- TestModesMatchParseModes checks
// that every token here (and its aliases) parses and admits the listed exts.
func Modes() []ModeInfo {
	return []ModeInfo{
		{"json", nil, []string{".json", ".jsons"}, "structured", "one JSON value, or an array of values, per file"},
		{"jsonl", []string{"ndjson"}, []string{".jsonl", ".ndjson"}, "structured", "JSON Lines: one JSON value per line"},
		{"csv", nil, []string{".csv"}, "structured", "comma-separated values (header row = field names)"},
		{"tsv", nil, []string{".tsv"}, "structured", "tab-separated values (header row = field names)"},
		{"yaml", []string{"yml"}, []string{".yaml", ".yml"}, "structured", "one YAML document, or a multi-doc (--- separated) stream, per file"},
		{"extract", nil, nil, "extract", "every extractable format below (text + metadata)"},
		{"doc", []string{"docs", "office"}, []string{".pdf", ".docx", ".xlsx", ".pptx"}, "extract", "office & PDF documents"},
		{"text", nil, []string{".txt", ".log", ".md", ".markdown", ".rtf"}, "extract", "plain / rich text files"},
		{"image", []string{"img"}, []string{".png", ".jpg", ".jpeg"}, "extract", "images (metadata only)"},
		{"video", nil, []string{".mp4", ".mov"}, "extract", "video files (metadata only)"},
		{"gzip", []string{"gz"}, nil, "modifier", "also read .gz-compressed files (transparent)"},
		{"recurse", []string{"recursive"}, nil, "modifier", "descend into subdirectories"},
		{"all", nil, nil, "meta", "everything (the default when -formats is unset)"},
	}
}

// eligible reports whether path passes the options' format/compression filter.
func (o WalkOptions) eligible(path string) bool {
	if !IsRecordFile(path) {
		return false
	}
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".gz" && !o.AllowGzip {
		return false
	}
	if ext == ".zst" && !o.AllowZstd {
		return false
	}
	if o.Formats != nil && !o.Formats[innerExt(path)] {
		return false
	}
	return true
}

// Walk returns a Source over the union of all eligible record files under dir,
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
		if opts.eligible(path) {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return &walkSource{dir: dir, files: files, opts: opts}, nil
}

// File returns a Source over exactly one record file, with no directory walk --
// the single-file analogue of Walk for DESIGN-data.md scenario B2, where the CLI
// arg is one JSONL/NDJSON/JSON/CSV/... file (optionally .gz) rather than a
// directory. The file must pass opts' format/compression filter. Synthetic IDs
// are prefixed with the file's base name, e.g. "events.jsonl#3".
func File(path string, opts WalkOptions) (Source, error) {
	if !opts.eligible(path) {
		return nil, fmt.Errorf("records: not a scannable file: %s", path)
	}
	// dir = the file's parent so walkSource's Rel(dir, path) yields the base name
	// as the synthetic-ID prefix; the file list is exactly this one file.
	return &walkSource{dir: filepath.Dir(path), files: []string{path}, opts: opts}, nil
}

// walkSource streams records across a sorted list of files, opening each lazily.
type walkSource struct {
	dir   string
	files []string
	opts  WalkOptions
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
			rel = filepath.ToSlash(rel)
			s, err := OpenFile(path, rel)
			if err != nil {
				return false, err
			}
			// Opt-in per-file metadata (_meta): extracted docs under auto, or all
			// records under -meta=on. Silently skipped if the file can't be stat'd.
			if w.opts.metaInclude(innerExt(path)) {
				if open, ferr := fileMetaOpen(path, w.opts.PathPrefix, rel); ferr == nil {
					s = &metaSource{inner: s, open: open}
				}
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
