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

// File metadata (_meta) injection. Since the fork's META() supports only a fixed
// bitmask of fields (id/cas/keyspace/...), per-file metadata (path/name/ext/
// size/mtime) rides in the document under a reserved "_meta" sub-object instead.
// It is opt-in per the -meta mode so structured data isn't silently changed
// (which would also break the exact-match conformance suite):
//
//	off  -> never
//	on   -> every record
//	auto -> the provider decides: extracted documents yes, structured data no.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// MetaMode controls whether a _meta sub-object is added to records.
type MetaMode int

const (
	MetaAuto MetaMode = iota // provider decides (extracted docs: yes, structured: no)
	MetaOn                   // every record
	MetaOff                  // no record
)

// String renders the mode in its flag/dot-command spelling (auto|on|off), so
// callers can echo the current setting back to the user.
func (m MetaMode) String() string {
	switch m {
	case MetaOn:
		return "on"
	case MetaOff:
		return "off"
	default:
		return "auto"
	}
}

// ParseMetaMode parses the -meta flag value ("", "auto", "on", "off").
func ParseMetaMode(s string) (MetaMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "auto":
		return MetaAuto, nil
	case "on", "true", "yes":
		return MetaOn, nil
	case "off", "false", "no":
		return MetaOff, nil
	default:
		return MetaAuto, fmt.Errorf("records: bad -meta %q (want on|off|auto)", s)
	}
}

// metaInclude reports whether a file with the given inner extension should get a
// _meta sub-object. Under auto, only extracted documents do -- structured
// JSON/CSV data is left untouched.
func (o WalkOptions) metaInclude(innerExt string) bool {
	switch o.Meta {
	case MetaOn:
		return true
	case MetaOff:
		return false
	default:
		return isExtractExt(innerExt)
	}
}

// fileMetaOpen builds the reusable, UNCLOSED `"_meta":{<file fields>` fragment
// for a file: path (PathPrefix joined with the dir-relative rel), name, ext,
// size, mtime. metaSource closes it per record, optionally appending the
// record's `index` within a multi-record container file.
func fileMetaOpen(absPath, pathPrefix, rel string) ([]byte, error) {
	fi, err := os.Stat(absPath)
	if err != nil {
		return nil, err
	}
	p := rel
	if pathPrefix != "" {
		p = pathPrefix + "/" + rel
	}
	obj, err := json.Marshal(map[string]interface{}{
		"path":  p, // NB: "path" is a SQL++ reserved word -- query via _meta.`path`
		"name":  filepath.Base(rel),
		"ext":   strings.ToLower(filepath.Ext(rel)),
		"size":  fi.Size(),
		"mtime": fi.ModTime().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return nil, err
	}
	// `"_meta":` + obj without its trailing '}' (metaSource closes it).
	return append([]byte(`"_meta":`), obj[:len(obj)-1]...), nil
}

// metaSource wraps a decoder Source and splices a "_meta" object into each
// (object) record's Doc: the file-level fields, plus a `pos` (the record's
// ordinal within its container file) when the record id carries a "#<n>" suffix
// (JSONL/CSV/gz/JSON-array). Built into reused buffers (borrowed-slice contract).
type metaSource struct {
	inner Source
	open  []byte // `"_meta":{<file fields>`  (no closing brace)
	frag  []byte // per-record `"_meta":{...}` (reused)
	buf   []byte // spliced doc (reused)
}

func (m *metaSource) Next(rec *Record) (bool, error) {
	ok, err := m.inner.Next(rec)
	if !ok || err != nil {
		return ok, err
	}
	m.frag = append(m.frag[:0], m.open...)
	if idx, has := recordIndex(rec.ID); has {
		// "pos" = the record's 0-based ordinal within its container file.
		// (Not "index"/"offset" -- both are SQL++ reserved words.)
		m.frag = append(m.frag, `,"pos":`...)
		m.frag = strconv.AppendInt(m.frag, int64(idx), 10)
	}
	m.frag = append(m.frag, '}')
	m.buf = spliceMeta(m.buf[:0], rec.Doc, m.frag)
	rec.Doc = m.buf
	return true, nil
}

// recordIndex extracts the trailing "#<n>" ordinal from a record id (e.g.
// "events/day1.jsonl#3" -> 3). Returns false for a bare stem (one-doc-per-file),
// which has no in-file position.
func recordIndex(id []byte) (int, bool) {
	h := -1
	for i := len(id) - 1; i >= 0; i-- {
		if id[i] == '#' {
			h = i
			break
		}
	}
	if h < 0 || h == len(id)-1 {
		return 0, false
	}
	n, err := strconv.Atoi(string(id[h+1:]))
	if err != nil {
		return 0, false
	}
	return n, true
}

func (m *metaSource) Close() error { return m.inner.Close() }

// spliceMeta inserts frag as the first field of the JSON object doc. A non-object
// doc (array/scalar) can't carry a field, so it's returned unchanged.
func spliceMeta(dst, doc, frag []byte) []byte {
	i := 0
	for i < len(doc) && isJSONSpace(doc[i]) {
		i++
	}
	if i >= len(doc) || doc[i] != '{' {
		return append(dst, doc...) // not an object
	}
	dst = append(dst, doc[:i+1]...) // up to and including '{'
	dst = append(dst, frag...)
	rest := doc[i+1:]
	j := 0
	for j < len(rest) && isJSONSpace(rest[j]) {
		j++
	}
	if j < len(rest) && rest[j] != '}' {
		dst = append(dst, ',') // object had fields
	}
	return append(dst, rest...)
}

func isJSONSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}
