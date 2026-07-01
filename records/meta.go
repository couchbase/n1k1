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
//	auto -> the provider decides: office/PDF documents yes, structured data no.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// MetaMode controls whether a _meta sub-object is added to records.
type MetaMode int

const (
	MetaAuto MetaMode = iota // provider decides (office docs: yes, structured: no)
	MetaOn                   // every record
	MetaOff                  // no record
)

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
// _meta sub-object. Under auto, only office/PDF documents do -- structured
// JSON/CSV data is left untouched.
func (o WalkOptions) metaInclude(innerExt string) bool {
	switch o.Meta {
	case MetaOn:
		return true
	case MetaOff:
		return false
	default:
		return isOfficeExt(innerExt)
	}
}

// fileMetaFragment builds the reusable `"_meta":{...}` JSON fragment for a file:
// path (PathPrefix joined with the dir-relative rel), name, ext, size, mtime.
func fileMetaFragment(absPath, pathPrefix, rel string) ([]byte, error) {
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
	return append([]byte(`"_meta":`), obj...), nil
}

// metaSource wraps a decoder Source and splices a per-file "_meta" object into
// each (object) record's Doc. The spliced Doc is built into a reused buffer,
// preserving the borrowed-slice contract.
type metaSource struct {
	inner Source
	frag  []byte // `"_meta":{...}`
	buf   []byte
}

func (m *metaSource) Next(rec *Record) (bool, error) {
	ok, err := m.inner.Next(rec)
	if !ok || err != nil {
		return ok, err
	}
	m.buf = spliceMeta(m.buf[:0], rec.Doc, m.frag)
	rec.Doc = m.buf
	return true, nil
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
