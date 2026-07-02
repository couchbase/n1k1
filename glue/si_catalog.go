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

// Reader for the .n1k1/catalog.json sidecar (see DESIGN-indexing.md "Sidecar
// layout"). v1 reads only the index-definition half: index defs the planner
// then gets to use via a secondary-index indexer (si.go). Source-mapping/view/manifest
// fields are ignored for now, so the same file can grow those later without
// breaking this reader (single-writer, declared-intent file).

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/parser/n1ql"
)

// sidecarDir is the per-dataRoot derived-artifact directory.
const sidecarDir = ".n1k1"

// catalog is the parsed .n1k1/catalog.json (index half only for v1).
type catalog struct {
	Indexes []*indexDef `json:"indexes"`
}

// indexDef is one declared index definition. Kind selects the machinery: "gsi"
// (default) is the bbolt range secondary index (si.go); "fts" is the bleve
// full-text index (fts.go), where Keys are the fields to index (empty = dynamic,
// index every field) and Where is not used.
type indexDef struct {
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"` // defaults to "default"
	Keyspace  string   `json:"keyspace"`
	Kind      string   `json:"kind,omitempty"`  // "gsi" (default) | "fts"
	Keys      []string `json:"keys"`            // gsi: key exprs (leading drives sargability); fts: field names
	Where     string   `json:"where,omitempty"` // gsi: optional partial-index condition

	// Parsed forms (filled by parse(); gsi only).
	rangeKey  expression.Expressions
	condition expression.Expression
}

// isFTS reports whether this is a full-text (bleve) index.
func (d *indexDef) isFTS() bool { return d.Kind == "fts" }

// loadCatalog reads and parses <dataRoot>/.n1k1/catalog.json. It returns
// (nil, nil) when no sidecar exists -- the common "no metadata, behave as today"
// case -- so callers can treat a missing catalog as "no secondary indexes".
func loadCatalog(dataRoot string) (*catalog, error) {
	path := filepath.Join(dataRoot, sidecarDir, "catalog.json")
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("loadCatalog %q: %w", path, err)
	}
	var cat catalog
	if err := json.Unmarshal(raw, &cat); err != nil {
		return nil, fmt.Errorf("loadCatalog %q: %w", path, err)
	}
	for _, def := range cat.Indexes {
		if def.Namespace == "" {
			def.Namespace = "default"
		}
		if def.Kind == "" {
			def.Kind = "gsi"
		}
		if err := def.parse(); err != nil {
			return nil, fmt.Errorf("loadCatalog %q, index %q: %w", path, def.Name, err)
		}
	}
	return &cat, nil
}

// parse validates the def and, for a gsi index, compiles its key/where expression
// strings via the n1ql parser. An fts index needs no expression parsing (its Keys
// are plain field names, empty = dynamic).
func (d *indexDef) parse() error {
	if d.Name == "" || d.Keyspace == "" {
		return fmt.Errorf("index def needs name and keyspace")
	}
	if d.isFTS() {
		return nil // fts: keys are field names (empty = index all); no expr/where
	}
	if len(d.Keys) == 0 {
		return fmt.Errorf("gsi index def needs at least one key")
	}
	d.rangeKey = make(expression.Expressions, 0, len(d.Keys))
	for _, k := range d.Keys {
		e, err := n1ql.ParseExpression(k)
		if err != nil {
			return fmt.Errorf("parsing key %q: %w", k, err)
		}
		d.rangeKey = append(d.rangeKey, e)
	}
	if strings.TrimSpace(d.Where) != "" {
		e, err := n1ql.ParseExpression(d.Where)
		if err != nil {
			return fmt.Errorf("parsing where %q: %w", d.Where, err)
		}
		d.condition = e
	}
	return nil
}

// defHash is a short, stable hex hash of the normalized definition (keys +
// where). It keys the built-index directory so redefining an index yields a new
// directory rather than corrupting the old one (see DESIGN-indexing.md
// "<name>__<kind>__<defhash>").
func (d *indexDef) defHash() string {
	h := sha1.New()
	// Keys are order-significant for a composite index, so hash in order.
	for _, k := range d.Keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
	}
	h.Write([]byte("|where|"))
	h.Write([]byte(d.Where))
	return hex.EncodeToString(h.Sum(nil))[:12]
}

// catalogIndexJSON is the wire form of an index def (just the JSON fields, no
// parsed/internal state) -- used to read an `.index create` fragment and to
// (re)write catalog.json.
type catalogIndexJSON struct {
	Name      string   `json:"name"`
	Namespace string   `json:"namespace,omitempty"`
	Keyspace  string   `json:"keyspace"`
	Kind      string   `json:"kind,omitempty"`
	Keys      []string `json:"keys,omitempty"`
	Where     string   `json:"where,omitempty"`
}

// CatalogAddIndexes validates the index definitions in fragmentJSON (either a
// {"indexes":[...]} object -- e.g. `.index suggest` output -- or a single {...}
// def) and merges them into <dataRoot>/.n1k1/catalog.json, returning the names
// added. Writing the human catalog is allowed here because it is *explicit* user
// intent (`.index create`), unlike autonomous machinery (see DESIGN-indexing.md
// single-writer rule). Errors on a malformed/invalid def, a duplicate name, or a
// catalog that carries non-index sections (won't clobber future data).
func CatalogAddIndexes(dataRoot string, fragmentJSON []byte) ([]string, error) {
	// Accept the array form or a single def.
	var frag struct {
		Indexes []catalogIndexJSON `json:"indexes"`
	}
	if err := json.Unmarshal(fragmentJSON, &frag); err != nil {
		return nil, fmt.Errorf("parsing index JSON: %w", err)
	}
	adds := frag.Indexes
	if len(adds) == 0 {
		var one catalogIndexJSON
		if err := json.Unmarshal(fragmentJSON, &one); err != nil || one.Name == "" {
			return nil, fmt.Errorf("no index definitions found in the JSON")
		}
		adds = []catalogIndexJSON{one}
	}

	// Validate each (name/keyspace present, key/where expressions parse for gsi).
	for _, a := range adds {
		kind := a.Kind
		if kind == "" {
			kind = "gsi"
		}
		d := &indexDef{Name: a.Name, Namespace: a.Namespace, Keyspace: a.Keyspace,
			Kind: kind, Keys: a.Keys, Where: a.Where}
		if err := d.parse(); err != nil {
			return nil, fmt.Errorf("index %q: %w", a.Name, err)
		}
	}

	path := filepath.Join(dataRoot, sidecarDir, "catalog.json")

	// Read the existing catalog. Guard against clobbering non-index top-level
	// sections a future catalog might carry (v1 has only "indexes").
	var existing []catalogIndexJSON
	if raw, err := os.ReadFile(path); err == nil {
		top := map[string]json.RawMessage{}
		if err := json.Unmarshal(raw, &top); err != nil {
			return nil, fmt.Errorf("reading %q: %w", path, err)
		}
		for k := range top {
			if k != "indexes" {
				return nil, fmt.Errorf("%q has a %q section; edit it by hand for now", path, k)
			}
		}
		if idx, ok := top["indexes"]; ok {
			if err := json.Unmarshal(idx, &existing); err != nil {
				return nil, fmt.Errorf("existing catalog indexes: %w", err)
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading %q: %w", path, err)
	}

	// Reject duplicate names, then append.
	have := map[string]bool{}
	for _, e := range existing {
		have[e.Name] = true
	}
	var added []string
	for _, a := range adds {
		if have[a.Name] {
			return nil, fmt.Errorf("index %q already exists in catalog.json", a.Name)
		}
		have[a.Name] = true
		existing = append(existing, a)
		added = append(added, a.Name)
	}

	out, err := json.MarshalIndent(struct {
		Indexes []catalogIndexJSON `json:"indexes"`
	}{existing}, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, append(out, '\n'), 0o644); err != nil {
		return nil, err
	}
	return added, nil
}

// indexesFor returns the catalog's index defs for one namespace:keyspace, in a
// stable (name) order so index advertising is deterministic.
func (c *catalog) indexesFor(namespace, keyspace string) []*indexDef {
	var out []*indexDef
	for _, d := range c.Indexes {
		if strings.EqualFold(d.Namespace, namespace) &&
			strings.EqualFold(d.Keyspace, keyspace) {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
