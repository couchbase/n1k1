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

// indexDef is one declared secondary-index definition.
type indexDef struct {
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"` // defaults to "default"
	Keyspace  string   `json:"keyspace"`
	Keys      []string `json:"keys"`  // key expression strings (leading key drives sargability)
	Where     string   `json:"where"` // optional partial-index condition

	// Parsed forms (filled by parse()).
	rangeKey  expression.Expressions
	condition expression.Expression
}

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
		if err := def.parse(); err != nil {
			return nil, fmt.Errorf("loadCatalog %q, index %q: %w", path, def.Name, err)
		}
	}
	return &cat, nil
}

// parse compiles the key/where expression strings via the n1ql parser.
func (d *indexDef) parse() error {
	if d.Name == "" || d.Keyspace == "" || len(d.Keys) == 0 {
		return fmt.Errorf("index def needs name, keyspace, and at least one key")
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
