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

// introspect.go is n1k1's public keyspace/schema INTROSPECTION API for library
// embedders: list a session's keyspaces (with their record-framing) and sample a
// keyspace's shape (top-level fields + observed JSON types + distinct scalar values).
// The CLI's .tables / .schema are thin renderers over these. Schema sampling runs
// through the ordinary query path (SELECT x.* FROM <ks> LIMIT n), so it sees exactly
// what queries see -- flat roots, single-file keyspaces, JSONL/CSV, gzip, extract
// recipes -- rather than a separate, drift-prone reader.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// KeyspaceInfo names one keyspace and summarizes how its files frame into records.
// Namespace is the keyspace's namespace (usually the session default "default"; a
// multi-source session can place sources under others -- see OpenSessionSources).
type KeyspaceInfo struct {
	Name      string
	Namespace string
	Framing   KeyspaceFraming
}

// Keyspaces lists this session's keyspaces (each with its record-framing summary:
// structured / whole-file blob / recipe / temp / Iceberg / mixed / empty). It reflects
// the datastore's flattening -- synthetic flat-root, single-file, TEMP, and Iceberg
// keyspaces, not just literal subdirectories -- and reads no file content (framing is
// classified from the file listing + the recipe registry). ALL namespaces are listed
// (the session default first, then others alphabetically; keyspaces sorted by name
// within each), so a multi-source session's non-default-namespace keyspaces appear too;
// each carries its Namespace. An empty datastore yields an empty slice, not an error.
func (s *Session) Keyspaces() ([]KeyspaceInfo, error) {
	nsNames, _ := s.Store.Datastore.NamespaceNames()
	// Session default namespace first, then the rest alphabetically (dedup'd).
	ordered := []string{s.Namespace}
	seen := map[string]bool{strings.ToLower(s.Namespace): true}
	rest := []string{}
	for _, n := range nsNames {
		if !seen[strings.ToLower(n)] {
			seen[strings.ToLower(n)] = true
			rest = append(rest, n)
		}
	}
	sort.Strings(rest)
	ordered = append(ordered, rest...)

	var out []KeyspaceInfo
	for _, nsName := range ordered {
		ns, err := s.Store.Datastore.NamespaceByName(nsName)
		if err != nil {
			continue // a missing namespace (e.g. the session default over an empty datastore).
		}
		names, err := ns.KeyspaceNames()
		if err != nil {
			continue
		}
		sort.Strings(names)
		for _, n := range names {
			ki := KeyspaceInfo{Name: n, Namespace: nsName}
			if ks, kerr := ns.KeyspaceByName(n); kerr == nil {
				ki.Framing, _ = KeyspaceFramingFor(ks) // best-effort; zero value on error.
			}
			out = append(out, ki)
		}
	}
	return out, nil
}

// SchemaSampleMaxValues caps how many distinct scalar values SampleSchema keeps per
// field (first-seen order); past it the field's Capped flag is set. Exported as a var
// so an embedder can tune the sample's value retention.
var SchemaSampleMaxValues = 16

// FieldStat is the sampled shape of one top-level field across a SampleSchema run.
type FieldStat struct {
	Types     []string          // observed JSON types (null/bool/number/string/array/object), sorted
	Values    []json.RawMessage // distinct scalar values, first-seen order, capped at SchemaSampleMaxValues
	Capped    bool              // more distinct scalar values than the cap were seen
	NonScalar bool              // an object- or array-valued occurrence was seen

	typeSeen map[string]bool // dedup for Types (accumulation only)
	valSeen  map[string]bool // dedup for Values (accumulation only)
}

// observe records one occurrence of the field (v decoded, raw its canonical JSON).
func (fs *FieldStat) observe(v interface{}, raw json.RawMessage) {
	if fs.typeSeen == nil {
		fs.typeSeen = map[string]bool{}
		fs.valSeen = map[string]bool{}
	}
	t := jsonTypeName(v)
	if !fs.typeSeen[t] {
		fs.typeSeen[t] = true
		fs.Types = append(fs.Types, t)
	}
	switch t {
	case "object", "array":
		fs.NonScalar = true
	case "null":
		// null is not a useful equality literal (`= null` is never true); not retained.
	default: // string / number / bool -- candidate WHERE literals
		key := string(raw)
		if fs.valSeen[key] {
			return
		}
		if len(fs.Values) >= SchemaSampleMaxValues {
			fs.Capped = true
			return
		}
		fs.valSeen[key] = true
		fs.Values = append(fs.Values, raw)
	}
}

// SchemaSample is a SampleSchema result: the docs sampled and per-field stats keyed by
// top-level field name.
type SchemaSample struct {
	Rows   int                   // documents sampled (result rows of the LIMIT query)
	Fields map[string]*FieldStat // top-level field name -> observed shape
}

// SampleSchema infers a keyspace's shape by running `SELECT x.* FROM <keyspace> AS x
// LIMIT n` through this session (the same resolution + decoding path a real query
// takes) and aggregating each result row's top-level fields into per-field FieldStats.
// It powers the CLI's .schema and gives an embedder schema inference without a fixed
// catalog. A non-object row (a bare scalar) contributes no fields; limit <= 0 samples
// nothing. keyspace may be namespace-qualified ("analytics:orders").
func (s *Session) SampleSchema(keyspace string, limit int) (*SchemaSample, error) {
	stmt := fmt.Sprintf("SELECT x.* FROM %s AS x LIMIT %d", qualifiedIdent(keyspace), limit)
	res, err := s.Run(stmt)
	if err != nil {
		return nil, err
	}
	fields := map[string]*FieldStat{}
	for _, row := range res.Rows {
		var m map[string]interface{}
		if json.Unmarshal(row, &m) != nil {
			continue // a non-object row contributes no top-level fields
		}
		for k, v := range m {
			fs := fields[k]
			if fs == nil {
				fs = &FieldStat{}
				fields[k] = fs
			}
			raw, _ := json.Marshal(v)
			fs.observe(v, raw)
		}
	}
	for _, fs := range fields {
		sort.Strings(fs.Types)
	}
	return &SchemaSample{Rows: len(res.Rows), Fields: fields}, nil
}

// jsonTypeName maps a decoded JSON value to a short type name.
func jsonTypeName(v interface{}) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case float64:
		return "number"
	case string:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "unknown"
	}
}

// backtickIdent backtick-quotes an identifier (e.g. a keyspace name) so any name --
// dotted, hyphenated, or a reserved word -- parses in a FROM clause; an embedded
// backtick is doubled.
func backtickIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// qualifiedIdent backtick-quotes a possibly namespace-qualified keyspace reference for a
// FROM clause: "orders" -> `orders`, "analytics:orders" -> `analytics`:`orders` (the ':'
// namespace separator kept outside the backticks so the parser still splits on it). Split
// on the LAST ':' -- a bare keyspace name never contains one.
func qualifiedIdent(ref string) string {
	if i := strings.LastIndex(ref, ":"); i > 0 && i < len(ref)-1 {
		return backtickIdent(ref[:i]) + ":" + backtickIdent(ref[i+1:])
	}
	return backtickIdent(ref)
}
