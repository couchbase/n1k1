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
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestKeyspacesAndSampleSchema covers the public introspection API: Session.Keyspaces
// (list keyspaces + their framing) and Session.SampleSchema (infer a keyspace's shape).
func TestKeyspacesAndSampleSchema(t *testing.T) {
	dir := t.TempDir()
	write := func(ks string, docs ...string) {
		d := filepath.Join(dir, "default", ks)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		for i, doc := range docs {
			if err := os.WriteFile(filepath.Join(d, string(rune('a'+i))+".json"), []byte(doc), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("a", `{"k":1,"name":"x","meta":{"z":1}}`, `{"k":2,"name":"y","meta":{"z":2}}`)
	write("b", `{"v":true}`)

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	defer sess.Close()

	// --- Keyspaces(): sorted names, with per-keyspace framing (file counts). ---
	infos, err := sess.Keyspaces()
	if err != nil {
		t.Fatalf("Keyspaces: %v", err)
	}
	gotNames := make([]string, len(infos))
	files := map[string]int{}
	for i, ki := range infos {
		gotNames[i] = ki.Name
		files[ki.Name] = ki.Framing.Files
	}
	if !reflect.DeepEqual(gotNames, []string{"a", "b"}) {
		t.Fatalf("Keyspaces names = %v, want [a b]", gotNames)
	}
	if files["a"] != 2 || files["b"] != 1 {
		t.Errorf("framing file counts = %v, want a=2 b=1", files)
	}

	// --- SampleSchema(): per-field types, distinct scalar values, non-scalar flag. ---
	ss, err := sess.SampleSchema("a", 50)
	if err != nil {
		t.Fatalf("SampleSchema: %v", err)
	}
	if ss.Rows != 2 {
		t.Fatalf("sampled rows = %d, want 2", ss.Rows)
	}
	k := ss.Fields["k"]
	if k == nil || !reflect.DeepEqual(k.Types, []string{"number"}) || len(k.Values) != 2 || k.Capped || k.NonScalar {
		t.Errorf("field k = %+v, want number type / 2 values / not capped / scalar", k)
	}
	nm := ss.Fields["name"]
	if nm == nil || !reflect.DeepEqual(nm.Types, []string{"string"}) || len(nm.Values) != 2 {
		t.Errorf("field name = %+v, want string type / 2 values", nm)
	}
	meta := ss.Fields["meta"]
	if meta == nil || !reflect.DeepEqual(meta.Types, []string{"object"}) || !meta.NonScalar || len(meta.Values) != 0 {
		t.Errorf("field meta = %+v, want object type / NonScalar / no scalar values", meta)
	}

	// The value-retention cap is a public tunable: lowering it sets Capped.
	old := SchemaSampleMaxValues
	SchemaSampleMaxValues = 1
	defer func() { SchemaSampleMaxValues = old }()
	ss2, err := sess.SampleSchema("a", 50)
	if err != nil {
		t.Fatalf("SampleSchema (capped): %v", err)
	}
	if k2 := ss2.Fields["k"]; k2 == nil || !k2.Capped || len(k2.Values) != 1 {
		t.Errorf("with cap=1, field k = %+v, want Capped with 1 value", k2)
	}
}

// TestJsonTypeName covers the JSON value -> short type-name mapping.
func TestJsonTypeName(t *testing.T) {
	cases := []struct {
		v    interface{}
		want string
	}{
		{nil, "null"},
		{true, "bool"},
		{float64(3), "number"},
		{"s", "string"},
		{[]interface{}{1, 2}, "array"},
		{map[string]interface{}{"a": 1}, "object"},
	}
	for _, tc := range cases {
		if got := jsonTypeName(tc.v); got != tc.want {
			t.Errorf("jsonTypeName(%T) = %q, want %q", tc.v, got, tc.want)
		}
	}
}

// TestFieldStatObserve: distinct scalar values are capped (at SchemaSampleMaxValues)
// and marked; duplicates aren't re-added; null is recorded as a type but never kept as
// a value literal.
func TestFieldStatObserve(t *testing.T) {
	fs := &FieldStat{}
	for i := 0; i < SchemaSampleMaxValues+5; i++ {
		raw, _ := json.Marshal(i)
		fs.observe(float64(i), raw)
	}
	if len(fs.Values) != SchemaSampleMaxValues || !fs.Capped {
		t.Errorf("expected %d values + capped, got %d capped=%v", SchemaSampleMaxValues, len(fs.Values), fs.Capped)
	}
	// A duplicate isn't re-added; null contributes a type but no value.
	fs2 := &FieldStat{}
	fs2.observe("x", json.RawMessage(`"x"`))
	fs2.observe("x", json.RawMessage(`"x"`))
	fs2.observe(nil, json.RawMessage(`null`))
	if len(fs2.Values) != 1 || !fs2.typeSeen["null"] {
		t.Errorf("dedup/null handling off: values=%v typeSeen=%v", fs2.Values, fs2.typeSeen)
	}
}
