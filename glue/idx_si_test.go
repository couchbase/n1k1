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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/records"

	"github.com/couchbase/query/value"
)

// jsonlDocs builds n JSONL lines, each with a unique high-cardinality "sku" and a
// low-cardinality "kind" -- so the advisor should suggest "sku" but never "kind".
func jsonlDocs(n int) []string {
	lines := make([]string, n)
	for i := 0; i < n; i++ {
		kind := "a"
		if i%2 == 1 {
			kind = "b"
		}
		lines[i] = fmt.Sprintf(`{"sku":"SKU-%04d","kind":%q}`, i, kind)
	}
	return lines
}

func hasField(sugg []IndexSuggestion, field string) *IndexSuggestion {
	for i := range sugg {
		if sugg[i].Field == field {
			return &sugg[i]
		}
	}
	return nil
}

func drainCount(t *testing.T, src records.Source) int {
	t.Helper()
	n := 0
	var rec records.Record
	for {
		ok, err := src.Next(&rec)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		n++
	}
	return n
}

// TestSuggestIndexesSingleFile: .index suggest samples a single multi-record file
// (the layout the old .schema mis-handled). If sampling regressed to one-doc-per-
// *.json it would see 0 docs and suggest nothing.
func TestSuggestIndexesSingleFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "products.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(jsonlDocs(12), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := FileStore(path)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}

	sugg, _, err := SuggestIndexes(store, "default", "", 0)
	if err != nil {
		t.Fatalf("SuggestIndexes: %v", err)
	}
	sku := hasField(sugg, "sku")
	if sku == nil {
		t.Fatalf("expected a suggestion for high-cardinality 'sku', got %+v", sugg)
	}
	if sku.Sampled != 12 || sku.Keyspace != "products" {
		t.Errorf("sku suggestion sampled=%d keyspace=%q, want 12/products", sku.Sampled, sku.Keyspace)
	}
	if hasField(sugg, "kind") != nil {
		t.Errorf("low-cardinality 'kind' should not be suggested: %+v", sugg)
	}
}

// TestSuggestIndexesFlatRoot: same, but the keyspace is a flat-root dir whose
// records are spread across multiple files (must union them).
func TestSuggestIndexesFlatRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "inv")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	docs := jsonlDocs(12)
	if err := os.WriteFile(filepath.Join(root, "a.jsonl"), []byte(strings.Join(docs[:6], "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.jsonl"), []byte(strings.Join(docs[6:], "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := FileStore(root)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}

	sugg, _, err := SuggestIndexes(store, "default", "", 0)
	if err != nil {
		t.Fatalf("SuggestIndexes: %v", err)
	}
	sku := hasField(sugg, "sku")
	if sku == nil || sku.Sampled != 12 {
		t.Fatalf("expected 'sku' suggestion sampled across both files (12), got %+v", sugg)
	}
	if sku.Keyspace != "inv" {
		t.Errorf("keyspace = %q, want inv", sku.Keyspace)
	}
}

// TestSuggestIndexesSmallSampleNote: a tiny sample (fewer than suggestMinDistinct
// docs) yields no suggestions but an explanatory note -- this is the examples/
// archive case (5 gzip'd docs) that read as "broken" without the diagnostic.
func TestSuggestIndexesSmallSampleNote(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orders.jsonl")
	// 5 docs, every field unique -> selective, but 5 < suggestMinDistinct(8).
	if err := os.WriteFile(path, []byte(strings.Join(jsonlDocs(5), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := FileStore(path)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}

	sugg, note, err := SuggestIndexes(store, "default", "", 0)
	if err != nil {
		t.Fatalf("SuggestIndexes: %v", err)
	}
	if len(sugg) != 0 {
		t.Fatalf("expected no suggestions from a 5-doc sample, got %+v", sugg)
	}
	if !strings.Contains(note, "5") || !strings.Contains(note, "too few") {
		t.Errorf("note should flag the tiny sample, got %q", note)
	}
}

// TestKeyspaceRecordsOpen: the shared resolver opens the right records for both a
// single-file and a directory keyspace -- the one seam both the scan op and the
// suggest sampler depend on.
func TestKeyspaceRecordsOpen(t *testing.T) {
	// Single file.
	fdir := t.TempDir()
	fpath := filepath.Join(fdir, "events.jsonl")
	if err := os.WriteFile(fpath, []byte(strings.Join(jsonlDocs(5), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Flat-root dir.
	ddir := filepath.Join(t.TempDir(), "many")
	if err := os.MkdirAll(ddir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ddir, "x.jsonl"), []byte(strings.Join(jsonlDocs(7), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		path string
		want int
	}{{fpath, 5}, {ddir, 7}} {
		store, err := FileStore(tc.path)
		if err != nil {
			t.Fatalf("FileStore(%q): %v", tc.path, err)
		}
		ns, err := store.Datastore.NamespaceByName("default")
		if err != nil {
			t.Fatalf("NamespaceByName: %v", err)
		}
		names, err := ns.KeyspaceNames()
		if err != nil || len(names) != 1 {
			t.Fatalf("KeyspaceNames: %v names=%v", err, names)
		}
		ks, err := ns.KeyspaceByName(names[0])
		if err != nil {
			t.Fatalf("KeyspaceByName: %v", err)
		}
		src, err := KeyspaceRecordsOpen(ks, records.AllModes(), nil)
		if err != nil {
			t.Fatalf("KeyspaceRecordsOpen(%q): %v", tc.path, err)
		}
		got := drainCount(t, src)
		src.Close()
		if got != tc.want {
			t.Errorf("%q: opened %d records, want %d", tc.path, got, tc.want)
		}
	}
}

// --- Order-preserving secondary-index key codec (idx_si_encode.go) ---
//
// The codec's two contracts are (1) round-trip fidelity for scalars and (2) byte
// order == N1QL collation order for scalars (so a bbolt Seek/Next walks in order).
// These tests assert both directly, plus self-delimiting docID recovery and
// malformed-input safety -- sweeping the encode/decode edge branches (containers,
// escaped 0x00, truncated/garbage prefixes) the coverage audit flagged as thin.

// TestSIEncodeRoundTripScalars: decode(encode(v)) collates equal to v, and decode
// consumes exactly the encoded length (self-delimiting).
func TestSIEncodeRoundTripScalars(t *testing.T) {
	for _, v := range []value.Value{
		value.MISSING_VALUE, value.NULL_VALUE, value.FALSE_VALUE, value.TRUE_VALUE,
		value.NewValue(int64(0)), value.NewValue(int64(-7)), value.NewValue(int64(42)),
		value.NewValue(3.14), value.NewValue(-2.5), value.NewValue(1e300), value.NewValue(-1e-300),
		value.NewValue(""), value.NewValue("a"), value.NewValue("abc"),
		value.NewValue("with\x00nul"), // exercises the 0x00 escape/unescape
		value.NewValue("emoji-✓"),
	} {
		enc := encodeValue(nil, v)
		got, n, ok := decodeValue(enc)
		if !ok {
			t.Errorf("%v: decode ok=false", v.Actual())
			continue
		}
		if n != len(enc) {
			t.Errorf("%v: consumed n=%d, want %d", v.Actual(), n, len(enc))
		}
		if got.Collate(v) != 0 {
			t.Errorf("round-trip %v -> %v", v.Actual(), got.Actual())
		}
	}
}

// TestSIEncodeOrderPreserving: encoded bytes are STRICTLY increasing in N1QL
// collation order -- cross-type (MISSING<NULL<FALSE<TRUE<number<string) and
// within-type (numeric magnitude; string lexicographic incl. the prefix rule).
func TestSIEncodeOrderPreserving(t *testing.T) {
	ordered := []value.Value{
		value.MISSING_VALUE, value.NULL_VALUE, value.FALSE_VALUE, value.TRUE_VALUE,
		value.NewValue(-1e10), value.NewValue(-1.5), value.NewValue(int64(0)),
		value.NewValue(int64(1)), value.NewValue(2.5), value.NewValue(1e10),
		value.NewValue(""), value.NewValue("a"), value.NewValue("ab"), value.NewValue("b"),
	}
	for i := 1; i < len(ordered); i++ {
		a := encodeValue(nil, ordered[i-1])
		b := encodeValue(nil, ordered[i])
		if bytes.Compare(a, b) >= 0 {
			t.Errorf("order broken: %v (%x) not < %v (%x)",
				ordered[i-1].Actual(), a, ordered[i].Actual(), b)
		}
	}
}

// TestSIEncodeSelfDelimiting: an encoded value followed by raw docID bytes decodes
// back to the value AND leaves the docID suffix intact; encodeSeq + splitKey +
// decodeKeyComponents recover a multi-component composite key.
func TestSIEncodeSelfDelimiting(t *testing.T) {
	docID := []byte("doc::abc\x00def") // includes a 0x00 to ensure no separator confusion
	for _, v := range []value.Value{
		value.NewValue(int64(5)), value.NewValue("k\x00ey"), value.NULL_VALUE,
	} {
		full := append(encodeValue(nil, v), docID...)
		got, n, ok := decodeValue(full)
		if !ok || got.Collate(v) != 0 {
			t.Errorf("%v: decode ok=%v got=%v", v.Actual(), ok, got)
		}
		if !bytes.Equal(full[n:], docID) {
			t.Errorf("%v: docID suffix = %q, want %q", v.Actual(), full[n:], docID)
		}
	}

	if encodeSeq(nil) != nil {
		t.Error("encodeSeq(nil) should be nil (no bound values)")
	}
	comp := encodeSeq(value.Values{value.NewValue("node1"), value.NewValue(int64(99))})
	key := append(append([]byte{}, comp...), docID...)
	ends, gotDoc, ok := splitKey(key, 2)
	if !ok || len(ends) != 2 {
		t.Fatalf("splitKey ok=%v ends=%v", ok, ends)
	}
	if !bytes.Equal(gotDoc, docID) {
		t.Errorf("splitKey docID = %q, want %q", gotDoc, docID)
	}
	keys := decodeKeyComponents(key, ends)
	if len(keys) != 2 || keys[0].Collate(value.NewValue("node1")) != 0 ||
		keys[1].Collate(value.NewValue(int64(99))) != 0 {
		t.Errorf("decodeKeyComponents = %v", keys)
	}
}

// TestSIEncodeContainers: arrays/objects are stored (index completeness) via
// canonicalBytes and stay self-delimiting so the docID is still recoverable, even
// though their intra-type byte order is not collation order (a documented v1 caveat).
func TestSIEncodeContainers(t *testing.T) {
	docID := []byte("d1")
	for _, v := range []value.Value{
		value.NewValue([]interface{}{int64(1), "x"}),          // array -> MarshalJSON
		value.NewValue(map[string]interface{}{"a": int64(1)}), // object -> MarshalJSON
		value.NewBinaryValue([]byte("rawbytes")),              // Actual []byte -> raw path
	} {
		full := append(encodeValue(nil, v), docID...)
		_, n, ok := decodeValue(full)
		if !ok {
			t.Errorf("%v: decode ok=false", v.Type())
			continue
		}
		if !bytes.Equal(full[n:], docID) {
			t.Errorf("%v: docID suffix lost: %q", v.Type(), full[n:])
		}
	}
}

// TestSIDecodeMalformed: a garbage/truncated prefix returns ok=false and never
// panics (defensive decoding), including the string-escape error branches.
func TestSIDecodeMalformed(t *testing.T) {
	for _, b := range [][]byte{
		{},                            // empty
		{0x7f},                        // unknown type tag
		{tagNumber, 1, 2, 3},          // number needs 8 payload bytes
		{tagString, 0x00},             // 0x00 at end, no follow byte
		{tagString, 'a', 0x00, 0x42},  // bad escape (0x00 then neither 0x00 nor 0xFF)
		{tagString, 'a'},              // unterminated string (no 0x00 0x00)
	} {
		if _, _, ok := decodeValue(b); ok {
			t.Errorf("decodeValue(%x) ok=true, want false", b)
		}
	}
	if _, _, ok := splitKey([]byte{tagNumber, 1}, 1); ok {
		t.Error("splitKey on a truncated component: ok=true, want false")
	}
}

// TestSIToFloat64: number representations unwrap to float64; a non-number yields 0.
func TestSIToFloat64(t *testing.T) {
	if got := toFloat64(value.NewValue(int64(7))); got != 7 {
		t.Errorf("int64 7 -> %v", got)
	}
	if got := toFloat64(value.NewValue(3.5)); got != 3.5 {
		t.Errorf("float 3.5 -> %v", got)
	}
	if got := toFloat64(value.NewValue("not-a-number")); got != 0 {
		t.Errorf("non-number -> %v, want 0", got)
	}
}
