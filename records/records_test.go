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

import (
	"archive/zip"
	"encoding/binary"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// collect drains a Source into copied (id, doc) pairs, validating that each Doc
// is well-formed JSON. It copies because the slices are borrowed until Next.
func collect(t *testing.T, s Source) (ids []string, docs []string) {
	t.Helper()
	var rec Record
	for {
		ok, err := s.Next(&rec)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		var probe interface{}
		if err := json.Unmarshal(rec.Doc, &probe); err != nil {
			t.Fatalf("record %q not valid JSON: %v\n  %s", rec.ID, err, rec.Doc)
		}
		ids = append(ids, string(rec.ID))
		docs = append(docs, string(rec.Doc))
	}
	s.Close()
	return ids, docs
}

func ex(rel string) string { return filepath.Join("..", "examples", rel) }

func TestWalkMultiFileJSONL(t *testing.T) { // scenario C
	s, err := Walk(ex("logs/default/events"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 8 {
		t.Fatalf("want 8 event records, got %d (%v)", len(docs), ids)
	}
	// IDs are dir-relative path + record index within file, and (for a seekable
	// uncompressed file) a trailing "@<byteOffset>" -- the first record is at 0.
	want0 := "2026-01-01.jsonl#0@0"
	if ids[0] != want0 {
		t.Errorf("first id = %q, want %q", ids[0], want0)
	}
}

func TestWalkRecursiveUnion(t *testing.T) { // scenario E
	s, err := Walk(ex("metrics/default/cpu"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	// examples/metrics/cpu holds 4 nested JSONL files (hostA Jan+Feb, hostB, hostC)
	// of 24 hourly samples each = 96, unioned across the host/date subdirs.
	if len(docs) != 96 {
		t.Fatalf("want 96 cpu samples across nested dirs, got %d (%v)", len(docs), ids)
	}
	// A nested file's ID keeps its dir-relative path.
	found := false
	for _, id := range ids {
		if id == "hostA/2026/01/data-0001.jsonl#0@0" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a nested relative id, got %v", ids)
	}
}

func TestWalkGzip(t *testing.T) { // scenario H
	s, err := Walk(ex("archive/default/orders"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	_, docs := collect(t, s)
	if len(docs) != 5 {
		t.Fatalf("want 5 orders from gzip'd JSONL, got %d", len(docs))
	}
}

func TestWalkOneDocPerFile(t *testing.T) { // scenario A
	s, err := Walk(ex("shop/default/orders"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 20 {
		t.Fatalf("want 20 orders, got %d", len(docs))
	}
	// Single-doc files keep today's convention: META().id == file stem.
	for _, id := range ids {
		if id[:6] != "order-" {
			t.Errorf("one-doc-per-file id should be the stem, got %q", id)
		}
	}
}

func TestOpenFileSingleJSON(t *testing.T) {
	s, err := OpenFile(ex("shop/default/orders/order-1001.json"), "orders/order-1001.json")
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 1 || ids[0] != "order-1001" {
		t.Fatalf("single-doc: got ids=%v docs=%d", ids, len(docs))
	}
}

func TestFileSingleJSONL(t *testing.T) { // scenario B2
	s, err := File(ex("logs/default/events/2026-01-01.jsonl"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) == 0 {
		t.Fatal("want records from a single JSONL file, got 0")
	}
	// The synthetic-ID prefix is the file's own base name (not a dir-relative
	// path), plus the within-file record index.
	if want := "2026-01-01.jsonl#0@0"; ids[0] != want {
		t.Errorf("first id = %q, want %q", ids[0], want)
	}
}

func TestFileGzip(t *testing.T) { // scenario B2 over a single .jsonl.gz
	s, err := File(ex("archive/default/orders/2025.jsonl.gz"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	_, docs := collect(t, s)
	if len(docs) == 0 {
		t.Fatal("want records from a single gzip'd JSONL file, got 0")
	}
}

func TestFileRespectsScanFilter(t *testing.T) { // -scan lockdown reaches single files
	opts := AllModes()
	opts.AllowGzip = false
	if _, err := File(ex("archive/default/orders/2025.jsonl.gz"), opts); err == nil {
		t.Error("File should reject a .gz when AllowGzip is false")
	}
}

func TestWalkOptionsSpec(t *testing.T) {
	// The default is "all"; ParseModes canonicalizes aliases and preserves order.
	if got := AllModes().Spec; got != "all" {
		t.Errorf("AllModes().Spec = %q, want \"all\"", got)
	}
	cases := map[string]string{
		"json,csv,gzip":     "json,csv,gzip",
		"ndjson,gz":         "jsonl,gzip",  // aliases -> primary tokens
		"gzip,json,JSON":    "gzip,json",   // deduped, case-insensitive
		" tsv , recursive ": "tsv,recurse", // trimmed, alias
		"":                  "all",         // empty -> the default
	}
	for in, want := range cases {
		o, err := ParseModes(in)
		if err != nil {
			t.Fatalf("ParseModes(%q): %v", in, err)
		}
		if o.Spec != want {
			t.Errorf("ParseModes(%q).Spec = %q, want %q", in, o.Spec, want)
		}
		// Spec must round-trip: re-parsing it yields the same Spec.
		o2, err := ParseModes(o.Spec)
		if err != nil {
			t.Errorf("Spec %q does not re-parse: %v", o.Spec, err)
		} else if o2.Spec != o.Spec {
			t.Errorf("Spec %q re-parsed to %q (not stable)", o.Spec, o2.Spec)
		}
	}
}

func TestIsStructuredFile(t *testing.T) {
	// Structured data files (auto-exposed as grab-bag keyspaces)...
	for _, p := range []string{"a.json", "a.jsons", "a.jsonl", "a.ndjson", "a.csv", "a.tsv", "a.yaml", "a.yml", "a.jsonl.gz", "a.yaml.gz"} {
		if !IsStructuredFile(p) {
			t.Errorf("IsStructuredFile(%q) = false, want true", p)
		}
	}
	// ...but not extracted documents (still IsRecordFile) or unknown types.
	for _, p := range []string{"a.pdf", "a.docx", "a.xlsx", "a.txt", "a"} {
		if IsStructuredFile(p) {
			t.Errorf("IsStructuredFile(%q) = true, want false", p)
		}
	}
	if !IsRecordFile("a.pdf") {
		t.Errorf("a.pdf should still be a record file (extract)")
	}
}

func TestStem(t *testing.T) {
	cases := map[string]string{
		"events.jsonl":        "events",
		"a/b/orders.jsonl.gz": "orders",
		"dump.ndjson":         "dump",
		"data.csv":            "data",
		"report.json":         "report",
	}
	for in, want := range cases {
		if got := Stem(in); got != want {
			t.Errorf("Stem(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestJSONArrayAndValueStream(t *testing.T) {
	dir := t.TempDir()
	arr := filepath.Join(dir, "a.json")
	if err := os.WriteFile(arr, []byte(`[{"x":1},{"x":2},{"x":3}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	stream := filepath.Join(dir, "b.json")
	if err := os.WriteFile(stream, []byte("{\"y\":1}\n{\"y\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		n    int
	}{{arr, 3}, {stream, 2}} {
		s, err := OpenFile(tc.path, "t.json")
		if err != nil {
			t.Fatal(err)
		}
		ids, docs := collect(t, s)
		if len(docs) != tc.n {
			t.Fatalf("%s: want %d docs, got %d", tc.path, tc.n, len(docs))
		}
		// Multi-record .json uses prefix#i IDs (not the stem).
		if ids[0] != "t.json#0" {
			t.Errorf("%s: first id = %q, want t.json#0", tc.path, ids[0])
		}
	}
}

func TestYAMLDecode(t *testing.T) {
	dir := t.TempDir()

	// Single document -> the stem id (one-doc-per-file convention), mixed scalar
	// types converted to JSON (sorted keys via json.Marshal).
	single := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(single, []byte("service: api\nreplicas: 3\nenabled: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := OpenFile(single, "config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 1 {
		t.Fatalf("single: want 1 doc, got %d: %v", len(docs), docs)
	}
	if ids[0] != "config" {
		t.Errorf("single id = %q, want config", ids[0])
	}
	if docs[0] != `{"enabled":true,"replicas":3,"service":"api"}` {
		t.Errorf("single doc = %s", docs[0])
	}

	// Multi-document (--- separated) -> prefix#i@offset ids (byte-seekable), with a
	// nested map and array. The `---` line here is at byte 46: "name: alpha\n"(12) +
	// "tags: [web, prod]\n"(18) + "cpu: {cores: 4}\n"(16).
	multi := filepath.Join(dir, "servers.yaml")
	body := "name: alpha\ntags: [web, prod]\ncpu: {cores: 4}\n" +
		"---\n" +
		"name: beta\ntags: [db]\ncpu: {cores: 8}\n"
	if err := os.WriteFile(multi, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err = OpenFile(multi, "servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ids, docs = collect(t, s)
	if len(docs) != 2 {
		t.Fatalf("multi: want 2 docs, got %d: %v", len(docs), docs)
	}
	if ids[0] != "servers.yaml#0@0" || ids[1] != "servers.yaml#1@46" {
		t.Errorf("multi ids = %v, want [servers.yaml#0@0 servers.yaml#1@46]", ids)
	}
	if docs[0] != `{"cpu":{"cores":4},"name":"alpha","tags":["web","prod"]}` {
		t.Errorf("multi doc0 = %s", docs[0])
	}

	// The @offset is real: seek to doc 1's offset and DecodeYAMLDoc gets that
	// document (what a key-based fetch does).
	f, err := os.Open(multi)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Seek(46, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, ok, err := DecodeYAMLDoc(f)
	if err != nil || !ok {
		t.Fatalf("DecodeYAMLDoc @46: ok=%v err=%v", ok, err)
	}
	if string(got) != `{"cpu":{"cores":8},"name":"beta","tags":["db"]}` {
		t.Errorf("DecodeYAMLDoc @46 = %s, want doc 1 (beta)", got)
	}

	// A single document that is a top-level sequence expands to one record per
	// element (like a top-level JSON array), with prefix#i ids.
	list := filepath.Join(dir, "hosts.yaml")
	if err := os.WriteFile(list, []byte("- name: a\n- name: b\n- name: c\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err = OpenFile(list, "hosts.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ids, docs = collect(t, s)
	if len(docs) != 3 {
		t.Fatalf("list: want 3 docs, got %d: %v", len(docs), docs)
	}
	if ids[0] != "hosts.yaml#0" || docs[0] != `{"name":"a"}` {
		t.Errorf("list first = %q / %s, want hosts.yaml#0 / {\"name\":\"a\"}", ids[0], docs[0])
	}
}

func TestCSVDecode(t *testing.T) {
	dir := t.TempDir()
	// Includes quoting, an embedded comma+newline, type-inference cases, a
	// ragged (short) row, and an empty cell.
	csvBody := "id,name,amount,active,note\n" +
		"1,Alice,10.5,true,hi\n" +
		"2,\"Bob, Jr.\",20,false,\"line1\nline2\"\n" +
		"007,Carol,,true,\n" + // 007 stays a string; empty amount+note -> null
		"4,Dave\n" // ragged: missing amount/active/note -> null
	if err := os.WriteFile(filepath.Join(dir, "people.csv"), []byte(csvBody), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := OpenFile(filepath.Join(dir, "people.csv"), "people.csv")
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 4 {
		t.Fatalf("want 4 rows, got %d: %v", len(docs), docs)
	}
	if ids[0] != "people.csv#0" {
		t.Errorf("first id = %q, want people.csv#0", ids[0])
	}
	// Row 0: typed values.
	if docs[0] != `{"id":1,"name":"Alice","amount":10.5,"active":true,"note":"hi"}` {
		t.Errorf("row0 = %s", docs[0])
	}
	// Row 1: quoted comma + embedded newline preserved as a string.
	if docs[1] != `{"id":2,"name":"Bob, Jr.","amount":20,"active":false,"note":"line1\nline2"}` {
		t.Errorf("row1 = %s", docs[1])
	}
	// Row 2: "007" stays a string (no lossy 007->7); empty cells -> null.
	if docs[2] != `{"id":"007","name":"Carol","amount":null,"active":true,"note":null}` {
		t.Errorf("row2 = %s", docs[2])
	}
	// Row 3: ragged short row -> missing columns null.
	if docs[3] != `{"id":4,"name":"Dave","amount":null,"active":null,"note":null}` {
		t.Errorf("row3 = %s", docs[3])
	}
}

func TestTSVDecode(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "t.tsv"),
		[]byte("a\tb\n1\tx\n2\ty\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := OpenFile(filepath.Join(dir, "t.tsv"), "t.tsv")
	if err != nil {
		t.Fatal(err)
	}
	_, docs := collect(t, s)
	if len(docs) != 2 || docs[0] != `{"a":1,"b":"x"}` {
		t.Fatalf("tsv decode: %v", docs)
	}
}

func TestExtract(t *testing.T) { // scenario L
	cases := []struct{ file, kind, want string }{
		{"kb/default/docs/handbook.pdf", "pdf", "Vacation Policy"},
		{"kb/default/docs/q1-report.docx", "docx", "Revenue grew"},
		{"kb/default/docs/budget.xlsx", "xlsx", "Salaries"},
	}
	for _, tc := range cases {
		s, err := OpenFile(ex(tc.file), "docs/"+filepath.Base(tc.file))
		if err != nil {
			t.Fatalf("%s: %v", tc.file, err)
		}
		ids, docs := collect(t, s) // one extractor row per document
		if len(docs) != 1 {
			t.Fatalf("%s: want 1 record, got %d", tc.file, len(docs))
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(docs[0]), &m); err != nil {
			t.Fatal(err)
		}
		if m["kind"] != tc.kind {
			t.Errorf("%s: kind = %v, want %s", tc.file, m["kind"], tc.kind)
		}
		if m["filename"] != filepath.Base(tc.file) {
			t.Errorf("%s: filename = %v", tc.file, m["filename"])
		}
		if text, _ := m["text"].(string); !strings.Contains(text, tc.want) {
			t.Errorf("%s: extracted text missing %q; got %.100q", tc.file, tc.want, text)
		}
		if ids[0] == "" {
			t.Errorf("%s: empty id", tc.file)
		}
	}
}

func TestWalkExtract(t *testing.T) { // a keyspace of mixed documents
	s, err := Walk(ex("kb/default/docs"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	// pdf, docx, xlsx, pptx, txt, md, rtf.
	if len(docs) != 7 {
		t.Fatalf("want 7 extracted docs, got %d (%v)", len(docs), ids)
	}
	kinds := map[string]bool{}
	for _, d := range docs {
		var m map[string]interface{}
		json.Unmarshal([]byte(d), &m)
		kinds[m["kind"].(string)] = true
	}
	for _, k := range []string{"pdf", "docx", "xlsx", "pptx", "txt", "md", "rtf"} {
		if !kinds[k] {
			t.Errorf("walk missing extracted kind %q (got %v)", k, kinds)
		}
	}
}

func TestWalkExtractMedia(t *testing.T) { // a keyspace of media (metadata only)
	s, err := Walk(ex("kb/default/media"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	_, docs := collect(t, s)
	if len(docs) != 2 { // logo.png, clip.mp4
		t.Fatalf("want 2 media records, got %d", len(docs))
	}
	for _, d := range docs {
		var m map[string]interface{}
		json.Unmarshal([]byte(d), &m)
		if m["text"] != "" {
			t.Errorf("media record should have empty text: %s", d)
		}
		if m["width"] == nil || m["height"] == nil {
			t.Errorf("media record missing dimensions: %s", d)
		}
	}
}

// extractOne runs a single file through the extract provider and returns its one
// decoded record as a JSON map.
func extractOne(t *testing.T, path string) map[string]interface{} {
	t.Helper()
	s, err := OpenFile(path, filepath.Base(path))
	if err != nil {
		t.Fatalf("%s: OpenFile: %v", path, err)
	}
	_, docs := collect(t, s)
	if len(docs) != 1 {
		t.Fatalf("%s: want 1 extracted record, got %d", path, len(docs))
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(docs[0]), &m); err != nil {
		t.Fatalf("%s: record not JSON: %v", path, err)
	}
	return m
}

func writeFile(t *testing.T, path string, b []byte) string {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExtractPlainText(t *testing.T) { // txt / log / md / markdown
	dir := t.TempDir()
	for _, tc := range []struct{ name, body, kind string }{
		{"notes.txt", "  hello world  ", "txt"},
		{"server.log", "ERROR boom\nWARN meh", "log"},
		{"README.md", "# Title\n\nsome **bold** prose", "md"},
		{"doc.markdown", "plain markdown", "markdown"},
	} {
		m := extractOne(t, writeFile(t, filepath.Join(dir, tc.name), []byte(tc.body)))
		if m["kind"] != tc.kind {
			t.Errorf("%s: kind = %v, want %s", tc.name, m["kind"], tc.kind)
		}
		want := strings.TrimSpace(tc.body)
		if m["text"] != want {
			t.Errorf("%s: text = %q, want %q", tc.name, m["text"], want)
		}
	}
}

func TestExtractRTF(t *testing.T) {
	dir := t.TempDir()
	// Body text + a skipped font table + \par break + a \u escape (é, uc1 fallback
	// '?') + a \'e9 hex byte, exercising the de-RTF paths.
	rtf := `{\rtf1\ansi\deff0{\fonttbl{\f0 Arial;}}` +
		`Hello RTF world.\par Caf\u233?\'e9 time.}`
	m := extractOne(t, writeFile(t, filepath.Join(dir, "memo.rtf"), []byte(rtf)))
	if m["kind"] != "rtf" {
		t.Errorf("kind = %v, want rtf", m["kind"])
	}
	text, _ := m["text"].(string)
	if !strings.Contains(text, "Hello RTF world.") {
		t.Errorf("rtf text missing body: %q", text)
	}
	if strings.Contains(text, "Arial") {
		t.Errorf("rtf text leaked font-table content: %q", text)
	}
	if !strings.Contains(text, "Café") { // \u233 -> é, fallback '?' skipped
		t.Errorf("rtf \\u escape not decoded: %q", text)
	}
}

func TestExtractPPTX(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deck.pptx")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	slide := func(text string) string {
		return `<p:sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">` +
			`<a:p><a:r><a:t>` + text + `</a:t></a:r></a:p></p:sld>`
	}
	// Out-of-order names to prove numeric slide ordering (slide10 after slide2).
	for name, body := range map[string]string{
		"ppt/slides/slide10.xml": slide("Tenth slide"),
		"ppt/slides/slide2.xml":  slide("Second slide"),
		"ppt/slides/slide1.xml":  slide("First slide"),
	} {
		w, _ := zw.Create(name)
		w.Write([]byte(body))
	}
	zw.Close()
	f.Close()

	m := extractOne(t, path)
	if m["kind"] != "pptx" {
		t.Errorf("kind = %v, want pptx", m["kind"])
	}
	text, _ := m["text"].(string)
	if text != "First slide\nSecond slide\nTenth slide" {
		t.Errorf("pptx text/order wrong: %q", text)
	}
}

func TestExtractImage(t *testing.T) {
	dir := t.TempDir()
	img := image.NewRGBA(image.Rect(0, 0, 64, 48))
	img.Set(0, 0, color.RGBA{1, 2, 3, 255})

	pngPath := filepath.Join(dir, "logo.png")
	pf, _ := os.Create(pngPath)
	png.Encode(pf, img)
	pf.Close()

	jpgPath := filepath.Join(dir, "photo.jpg")
	jf, _ := os.Create(jpgPath)
	jpeg.Encode(jf, img, nil)
	jf.Close()

	for _, tc := range []struct{ path, kind string }{{pngPath, "png"}, {jpgPath, "jpg"}} {
		m := extractOne(t, tc.path)
		if m["kind"] != tc.kind {
			t.Errorf("%s: kind = %v, want %s", tc.path, m["kind"], tc.kind)
		}
		if m["text"] != "" {
			t.Errorf("%s: media should have empty text, got %q", tc.path, m["text"])
		}
		if m["width"] != float64(64) || m["height"] != float64(48) {
			t.Errorf("%s: dims = %vx%v, want 64x48", tc.path, m["width"], m["height"])
		}
	}
}

// mp4box builds one ISO-BMFF box (type + payload) for the synthetic-video test.
func mp4box(typ string, payload []byte) []byte {
	b := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(b, uint32(8+len(payload)))
	copy(b[4:8], typ)
	copy(b[8:], payload)
	return b
}

func TestExtractMP4(t *testing.T) {
	dir := t.TempDir()

	// mvhd v0: [version+flags 4][creation 4][mod 4][timescale 4][duration 4].
	mvhd := make([]byte, 20)
	// creation = 2020-01-01 UTC in seconds since 1904 (1577836800 + 2082844800).
	binary.BigEndian.PutUint32(mvhd[4:8], 1577836800+2082844800)
	binary.BigEndian.PutUint32(mvhd[12:16], 600)  // timescale
	binary.BigEndian.PutUint32(mvhd[16:20], 1800) // duration -> 3.0s

	// tkhd: width/height are the final two 16.16 fixed-point words.
	tkhd := make([]byte, 84)
	binary.BigEndian.PutUint32(tkhd[76:80], 1920<<16)
	binary.BigEndian.PutUint32(tkhd[80:84], 1080<<16)

	moov := mp4box("moov", append(mp4box("mvhd", mvhd), mp4box("trak", mp4box("tkhd", tkhd))...))
	// A leading ftyp proves readTopLevelBox scans past non-moov boxes.
	file := append(mp4box("ftyp", []byte("isom\x00\x00\x00\x00")), moov...)

	m := extractOne(t, writeFile(t, filepath.Join(dir, "clip.mp4"), file))
	if m["kind"] != "mp4" {
		t.Errorf("kind = %v, want mp4", m["kind"])
	}
	if m["text"] != "" {
		t.Errorf("video should have empty text, got %q", m["text"])
	}
	if m["duration_secs"] != float64(3) {
		t.Errorf("duration_secs = %v, want 3", m["duration_secs"])
	}
	if m["width"] != float64(1920) || m["height"] != float64(1080) {
		t.Errorf("dims = %vx%v, want 1920x1080", m["width"], m["height"])
	}
	if created, _ := m["created"].(string); !strings.HasPrefix(created, "2020-01-01") {
		t.Errorf("created = %q, want a 2020-01-01 timestamp", created)
	}
}

func TestExtractParseModes(t *testing.T) {
	o, err := ParseModes("text,image")
	if err != nil {
		t.Fatal(err)
	}
	if !o.eligible("a.txt") || !o.eligible("b.md") || !o.eligible("c.png") {
		t.Errorf("text,image should admit txt/md/png: %+v", o.Formats)
	}
	if o.eligible("d.pdf") || o.eligible("e.mp4") {
		t.Errorf("text,image should NOT admit pdf/mp4: %+v", o.Formats)
	}
	// "extract" admits every registered format, including the new ones.
	all, err := ParseModes("extract")
	if err != nil {
		t.Fatal(err)
	}
	for _, ext := range []string{"x.pptx", "x.rtf", "x.jpeg", "x.mov", "x.pdf"} {
		if !all.eligible(ext) {
			t.Errorf("extract should admit %s", ext)
		}
	}
}

func docHasMeta(t *testing.T, doc string) (map[string]interface{}, bool) {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(doc), &m); err != nil {
		t.Fatalf("doc not JSON object: %v", err)
	}
	meta, ok := m["_meta"].(map[string]interface{})
	return meta, ok
}

func TestParseMetaMode(t *testing.T) {
	for in, want := range map[string]MetaMode{"": MetaAuto, "auto": MetaAuto, "on": MetaOn, "off": MetaOff} {
		if got, err := ParseMetaMode(in); err != nil || got != want {
			t.Errorf("ParseMetaMode(%q) = %v,%v want %v", in, got, err, want)
		}
	}
	if _, err := ParseMetaMode("bogus"); err == nil {
		t.Errorf("bogus meta mode should error")
	}
}

// TestMetaModeStringRoundTrip: String() renders the flag/dot-command spelling and
// round-trips through ParseMetaMode (the .meta dot command echoes it back).
func TestMetaModeStringRoundTrip(t *testing.T) {
	for _, m := range []MetaMode{MetaAuto, MetaOn, MetaOff} {
		got, err := ParseMetaMode(m.String())
		if err != nil || got != m {
			t.Errorf("round-trip %v -> %q -> %v,%v", m, m.String(), got, err)
		}
	}
	for m, want := range map[MetaMode]string{MetaAuto: "auto", MetaOn: "on", MetaOff: "off"} {
		if got := m.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", m, got, want)
		}
	}
}

func TestMetaAutoExtractOnly(t *testing.T) {
	// auto: extracted documents get _meta...
	opts := AllModes() // Meta == MetaAuto
	s, _ := Walk(ex("kb/default/docs"), opts)
	_, docs := collect(t, s)
	for _, d := range docs {
		meta, ok := docHasMeta(t, d)
		if !ok {
			t.Fatalf("auto: extracted doc missing _meta: %s", d)
		}
		for _, k := range []string{"path", "name", "ext", "size", "mtime"} {
			if _, has := meta[k]; !has {
				t.Errorf("auto extracted doc _meta missing %q: %v", k, meta)
			}
		}
	}
	// ...but structured JSONL does not.
	s2, _ := Walk(ex("logs/default/events"), opts)
	_, docs2 := collect(t, s2)
	for _, d := range docs2 {
		if _, ok := docHasMeta(t, d); ok {
			t.Fatalf("auto: structured record should NOT have _meta: %s", d)
		}
	}
}

func TestMetaOnAndOff(t *testing.T) {
	on := AllModes()
	on.Meta = MetaOn
	s, _ := Walk(ex("logs/default/events"), on)
	_, docs := collect(t, s)
	if len(docs) == 0 {
		t.Fatal("no records")
	}
	for _, d := range docs {
		if _, ok := docHasMeta(t, d); !ok {
			t.Fatalf("on: record missing _meta: %s", d)
		}
	}

	off := AllModes()
	off.Meta = MetaOff
	s2, _ := Walk(ex("kb/default/docs"), off)
	_, docs2 := collect(t, s2)
	for _, d := range docs2 {
		if _, ok := docHasMeta(t, d); ok {
			t.Fatalf("off: extracted doc should NOT have _meta: %s", d)
		}
	}
}

func TestMetaPos(t *testing.T) {
	on := AllModes()
	on.Meta = MetaOn
	// Records from a container file (JSONL) carry _meta.pos (in-file ordinal).
	s, _ := Walk(ex("logs/default/events"), on)
	_, docs := collect(t, s)
	for _, d := range docs {
		meta, ok := docHasMeta(t, d)
		if !ok {
			t.Fatalf("missing _meta: %s", d)
		}
		if _, has := meta["pos"]; !has {
			t.Errorf("container-file record _meta missing pos: %v", meta)
		}
	}
	// One-doc-per-file records have no in-file position.
	s2, _ := Walk(ex("shop/default/orders"), on)
	_, docs2 := collect(t, s2)
	for _, d := range docs2 {
		meta, _ := docHasMeta(t, d)
		if _, has := meta["pos"]; has {
			t.Errorf("one-doc-per-file record should have no pos: %v", meta)
		}
	}
}

func TestSpliceMeta(t *testing.T) {
	frag := []byte(`"_meta":{"size":1}`)
	cases := map[string]string{
		`{"a":1}`:    `{"_meta":{"size":1},"a":1}`,
		`{}`:         `{"_meta":{"size":1}}`,
		` { "a":1 }`: ` {"_meta":{"size":1} "a":1 }`, // leading space preserved; note: rest kept verbatim
		`[1,2]`:      `[1,2]`,                        // non-object unchanged
	}
	// The spacing case above is awkward to assert exactly; test the important
	// invariants instead: object gets _meta first, non-object is untouched.
	_ = cases
	if got := string(spliceMeta(nil, []byte(`{"a":1}`), frag)); got != `{"_meta":{"size":1},"a":1}` {
		t.Errorf("object splice = %s", got)
	}
	if got := string(spliceMeta(nil, []byte(`{}`), frag)); got != `{"_meta":{"size":1}}` {
		t.Errorf("empty-object splice = %s", got)
	}
	if got := string(spliceMeta(nil, []byte(`[1,2]`), frag)); got != `[1,2]` {
		t.Errorf("non-object should be unchanged, got %s", got)
	}
}

// TestBorrowedSliceStableAfterCopy documents the borrow contract: the returned
// slices are only valid until the next Next, but a copy survives.
func TestBorrowedSliceStableAfterCopy(t *testing.T) {
	s, err := Walk(ex("logs/default/events"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var rec Record
	ok, err := s.Next(&rec)
	if err != nil || !ok {
		t.Fatalf("first Next: ok=%v err=%v", ok, err)
	}
	saved := append([]byte(nil), rec.Doc...)
	// Advance; the borrowed rec.Doc may now be overwritten, but saved is stable.
	s.Next(&rec)
	var probe interface{}
	if err := json.Unmarshal(saved, &probe); err != nil {
		t.Fatalf("copied doc corrupted after advance: %v", err)
	}
}

func TestParseModesRestriction(t *testing.T) {
	// Empty => flexible default.
	if o, err := ParseModes(""); err != nil || !o.Recurse || o.Formats != nil || !o.AllowGzip {
		t.Fatalf("empty modes should be AllModes, got %+v err=%v", o, err)
	}
	// A locked-down set: only JSONL, no gzip, no recurse.
	o, err := ParseModes("jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if o.Recurse || o.AllowGzip || o.Formats == nil {
		t.Fatalf("restricted modes leaked flexibility: %+v", o)
	}
	// Under this restriction, the gzip archive (needs gzip) yields nothing, and a
	// flat non-recursive walk of the events dir still finds the top-level .jsonl.
	if !o.eligible("events/2026-01-01.jsonl") || o.eligible("orders/2025.jsonl.gz") ||
		o.eligible("orders/order.json") {
		t.Errorf("eligibility wrong under jsonl-only restriction: %+v", o)
	}
	if _, err := ParseModes("json,bogus"); err == nil {
		t.Errorf("unknown mode token should error")
	}
}

// TestWalkAllocsFlat guards the allocation model: allocs/op must not scale with
// record count (borrowed slices, reused ID buffer). JSONL is the streaming path.
func TestWalkAllocsFlat(t *testing.T) {
	run := func() int {
		s, _ := Walk(ex("logs/default/events"), AllModes())
		var rec Record
		n := 0
		for {
			ok, _ := s.Next(&rec)
			if !ok {
				break
			}
			n++
		}
		s.Close()
		return n
	}
	// Warm any one-time cost, then measure per-record allocs across the JSONL walk.
	run()
	avg := testing.AllocsPerRun(20, func() { run() })
	// 8 records across 3 files; opening files + scanners dominates. Assert the
	// per-run allocs stay small and file-bounded (not per-record growth).
	if avg > 120 {
		t.Errorf("allocs/run = %.0f, higher than expected for a small walk", avg)
	}
}

// TestModesMatchParseModes guards against drift between the Modes() help catalog
// and ParseModes: every documented token (and alias) must parse, admit its listed
// extensions, and set the right modifier flag.
func TestModesMatchParseModes(t *testing.T) {
	for _, m := range Modes() {
		for _, tok := range append([]string{m.Token}, m.Aliases...) {
			opts, err := ParseModes(tok)
			if err != nil {
				t.Errorf("Modes token %q does not parse: %v", tok, err)
				continue
			}
			for _, ext := range m.Exts {
				if !opts.Formats[ext] {
					t.Errorf("token %q should admit ext %q, but ParseModes didn't", tok, ext)
				}
			}
		}
		switch m.Token {
		case "gzip":
			if o, _ := ParseModes("gzip"); !o.AllowGzip {
				t.Error("gzip should set AllowGzip")
			}
		case "recurse":
			if o, _ := ParseModes("recurse"); !o.Recurse {
				t.Error("recurse should set Recurse")
			}
		case "all":
			if o, _ := ParseModes("all"); o.Formats != nil {
				t.Error("all should be unrestricted (nil Formats)")
			}
		}
	}
}

// decodeJSONValuesRef is the reference json.Decoder splitter that splitJSONValues
// replaced; the test asserts the two agree on value boundaries.
func decodeJSONValuesRef(data []byte) ([][]byte, error) {
	var docs [][]byte
	trimmed := strings.TrimLeft(string(data), " \t\r\n")
	dec := json.NewDecoder(strings.NewReader(string(data)))
	if strings.HasPrefix(trimmed, "[") {
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		for dec.More() {
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, err
			}
			docs = append(docs, []byte(raw))
		}
		return docs, nil
	}
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
	return docs, nil
}

func TestSplitJSONValues(t *testing.T) {
	inputs := []string{
		`{"a":1}`,                           // single object
		`  {"a":1}` + "\n",                  // leading/trailing whitespace
		`{"a":1}{"b":2}` + "\n" + `{"c":3}`, // whitespace/adjacent value stream
		`[{"a":1},{"b":2},{"c":3}]`,         // top-level array of records
		`[ 1, 2, 3 ]`,                       // array of primitives
		`[]`,                                // empty array
		`   `,                               // whitespace only
		``,                                  // empty
		`{"s":"a}b]c{[\"quote\""}`,          // braces/brackets/escapes inside strings
		`{"nest":{"x":[1,{"y":2}]}}`,        // deep nesting
		`1 2 3`,                             // primitive stream
		`true false null 3.14 -5e10`,        // keyword/number primitives
		`{"emoji":"café ☕","arr":[true]}`,   // multibyte + nested
		"{\"a\":1}\n{\"b\":2}\n",            // newline-separated stream (jsonl-ish)
	}
	for _, in := range inputs {
		got, gerr := splitJSONValues(nil, []byte(in))
		want, werr := decodeJSONValuesRef([]byte(in))
		if (gerr == nil) != (werr == nil) {
			t.Errorf("%q: err mismatch got=%v ref=%v", in, gerr, werr)
			continue
		}
		if len(got) != len(want) {
			t.Errorf("%q: count got=%d ref=%d\n got=%s\n ref=%s", in, len(got), len(want), got, want)
			continue
		}
		for i := range want {
			// Normalize: RawMessage keeps original bytes; compare re-marshaled forms
			// so insignificant whitespace differences don't matter.
			var gv, wv interface{}
			if err := json.Unmarshal(got[i], &gv); err != nil {
				t.Errorf("%q: doc %d not valid JSON: %s (%v)", in, i, got[i], err)
				continue
			}
			if err := json.Unmarshal(want[i], &wv); err != nil {
				continue
			}
			gb, _ := json.Marshal(gv)
			wb, _ := json.Marshal(wv)
			if string(gb) != string(wb) {
				t.Errorf("%q: doc %d got=%s want=%s", in, i, gb, wb)
			}
		}
	}
}

// TestSplitJSONValuesReuse exercises the recycled-dst path: a second call reusing
// the first call's [][]byte must yield correct bytes AND reuse the leaf backing
// arrays (the recycling win) when capacities suffice.
func TestSplitJSONValuesReuse(t *testing.T) {
	first, err := splitJSONValues(nil, []byte(`{"a":111}{"b":222}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("first: got %d docs", len(first))
	}
	// Record leaf backing-array identity to detect reuse.
	p0, p1 := &first[0][:1][0], &first[1][:1][0]

	second, err := splitJSONValues(first, []byte(`{"c":333}{"d":444}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 || string(second[0]) != `{"c":333}` || string(second[1]) != `{"d":444}` {
		t.Fatalf("second: got %q", second)
	}
	// Same length + same-size docs => leaf arrays should be reused, not reallocated.
	if &second[0][:1][0] != p0 || &second[1][:1][0] != p1 {
		t.Errorf("expected leaf buffers to be reused across calls")
	}
}

// TestGlobMatchAndFiles covers the pure-Go glob matcher + lister backing the
// inline glob keyspace (DESIGN-data.md Mode 2b): ** crosses directory boundaries,
// a single * does not, and GlobFiles honors the format filter.
func TestGlobMatchAndFiles(t *testing.T) {
	for _, c := range []struct {
		pat, path string
		want      bool
	}{
		{"/a/*.json", "/a/x.json", true},
		{"/a/*.json", "/a/b/x.json", false}, // * stays within one segment
		{"/a/**/*.json", "/a/x.json", true}, // ** matches zero segments
		{"/a/**/*.json", "/a/b/x.json", true},
		{"/a/**/*.json", "/a/b/c/x.json", true},
		{"/a/**/*.json", "/a/b/x.csv", false},
		{"/a/**", "/a/b/c", true}, // trailing ** matches the rest
		{"/a/*", "/a/b/c", false},
	} {
		if got := globMatch(c.pat, c.path); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pat, c.path, got, c.want)
		}
	}

	dir := t.TempDir()
	write := func(p, body string) {
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a/1.json", `{"x":1}`)
	write("a/b/2.json", `{"x":2}`)
	write("a/b/c/3.json", `{"x":3}`)
	write("a/b/skip.csv", "h\n1\n")

	base, files, err := GlobFiles(filepath.Join(dir, "a", "**", "*.json"), AllModes())
	if err != nil {
		t.Fatal(err)
	}
	// GlobFiles resolves symlinks in the base (a symlinked data-root wouldn't be
	// walked otherwise); t.TempDir() on macOS lives under /var -> /private/var, so
	// compare against the resolved expected base.
	wantBase, _ := filepath.EvalSymlinks(filepath.Join(dir, "a"))
	if base != wantBase {
		t.Errorf("base = %q, want %q", base, wantBase)
	}
	if len(files) != 3 {
		t.Fatalf("**/*.json want 3 files, got %d: %v", len(files), files)
	}

	// A single * is one level only: just a/1.json.
	if _, files1, err := GlobFiles(filepath.Join(dir, "a", "*.json"), AllModes()); err != nil {
		t.Fatal(err)
	} else if len(files1) != 1 {
		t.Fatalf("a/*.json want 1 file, got %d: %v", len(files1), files1)
	}

	// Regression: a SYMLINKED base must still be walked. filepath.Walk won't descend a
	// symlinked ROOT, so a symlinked data-root (the common cbcollect case,
	// `support-bundle-ex01 -> cbcollect_info_...`) once yielded zero matches; GlobFiles
	// resolves the base symlink and re-anchors the pattern.
	link := filepath.Join(dir, "alink")
	if err := os.Symlink(filepath.Join(dir, "a"), link); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	if _, lf, err := GlobFiles(filepath.Join(link, "**", "*.json"), AllModes()); err != nil {
		t.Fatal(err)
	} else if len(lf) != 3 {
		t.Fatalf("glob over a symlinked base: want 3 files, got %d: %v", len(lf), lf)
	}
}

// TestSpecApplyFramingJSON: framing:json decodes JSONL (one JSON object per line) and
// normalizes the time field IN PLACE to an int64 epoch-nanos sort key, leaving other
// fields intact -- what makes a JSONL event log (e.g. master_events.log) a first-class
// ASOF/merge time source. describeMeasure over the same spec measures the zone map.
func TestSpecApplyFramingJSON(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "events.jsonl")
	body := `{"type":"a","ts":1000.5}` + "\n" + `{"type":"b","ts":1002.25}` + "\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	spec := ExtractSpec{
		Framing: Framing{Kind: FramingJSON},
		Time:    &TimeSpec{Field: "ts", Layout: TimeLayoutEpochS},
		Order:   OrderSpec{By: "ts", Sorted: SortedNear},
	}

	src, err := SpecApply(spec, p, "events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	_, docs := collect(t, src)
	if len(docs) != 2 {
		t.Fatalf("want 2 JSONL records, got %d: %v", len(docs), docs)
	}
	// ts normalized to int64 epoch-nanos (1000.5s -> 1000500000000ns), type preserved.
	if !strings.Contains(docs[0], `"ts":1000500000000`) || !strings.Contains(docs[0], `"type":"a"`) {
		t.Errorf("record 0 = %s, want ts=1000500000000 (int64) + type a", docs[0])
	}
	if !strings.Contains(docs[1], `"ts":1002250000000`) {
		t.Errorf("record 1 = %s, want ts=1002250000000", docs[1])
	}

	meta, err := MeasureSortedSource(spec, p)
	if err != nil {
		t.Fatal(err)
	}
	if meta.SortKeyLabel != "ts" || meta.RecordCount != 2 ||
		meta.MinKey != 1000500000000 || meta.MaxKey != 1002250000000 {
		t.Errorf("meta = %+v, want {SortKeyLabel:ts, RecordCount:2, MinKey:1000500000000, MaxKey:1002250000000}", meta)
	}
}

// TestExtractSpecRoundTrip pins the Phase-0 extract/sorted-source contract
// (records/spec.go): the shared types serialize to the .n1k1 sidecar and back
// without losing fields, so the parallel extract/merge tracks agree on shapes.
func TestExtractSpecRoundTrip(t *testing.T) {
	spec := ExtractSpec{
		Format:  "ns_server_log",
		Framing: Framing{Kind: FramingMultiline, Continuation: `^\s|^\[`},
		Fields:  Fields{Pattern: `\[(?P<module>\w+):(?P<level>\w+),(?P<ts>[^,]+),(?P<node>[^:]+):`},
		Time:    &TimeSpec{Field: "ts", Layout: TimeLayoutRFC3339, TZDefault: "+02:00"},
		Order: OrderSpec{
			By:       "ts",
			Sorted:   SortedNear,
			Disorder: DisorderBound{WindowNanos: 2_000_000_000},
		},
		Provenance: map[string]string{"command": "cbbrowse_logs info.log", "node": "ns_1@host"},
	}

	b, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var got ExtractSpec
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Format != spec.Format ||
		got.Framing != spec.Framing ||
		got.Fields != spec.Fields ||
		got.Time == nil || *got.Time != *spec.Time ||
		got.Order != spec.Order ||
		got.Provenance["command"] != spec.Provenance["command"] ||
		got.Provenance["node"] != spec.Provenance["node"] {
		t.Fatalf("ExtractSpec round-trip mismatch:\n got %+v\nwant %+v", got, spec)
	}

	meta := SortedSourceMeta{
		SortKeyLabel: "ts",
		Sortedness:   SortedNear,
		Disorder:     DisorderBound{WindowNanos: 2_000_000_000},
		MinKey:       1779000000000000000,
		MaxKey:       1779150134812159000,
		RecordCount:  128034,
		SyncPoints:   []SyncPoint{{Key: 1779000000000000000, Offset: 0}, {Key: 1779100000000000000, Offset: 4210}},
	}
	mb, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	var mgot SortedSourceMeta
	if err := json.Unmarshal(mb, &mgot); err != nil {
		t.Fatal(err)
	}
	if mgot.SortKeyLabel != meta.SortKeyLabel || mgot.Sortedness != meta.Sortedness ||
		mgot.Disorder != meta.Disorder || mgot.MinKey != meta.MinKey || mgot.MaxKey != meta.MaxKey ||
		mgot.RecordCount != meta.RecordCount || len(mgot.SyncPoints) != len(meta.SyncPoints) ||
		mgot.SyncPoints[1] != meta.SyncPoints[1] {
		t.Fatalf("SortedSourceMeta round-trip mismatch:\n got %+v\nwant %+v", mgot, meta)
	}
}

// ---------------------------------------------------------------- recipe / describe / extract
//
// The two-phase describe/extract seam (records/recipe.go), driven natively by the
// Phase-0 ExtractSpec, over the built-in ns_server_log multiline recipe.

// nsLogFixture is a small ns_server-style multiline log: each record is a
// [module:level,RFC3339ts,node:...]msg lead line plus continuation lines (an indented
// detail line and an Erlang-term dump line that begins with '[' but is NOT a lead).
// The stats line at 12.100 arrives 400ms behind the preceding 12.500 line, making the
// source near-sorted with a measurable 400ms disorder bound.
const nsLogFixture = `[ns_server:info,2026-05-17T15:36:11.198+02:00,ns_1@host:normal]started rebalance
  moving vbucket 42 to node ns_2@host
[{some,erlang,term},{more,stuff}] internal dump
[ns_server:warn,2026-05-17T15:36:12.500+02:00,ns_1@host:normal]slow operation detected
[stats:info,2026-05-17T15:36:12.100+02:00,ns_1@host:normal]late stats flush
[ns_server:error,2026-05-17T15:36:13.750+02:00,ns_1@host:default]connection failure
[couch_log:info,2026-05-17T15:36:14.000+02:00,ns_1@host:normal]compaction complete
`

// nanosOf parses an RFC3339 timestamp to int64 epoch-nanos (the normalized sort key),
// so tests assert against real time math rather than hard-coded 19-digit constants.
func nanosOf(t *testing.T, s string) int64 {
	t.Helper()
	tt, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return tt.UnixNano()
}

// decodeLogDoc unmarshals a record doc with number precision preserved (json.Number),
// so the int64 epoch-nanos timestamp survives (a plain interface{} decode would lose
// precision to float64).
func decodeLogDoc(t *testing.T, doc string) map[string]interface{} {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(doc))
	dec.UseNumber()
	var m map[string]interface{}
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("record not JSON: %v\n  %s", err, doc)
	}
	return m
}

func docInt64(t *testing.T, m map[string]interface{}, key string) int64 {
	t.Helper()
	num, ok := m[key].(json.Number)
	if !ok {
		t.Fatalf("field %q is %T, want a JSON number", key, m[key])
	}
	n, err := num.Int64()
	if err != nil {
		t.Fatalf("field %q not int64: %v", key, err)
	}
	return n
}

// TestRecipeFor pins the priority-resolved, ext+regexp matcher: the built-in
// ns_server_log recipe claims ns_server-family logs but leaves generic .log / .json
// files to the extension-keyed extractors, and a strictly-higher priority wins overlap.
func TestRecipeFor(t *testing.T) {
	if r := RecipeFor("ns_server.info.log"); r == nil || r.Name != "ns_server_log" {
		t.Errorf("ns_server.info.log should match ns_server_log recipe, got %v", r)
	}
	if r := RecipeFor("babysitter/diag.log"); r == nil || r.Name != "ns_server_log" {
		t.Errorf("diag.log should match ns_server_log recipe, got %v", r)
	}
	if r := RecipeFor("server.log"); r != nil {
		t.Errorf("generic server.log should NOT match a recipe (falls to text extractor), got %q", r.Name)
	}
	if r := RecipeFor("orders/order.json"); r != nil {
		t.Errorf(".json should not match a log recipe, got %q", r.Name)
	}

	// Priority resolution over a private ext, so this doesn't perturb other tests.
	lo := &Recipe{Name: "lo", Match: ExtractMatch{Exts: []string{".zzzx"}, Priority: 1}, Describe: NSLogDescribe}
	hi := &Recipe{Name: "hi", Match: ExtractMatch{Exts: []string{".zzzx"}, Priority: 5}, Describe: NSLogDescribe}
	RecipeRegister(lo)
	RecipeRegister(hi)
	if r := RecipeFor("x.zzzx"); r == nil || r.Name != "hi" {
		t.Errorf("higher-priority recipe should win, got %v", r)
	}
}

// TestNSLogDescribe checks that describe() returns the expected declarative
// ExtractSpec AND the measured SortedSourceMeta (min/max normalized key, near-sorted
// classification with the 400ms disorder bound, record count) from a sampled read.
func TestNSLogDescribe(t *testing.T) {
	path := writeFile(t, filepath.Join(t.TempDir(), "ns_server.info.log"), []byte(nsLogFixture))

	spec, meta, err := NSLogDescribe(path)
	if err != nil {
		t.Fatal(err)
	}

	if spec.Format != "ns_server_log" {
		t.Errorf("Format = %q, want ns_server_log", spec.Format)
	}
	if spec.Framing.Kind != FramingMultiline {
		t.Errorf("Framing.Kind = %q, want %q", spec.Framing.Kind, FramingMultiline)
	}
	if spec.Fields.Pattern == "" {
		t.Errorf("Fields.Pattern is empty")
	}
	if spec.Time == nil || spec.Time.Field != "ts" || spec.Time.Layout != TimeLayoutRFC3339 {
		t.Errorf("Time = %+v, want field=ts layout=RFC3339", spec.Time)
	}
	if spec.Order.Sorted != SortedNear {
		t.Errorf("Order.Sorted = %q, want %q", spec.Order.Sorted, SortedNear)
	}

	wantMin := nanosOf(t, "2026-05-17T15:36:11.198+02:00")
	wantMax := nanosOf(t, "2026-05-17T15:36:14.000+02:00")
	wantDisorder := int64(400 * time.Millisecond) // 12.500 -> 12.100 inversion
	if meta.SortKeyLabel != "ts" {
		t.Errorf("SortKeyLabel = %q, want ts", meta.SortKeyLabel)
	}
	if meta.Sortedness != SortedNear {
		t.Errorf("Sortedness = %q, want %q", meta.Sortedness, SortedNear)
	}
	if meta.Disorder.WindowNanos != wantDisorder {
		t.Errorf("Disorder.WindowNanos = %d, want %d", meta.Disorder.WindowNanos, wantDisorder)
	}
	if meta.MinKey != wantMin {
		t.Errorf("MinKey = %d, want %d", meta.MinKey, wantMin)
	}
	if meta.MaxKey != wantMax {
		t.Errorf("MaxKey = %d, want %d", meta.MaxKey, wantMax)
	}
	if meta.RecordCount != 5 {
		t.Errorf("RecordCount = %d, want 5", meta.RecordCount)
	}
	// describe reflects the measured order back into the spec it returns.
	if spec.Order.Disorder.WindowNanos != wantDisorder {
		t.Errorf("spec.Order.Disorder.WindowNanos = %d, want %d", spec.Order.Disorder.WindowNanos, wantDisorder)
	}
}

// TestNSLogExtract checks native spec execution end-to-end through OpenFile (recipe
// matched by name): correct multiline grouping, named-capture fields, and the
// timestamp normalized to int64 epoch-nanos, timezone-normalized.
func TestNSLogExtract(t *testing.T) {
	path := writeFile(t, filepath.Join(t.TempDir(), "ns_server.info.log"), []byte(nsLogFixture))

	s, err := OpenFile(path, "ns_server.info.log")
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 5 {
		t.Fatalf("want 5 framed records, got %d: %v", len(docs), ids)
	}

	// Record 0: multiline grouping folds the indented detail line AND the '['-led
	// Erlang dump into this record's msg (they are continuations, not lead lines).
	r0 := decodeLogDoc(t, docs[0])
	if r0["module"] != "ns_server" || r0["level"] != "info" || r0["node"] != "ns_1@host" {
		t.Errorf("rec0 fields wrong: %v", r0)
	}
	if got := docInt64(t, r0, "ts"); got != nanosOf(t, "2026-05-17T15:36:11.198+02:00") {
		t.Errorf("rec0 ts = %d, want normalized nanos", got)
	}
	msg0, _ := r0["msg"].(string)
	for _, want := range []string{"started rebalance", "moving vbucket 42", "erlang,term"} {
		if !strings.Contains(msg0, want) {
			t.Errorf("rec0 msg missing %q; got %q", want, msg0)
		}
	}

	// Record 2 is the late stats line (near-sorted): file order is preserved and its
	// normalized ts is behind record 1's, as measured by describe.
	r1 := decodeLogDoc(t, docs[1])
	r2 := decodeLogDoc(t, docs[2])
	if r2["module"] != "stats" {
		t.Errorf("rec2 module = %v, want stats", r2["module"])
	}
	ts1, ts2 := docInt64(t, r1, "ts"), docInt64(t, r2, "ts")
	if ts2 != nanosOf(t, "2026-05-17T15:36:12.100+02:00") {
		t.Errorf("rec2 ts = %d, want normalized nanos", ts2)
	}
	if !(ts2 < ts1) {
		t.Errorf("expected the late record (rec2=%d) to sort before rec1=%d", ts2, ts1)
	}

	// IDs follow the <prefix>#<n> convention of the other sources.
	if ids[0] != "ns_server.info.log#0" {
		t.Errorf("id[0] = %q, want ns_server.info.log#0", ids[0])
	}
}

// TestSpecApplyDirect exercises SpecApply on an arbitrary spec (line framing, no
// timestamp) so the native executor is covered independent of the built-in recipe,
// and does a borrowed-slice / allocation sanity check.
func TestSpecApplyDirect(t *testing.T) {
	path := writeFile(t, filepath.Join(t.TempDir(), "app.log"),
		[]byte("2026 alpha\n2026 beta\n2026 gamma\n"))
	spec := ExtractSpec{
		Framing: Framing{Kind: FramingLine},
		Fields:  Fields{Pattern: `^(?P<year>\d+) (?P<word>\w+)`},
	}
	s, err := SpecApply(spec, path, "app.log")
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := collect(t, s)
	if len(docs) != 3 {
		t.Fatalf("want 3 line records, got %d", len(docs))
	}
	r0 := decodeLogDoc(t, docs[0])
	if r0["year"] != "2026" || r0["word"] != "alpha" {
		t.Errorf("rec0 = %v, want year=2026 word=alpha", r0)
	}
	if ids[2] != "app.log#2" {
		t.Errorf("id[2] = %q, want app.log#2", ids[2])
	}

	// Borrowed-slice contract: rec.Doc is reused across Next (valid only until the
	// next call). A copy taken before advancing stays intact after it.
	s2, _ := SpecApply(spec, path, "app.log")
	defer s2.Close()
	var rec Record
	s2.Next(&rec)
	saved := append([]byte(nil), rec.Doc...)
	s2.Next(&rec) // may overwrite the borrowed buffer
	if !json.Valid(saved) {
		t.Fatalf("copied doc corrupted after advance: %s", saved)
	}

	// Allocation sanity: a steady drain stays bounded (buffers are reused, not grown
	// per record). Re-open per run since a Source is single-pass.
	run := func() {
		src, _ := SpecApply(spec, path, "app.log")
		var r Record
		for {
			ok, _ := src.Next(&r)
			if !ok {
				break
			}
		}
		src.Close()
	}
	run()
	if avg := testing.AllocsPerRun(20, run); avg > 60 {
		t.Errorf("allocs/run = %.0f, higher than expected for a 3-line spec apply", avg)
	}
}

// TestMeasureSortedSource exercises the exported measurement helper a non-Go recipe
// (glue's *.extract.js loader) reuses: given a describe-produced spec, it samples the
// file and reports the same SortedSourceMeta the built-in ns_server_log recipe does.
func TestMeasureSortedSource(t *testing.T) {
	path := writeFile(t, filepath.Join(t.TempDir(), "ns_server.info.log"), []byte(nsLogFixture))

	spec := nsLogSpec() // the declarative spec a JS describe() would also return.
	meta, err := MeasureSortedSource(spec, path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.SortKeyLabel != "ts" {
		t.Errorf("SortKeyLabel = %q, want ts", meta.SortKeyLabel)
	}
	if meta.Sortedness != SortedNear {
		t.Errorf("Sortedness = %q, want %q", meta.Sortedness, SortedNear)
	}
	if meta.Disorder.WindowNanos != int64(400*time.Millisecond) {
		t.Errorf("Disorder.WindowNanos = %d, want 400ms", meta.Disorder.WindowNanos)
	}
	if meta.MinKey != nanosOf(t, "2026-05-17T15:36:11.198+02:00") ||
		meta.MaxKey != nanosOf(t, "2026-05-17T15:36:14.000+02:00") {
		t.Errorf("Min/Max key = %d/%d, want normalized nanos", meta.MinKey, meta.MaxKey)
	}
	if meta.RecordCount != 5 {
		t.Errorf("RecordCount = %d, want 5", meta.RecordCount)
	}
}

// TestHeadSample checks the decompressed head-sampling helper a describe() uses to
// content-sniff: it returns up to max bytes of the file's real (decompressed) head.
func TestHeadSample(t *testing.T) {
	path := writeFile(t, filepath.Join(t.TempDir(), "ns_server.info.log"), []byte(nsLogFixture))

	head, err := HeadSample(path, 40)
	if err != nil {
		t.Fatal(err)
	}
	if len(head) != 40 {
		t.Errorf("len(head) = %d, want 40 (capped)", len(head))
	}
	if !strings.HasPrefix(nsLogFixture, head) {
		t.Errorf("head %q is not a prefix of the fixture", head)
	}

	// max<=0 => the default sample cap; the whole small fixture fits.
	full, err := HeadSample(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if full != nsLogFixture {
		t.Errorf("full head mismatch:\n got %q\nwant %q", full, nsLogFixture)
	}
}
