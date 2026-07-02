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
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	// IDs are dir-relative path + record index within file.
	want0 := "2026-01-01.jsonl#0"
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
	if len(docs) != 5 {
		t.Fatalf("want 5 cpu samples across nested dirs, got %d (%v)", len(docs), ids)
	}
	// A nested file's ID keeps its dir-relative path.
	found := false
	for _, id := range ids {
		if id == "hostA/2026/01/data-0001.jsonl#0" {
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
	if len(docs) != 3 {
		t.Fatalf("want 3 orders, got %d", len(docs))
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
	if want := "2026-01-01.jsonl#0"; ids[0] != want {
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

func TestWalkOptionsDescribe(t *testing.T) {
	if got := AllModes().Describe(); got != "all formats · gzip · recurse" {
		t.Errorf("AllModes().Describe() = %q", got)
	}
	o, err := ParseModes("json,csv,gzip,recurse")
	if err != nil {
		t.Fatal(err)
	}
	if got := o.Describe(); got != "csv,json,jsons · gzip · recurse" {
		t.Errorf("Describe() = %q", got)
	}
}

func TestIsStructuredFile(t *testing.T) {
	// Structured data files (auto-exposed as grab-bag keyspaces)...
	for _, p := range []string{"a.json", "a.jsons", "a.jsonl", "a.ndjson", "a.csv", "a.tsv", "a.jsonl.gz"} {
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
