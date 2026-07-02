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

// The "extract" provider: content + metadata extraction from unstructured files
// (DESIGN-data.md §4). Each file yields a single record { filename, kind, text,
// <format-specific meta> }, so documents/media become queryable and full-text-
// searchable (SELECT filename FROM docs WHERE text LIKE '%x%').
//
// Pure-Go, no cgo, stdlib-preferred:
//   - Documents (text):  PDF/DOCX/XLSX/PPTX and TXT/LOG/MD/RTF -> extracted text.
//     OOXML is ZIP+XML (archive/zip + encoding/xml); PDF text comes from content-
//     stream show-text operators, inflating FlateDecode with compress/zlib; RTF
//     is de-controlled to plain text.
//   - Media (metadata only):  PNG/JPG/JPEG -> width/height (image.DecodeConfig);
//     MP4/MOV -> duration/width/height/created (a minimal ISO-BMFF box reader).
//     There is no `text` for media -- extracting text *from* an image or the
//     speech *in* a video is OCR/ASR, which is the deliberately-deferred optional
//     Tika/extractous+Tesseract backend (a later cgo build tag), not this path.
//
// This is the "pure-Go default" of the design: visible text from typical text
// documents (not scanned/OCR PDFs or exotic font encodings) plus lightweight
// media metadata. The Extractor/ExtractedDoc seam below is the reusable unit the
// records Source emits today and the bleve FTS indexer will consume later, so
// extraction logic lives here once rather than in each consumer.

import (
	"archive/zip"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"image"
	_ "image/jpeg" // register the JPEG decoder for image.DecodeConfig
	_ "image/png"  // register the PNG decoder for image.DecodeConfig
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ExtractedDoc is the result of cracking open one unstructured file: its full-
// text content (empty for media, which carry none without OCR/ASR) plus any
// format-specific metadata fields (width/height, duration_secs, created, ...).
// It is the reusable unit of the extract provider -- the records Source turns it
// into one JSON record per file, and the bleve FTS indexer will consume the same
// shape -- so extraction lives here once rather than in each consumer.
type ExtractedDoc struct {
	Kind string                 // format tag: the lower-cased extension sans dot ("pdf","txt","png")
	Text string                 // extracted full text; "" for formats that carry none (images/video)
	Meta map[string]interface{} // format-specific fields; nil when the format has none
}

// Extractor cracks a single file into an ExtractedDoc. Implementations are pure-
// Go (no cgo) and registered per extension in the extractors table. An Extractor
// fills Text and/or Meta; Extract sets Kind from the extension.
type Extractor func(path string) (ExtractedDoc, error)

// extractors maps a lower-cased extension to its handler. Adding a format is a
// one-line entry here plus its handler func -- IsRecordFile, the extract scan
// mode, and _meta injection all key off this table.
var extractors = map[string]Extractor{
	// Documents -> extracted text.
	".pdf":      pdfExtract,
	".docx":     docxExtract,
	".xlsx":     xlsxExtract,
	".pptx":     pptxExtract,
	".txt":      textExtract,
	".log":      textExtract,
	".md":       textExtract,
	".markdown": textExtract,
	".rtf":      rtfExtract,
	// Media -> metadata only (no text without OCR/ASR).
	".png":  imageExtract,
	".jpg":  imageExtract,
	".jpeg": imageExtract,
	".mp4":  mp4Extract,
	".mov":  mp4Extract,
}

func isExtractExt(ext string) bool { _, ok := extractors[ext]; return ok }

// Extract cracks path into an ExtractedDoc, choosing the handler by extension
// and stamping Kind. This is the entry point for both the records Source and
// (later) the FTS indexer.
func Extract(path string) (ExtractedDoc, error) {
	ext := strings.ToLower(filepath.Ext(path))
	fn := extractors[ext]
	if fn == nil {
		return ExtractedDoc{}, fmt.Errorf("records: unsupported document for extraction: %s", path)
	}
	ed, err := fn(path)
	if err != nil {
		return ExtractedDoc{}, err
	}
	ed.Kind = strings.TrimPrefix(ext, ".")
	return ed, nil
}

// extractSource yields exactly one record (the extracted document) per file.
type extractSource struct {
	rec     Record
	emitted bool
}

func newExtractSource(path string) (*extractSource, error) {
	ed, err := Extract(path)
	if err != nil {
		return nil, err
	}
	fields := map[string]interface{}{
		"filename": filepath.Base(path),
		"kind":     ed.Kind,
		"text":     ed.Text,
	}
	// Flatten format-specific metadata as top-level fields (so they're directly
	// queryable), guarding the three reserved keys above.
	for k, v := range ed.Meta {
		switch k {
		case "filename", "kind", "text":
		default:
			fields[k] = v
		}
	}
	doc, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	return &extractSource{rec: Record{ID: []byte(stem(path)), Doc: doc}}, nil
}

func (s *extractSource) Next(rec *Record) (bool, error) {
	if s.emitted {
		return false, nil
	}
	*rec = s.rec
	s.emitted = true
	return true, nil
}

func (s *extractSource) Close() error { return nil }

// ------------------------------------------------------------------ DOCX

func docxExtract(path string) (ExtractedDoc, error) {
	t, err := docxText(path)
	return ExtractedDoc{Text: t}, err
}

// docxText concatenates the text runs (<w:t>) of a .docx's word/document.xml,
// with a newline per paragraph (<w:p>).
func docxText(path string) (string, error) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return "", err
	}
	defer zr.Close()
	for _, f := range zr.File {
		if f.Name == "word/document.xml" {
			rc, err := f.Open()
			if err != nil {
				return "", err
			}
			defer rc.Close()
			return xmlRunText(rc, "t", "p")
		}
	}
	return "", nil
}

// xmlRunText streams XML, collecting CharData inside <textElem> and emitting a
// newline at each </breakElem>. Element names match by local part (namespace
// prefix ignored), which suits OOXML (w:t/w:p, a:t/a:p).
func xmlRunText(r io.Reader, textElem, breakElem string) (string, error) {
	dec := xml.NewDecoder(r)
	var sb strings.Builder
	depth := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if t.Name.Local == textElem {
				depth++
			}
		case xml.EndElement:
			if t.Name.Local == textElem && depth > 0 {
				depth--
			}
			if t.Name.Local == breakElem {
				sb.WriteByte('\n')
			}
		case xml.CharData:
			if depth > 0 {
				sb.Write(t)
			}
		}
	}
	return strings.TrimSpace(sb.String()), nil
}

// ------------------------------------------------------------------ PPTX

// pptxExtract concatenates the text of a .pptx's slides in slide order. Slides
// live at ppt/slides/slideN.xml and use DrawingML runs (<a:t>) / paragraphs
// (<a:p>) -- the same local-part names xmlRunText matches, so the OOXML machinery
// is shared with DOCX.
func pptxExtract(path string) (ExtractedDoc, error) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return ExtractedDoc{}, err
	}
	defer zr.Close()

	byName := map[string]*zip.File{}
	var slides []string
	for _, f := range zr.File {
		if strings.HasPrefix(f.Name, "ppt/slides/slide") && strings.HasSuffix(f.Name, ".xml") {
			byName[f.Name] = f
			slides = append(slides, f.Name)
		}
	}
	sortSlides(slides)

	var sb strings.Builder
	for _, name := range slides {
		rc, err := byName[name].Open()
		if err != nil {
			return ExtractedDoc{}, err
		}
		t, err := xmlRunText(rc, "t", "p")
		rc.Close()
		if err != nil {
			return ExtractedDoc{}, err
		}
		if t != "" {
			if sb.Len() > 0 {
				sb.WriteByte('\n')
			}
			sb.WriteString(t)
		}
	}
	return ExtractedDoc{Text: sb.String()}, nil
}

// sortSlides orders "ppt/slides/slideN.xml" names by their numeric N, so slide10
// follows slide9 (a lexical sort would put it after slide1).
func sortSlides(names []string) {
	sort.Slice(names, func(i, j int) bool {
		return slideNum(names[i]) < slideNum(names[j])
	})
}

func slideNum(name string) int {
	base := strings.TrimSuffix(filepath.Base(name), ".xml") // "slide12"
	base = strings.TrimPrefix(base, "slide")
	n, err := strconv.Atoi(base)
	if err != nil {
		return 1 << 30 // unknown -> sort last, stably
	}
	return n
}

// ------------------------------------------------------------------ TXT/MD/LOG

// textExtract reads a plain-text file (TXT/LOG/MD/MARKDOWN) verbatim as its text.
// Markdown is kept raw -- it's already human-readable and lossless for LIKE/FTS,
// and preserving the source avoids a lossy markdown->plain conversion.
func textExtract(path string) (ExtractedDoc, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return ExtractedDoc{}, err
	}
	return ExtractedDoc{Text: strings.TrimSpace(string(b))}, nil
}

// ------------------------------------------------------------------ RTF

// rtfIgnoreDest names RTF destination control words whose group content is
// bookkeeping (font/color/style tables, metadata, embedded pictures) rather than
// body text, so their groups are skipped wholesale.
var rtfIgnoreDest = map[string]bool{
	"fonttbl": true, "colortbl": true, "stylesheet": true, "info": true,
	"pict": true, "header": true, "footer": true, "headerl": true,
	"headerr": true, "footerl": true, "footerr": true, "footnote": true,
	"annotation": true, "generator": true, "themedata": true,
	"colorschememapping": true, "latentstyles": true, "datastore": true,
	"listtable": true, "listoverridetable": true, "rsidtbl": true,
	"xmlnstbl": true, "revtbl": true, "mmath": true,
}

func rtfExtract(path string) (ExtractedDoc, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ExtractedDoc{}, err
	}
	return ExtractedDoc{Text: rtfToText(data)}, nil
}

// rtfToText strips RTF control words/symbols and skips non-body destination
// groups, leaving the visible text. It honors the common formatting breaks
// (\par/\line/\tab), \'hh hex bytes, and \uN Unicode with \ucN fallback-skip.
// Best-effort (like the PDF path): fine for typical documents, not a full RTF
// reader for exotic files.
func rtfToText(data []byte) string {
	type frame struct {
		ignore bool
		uc     int
	}
	stack := []frame{}
	cur := frame{ignore: false, uc: 1}
	var sb strings.Builder
	skip := 0 // count of literal fallback chars to swallow after a \uN

	emit := func(b byte) {
		if cur.ignore {
			return
		}
		if skip > 0 {
			skip--
			return
		}
		sb.WriteByte(b)
	}

	n := len(data)
	for i := 0; i < n; {
		c := data[i]
		switch c {
		case '{':
			stack = append(stack, cur)
			skip = 0
			i++
		case '}':
			if len(stack) > 0 {
				cur = stack[len(stack)-1]
				stack = stack[:len(stack)-1]
			}
			skip = 0
			i++
		case '\r', '\n':
			i++ // RTF line breaks aren't literal text
		case '\\':
			i++
			if i >= n {
				break
			}
			ch := data[i]
			if !isRTFLetter(ch) { // control symbol
				switch ch {
				case '\\', '{', '}':
					emit(ch)
					i++
				case '\'': // \'hh hex byte
					if i+2 < n {
						if v, e := strconv.ParseUint(string(data[i+1:i+3]), 16, 8); e == nil {
							emit(byte(v))
						}
						i += 3
					} else {
						i++
					}
				case '*': // \* -> skip this destination group's content
					cur.ignore = true
					i++
				case '~': // non-breaking space
					emit(' ')
					i++
				default: // \-, \_, \| etc.: skip the symbol
					i++
				}
				continue
			}
			// control word: letters, optional signed number, optional space delim.
			j := i
			for j < n && isRTFLetter(data[j]) {
				j++
			}
			word := string(data[i:j])
			k := j
			if k < n && data[k] == '-' {
				k++
			}
			ns := k
			for k < n && data[k] >= '0' && data[k] <= '9' {
				k++
			}
			param, hasParam := 0, k > ns
			if hasParam {
				param, _ = strconv.Atoi(string(data[j:k]))
			}
			if k < n && data[k] == ' ' { // one trailing space is the delimiter
				k++
			}
			i = k
			switch word {
			case "par", "pard", "line", "sect", "row", "cell", "nestcell":
				if !cur.ignore {
					sb.WriteByte('\n')
				}
				skip = 0
			case "tab":
				if !cur.ignore {
					sb.WriteByte('\t')
				}
				skip = 0
			case "uc":
				if hasParam {
					cur.uc = param
				}
			case "u":
				if hasParam && !cur.ignore {
					if param < 0 {
						param += 65536
					}
					sb.WriteRune(rune(param))
				}
				skip = cur.uc // swallow the ANSI fallback that follows
			default:
				if rtfIgnoreDest[word] {
					cur.ignore = true
				}
			}
		default:
			emit(c)
			i++
		}
	}
	return strings.TrimSpace(collapseSpaces(sb.String()))
}

func isRTFLetter(b byte) bool { return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') }

// ------------------------------------------------------------------ Images

// imageExtract reads only the header of a PNG/JPEG (image.DecodeConfig) to record
// its pixel dimensions. Images carry no text without OCR (the deferred backend),
// so Text stays empty -- this is metadata only.
func imageExtract(path string) (ExtractedDoc, error) {
	f, err := os.Open(path)
	if err != nil {
		return ExtractedDoc{}, err
	}
	defer f.Close()
	cfg, _, err := image.DecodeConfig(f)
	if err != nil {
		return ExtractedDoc{}, err
	}
	return ExtractedDoc{Meta: map[string]interface{}{
		"width":  cfg.Width,
		"height": cfg.Height,
	}}, nil
}

// ------------------------------------------------------------------ MP4/MOV

// mp4Extract pulls container metadata (duration, dimensions, creation time) from
// an ISO base-media file (MP4/MOV) with a minimal box reader. Like images, video
// carries no text without ASR/OCR, so Text stays empty -- metadata only. It reads
// box headers via ReadAt and loads only the (small) moov box, never the whole
// file, so multi-gigabyte videos stay cheap.
func mp4Extract(path string) (ExtractedDoc, error) {
	f, err := os.Open(path)
	if err != nil {
		return ExtractedDoc{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return ExtractedDoc{}, err
	}
	meta := map[string]interface{}{}
	moov, err := readTopLevelBox(f, fi.Size(), "moov")
	if err != nil || moov == nil {
		return ExtractedDoc{Meta: meta}, nil // no moov (or unreadable): metadata-less
	}
	if mvhd := findChild(moov, "mvhd"); mvhd != nil {
		if secs, created, ok := parseMvhd(mvhd); ok {
			meta["duration_secs"] = math.Round(secs*1000) / 1000
			if !created.IsZero() {
				meta["created"] = created.UTC().Format(time.RFC3339)
			}
		}
	}
	for _, trak := range findChildren(moov, "trak") {
		tkhd := findChild(trak, "tkhd")
		if tkhd == nil {
			continue
		}
		if w, h, ok := parseTkhd(tkhd); ok && w > 0 && h > 0 {
			meta["width"], meta["height"] = w, h
			break // first video track with real dimensions
		}
	}
	return ExtractedDoc{Meta: meta}, nil
}

// mp4EpochOffset is seconds between the ISO-BMFF epoch (1904-01-01 UTC) and the
// Unix epoch (1970-01-01 UTC).
const mp4EpochOffset = 2082844800

// readTopLevelBox scans the top-level box sequence of an ISO-BMFF file and returns
// the payload (past the header) of the first box of type want, or nil if absent.
// It uses ReadAt so only headers and the wanted box are read.
func readTopLevelBox(f io.ReaderAt, size int64, want string) ([]byte, error) {
	hdr := make([]byte, 16)
	for pos := int64(0); pos+8 <= size; {
		if _, err := f.ReadAt(hdr[:8], pos); err != nil {
			return nil, err
		}
		boxLen := int64(binary.BigEndian.Uint32(hdr[:4]))
		typ := string(hdr[4:8])
		hdrLen := int64(8)
		switch boxLen {
		case 1: // 64-bit largesize follows the 8-byte header
			if _, err := f.ReadAt(hdr[8:16], pos+8); err != nil {
				return nil, err
			}
			boxLen = int64(binary.BigEndian.Uint64(hdr[8:16]))
			hdrLen = 16
		case 0: // extends to end of file
			boxLen = size - pos
		}
		if boxLen < hdrLen || pos+boxLen > size {
			break
		}
		if typ == want {
			buf := make([]byte, boxLen-hdrLen)
			if _, err := f.ReadAt(buf, pos+hdrLen); err != nil {
				return nil, err
			}
			return buf, nil
		}
		pos += boxLen
	}
	return nil, nil
}

// childBoxes splits a container box's payload into its direct child boxes.
type mp4Box struct {
	typ     string
	payload []byte
}

func childBoxes(payload []byte) []mp4Box {
	var out []mp4Box
	for i := 0; i+8 <= len(payload); {
		boxLen := int(binary.BigEndian.Uint32(payload[i:]))
		typ := string(payload[i+4 : i+8])
		hdrLen := 8
		switch boxLen {
		case 1:
			if i+16 > len(payload) {
				return out
			}
			boxLen = int(binary.BigEndian.Uint64(payload[i+8:]))
			hdrLen = 16
		case 0:
			boxLen = len(payload) - i
		}
		if boxLen < hdrLen || i+boxLen > len(payload) {
			return out
		}
		out = append(out, mp4Box{typ: typ, payload: payload[i+hdrLen : i+boxLen]})
		i += boxLen
	}
	return out
}

func findChild(payload []byte, typ string) []byte {
	for _, b := range childBoxes(payload) {
		if b.typ == typ {
			return b.payload
		}
	}
	return nil
}

func findChildren(payload []byte, typ string) [][]byte {
	var out [][]byte
	for _, b := range childBoxes(payload) {
		if b.typ == typ {
			out = append(out, b.payload)
		}
	}
	return out
}

// parseMvhd reads a Movie Header box: duration in seconds (duration/timescale)
// and creation time. Handles both version 0 (32-bit) and version 1 (64-bit).
func parseMvhd(p []byte) (secs float64, created time.Time, ok bool) {
	if len(p) < 4 {
		return
	}
	var createRaw, timescale, duration uint64
	if p[0] == 1 {
		if len(p) < 32 {
			return
		}
		createRaw = binary.BigEndian.Uint64(p[4:12])
		timescale = uint64(binary.BigEndian.Uint32(p[20:24]))
		duration = binary.BigEndian.Uint64(p[24:32])
	} else {
		if len(p) < 20 {
			return
		}
		createRaw = uint64(binary.BigEndian.Uint32(p[4:8]))
		timescale = uint64(binary.BigEndian.Uint32(p[12:16]))
		duration = uint64(binary.BigEndian.Uint32(p[16:20]))
	}
	if timescale == 0 {
		return
	}
	secs = float64(duration) / float64(timescale)
	if createRaw != 0 {
		created = time.Unix(int64(createRaw)-mp4EpochOffset, 0)
	}
	return secs, created, true
}

// parseTkhd reads a Track Header box's display width/height, which are the final
// two 16.16 fixed-point fields regardless of box version.
func parseTkhd(p []byte) (w, h int, ok bool) {
	if len(p) < 8 {
		return
	}
	wFixed := binary.BigEndian.Uint32(p[len(p)-8 : len(p)-4])
	hFixed := binary.BigEndian.Uint32(p[len(p)-4:])
	return int(wFixed >> 16), int(hFixed >> 16), true
}

// ------------------------------------------------------------------ XLSX

type xlsxSST struct {
	Si []struct {
		T string `xml:"t"`
		R []struct {
			T string `xml:"t"`
		} `xml:"r"`
	} `xml:"si"`
}

type xlsxSheet struct {
	Rows []struct {
		Cells []struct {
			T  string `xml:"t,attr"` // "s"(shared) | "inlineStr" | "str" | "b" | ""(number)
			V  string `xml:"v"`
			Is struct {
				T string `xml:"t"`
			} `xml:"is"`
		} `xml:"c"`
	} `xml:"sheetData>row"`
}

func xlsxExtract(path string) (ExtractedDoc, error) {
	t, err := xlsxText(path)
	return ExtractedDoc{Text: t}, err
}

// xlsxText renders a workbook's cells as text: one line per row, tab-separated
// cells, resolving shared strings, inline strings, and literal (number/bool)
// values.
func xlsxText(path string) (string, error) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return "", err
	}
	defer zr.Close()

	var shared []string
	for _, f := range zr.File {
		if f.Name == "xl/sharedStrings.xml" {
			var sst xlsxSST
			if err := unmarshalZip(f, &sst); err != nil {
				return "", err
			}
			for _, si := range sst.Si {
				s := si.T
				for _, r := range si.R {
					s += r.T
				}
				shared = append(shared, s)
			}
		}
	}

	var sb strings.Builder
	for _, f := range zr.File {
		if !strings.HasPrefix(f.Name, "xl/worksheets/sheet") || !strings.HasSuffix(f.Name, ".xml") {
			continue
		}
		var sh xlsxSheet
		if err := unmarshalZip(f, &sh); err != nil {
			return "", err
		}
		for _, row := range sh.Rows {
			for i, c := range row.Cells {
				if i > 0 {
					sb.WriteByte('\t')
				}
				switch c.T {
				case "s":
					if idx, e := strconv.Atoi(strings.TrimSpace(c.V)); e == nil && idx >= 0 && idx < len(shared) {
						sb.WriteString(shared[idx])
					}
				case "inlineStr":
					sb.WriteString(c.Is.T)
				default: // "str", "b", "" (number) -> literal value
					sb.WriteString(c.V)
				}
			}
			sb.WriteByte('\n')
		}
	}
	return strings.TrimSpace(sb.String()), nil
}

func unmarshalZip(f *zip.File, v interface{}) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	return xml.NewDecoder(rc).Decode(v)
}

// ------------------------------------------------------------------ PDF

func pdfExtract(path string) (ExtractedDoc, error) {
	t, err := pdfText(path)
	return ExtractedDoc{Text: t}, err
}

// pdfText extracts visible text from a PDF's content streams: it inflates each
// FlateDecode stream (falling back to raw), then pulls the strings shown by the
// text operators (the (...) literals used by Tj/TJ/'/"), joining them. Good for
// text-based PDFs with standard encodings; not for scanned/OCR or CID-font PDFs.
func pdfText(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	streamKW, endKW := []byte("stream"), []byte("endstream")
	for i := 0; ; {
		s := indexFrom(data, streamKW, i)
		if s < 0 {
			break
		}
		body := s + len(streamKW)
		// skip the EOL after "stream" (\r\n or \n)
		if body < len(data) && data[body] == '\r' {
			body++
		}
		if body < len(data) && data[body] == '\n' {
			body++
		}
		e := indexFrom(data, endKW, body)
		if e < 0 {
			break
		}
		raw := data[body:e]
		content := maybeInflate(raw)
		for _, str := range pdfShowStrings(content) {
			sb.WriteString(str)
			sb.WriteByte(' ')
		}
		i = e + len(endKW)
	}
	return strings.TrimSpace(collapseSpaces(sb.String())), nil
}

func indexFrom(b, sep []byte, from int) int {
	if from >= len(b) {
		return -1
	}
	j := bytes.Index(b[from:], sep)
	if j < 0 {
		return -1
	}
	return from + j
}

// maybeInflate zlib-decompresses b (FlateDecode); if that fails, b is returned
// as-is (an uncompressed content stream).
func maybeInflate(b []byte) []byte {
	zr, err := zlib.NewReader(bytes.NewReader(bytes.TrimSpace(b)))
	if err != nil {
		return b
	}
	defer zr.Close()
	out, err := io.ReadAll(zr)
	if err != nil || len(out) == 0 {
		return b
	}
	return out
}

// pdfShowStrings returns the decoded PDF literal strings "(...)" in content, in
// order. It respects escapes and balanced nested parens. This captures Tj/TJ/'/"
// text (the numbers in TJ kerning arrays are outside parens and ignored).
func pdfShowStrings(content []byte) []string {
	var out []string
	for i := 0; i < len(content); i++ {
		if content[i] != '(' {
			continue
		}
		var sb strings.Builder
		depth := 1
		i++
		for i < len(content) && depth > 0 {
			c := content[i]
			switch c {
			case '\\':
				i++
				if i >= len(content) {
					break
				}
				switch content[i] {
				case 'n':
					sb.WriteByte('\n')
				case 'r':
					sb.WriteByte('\r')
				case 't':
					sb.WriteByte('\t')
				case 'b':
					sb.WriteByte('\b')
				case 'f':
					sb.WriteByte('\f')
				case '(', ')', '\\':
					sb.WriteByte(content[i])
				case '\r', '\n': // line continuation: drop
				case '0', '1', '2', '3', '4', '5', '6', '7':
					oct := []byte{content[i]}
					for len(oct) < 3 && i+1 < len(content) && content[i+1] >= '0' && content[i+1] <= '7' {
						i++
						oct = append(oct, content[i])
					}
					if n, e := strconv.ParseUint(string(oct), 8, 16); e == nil {
						sb.WriteByte(byte(n))
					}
				default:
					sb.WriteByte(content[i])
				}
			case '(':
				depth++
				sb.WriteByte(c)
			case ')':
				depth--
				if depth > 0 {
					sb.WriteByte(c)
				}
			default:
				sb.WriteByte(c)
			}
			i++
		}
		i-- // for-loop will re-increment
		if s := sb.String(); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// collapseSpaces squeezes runs of spaces/tabs (but keeps newlines) so extracted
// text is tidy.
func collapseSpaces(s string) string {
	var sb strings.Builder
	prevSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' {
			if !prevSpace {
				sb.WriteByte(' ')
			}
			prevSpace = true
			continue
		}
		prevSpace = false
		sb.WriteRune(r)
	}
	return sb.String()
}
