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

// The "extract" provider: text extraction from documents (DESIGN-data.md §4).
// Each document file (PDF/DOCX/XLSX) yields a single record { filename, kind,
// text }, so unstructured docs become queryable and full-text-searchable
// (SELECT filename FROM docs WHERE text LIKE '%x%').
//
// Pure-Go, stdlib-only (no third-party deps): DOCX/XLSX are ZIP+OOXML, decoded
// with archive/zip + encoding/xml; PDF text is pulled from content-stream show-
// text operators, inflating FlateDecode streams with compress/zlib. This is the
// deliberately-narrow "pure-Go default" of the design -- it extracts visible
// text from typical text documents, but not scanned/OCR PDFs or exotic font
// encodings (those want the optional Tika/extractous backend, a later build tag).

import (
	"archive/zip"
	"bytes"
	"compress/zlib"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// extractExts are the document extensions handled by the extract provider.
var extractExts = map[string]bool{".pdf": true, ".docx": true, ".xlsx": true}

func isExtractExt(ext string) bool { return extractExts[ext] }

// extractSource yields exactly one record (the extracted document) per file.
type extractSource struct {
	rec     Record
	emitted bool
}

func newExtractSource(path string) (*extractSource, error) {
	ext := strings.ToLower(filepath.Ext(path))
	kind := strings.TrimPrefix(ext, ".")
	var text string
	var err error
	switch ext {
	case ".docx":
		text, err = docxText(path)
	case ".xlsx":
		text, err = xlsxText(path)
	case ".pdf":
		text, err = pdfText(path)
	default:
		return nil, fmt.Errorf("records: unsupported document for extraction: %s", path)
	}
	if err != nil {
		return nil, err
	}
	doc, err := json.Marshal(map[string]interface{}{
		"filename": filepath.Base(path),
		"kind":     kind,
		"text":     text,
	})
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
