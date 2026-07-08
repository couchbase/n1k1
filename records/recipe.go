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

// The two-phase describe/extract seam of the extract provider (DESIGN-data.md §4
// "Two things an extractor produces"), driven natively by the Phase-0 ExtractSpec
// contract (records/spec.go):
//
//   - describe(path) -> (ExtractSpec, SortedSourceMeta): the cheap, once-per-file
//     pass. It may SAMPLE (head) rather than full-scan, and returns the declarative
//     spec n1k1 executes natively PLUS the measured sorted-source metadata (min/max
//     epoch-nanos key, sortedness, disorder bound, record count from the sample).
//     This is the pluggable seam where format-specific knowledge lives.
//   - SpecApply(spec, path, idPrefix) -> Source: the streaming, per-record pass. It
//     frames the file (framing -> fields regex -> timestamp normalized to int64
//     epoch-nanos, timezone-normalized) and yields one typed JSON Record per framed
//     record. NO per-row JS: the spec is applied by native Go on the byte lane.
//
// A Recipe binds the two behind an ExtractMatch (extension AND/OR regexp over the
// dataset-relative path, priority-resolved). This coexists with the extension-keyed
// `extractors` table in extract.go: the office/PDF/media extractors remain the
// built-in {framing: whole} baseline, while a Recipe describes/extracts a *regular*
// format (a timestamped multiline log) declaratively. RecipeRegister adds one;
// RecipeFor(relPath) resolves the highest-priority claimant.
//
// Allocation model (see records.go): SpecApply's Source honors the borrowed-slice
// contract -- each Next(*Record) fills rec.ID/rec.Doc from internal buffers that are
// reused across calls, so they are valid only until the next Next; callers copy to
// retain (see collect() in the tests).

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
)

// DescribeFunc is a recipe's cheap once-per-file pass: it samples path and returns
// the declarative ExtractSpec plus the measured SortedSourceMeta (DESIGN-data.md §4).
type DescribeFunc func(path string) (ExtractSpec, SortedSourceMeta, error)

// ExtractFunc is a recipe's OPTIONAL imperative escape hatch for formats too
// irregular for a declarative spec (DESIGN-data.md "Declarative spec vs imperative
// extract"). When nil, SpecApply runs the spec natively -- the preferred path. It is
// handed this file's earlier describe() spec (`meta`) rather than re-sniffing.
type ExtractFunc func(path, idPrefix string, spec ExtractSpec) (Source, error)

// Recipe is one extractor recipe: which files it claims (Match), how to describe a
// matched file, and (optionally) an imperative extract. The declarative core is
// {Match, Describe}; Extract is the irregular-format fallback.
type Recipe struct {
	Name     string       // recipe/format tag, e.g. "ns_server_log"
	Match    ExtractMatch // extension AND/OR path-regexp claim (priority-resolved)
	Describe DescribeFunc // required: sample -> ExtractSpec + SortedSourceMeta
	Extract  ExtractFunc  // optional: nil => SpecApply runs the spec natively
}

// recipes is the open, priority-resolved recipe registry -- the regexp-capable
// sibling of extract.go's extension-keyed `extractors` table. Entries are consulted
// in registration order, with a strictly-higher Priority winning on overlap (so a
// specific `ns_server\..*\.log` beats a generic `\.log$`, and equal priorities keep
// load order). Populated by RecipeRegister; the built-in log recipe registers in
// init() below.
var recipes []*Recipe

// RecipeRegister adds a recipe to the registry. Later registrations do NOT displace
// an equal-priority earlier one at match time (RecipeFor keeps load order on ties).
func RecipeRegister(r *Recipe) { recipes = append(recipes, r) }

// RecipeFor returns the highest-priority recipe claiming relPath (its dataset-
// relative path), or nil when none match. Extension and path-regexp are ANDed within
// a single ExtractMatch (an empty dimension is a wildcard; a match with neither Exts
// nor Names claims nothing). See ExtractMatch (records/spec.go).
func RecipeFor(relPath string) *Recipe {
	var best *Recipe
	for _, r := range recipes {
		if !matchClaims(r.Match, relPath) {
			continue
		}
		if best == nil || r.Match.Priority > best.Match.Priority {
			best = r
		}
	}
	return best
}

// matchClaims reports whether m claims relPath: (Exts empty || inner-ext in Exts)
// AND (Names empty || some Names regexp matches relPath). A match with neither Exts
// nor Names claims nothing (an all-wildcard recipe would shadow every file).
func matchClaims(m ExtractMatch, relPath string) bool {
	if len(m.Exts) == 0 && len(m.Names) == 0 {
		return false
	}
	if len(m.Exts) > 0 {
		ext := innerExt(relPath)
		hit := false
		for _, e := range m.Exts {
			if strings.EqualFold(e, ext) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	if len(m.Names) > 0 {
		hit := false
		for _, pat := range m.Names {
			if re, err := regexp.Compile(pat); err == nil && re.MatchString(relPath) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	return true
}

// DescribeMemo, when non-nil, wraps a recipe's per-file describe() pass with a
// caller-supplied memoization layer. glue installs one backed by the .n1k1 sidecar
// (DESIGN-data.md §4 "Describe once, reuse forever"; §5 per-file fingerprint), so a
// second open of an UNCHANGED file skips the expensive describe() and reads the
// cached ExtractSpec + SortedSourceMeta straight back -- once per file across queries
// AND processes. records itself stays pure-Go and sidecar-unaware: runDescribe calls
// through this seam when set, else runs describe() directly (the default for a bare
// `records` import or a records-package test). A changed file (fingerprint mismatch)
// re-describes only that file; that logic lives entirely on the glue side.
var DescribeMemo func(path string, describe DescribeFunc) (ExtractSpec, SortedSourceMeta, error)

// runDescribe applies a recipe's describe() to path, routing through DescribeMemo
// when a memoization layer is installed (the sidecar cache), else calling describe()
// directly. It returns the declarative spec (the scan path's input to SpecApply) and
// the measured SortedSourceMeta. This is the single seam OpenFile uses so the cache
// covers every recipe-matched open.
func runDescribe(rp *Recipe, path string) (ExtractSpec, SortedSourceMeta, error) {
	if DescribeMemo != nil {
		return DescribeMemo(path, rp.Describe)
	}
	return rp.Describe(path)
}

// MeasureSortedSource samples path through spec's native framing/time path and
// returns the SortedSourceMeta the K-way merge consumes (min/max epoch-nanos key,
// record count, sortedness + disorder bound). It is the exported wrapper around the
// same describeMeasure the built-in recipes use, so a NON-Go recipe author -- e.g.
// glue's *.extract.js loader, whose describe() runs in goja -- can reuse the exact
// native measurement rather than reimplementing it in JS. Cheap: it samples the
// file's head, not a full scan. Keeping this on the native side is what lets a JS
// recipe stay JS-only for describe() while per-row extract stays on the byte lane.
func MeasureSortedSource(spec ExtractSpec, path string) (SortedSourceMeta, error) {
	return describeMeasure(spec, path)
}

// HeadSample returns up to max bytes of path's DECOMPRESSED head as a string, for a
// describe() that content-sniffs (e.g. a *.extract.js recipe choosing a pattern by
// peeking at the first lines). It reuses SpecApply's transparent decompression so a
// .gz/.zst log samples its real text, not compressed bytes. Cheap and once-per-file
// (planning phase), so a string copy here is fine -- NOT the per-row hot path.
func HeadSample(path string, max int) (string, error) {
	r, closers, err := openDecompressed(path)
	if err != nil {
		return "", err
	}
	defer closeAll(closers)
	if max <= 0 {
		max = describeSampleBytes
	}
	buf := make([]byte, max)
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}
	return string(buf[:n]), nil
}

// -------------------------------------------------------------- native spec exec

// SpecApply returns a streaming Source that applies spec natively to path: it frames
// the file per spec.Framing, lifts named-capture fields per spec.Fields, and (when
// spec.Time is set) normalizes the timestamp field to an int64 epoch-nanos key,
// timezone-normalized, emitting one JSON Record per framed record. idPrefix prefixes
// the synthetic record IDs ("<prefix>#<n>"), matching the jsonl/csv sources.
//
// This is the hot per-record path and stays entirely in Go (byte-oriented regex +
// time parse, or jsonparser for json framing) -- no JS. Framing kinds handled:
// multiline, line, whole, json (JSONL: one JSON object per line, whose time field
// is normalized in place to the int64 sort key). Others (section) error here.
func SpecApply(spec ExtractSpec, path, idPrefix string) (Source, error) {
	r, closers, err := openDecompressed(path)
	if err != nil {
		return nil, err
	}
	s := &specSource{spec: spec, closers: closers, idPrefix: idPrefix}
	if err := s.compile(); err != nil {
		closeAll(closers)
		return nil, err
	}
	switch spec.Framing.Kind {
	case FramingWhole:
		all, err := io.ReadAll(r)
		if err != nil {
			closeAll(closers)
			return nil, err
		}
		s.whole = all
	case FramingMultiline, FramingLine, FramingJSON, "":
		// json framing is line-oriented too (one JSON object per line = one record),
		// so it shares the scanner path; only buildDoc/recordNanos differ (they read
		// the JSON directly instead of a field regex).
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		s.sc = sc
	default:
		closeAll(closers)
		return nil, fmt.Errorf("records: SpecApply: unsupported framing %q", spec.Framing.Kind)
	}
	return s, nil
}

// specSource is SpecApply's streaming Source. It ping-pongs two byte buffers -- cur
// (the record being assembled) and leadBuf (a one-line lookahead holding the next
// record's lead line) -- so multiline assembly streams with O(one record) memory.
// docBuf/idBuf are the borrowed output buffers, reused across Next (see records.go).
type specSource struct {
	spec     ExtractSpec
	closers  []io.Closer
	idPrefix string

	sc    *bufio.Scanner // line-based framing (multiline/line)
	whole []byte         // whole-file framing: the entire file as one record

	fieldRe *regexp.Regexp // spec.Fields.Pattern (named captures); nil if none
	tsIdx   int            // index of the timestamp subexp in fieldRe, or -1
	multi   bool           // multiline framing (else one record per line)
	json    bool           // json framing: each line is a JSON object (JSONL)

	cur      []byte // the record currently being assembled
	leadBuf  []byte // lookahead: the next record's lead line (when haveLead)
	haveLead bool   // leadBuf holds a pending lead line
	done     bool

	row    int
	docBuf []byte // borrowed record JSON (valid until next Next)
	idBuf  []byte // borrowed record ID   (valid until next Next)
}

// compile prepares the field regexp and locates the timestamp capture group.
func (s *specSource) compile() error {
	s.tsIdx = -1
	s.multi = s.spec.Framing.Kind == FramingMultiline
	s.json = s.spec.Framing.Kind == FramingJSON
	if pat := s.spec.Fields.Pattern; pat != "" {
		re, err := regexp.Compile(pat)
		if err != nil {
			return fmt.Errorf("records: SpecApply: bad fields pattern: %w", err)
		}
		s.fieldRe = re
		if s.spec.Time != nil {
			for i, name := range re.SubexpNames() {
				if name != "" && name == s.spec.Time.Field {
					s.tsIdx = i
					break
				}
			}
		}
	}
	return nil
}

func (s *specSource) Close() error { return closeAll(s.closers) }

func (s *specSource) Next(rec *Record) (bool, error) {
	if s.done {
		return false, nil
	}
	if s.spec.Framing.Kind == FramingWhole {
		s.done = true
		return s.emit(rec, s.whole)
	}
	rb, ok, err := s.frameNext()
	if err != nil || !ok {
		return false, err
	}
	return s.emit(rec, rb)
}

// frameNext assembles and returns the next complete record's bytes (borrowed from
// s.cur, valid until the next call). For multiline framing a record is a lead line
// plus following continuation lines; a line is a lead iff it matches the field
// pattern anchored at its start (robust even when a continuation itself begins with
// '[', unlike a bare continuation regexp). For line framing every line is a record.
func (s *specSource) frameNext() (rec []byte, ok bool, err error) {
	// Seed s.cur with this record's lead line.
	if s.haveLead {
		s.cur = append(s.cur[:0], s.leadBuf...)
		s.haveLead = false
	} else {
		l, got := s.scanNonEmpty()
		if !got {
			s.done = true
			return nil, false, s.sc.Err()
		}
		s.cur = append(s.cur[:0], l...)
	}
	if !s.multi {
		return s.cur, true, nil // line framing: one line == one record
	}
	// Multiline: fold following continuation lines until the next lead (or EOF).
	for {
		if !s.sc.Scan() {
			s.done = true
			return s.cur, true, s.sc.Err()
		}
		line := s.sc.Bytes()
		if s.isLead(line) {
			s.leadBuf = append(s.leadBuf[:0], line...)
			s.haveLead = true
			return s.cur, true, nil
		}
		s.cur = append(s.cur, '\n')
		s.cur = append(s.cur, line...)
	}
}

// scanNonEmpty returns the next non-blank line (borrowed from the scanner).
func (s *specSource) scanNonEmpty() ([]byte, bool) {
	for s.sc.Scan() {
		if len(strings.TrimSpace(string(s.sc.Bytes()))) > 0 {
			return s.sc.Bytes(), true
		}
	}
	return nil, false
}

// isLead reports whether line begins a new record: its field pattern matches
// anchored at offset 0. With no field pattern every non-blank line is a lead
// (degenerating multiline to line framing).
func (s *specSource) isLead(line []byte) bool {
	if s.fieldRe == nil {
		return true
	}
	loc := s.fieldRe.FindIndex(line)
	return loc != nil && loc[0] == 0
}

// emit builds one JSON record from recBytes into the borrowed docBuf: each named
// capture becomes a field; the timestamp field is normalized to an int64 epoch-nanos
// number; any spec provenance rides along as constant fields. A record that doesn't
// match the field pattern degrades to {"text": <raw>} so nothing is dropped.
func (s *specSource) emit(rec *Record, recBytes []byte) (bool, error) {
	s.docBuf = s.buildDoc(s.docBuf[:0], recBytes)
	rec.Doc = s.docBuf
	s.idBuf = appendRecordID(s.idBuf[:0], s.idPrefix, s.row)
	rec.ID = s.idBuf
	s.row++
	return true, nil
}

func (s *specSource) buildDoc(dst, recBytes []byte) []byte {
	if s.json {
		return s.buildDocJSON(dst, recBytes)
	}
	dst = append(dst, '{')
	wrote := false
	comma := func() {
		if wrote {
			dst = append(dst, ',')
		}
		wrote = true
	}
	if s.fieldRe != nil {
		if m := s.fieldRe.FindSubmatchIndex(recBytes); m != nil {
			for i, name := range s.fieldRe.SubexpNames() {
				if name == "" || m[2*i] < 0 {
					continue // unnamed group, or group didn't participate
				}
				val := recBytes[m[2*i]:m[2*i+1]]
				comma()
				dst = strconv.AppendQuote(dst, name)
				dst = append(dst, ':')
				if i == s.tsIdx {
					if ns, ok := timeToNanos(s.spec.Time, string(val)); ok {
						dst = strconv.AppendInt(dst, ns, 10)
						continue
					}
					// unparseable timestamp: keep the raw string so it's not lost.
				}
				dst = strconv.AppendQuote(dst, string(val))
			}
		} else {
			comma()
			dst = append(dst, `"text":`...)
			dst = strconv.AppendQuote(dst, string(recBytes))
		}
	} else {
		comma()
		dst = append(dst, `"text":`...)
		dst = strconv.AppendQuote(dst, string(recBytes))
	}
	// Provenance constants (lifted once by describe) ride every record.
	for k, v := range s.spec.Provenance {
		comma()
		dst = strconv.AppendQuote(dst, k)
		dst = append(dst, ':')
		dst = strconv.AppendQuote(dst, v)
	}
	return append(dst, '}')
}

// buildDocJSON emits a JSONL record: the JSON line itself, with its time field
// rewritten IN PLACE to the normalized int64 epoch-nanos sort key (so r.<ts> is a
// bare int64 the merge/ASOF can compare), plus any provenance constants. The record
// is already structured JSON, so there's no field-regex step. (jsonparser.Set
// allocates a rewritten copy per record -- fine on the extract/enrichment path; a
// record with a missing/unparseable ts is passed through unchanged so nothing drops.)
func (s *specSource) buildDocJSON(dst, recBytes []byte) []byte {
	doc := recBytes
	if s.spec.Time != nil {
		if ns, ok := s.jsonFieldNanos(doc); ok {
			if set, err := jsonparser.Set(doc, strconv.AppendInt(nil, ns, 10), s.spec.Time.Field); err == nil {
				doc = set
			}
		}
	}
	for k, v := range s.spec.Provenance {
		if set, err := jsonparser.Set(doc, strconv.AppendQuote(nil, v), k); err == nil {
			doc = set
		}
	}
	return append(dst, doc...)
}

// jsonFieldNanos reads spec.Time.Field out of a JSON record and normalizes it to an
// int64 epoch-nanos key (shared by buildDocJSON's per-row emit and describeMeasure's
// recordNanos sampling). A number value hands timeToNanos the raw digits (epoch_*);
// a string hands it the text (RFC3339/layout).
func (s *specSource) jsonFieldNanos(recBytes []byte) (int64, bool) {
	if s.spec.Time == nil {
		return 0, false
	}
	val, typ, _, err := jsonparser.Get(recBytes, s.spec.Time.Field)
	if err != nil || typ == jsonparser.NotExist {
		return 0, false
	}
	return timeToNanos(s.spec.Time, string(val))
}

// -------------------------------------------------------------- time normalization

// timeToNanos parses a timestamp value per spec into an int64 epoch-nanos key,
// timezone-normalized so streams from different files/nodes are directly comparable
// (DESIGN-data.md "The normalized sort key"). It handles the RFC3339 and epoch_*
// layout tags; any other Layout is treated as a Go reference-time layout. When the
// value carries no zone and spec.TZDefault is set, that zone is applied.
func timeToNanos(spec *TimeSpec, val string) (int64, bool) {
	if spec == nil {
		return 0, false
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return 0, false
	}
	switch spec.Layout {
	case TimeLayoutEpochS:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return int64(math.Round(f * 1e9)), true
		}
	case TimeLayoutEpochMs:
		return epochScaled(val, 1e6)
	case TimeLayoutEpochUs:
		return epochScaled(val, 1e3)
	case TimeLayoutEpochNs:
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			return n, true
		}
	case TimeLayoutRFC3339, "":
		if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
			return t.UnixNano(), true
		}
		// No zone in the value: parse in TZDefault (or UTC) then normalize.
		if loc := tzLocation(spec.TZDefault); loc != nil {
			if t, err := time.ParseInLocation("2006-01-02T15:04:05.999999999", val, loc); err == nil {
				return t.UnixNano(), true
			}
		}
	default: // a Go reference-time layout string.
		if loc := tzLocation(spec.TZDefault); loc != nil {
			if t, err := time.ParseInLocation(spec.Layout, val, loc); err == nil {
				return t.UnixNano(), true
			}
		}
		if t, err := time.Parse(spec.Layout, val); err == nil {
			return t.UnixNano(), true
		}
	}
	return 0, false
}

// epochScaled parses an epoch value (possibly fractional) in the given sub-second
// unit (scale = nanos-per-unit) into int64 nanos.
func epochScaled(val string, scale float64) (int64, bool) {
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		return int64(math.Round(f * scale)), true
	}
	return 0, false
}

// tzLocation resolves a TZDefault string ("+02:00", "-0730", "UTC", "America/...")
// to a *time.Location, or nil when it can't. A "+HH:MM"/"±HHMM" offset becomes a
// FixedZone so no tzdata is needed; a named zone goes through time.LoadLocation.
func tzLocation(tz string) *time.Location {
	tz = strings.TrimSpace(tz)
	if tz == "" || strings.EqualFold(tz, "UTC") || strings.EqualFold(tz, "Z") {
		return time.UTC
	}
	if tz[0] == '+' || tz[0] == '-' {
		sign := 1
		if tz[0] == '-' {
			sign = -1
		}
		digits := strings.ReplaceAll(tz[1:], ":", "")
		if len(digits) == 4 {
			h, e1 := strconv.Atoi(digits[:2])
			m, e2 := strconv.Atoi(digits[2:])
			if e1 == nil && e2 == nil {
				return time.FixedZone(tz, sign*(h*3600+m*60))
			}
		}
		return nil
	}
	if loc, err := time.LoadLocation(tz); err == nil {
		return loc
	}
	return nil
}

// -------------------------------------------------------------- sample measurement

// describeSampleBytes caps how much of a file's head describe reads to measure the
// sorted-source metadata -- keeping describe cheap (DESIGN-data.md §4 "cheap, once-
// per-file"); the measured disorder bound is a conservative claim the merge operator
// still validates (DESIGN-merging.md).
const describeSampleBytes = 256 * 1024

// describeMeasure samples path (its head, up to describeSampleBytes) through the same
// native framing+time path SpecApply uses, and measures the SortedSourceMeta the
// merge join consumes: min/max epoch-nanos key, record count, sortedness, and (for
// near-sorted) a disorder bound.
func describeMeasure(spec ExtractSpec, path string) (SortedSourceMeta, error) {
	meta := SortedSourceMeta{Sortedness: SortedStrict}
	if spec.Time != nil {
		meta.SortKeyLabel = spec.Time.Field
	}
	f, err := os.Open(path)
	if err != nil {
		return meta, err
	}
	defer f.Close()

	sample := make([]byte, describeSampleBytes)
	n, err := io.ReadFull(f, sample)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return meta, err
	}
	sample = sample[:n]
	// Drop a trailing partial line so a truncated sample doesn't misframe a record.
	if n == describeSampleBytes {
		if nl := lastIndexByte(sample, '\n'); nl > 0 {
			sample = sample[:nl]
		}
	}

	ss := &specSource{spec: spec}
	if err := ss.compile(); err != nil {
		return meta, err
	}
	ss.sc = bufio.NewScanner(strings.NewReader(string(sample)))
	ss.sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	var runningMax, maxLate int64
	var haveAny bool
	for {
		rb, ok, ferr := ss.frameNext()
		if ferr != nil {
			return meta, ferr
		}
		if !ok {
			break
		}
		meta.RecordCount++
		ns, tsOk := ss.recordNanos(rb)
		if !tsOk {
			continue
		}
		if !haveAny {
			meta.MinKey, meta.MaxKey, runningMax = ns, ns, ns
			haveAny = true
			continue
		}
		if ns < meta.MinKey {
			meta.MinKey = ns
		}
		if ns > meta.MaxKey {
			meta.MaxKey = ns
		}
		if late := runningMax - ns; late > maxLate { // key fell behind an already-seen key
			maxLate = late
		}
		if ns > runningMax {
			runningMax = ns
		}
	}

	// Classify sortedness from the measured max lateness (the watermark model): none
	// out of order -> strict; bounded lateness -> near with a conservative window
	// equal to the largest observed lateness (DESIGN-data.md "Sortedness, classified").
	switch {
	case !haveAny || maxLate == 0:
		meta.Sortedness = SortedStrict
	default:
		meta.Sortedness = SortedNear
		meta.Disorder = DisorderBound{WindowNanos: maxLate}
	}
	if spec.Order.By != "" {
		meta.SortKeyLabel = spec.Order.By
	}
	return meta, nil
}

// recordNanos extracts and normalizes the timestamp of one framed record to int64
// epoch-nanos (used by describe's measurement pass).
func (s *specSource) recordNanos(recBytes []byte) (int64, bool) {
	if s.json {
		return s.jsonFieldNanos(recBytes)
	}
	if s.fieldRe == nil || s.tsIdx < 0 || s.spec.Time == nil {
		return 0, false
	}
	m := s.fieldRe.FindSubmatchIndex(recBytes)
	if m == nil || m[2*s.tsIdx] < 0 {
		return 0, false
	}
	return timeToNanos(s.spec.Time, string(recBytes[m[2*s.tsIdx]:m[2*s.tsIdx+1]]))
}

func lastIndexByte(b []byte, c byte) int {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == c {
			return i
		}
	}
	return -1
}
