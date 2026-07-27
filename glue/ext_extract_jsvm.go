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
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"
)

// ext_extract_jsvm.go loads a "*.extract.js" JS-authored EXTRACT RECIPE into the
// pure-Go records recipe registry (DESIGN-data.md §4, DESIGN-extensions.md
// "Extract functions"). A recipe supplies describe and/or one of extract/extractStream,
// on very different cadences:
//
//   - describe(file) -- the cheap, once-per-file PLANNING pass. Runs in goja ONCE
//     per matched file (a cold path -- garbage is fine here) and RETURNS a
//     declarative records.ExtractSpec; records.SpecApply then frames every record
//     natively (byte-oriented framing + regex + int64-nanos time parse, NO per-row
//     JS). So a describe-only recipe costs one JS call per file, never one per row.
//     This is the preferred path for line/multiline/section-framed text.
//
//   - extract(file, emit) -- the IMPERATIVE escape hatch. JS receives the WHOLE
//     decompressed file and EMITS records itself, so it owns framing AND parsing.
//     For self-contained or irregular formats a declarative spec can't frame -- e.g.
//     a full TOML document (see extensions/extract_recipes/toml2.extract.js). It
//     pays the JS boundary once per file (not per row, since it buffers), and wires
//     the records.Recipe.Extract seam that DESIGN-extensions.md sketched. A recipe
//     with extract but no describe is purely imperative (records.OpenFile skips the
//     spec/native-framer entirely).
//
//   - extractStream(file, emit) -- the STREAMING imperative form. JS reads the file
//     incrementally (file.readLine/readAll) and emits records that flow out one at a
//     time with BACKPRESSURE, so a large multi-record file frames at bounded memory
//     (see runJSExtractStream / stanza.extract.js). extract and extractStream are
//     mutually exclusive; both wire records.Recipe.Extract.
//
// Either way the goja dependency stays in glue (records is pure-Go, goja-free): the
// JS runs here, produces the neutral ExtractSpec/ExtractMatch structs (JSON-shaped
// per records/spec.go) or JSON docs, and registers a records.Recipe whose Describe/
// Extract close over the compiled program. Reuses ext_jsvm.go's goja lifetime/
// timeout/console patterns and ext_jsvm_stream.go's emit marshaling.
//
// The JS module contract (see extensions/extract_recipes/*.extract.js):
//
//	// `match` (module scope): which files this recipe claims. Shape ==
//	// records.ExtractMatch's json: {exts:[".log"], names:["re",...], priority:N}.
//	// A recipe may claim a BRAND-NEW extension (e.g. ".toml2"); the claim makes its
//	// files records (records.IsRecordFile honors registered recipes).
//	var match = { exts: [".log"], names: ["ns_server\\..*\\.log$"], priority: 20 };
//
//	// describe(file) -> ExtractSpec object (shape == records.ExtractSpec's json).
//	// file = { path, name, ext, head } where head is a decompressed head sample
//	// for content-sniffing. Runs once per file. (Optional if extract is defined.)
//	function describe(file) {
//	  return { format:"...", framing:{kind:"multiline",continuation:"..."},
//	           fields:{pattern:"...(?P<ts>...)..."},
//	           time:{field:"ts",layout:"RFC3339",tz_default:"+02:00"},
//	           order:{by:"ts",sorted:"near"}, provenance:{...} };
//	}
//
//	// extract(file, emit) -- imperative alternative. file = { path, name, ext, stem,
//	// text } where text is the WHOLE decompressed file. Call emit(doc[, id]) per
//	// record; doc is any JSON-able value, the optional id overrides the default
//	// (stem for a single record, else "<prefix>#<n>"). (Optional if describe is set.)
//	function extract(file, emit) { emit(JSON.parse(file.text), file.stem); }
//
//	// extractStream(file, emit) -- STREAMING imperative alternative for a large
//	// multi-record file. file = { path, name, ext, stem, readLine(), readAll() }.
//	// Read incrementally; emit(doc[, id]) returns false once the consumer stops, so
//	// the loop can break. ids default to "<prefix>#<n>".
//	function extractStream(file, emit) {
//	  var line; while ((line = file.readLine()) !== null) { emit(parseLine(line)); }
//	}

// extractHeadSampleBytes caps the decompressed head passed to a JS describe() for
// content-sniffing. Generous (describe is once-per-file, not a hot loop) but bounded
// so a huge file doesn't balloon the planning-phase string.
const extractHeadSampleBytes = 64 * 1024

// ExtractRecipeInfo describes one loaded JS extract recipe (for listing). Kept
// separate from ext.go's extLoaded (scalar/agg/stream FUNCTIONS, unloadable via the
// cbq function registry): a recipe lives in the records registry, which has no
// delete, so recipes are load-only.
type ExtractRecipeInfo struct {
	Name     string   // recipe/format label (the file's "<name>.extract.js" stem)
	Source   string   // originating file path, or "(inline)"
	Exts     []string // file extensions the recipe claims (its match.exts)
	Names    []string // path/name regexps the recipe claims (its match.names)
	Priority int      // match priority (higher wins on overlap)
}

// extractRecipesLoaded tracks loaded JS extract recipes for ListExtractRecipes.
// Mutated only by RegisterJSExtractRecipe (startup / between queries, never
// concurrently with parsing/execution), so no lock -- matching ext_jsvm.go.
var extractRecipesLoaded []ExtractRecipeInfo

// RegisterJSExtractRecipe compiles a *.extract.js source and registers it as a
// records.Recipe. name is the recipe/format label. The source must define a
// module-scope `match` object (which files it claims) and a describe(file) function
// returning an ExtractSpec object. describe is NOT run here (it needs a file); it is
// compiled once and invoked per-file from the returned recipe's Describe closure.
// Safe at startup before parsing; not safe concurrently with query parsing.
func RegisterJSExtractRecipe(name, source string) error {
	name = strings.ToLower(name)

	prog, err := goja.Compile(name+".extract.js", source, true)
	if err != nil {
		return fmt.Errorf("goja compile of extract recipe %q: %w", name, err)
	}

	// Run the program once at registration to (a) surface top-level JS errors early
	// and (b) read the module-scope `match` -- RecipeFor needs the claim up front,
	// before any file is seen. A throwaway runtime: the per-file describe builds its
	// own (goja runtimes aren't goroutine-safe; see runJSDescribe).
	rt := goja.New()
	installJSConsole(rt)
	if _, err := rt.RunProgram(prog); err != nil {
		return fmt.Errorf("extract recipe %q: %w", name, err)
	}
	_, hasDescribe := goja.AssertFunction(rt.Get("describe"))
	_, hasExtract := goja.AssertFunction(rt.Get("extract"))
	_, hasExtractStream := goja.AssertFunction(rt.Get("extractStream"))
	if !hasDescribe && !hasExtract && !hasExtractStream {
		return fmt.Errorf("extract recipe %q: source defines none of describe(file), extract(file, emit), or extractStream(file, emit)", name)
	}
	if hasExtract && hasExtractStream {
		return fmt.Errorf("extract recipe %q: define extract OR extractStream, not both", name)
	}
	match, err := jsExtractMatch(rt)
	if err != nil {
		return fmt.Errorf("extract recipe %q: %w", name, err)
	}

	// Fingerprint from the source hash: an edited *.extract.js changes describe's
	// output (or extract's), so it must invalidate any memoized describe result
	// (extract_cache.go).
	recipe := &records.Recipe{
		Name:        name,
		Match:       match,
		Fingerprint: name + "@" + jsSourceHash(source),
	}
	if hasDescribe {
		// Declarative: SpecApply runs the JS-produced spec natively (no per-row JS).
		recipe.Describe = func(path string) (records.ExtractSpec, records.SortedSourceMeta, error) {
			return runJSDescribe(prog, name, path)
		}
	}
	switch {
	case hasExtractStream:
		// Imperative + STREAMING: JS reads the file incrementally (file.readLine) and
		// emits records that flow out one at a time with backpressure (bounded memory).
		recipe.Extract = func(path, idPrefix string, _ records.ExtractSpec) (records.Source, error) {
			return runJSExtractStream(prog, name, path, idPrefix)
		}
	case hasExtract:
		// Imperative + BUFFERED: JS parses the whole file and emits records itself.
		recipe.Extract = func(path, idPrefix string, _ records.ExtractSpec) (records.Source, error) {
			return runJSExtract(prog, name, path, idPrefix)
		}
	}
	records.RecipeRegister(recipe)
	base.Logf(1, "glue/recipe", "loaded JS extract recipe, name: %s, exts: %v, names: %v, priority: %d",
		name, match.Exts, match.Names, match.Priority)
	extractRecipesLoaded = append(extractRecipesLoaded, ExtractRecipeInfo{
		Name: name, Source: "(inline)",
		Exts: match.Exts, Names: match.Names, Priority: match.Priority,
	})

	// Inline goldens (ext_examples.go): an extract example is {in:<sample text>, out:
	// [<rows>]}. We capture the `examples` array from the registration runtime, and
	// register an executor that FRAMES the sample through this recipe (describe ->
	// records.SpecApply) exactly as a real file would be framed.
	recordExtExamples("extract", name, readJSExamples(rt))
	sampleExt := ".log"
	if len(match.Exts) > 0 {
		sampleExt = match.Exts[0]
	}
	switch {
	case hasDescribe:
		// Native framing path: describe -> records.SpecApply, exactly as a real file.
		extractExamplers[name] = func(sample string) (json.RawMessage, error) {
			return frameExtractSample(name, prog, sampleExt, sample)
		}
	case hasExtractStream:
		// Purely imperative + streaming: drive the streaming Source over the sample.
		extractExamplers[name] = func(sample string) (json.RawMessage, error) {
			return frameExtractSampleWith(sampleExt, sample, func(path string) (records.Source, error) {
				return runJSExtractStream(prog, name, path, "")
			})
		}
	default: // hasExtract
		// Purely imperative + buffered: run the JS extract over the sample.
		extractExamplers[name] = func(sample string) (json.RawMessage, error) {
			return frameExtractSampleWith(sampleExt, sample, func(path string) (records.Source, error) {
				return runJSExtract(prog, name, path, "")
			})
		}
	}
	return nil
}

// frameExtractSample writes `sample` to a temp file (named with the recipe's primary
// extension, so describe's ext/name sniffing behaves as it would for a real file),
// runs describe() to obtain the ExtractSpec, then frames the file natively via
// records.SpecApply -- the exact path a real bundle file takes -- and returns the
// produced rows as one JSON array. Used by the inline extract examples (ext_examples.go).
func frameExtractSample(name string, prog *goja.Program, ext, sample string) (json.RawMessage, error) {
	f, err := os.CreateTemp("", "n1k1-extract-example-*"+ext)
	if err != nil {
		return nil, err
	}
	path := f.Name()
	defer os.Remove(path)
	if _, werr := f.WriteString(sample); werr != nil {
		f.Close()
		return nil, werr
	}
	if cerr := f.Close(); cerr != nil {
		return nil, cerr
	}

	spec, _, err := runJSDescribe(prog, name, path)
	if err != nil {
		return nil, err
	}
	src, err := records.SpecApply(spec, path, "")
	if err != nil {
		return nil, err
	}
	defer src.Close()

	var rows [][]byte
	for {
		var rec records.Record
		ok, nerr := src.Next(&rec)
		if nerr != nil {
			return nil, nerr
		}
		if !ok {
			break
		}
		rows = append(rows, append([]byte(nil), rec.Doc...))
	}
	return jsonArray(rows), nil
}

// frameExtractSampleWith is frameExtractSample's imperative sibling (for an extract-
// or extractStream-only recipe): it writes `sample` to a temp file named with the
// recipe's primary extension, builds a records.Source over it via `build` (the JS
// buffered or streaming extract), and returns the emitted rows as one JSON array.
// Used by the inline extract examples for imperative recipes.
func frameExtractSampleWith(ext, sample string, build func(path string) (records.Source, error)) (json.RawMessage, error) {
	f, err := os.CreateTemp("", "n1k1-extract-example-*"+ext)
	if err != nil {
		return nil, err
	}
	path := f.Name()
	defer os.Remove(path)
	if _, werr := f.WriteString(sample); werr != nil {
		f.Close()
		return nil, werr
	}
	if cerr := f.Close(); cerr != nil {
		return nil, cerr
	}

	src, err := build(path)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	var rows [][]byte
	for {
		var rec records.Record
		ok, nerr := src.Next(&rec)
		if nerr != nil {
			return nil, nerr
		}
		if !ok {
			break
		}
		rows = append(rows, append([]byte(nil), rec.Doc...))
	}
	return jsonArray(rows), nil
}

// jsExtractRec is one buffered record a JS imperative extract emitted (id + JSON doc).
type jsExtractRec struct {
	id  string
	doc []byte
}

// jsExtractSource is a records.Source over the records a JS imperative extract emitted.
// All rows are buffered up front (imperative extract isn't streaming), and each row's
// bytes are owned for the source's lifetime -- so the borrowed-until-next-Next Source
// contract holds trivially.
type jsExtractSource struct {
	recs []jsExtractRec
	i    int
}

func (s *jsExtractSource) Next(rec *records.Record) (bool, error) {
	if s.i >= len(s.recs) {
		return false, nil
	}
	rec.ID = []byte(s.recs[s.i].id)
	rec.Doc = s.recs[s.i].doc
	s.i++
	return true, nil
}

func (s *jsExtractSource) Close() error { return nil }

// runJSExtract is a recipe's per-file IMPERATIVE extract pass (the records.Recipe.Extract
// seam). Unlike describe -- which only returns a declarative spec for the native framer
// -- extract OWNS framing and parsing: it receives the whole decompressed file plus an
// emit(doc[, id]) callback and pushes out each record itself. It is the escape hatch for
// self-contained/irregular formats a declarative ExtractSpec can't frame (e.g. a full
// TOML document). A fresh goja runtime per call (goja isn't goroutine-safe, and OpenFile
// can run concurrently across scans), with panic/timeout contained so a bad recipe can't
// crash the engine. Records are buffered into a jsExtractSource.
func runJSExtract(prog *goja.Program, name, path, idPrefix string) (src records.Source, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("extract recipe %q extract(%s) panicked: %v", name, path, r)
			src = nil
		}
	}()

	// An imperative parser (e.g. TOML) needs the WHOLE document, not a head sample.
	data, rerr := records.ReadWholeDecompressed(path)
	if rerr != nil {
		return nil, rerr
	}

	rt := goja.New()
	installJSConsole(rt)
	if _, e := rt.RunProgram(prog); e != nil {
		return nil, e
	}
	fn, ok := goja.AssertFunction(rt.Get("extract"))
	if !ok {
		return nil, fmt.Errorf("extract recipe %q: extract not callable", name)
	}

	// The file arg: path/name/ext/stem plus the whole decompressed text.
	fileObj := rt.NewObject()
	_ = fileObj.Set("path", path)
	_ = fileObj.Set("name", filepath.Base(path))
	_ = fileObj.Set("ext", strings.ToLower(filepath.Ext(path)))
	_ = fileObj.Set("stem", records.Stem(path))
	_ = fileObj.Set("text", string(data))

	var recs []jsExtractRec
	var emitErr error

	// emit(doc[, id]) buffers one record. doc is JSON-marshaled (the same Go<->JS
	// boundary the streaming js sources use, so Go's json.Marshal canonicalizes it --
	// sorted keys, integer-valued floats without a trailing ".0"). The optional id
	// overrides the default id assigned below.
	emit := func(call goja.FunctionCall) goja.Value {
		if emitErr != nil {
			return rt.ToValue(false)
		}
		if len(call.Arguments) == 0 {
			emitErr = fmt.Errorf("extract recipe %q: emit() needs a doc argument", name)
			return rt.ToValue(false)
		}
		jb, e := json.Marshal(call.Arguments[0].Export())
		if e != nil {
			emitErr = fmt.Errorf("extract recipe %q: cannot marshal emitted doc: %w", name, e)
			return rt.ToValue(false)
		}
		id := ""
		if len(call.Arguments) >= 2 {
			if a := call.Arguments[1]; a != nil && !goja.IsUndefined(a) && !goja.IsNull(a) {
				id = a.String()
			}
		}
		recs = append(recs, jsExtractRec{id: id, doc: jb})
		return rt.ToValue(true)
	}

	// Bound a runaway extract (a whole-file parse; a pathological script shouldn't hang
	// the scan). Reuses ext_jsvm.go's JSCallTimeout/interrupt pattern.
	var timedOut int32
	if JSCallTimeout > 0 {
		timer := time.AfterFunc(JSCallTimeout, func() {
			atomic.StoreInt32(&timedOut, 1)
			rt.Interrupt("n1k1: JS extract time limit exceeded")
		})
		defer func() {
			timer.Stop()
			rt.ClearInterrupt()
		}()
	}

	_, callErr := fn(goja.Undefined(), fileObj, rt.ToValue(emit))
	if atomic.LoadInt32(&timedOut) == 1 {
		return nil, fmt.Errorf("extract recipe %q extract exceeded the %s time limit", name, JSCallTimeout)
	}
	if callErr != nil {
		return nil, fmt.Errorf("extract recipe %q extract(%s): %w", name, path, callErr)
	}
	if emitErr != nil {
		return nil, emitErr
	}

	// Default ids (an explicit emit(doc, id) always wins): a single record keys by the
	// file stem (the one-doc-per-file META().id convention, like a single-object .json);
	// multiple records key by "<idPrefix>#<n>" (matching the native json/yaml sources).
	stem := records.Stem(path)
	for i := range recs {
		if recs[i].id == "" {
			if len(recs) == 1 {
				recs[i].id = stem
			} else {
				recs[i].id = fmt.Sprintf("%s#%d", idPrefix, i)
			}
		}
	}
	return &jsExtractSource{recs: recs}, nil
}

// jsExtractStreamSource is a records.Source over a STREAMING JS imperative extract. A
// goja runtime is single-threaded and calls emit synchronously during one extract()
// call, but records.Source.Next is pull-based -- so the JS runs on its own goroutine
// and emit hands each record across `recs` with backpressure: an UNBUFFERED channel, so
// JS blocks in emit until Next consumes, keeping only one record in flight (bounded
// memory, however large the file or record count). Close signals `done` (a blocked emit
// then returns false so the JS loop can stop), interrupts the runtime as a backstop,
// and waits for the goroutine to exit before releasing the file -- no goroutine/fd leak
// and no early-Close read/close race.
type jsExtractStreamSource struct {
	recs     chan jsExtractRec
	errc     chan error    // buffered(1): the goroutine's terminal error, set before recs closes.
	done     chan struct{} // closed by Close: tells emit (and the JS loop) to stop.
	finished chan struct{} // closed by the goroutine on exit.
	rc       io.ReadCloser
	rt       *goja.Runtime // interrupted by Close as a backstop (goja.Interrupt is goroutine-safe).

	eof       bool
	err       error
	closeOnce sync.Once
}

func (s *jsExtractStreamSource) Next(rec *records.Record) (bool, error) {
	if s.eof {
		return false, s.err
	}
	r, ok := <-s.recs
	if !ok { // goroutine finished; pick up any terminal error it recorded first.
		s.eof = true
		select {
		case e := <-s.errc:
			s.err = e
		default:
		}
		return false, s.err
	}
	// r.doc is a distinct, owned slice per record, so it satisfies the borrowed-until-
	// next-Next contract trivially (it stays valid).
	rec.ID = []byte(r.id)
	rec.Doc = r.doc
	return true, nil
}

func (s *jsExtractStreamSource) Close() error {
	s.closeOnce.Do(func() {
		close(s.done) // a blocked emit unblocks via the done case and returns false.
		if s.rt != nil {
			s.rt.Interrupt("n1k1: JS extract stream closed") // backstop for a loop that ignores emit's false.
		}
		<-s.finished // reads have stopped -> safe to close the file.
		s.rc.Close()
	})
	return nil
}

// runJSExtractStream is runJSExtract's STREAMING sibling: JS reads the file
// incrementally (file.readLine / file.readAll) and emits records that flow out one at a
// time with backpressure, so a large/irregular multi-record file frames at bounded
// memory (no whole-file buffer, no buffering of all records). The JS module defines
// extractStream(file, emit); emit(doc[, id]) returns false once the consumer has
// stopped (a LIMIT satisfied, the query cancelled), so the JS loop can break -- the same
// stop protocol as a *.stream.js source. ids default to "<idPrefix>#<n>" (streaming is
// inherently multi-record); an explicit emit(doc, id) wins.
func runJSExtractStream(prog *goja.Program, name, path, idPrefix string) (records.Source, error) {
	rc, err := records.OpenReadCloser(path) // streaming (never whole-file); Source.Close releases it.
	if err != nil {
		return nil, err
	}

	// The runtime is built here (parent) and used ONLY by the goroutine below; Close
	// touches it solely via Interrupt (goroutine-safe). Build fails close the file.
	rt := goja.New()
	installJSConsole(rt)
	if _, e := rt.RunProgram(prog); e != nil {
		rc.Close()
		return nil, e
	}
	fn, ok := goja.AssertFunction(rt.Get("extractStream"))
	if !ok {
		rc.Close()
		return nil, fmt.Errorf("extract recipe %q: extractStream not callable", name)
	}

	s := &jsExtractStreamSource{
		recs:     make(chan jsExtractRec), // unbuffered: strict backpressure, one record in flight.
		errc:     make(chan error, 1),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
		rc:       rc,
		rt:       rt,
	}

	go func() {
		var gerr error
		// Order matters: this recover/error send runs BEFORE close(recs) (defers are
		// LIFO), so a Next that sees recs closed already finds the error on errc.
		defer close(s.finished)
		defer close(s.recs)
		defer func() {
			if r := recover(); r != nil {
				gerr = fmt.Errorf("extract recipe %q extractStream(%s) panicked: %v", name, path, r)
			}
			if gerr != nil {
				select {
				case s.errc <- gerr:
				default:
				}
			}
		}()

		br := bufio.NewReader(rc)
		var readErr, emitErr error
		n := 0

		fileObj := rt.NewObject()
		_ = fileObj.Set("path", path)
		_ = fileObj.Set("name", filepath.Base(path))
		_ = fileObj.Set("ext", strings.ToLower(filepath.Ext(path)))
		_ = fileObj.Set("stem", records.Stem(path))
		// readLine(): the next line WITHOUT its trailing newline, or null at EOF. A blank
		// line returns "" (not null), so a recipe can use it as a record boundary.
		_ = fileObj.Set("readLine", func(goja.FunctionCall) goja.Value {
			line, e := br.ReadString('\n')
			if len(line) > 0 {
				return rt.ToValue(strings.TrimRight(line, "\r\n"))
			}
			if e == io.EOF {
				return goja.Null()
			}
			if e != nil {
				readErr = e
				return goja.Null()
			}
			return rt.ToValue("")
		})
		// readAll(): the rest of the file as one string (for a recipe that wants the
		// whole document but still streams its output).
		_ = fileObj.Set("readAll", func(goja.FunctionCall) goja.Value {
			b, e := io.ReadAll(br)
			if e != nil {
				readErr = e
				return rt.ToValue("")
			}
			return rt.ToValue(string(b))
		})

		// emit(doc[, id]): hand one record across `recs` with backpressure. Returns
		// false once Close has fired, so the JS loop stops.
		emit := func(call goja.FunctionCall) goja.Value {
			select { // fast path: already stopped.
			case <-s.done:
				return rt.ToValue(false)
			default:
			}
			if len(call.Arguments) == 0 {
				emitErr = fmt.Errorf("extract recipe %q: emit() needs a doc argument", name)
				return rt.ToValue(false)
			}
			jb, e := json.Marshal(call.Arguments[0].Export())
			if e != nil {
				emitErr = fmt.Errorf("extract recipe %q: cannot marshal emitted doc: %w", name, e)
				return rt.ToValue(false)
			}
			id := ""
			if len(call.Arguments) >= 2 {
				if a := call.Arguments[1]; a != nil && !goja.IsUndefined(a) && !goja.IsNull(a) {
					id = a.String()
				}
			}
			if id == "" {
				id = fmt.Sprintf("%s#%d", idPrefix, n)
			}
			n++
			select {
			case s.recs <- jb2rec(id, jb):
				return rt.ToValue(true)
			case <-s.done:
				return rt.ToValue(false)
			}
		}

		_, callErr := fn(goja.Undefined(), fileObj, rt.ToValue(emit))
		switch {
		case emitErr != nil:
			gerr = emitErr
		case readErr != nil:
			gerr = readErr
		case callErr != nil:
			// An interrupt from Close is expected teardown, not a recipe error.
			select {
			case <-s.done:
			default:
				gerr = fmt.Errorf("extract recipe %q extractStream(%s): %w", name, path, callErr)
			}
		}
	}()

	return s, nil
}

// jb2rec builds a jsExtractRec (kept tiny so the emit hot path reads cleanly).
func jb2rec(id string, doc []byte) jsExtractRec { return jsExtractRec{id: id, doc: doc} }

// jsSourceHash returns a short hex digest of a recipe's JS source, used as the
// recipe Fingerprint (extract_cache.go) so editing an *.extract.js re-describes its
// files instead of serving a spec shaped by the old source. 12 hex chars (48 bits) is
// ample to distinguish edits; this runs once at registration, never in a hot loop.
func jsSourceHash(source string) string {
	sum := sha1.Sum([]byte(source))
	return hex.EncodeToString(sum[:])[:12]
}

// ListExtractRecipes returns the loaded JS extract recipes in load order.
func ListExtractRecipes() []ExtractRecipeInfo {
	out := make([]ExtractRecipeInfo, len(extractRecipesLoaded))
	copy(out, extractRecipesLoaded)
	return out
}

// jsExtractMatch reads the module-scope `match` object into a records.ExtractMatch
// via a JSON round-trip (records.ExtractMatch's json tags == the JS field names:
// exts/names/priority). A match that claims nothing (no exts AND no names) is
// rejected -- an all-wildcard recipe would shadow every file.
func jsExtractMatch(rt *goja.Runtime) (records.ExtractMatch, error) {
	var m records.ExtractMatch
	v := rt.Get("match")
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return m, fmt.Errorf("no module-scope `match` object (set exts and/or names)")
	}
	if err := jsExportInto(v, &m); err != nil {
		return m, fmt.Errorf("bad `match`: %w", err)
	}
	if len(m.Exts) == 0 && len(m.Names) == 0 {
		return m, fmt.Errorf("`match` claims nothing (set exts and/or names)")
	}
	return m, nil
}

// runJSDescribe is a recipe's per-file describe pass: it builds a fresh goja runtime
// (one goja.Runtime is not goroutine-safe, and describe can run concurrently across
// queries/keyspaces -- isolation beats sharing on this cold, once-per-file path),
// runs the compiled program, calls describe(file) with a {path,name,ext,head} arg,
// marshals the returned object into a records.ExtractSpec, then MEASURES the
// SortedSourceMeta natively (records.MeasureSortedSource) and reflects the measured
// order back into the spec. A panic/timeout is contained so a bad recipe can't crash
// the engine.
func runJSDescribe(prog *goja.Program, name, path string) (spec records.ExtractSpec, meta records.SortedSourceMeta, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("extract recipe %q describe(%s) panicked: %v", name, path, r)
		}
	}()

	rt := goja.New()
	installJSConsole(rt)
	if _, e := rt.RunProgram(prog); e != nil {
		return spec, meta, e
	}
	fn, ok := goja.AssertFunction(rt.Get("describe"))
	if !ok {
		return spec, meta, fmt.Errorf("extract recipe %q: describe not callable", name)
	}

	// The file arg: path/name/ext plus a decompressed head sample for sniffing.
	head, _ := records.HeadSample(path, extractHeadSampleBytes) // best-effort.
	fileObj := rt.NewObject()
	_ = fileObj.Set("path", path)
	_ = fileObj.Set("name", filepath.Base(path))
	_ = fileObj.Set("ext", strings.ToLower(filepath.Ext(path)))
	_ = fileObj.Set("head", head)

	// Bound a runaway describe (still a cold path, but a pathological script
	// shouldn't hang the scan). Reuses ext_jsvm.go's JSCallTimeout/interrupt pattern.
	var timedOut int32
	if JSCallTimeout > 0 {
		timer := time.AfterFunc(JSCallTimeout, func() {
			atomic.StoreInt32(&timedOut, 1)
			rt.Interrupt("n1k1: JS extract describe time limit exceeded")
		})
		defer func() {
			timer.Stop()
			rt.ClearInterrupt()
		}()
	}

	res, callErr := fn(goja.Undefined(), fileObj)
	if atomic.LoadInt32(&timedOut) == 1 {
		return spec, meta, fmt.Errorf("extract recipe %q describe exceeded the %s time limit", name, JSCallTimeout)
	}
	if callErr != nil {
		return spec, meta, fmt.Errorf("extract recipe %q describe(%s): %w", name, path, callErr)
	}
	if res == nil || goja.IsUndefined(res) || goja.IsNull(res) {
		return spec, meta, fmt.Errorf("extract recipe %q: describe(%s) returned no spec", name, path)
	}
	if e := jsExportInto(res, &spec); e != nil {
		return spec, meta, fmt.Errorf("extract recipe %q: bad spec from describe(%s): %w", name, path, e)
	}
	if spec.Format == "" {
		spec.Format = name // Default the format tag to the recipe name.
	}

	// Measure the sorted-source metadata natively (the same pass the built-in
	// recipes use) and reflect it back into the spec, so a spec-only consumer sees
	// this file's real sortedness/disorder bound.
	declaredSorted := spec.Order.Sorted     // the recipe author's declaration.
	declaredDisorder := spec.Order.Disorder //   (before measurement overwrites it).
	meta, err = records.MeasureSortedSource(spec, path)
	if err != nil {
		return spec, meta, err
	}

	// FLOOR the measured sortedness at the declaration. describeMeasure reads only a
	// HEAD SAMPLE, which can PROVE disorder (an inversion in the sample) but can NOT
	// prove global strictness -- a real log's first out-of-order timestamp often sits
	// past the sample (measured: a bundle's memcached.log sampled "strict" but inverts
	// ~1µs at row 2638). So a "near"/"none" declaration must never be downgraded to a
	// measured "strict"; take the MORE-disordered of the two, and keep the larger bound.
	meta.Sortedness = moreDisordered(declaredSorted, meta.Sortedness)
	if declaredDisorder.WindowNanos > meta.Disorder.WindowNanos {
		meta.Disorder = declaredDisorder
	}
	// A "near" source with no measured/declared bound still needs a reorder window, or
	// the watermarked merge degenerates to strict and aborts on the first deep inversion.
	// Default to a conservative window (clock skew / concurrent-writer disorder in real
	// logs is sub-second); the reorder buffer holds only rows within it, so RAM stays
	// bounded. A recipe can declare a tighter/looser order.disorder to override.
	if meta.Sortedness == records.SortedNear && meta.Disorder.WindowNanos == 0 {
		meta.Disorder = records.DisorderBound{WindowNanos: defaultNearDisorderNanos}
	}

	spec.Order.Sorted = meta.Sortedness
	spec.Order.Disorder = meta.Disorder
	return spec, meta, nil
}

// defaultNearDisorderNanos is the reorder window assumed for a "near" source that
// declares no explicit order.disorder and whose head sample measured no inversion. 5s
// comfortably covers real-log clock-skew / concurrent-writer timestamp disorder.
const defaultNearDisorderNanos = int64(5 * 1e9)

// moreDisordered returns whichever of two sortedness labels is LESS ordered
// (strict < near < none), so a measurement can raise disorder but never lower a
// declaration below what its author promised.
func moreDisordered(a, b string) string {
	rank := func(s string) int {
		switch s {
		case records.SortedNone:
			return 2
		case records.SortedNear:
			return 1
		default:
			return 0 // strict / unset
		}
	}
	if rank(b) > rank(a) {
		return b
	}
	if a == "" {
		return records.SortedStrict
	}
	return a
}

// jsExportInto marshals a goja value into dst via a JSON round-trip. This reuses the
// records/spec.go JSON contract (its structs are JSON-serializable by design), so a
// JS object whose keys are the json tags (snake_case: tz_default, window_nanos)
// deserializes straight into the typed Go struct with no hand-written field mapping.
func jsExportInto(v goja.Value, dst interface{}) error {
	raw, err := json.Marshal(v.Export())
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dst)
}
