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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbase/n1k1/records"
)

// These benchmarks isolate the two streaming-extract fast-path features -- readInto
// (zero-alloc reads) and emitBuffer (no-marshal emit) -- over the SAME length-prefixed
// JSON file, so `-benchmem` + a memprofile show exactly which allocations each removes:
//
//   BaselineReadBytesEmit   readBytes (fresh ArrayBuffer/read) + JSON.parse + emit  (both hops)
//   ReadBytesEmitBuffer     readBytes                          + emitBuffer          (skip marshal+parse)
//   ReadIntoEmitBuffer      readInto  (reused buffer)          + emitBuffer          (skip both)
//
// All three yield identical records (the input JSON is already canonical), so it's an
// apples-to-apples allocation comparison.

const benchStreamRecords = 2000

// benchLPJDocs builds n small canonical JSON docs.
func benchLPJDocs(n int) []string {
	docs := make([]string, n)
	for i := 0; i < n; i++ {
		docs[i] = fmt.Sprintf(`{"id":%d,"name":"item-%d","value":%d}`, i, i, i*7%100)
	}
	return docs
}

const benchPluginReadBytesEmit = `
var match = { exts: [".lpjb1"], priority: 10 };
function extractStream(file, emit, emitBuffer) {
  for (;;) {
    var hdr = file.readBytes(4);
    if (hdr === null) { return; }
    var n = new DataView(hdr).getUint32(0);
    var body = file.readBytes(n);
    if (body === null) { return; }
    var u8 = new Uint8Array(body), s = "";
    for (var i = 0; i < u8.length; i++) { s += String.fromCharCode(u8[i]); }
    emit(JSON.parse(s));                       // parse -> object -> (host) Export + Marshal.
  }
}
`

const benchPluginReadBytesEmitBuffer = `
var match = { exts: [".lpjb2"], priority: 10 };
function extractStream(file, emit, emitBuffer) {
  for (;;) {
    var hdr = file.readBytes(4);
    if (hdr === null) { return; }
    var n = new DataView(hdr).getUint32(0);
    var body = file.readBytes(n);
    if (body === null) { return; }
    emitBuffer(body);                          // raw JSON bytes straight through.
  }
}
`

const benchPluginReadIntoEmitBuffer = `
var match = { exts: [".lpjb3"], priority: 10 };
function extractStream(file, emit, emitBuffer) {
  var hdr = new Uint8Array(4);                 // REUSED across records.
  var hv = new DataView(hdr.buffer);
  var body = new Uint8Array(65536);            // REUSED across records.
  for (;;) {
    if (file.readInto(hdr) === 0) { return; }
    var n = hv.getUint32(0);
    var slice = body.subarray(0, n);
    if (file.readInto(slice) < n) { throw "short body"; }
    emitBuffer(slice);                         // raw JSON bytes straight through.
  }
}
`

func benchExtractStream(b *testing.B, name, src, ext string) {
	if records.ExtractPluginFor("x."+ext) == nil {
		if err := RegisterJSExtractPlugin(name, src); err != nil {
			b.Fatalf("RegisterJSExtractPlugin: %v", err)
		}
	}
	dir := b.TempDir()
	p := filepath.Join(dir, "data."+ext)
	writeLPJ(b, p, benchLPJDocs(benchStreamRecords))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := records.OpenFile(p, "data."+ext)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		var rec records.Record
		for {
			ok, err := s.Next(&rec)
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
			count++
		}
		s.Close()
		if count != benchStreamRecords {
			b.Fatalf("framed %d records, want %d", count, benchStreamRecords)
		}
	}
}

// --- Fixed-width read comparison: readBytes vs readInto with NO subarray (the whole
// reused buffer IS one record), same emit -- isolating the read primitive's alloc cost.

const benchPluginFixedReadBytes = `
var match = { exts: [".fwb1"], priority: 10 };
function extractStream(file, emit, emitBuffer) {
  for (;;) {
    var b = file.readBytes(8);                 // fresh 8-byte ArrayBuffer per record.
    if (b === null) { return; }
    var dv = new DataView(b);
    emit({ id: dv.getUint32(0), value: dv.getUint32(4) });
  }
}
`

const benchPluginFixedReadInto = `
var match = { exts: [".fwb2"], priority: 10 };
function extractStream(file, emit, emitBuffer) {
  var buf = new Uint8Array(8);                  // REUSED whole (no subarray).
  var dv = new DataView(buf.buffer);
  for (;;) {
    if (file.readInto(buf) === 0) { return; }
    emit({ id: dv.getUint32(0), value: dv.getUint32(4) });
  }
}
`

func benchExtractStreamFixed(b *testing.B, name, src, ext string) {
	if records.ExtractPluginFor("x."+ext) == nil {
		if err := RegisterJSExtractPlugin(name, src); err != nil {
			b.Fatalf("RegisterJSExtractPlugin: %v", err)
		}
	}
	dir := b.TempDir()
	p := filepath.Join(dir, "data."+ext)
	rec := make([]byte, 8*benchStreamRecords)
	for i := 0; i < benchStreamRecords; i++ {
		binary.BigEndian.PutUint32(rec[i*8:], uint32(i))
		binary.BigEndian.PutUint32(rec[i*8+4:], uint32(i*7%100))
	}
	if err := os.WriteFile(p, rec, 0o644); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := records.OpenFile(p, "data."+ext)
		if err != nil {
			b.Fatal(err)
		}
		var r records.Record
		for {
			ok, err := s.Next(&r)
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
		}
		s.Close()
	}
}

func BenchmarkExtractStreamFixedReadBytes(b *testing.B) {
	benchExtractStreamFixed(b, "fwb1", benchPluginFixedReadBytes, "fwb1")
}
func BenchmarkExtractStreamFixedReadInto(b *testing.B) {
	benchExtractStreamFixed(b, "fwb2", benchPluginFixedReadInto, "fwb2")
}

func BenchmarkExtractStreamReadBytesEmit(b *testing.B) {
	benchExtractStream(b, "lpjb1", benchPluginReadBytesEmit, "lpjb1")
}
func BenchmarkExtractStreamReadBytesEmitBuffer(b *testing.B) {
	benchExtractStream(b, "lpjb2", benchPluginReadBytesEmitBuffer, "lpjb2")
}
func BenchmarkExtractStreamReadIntoEmitBuffer(b *testing.B) {
	benchExtractStream(b, "lpjb3", benchPluginReadIntoEmitBuffer, "lpjb3")
}
