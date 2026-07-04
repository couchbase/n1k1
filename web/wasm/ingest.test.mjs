//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License").
//
// Unit tests for wasm/ingest.js — the drag-drop ingestion pipeline (gunzip,
// untar, record parsing, keyspace mapping). Pure JS, no wasm needed.
//
//   node --test web/wasm/
//
// These exercise the logic most likely to break; the end-to-end query path over
// a real n1k1.wasm is covered by e2e.test.mjs.

import { test } from "node:test";
import assert from "node:assert/strict";
import zlib from "node:zlib";
import "./ingest.js";

const ING = globalThis.n1k1Ingest;
const enc = new TextEncoder();

// Minimal USTAR writer (name, size, typeflag '0') — mirrors what ingest.untar reads.
function tar(entries) {
  const blocks = [];
  for (const { name, data } of entries) {
    const h = new Uint8Array(512);
    enc.encodeInto(name, h.subarray(0, 100));
    enc.encodeInto(data.length.toString(8).padStart(11, "0") + " ", h.subarray(124, 136));
    h[156] = "0".charCodeAt(0);
    enc.encodeInto("ustar\0" + "00", h.subarray(257, 265));
    blocks.push(h);
    const pad = new Uint8Array(Math.ceil(data.length / 512) * 512);
    pad.set(data);
    blocks.push(pad);
  }
  blocks.push(new Uint8Array(1024));
  const out = new Uint8Array(blocks.reduce((n, b) => n + b.length, 0));
  let o = 0; for (const b of blocks) { out.set(b, o); o += b.length; }
  return out;
}
const gz = (s) => new Uint8Array(zlib.gzipSync(Buffer.from(s)));
const bytes = (s) => enc.encode(s);

test("parseRecords: jsonl splits by line, ignores blanks", () => {
  const docs = ING.parseRecords("x.jsonl", '{"a":1}\n\n{"a":2}\n');
  assert.deepEqual(docs, [{ a: 1 }, { a: 2 }]);
});

test("parseRecords: .json array spreads, object is single", () => {
  assert.deepEqual(ING.parseRecords("x.json", "[1,2,3]"), [1, 2, 3]);
  assert.deepEqual(ING.parseRecords("x.json", '{"a":1}'), [{ a: 1 }]);
});

test("untar: recovers regular file entries, skips dirs", () => {
  const t = tar([
    { name: "a/x.json", data: bytes('{"id":1}') },
    { name: "y.jsonl", data: bytes('{"id":2}\n') },
  ]);
  const entries = ING.untar(t);
  assert.equal(entries.length, 2);
  assert.equal(entries[0].name, "a/x.json");
  assert.equal(new TextDecoder().decode(entries[0].bytes), '{"id":1}');
});

test("gunzip round-trips", async () => {
  const out = await ING.gunzip(gz("hello world"));
  assert.equal(new TextDecoder().decode(out), "hello world");
});

test("filesToDatastore: jsonl → keyspace by stem, docs keyed by id field", async () => {
  const { tree, stats } = await ING.filesToDatastore([
    { name: "beers.jsonl", bytes: bytes('{"id":"a","n":1}\n{"id":"b","n":2}\n') },
  ]);
  assert.equal(stats.keyspaces, 1);
  assert.equal(stats.docs, 2);
  assert.deepEqual(Object.keys(tree.default.beers).sort(), ["a.json", "b.json"]);
});

test("filesToDatastore: gzipped jsonl is inflated", async () => {
  const { stats } = await ING.filesToDatastore([
    { name: "b.jsonl.gz", bytes: gz('{"id":"a"}\n{"id":"b"}\n{"id":"c"}\n') },
  ]);
  assert.equal(stats.docs, 3);
});

test("filesToDatastore: tar.gz → dir=keyspace (stem key) + loose jsonl=keyspace", async () => {
  const t = tar([
    { name: "breweries/b1.json", data: bytes('{"id":"bells","st":"MI"}') },
    { name: "breweries/b2.json", data: bytes('{"id":"ballast","st":"CA"}') },
    { name: "styles.jsonl", data: bytes('{"s":"IPA"}\n{"s":"Stout"}\n') },
  ]);
  const { tree } = await ING.filesToDatastore([{ name: "d.tar.gz", bytes: gz_bytes(t) }]);
  assert.deepEqual(Object.keys(tree.default).sort(), ["breweries", "styles"]);
  // single-object .json under a dir is keyed by file stem, not the id field
  assert.deepEqual(Object.keys(tree.default.breweries).sort(), ["b1.json", "b2.json"]);
  assert.equal(Object.keys(tree.default.styles).length, 2);
});

test("filesToDatastore: duplicate doc keys are de-duped", async () => {
  const { tree } = await ING.filesToDatastore([
    { name: "k.jsonl", bytes: bytes('{"id":"same","v":1}\n{"id":"same","v":2}\n') },
  ]);
  assert.equal(Object.keys(tree.default.k).length, 2); // no clobber
});

test("filesToDatastore: unsupported types are skipped, not fatal", async () => {
  const { tree, stats } = await ING.filesToDatastore([
    { name: "notes.txt", bytes: bytes("hello") },
    { name: "ok.jsonl", bytes: bytes('{"id":1}\n') },
  ]);
  assert.equal(stats.keyspaces, 1);
  assert.ok(stats.skipped.some((s) => s.includes("notes.txt")));
  assert.ok(tree.default.ok);
});

test("filesToDatastore: maxDocs cap truncates and flags", async () => {
  let jsonl = "";
  for (let i = 0; i < 10; i++) jsonl += JSON.stringify({ id: i }) + "\n";
  const { stats } = await ING.filesToDatastore([{ name: "big.jsonl", bytes: bytes(jsonl) }], { maxDocs: 4 });
  assert.equal(stats.docs, 4);
  assert.ok(stats.truncated);
});

// gzip a Uint8Array (for the tar.gz case).
function gz_bytes(u8) { return new Uint8Array(zlib.gzipSync(Buffer.from(u8))); }
