//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License").
//
// Drag-and-drop / file ingestion for the n1k1 WebAssembly demo.
//
// Turns dropped files -- .json, .jsonl/.ndjson, their .gz variants, and .tar /
// .tar.gz(.tgz) archives -- into the in-memory <namespace>/<keyspace>/<key>.json
// document tree the engine already understands (see wasm/fs_mem.js). We normalize
// everything to that classic per-document layout in JS rather than lean on n1k1's
// flat-file discovery, so keyspace names are predictable and the whole pipeline is
// testable outside a browser (node has DecompressionStream too).
//
// Mapping:
//   beers.jsonl            -> keyspace "beers", one doc per line
//   beers.json (an array)  -> keyspace "beers", one doc per element
//   beers.json (an object) -> keyspace "beers", a single doc
//   sub/x.json (in a .tar) -> keyspace "sub",  doc keyed by file stem "x"
//   sub/y.jsonl (in a .tar)-> keyspace "sub",  one doc per line
// A document's key is its id/_id/uuid/key field when present, else its index/stem
// (deduped). .gz is inflated via DecompressionStream('gzip'); .zst is unsupported.
//
// Exposes globalThis.n1k1Ingest = { filesToDatastore, gunzip, untar, parseRecords }.

(function (g) {
  "use strict";

  const REC_EXT = /\.(jsonl|ndjson|json|jsons)$/i;
  const dec = new TextDecoder("utf-8");
  const enc = new TextEncoder();

  // Inflate gzip bytes using the platform DecompressionStream (browser + node18+).
  async function gunzip(bytes) {
    const ds = new DecompressionStream("gzip");
    const stream = new Response(new Blob([bytes]).stream().pipeThrough(ds));
    return new Uint8Array(await stream.arrayBuffer());
  }

  // Minimal USTAR tar reader: 512-byte header blocks, octal size at [124,136),
  // data padded to 512. Returns regular-file entries [{name, bytes}]; dirs and
  // other typeflags are skipped.
  function untar(bytes) {
    const out = [];
    let off = 0;
    while (off + 512 <= bytes.length) {
      const header = bytes.subarray(off, off + 512);
      // Two zero blocks mark end-of-archive; a zero name means padding.
      if (header.every((b) => b === 0)) break;
      let name = dec.decode(header.subarray(0, 100)).replace(/\0.*$/, "");
      // Honor the USTAR prefix field (155 bytes at 345) for long paths.
      const prefix = dec.decode(header.subarray(345, 500)).replace(/\0.*$/, "");
      if (prefix) name = prefix + "/" + name;
      const sizeStr = dec.decode(header.subarray(124, 136)).replace(/[\0 ]/g, "");
      const size = parseInt(sizeStr, 8) || 0;
      const typeflag = String.fromCharCode(header[156]);
      off += 512;
      if ((typeflag === "0" || typeflag === "\0") && name && !name.endsWith("/")) {
        out.push({ name, bytes: bytes.subarray(off, off + size) });
      }
      off += Math.ceil(size / 512) * 512;
    }
    return out;
  }

  // Parse a record file's text into an array of documents.
  function parseRecords(name, text) {
    const ext = (name.match(REC_EXT) || [, ""])[1].toLowerCase();
    if (ext === "jsonl" || ext === "ndjson") {
      const docs = [];
      for (const line of text.split("\n")) {
        const s = line.trim();
        if (s) docs.push(JSON.parse(s));
      }
      return docs;
    }
    const v = JSON.parse(text); // .json / .jsons
    return Array.isArray(v) ? v : [v];
  }

  function sanitize(s) {
    return String(s).replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 200) || "doc";
  }
  function stemOf(base) {
    return base.replace(REC_EXT, "").replace(/\.(gz|zst)$/i, "");
  }
  // A document's natural key: an id-like field if present, else the fallback.
  function docKey(doc, fallback) {
    if (doc && typeof doc === "object" && !Array.isArray(doc)) {
      for (const f of ["id", "_id", "uuid", "key"]) {
        if (typeof doc[f] === "string" || typeof doc[f] === "number") return sanitize(doc[f]);
      }
    }
    return sanitize(fallback);
  }

  // Given a list of {name, bytes(Uint8Array)}, produce the {default:{ks:{...}}}
  // datastore tree plus stats. opts: {maxDocs, maxBytes}.
  async function filesToDatastore(files, opts) {
    opts = opts || {};
    const maxDocs = opts.maxDocs || 200000;
    const maxBytes = opts.maxBytes || 64 * 1024 * 1024;
    const keyspaces = {};
    const stats = { docs: 0, bytes: 0, keyspaces: 0, files: 0, skipped: [], truncated: false };

    // Expand any archives / gzip wrappers into a flat list of record entries,
    // each tagged with a keyspace name and whether it's a per-file single doc.
    async function expand(name, bytes, sink) {
      let n = name, b = bytes;
      if (/\.gz$/i.test(n)) { b = await gunzip(b); n = n.replace(/\.gz$/i, ""); }
      else if (/\.zst$/i.test(n)) { stats.skipped.push(name + " (zstd unsupported)"); return; }
      if (/\.tar$/i.test(n)) {
        for (const e of untar(b)) await expand(e.name, e.bytes, sink);
        return;
      }
      if (/\.tgz$/i.test(n)) { // .tgz was gunzipped above? no -- handle .tgz directly
        for (const e of untar(await gunzip(bytes))) await expand(e.name, e.bytes, sink);
        return;
      }
      if (!REC_EXT.test(n)) { stats.skipped.push(name + " (unsupported type)"); return; }
      sink(n, b);
    }

    const entries = [];
    for (const f of files) {
      try { await expand(f.name, f.bytes, (n, b) => entries.push({ name: n, bytes: b })); }
      catch (e) { stats.skipped.push(f.name + " (" + e.message + ")"); }
    }

    for (const { name, bytes } of entries) {
      stats.files++;
      const slash = name.lastIndexOf("/");
      const dir = slash >= 0 ? name.slice(0, slash) : "";
      const base = slash >= 0 ? name.slice(slash + 1) : name;
      const stem = stemOf(base);
      // Keyspace = top directory segment if nested (tar), else the file stem.
      const ksName = sanitize(dir ? dir.split("/")[0] : stem);
      const ks = keyspaces[ksName] || (keyspaces[ksName] = {});
      let docs;
      try { docs = parseRecords(base, dec.decode(bytes)); }
      catch (e) { stats.skipped.push(name + " (parse: " + e.message + ")"); continue; }

      // A single-object .json living under a directory is one doc keyed by its
      // file stem (picker-style); everything else keys by id-field or index.
      const singleByStem = dir && docs.length === 1 && /\.(json|jsons)$/i.test(base);
      docs.forEach((doc, i) => {
        if (stats.docs >= maxDocs || stats.bytes >= maxBytes) { stats.truncated = true; return; }
        let key = singleByStem ? sanitize(stem) : docKey(doc, docs.length > 1 ? String(i) : stem);
        if (ks[key + ".json"]) key = key + "-" + i; // dedupe collisions
        const text = JSON.stringify(doc);
        ks[key + ".json"] = text;
        stats.docs++; stats.bytes += text.length;
      });
    }

    // Drop empty keyspaces (e.g. a file that parsed to nothing).
    for (const k of Object.keys(keyspaces)) if (!Object.keys(keyspaces[k]).length) delete keyspaces[k];
    stats.keyspaces = Object.keys(keyspaces).length;
    return { tree: { default: keyspaces }, stats };
  }

  g.n1k1Ingest = { filesToDatastore, gunzip, untar, parseRecords };
})(typeof globalThis !== "undefined" ? globalThis : this);
