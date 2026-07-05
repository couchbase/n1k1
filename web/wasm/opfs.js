//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License").
//
// OPFS (Origin Private File System) persistence for the n1k1 WebAssembly demo:
// a cross-browser, origin-private cache for built in-memory secondary indexes.
//
// The engine builds mem indexes by scanning a keyspace (idx_mem.go); on a large
// dataset that scan is the cost. We cache the serialized index blob in OPFS,
// keyed by the datastore's cache path, so a return visit loads it instead of
// re-scanning. The engine embeds the source signature in each blob and rejects a
// stale one, so a changed dataset simply rebuilds.
//
// Why this shape: on the main thread OPFS is async-only (sync access handles
// need a Worker), and Go's fs reads are synchronous — so JS orchestrates AROUND
// the engine: preload cached blobs into the in-memory fs BEFORE a query builds
// the indexes, then persist any freshly-built (cache-miss) blobs AFTER. Because
// the index is RAM-resident during the query, no Worker is needed.
//
// Exposes globalThis.n1k1OPFS = { available, preload(cachePlan), persist() }.
// All methods degrade to no-ops where OPFS is unavailable, so the demo works
// everywhere; the cache is a pure optimization.

(function (g) {
  "use strict";

  const available =
    typeof navigator !== "undefined" &&
    navigator.storage &&
    typeof navigator.storage.getDirectory === "function";

  // OPFS filenames can't contain "/"; flatten a cache path to a safe key.
  function keyFor(path) {
    return "idx_" + String(path).replace(/[^A-Za-z0-9._-]/g, "_");
  }

  async function get(name) {
    try {
      const root = await navigator.storage.getDirectory();
      const fh = await root.getFileHandle(name); // throws if absent
      const file = await fh.getFile();
      return new Uint8Array(await file.arrayBuffer());
    } catch (e) {
      return null; // missing / unreadable -> cache miss
    }
  }

  async function put(name, bytes) {
    try {
      const root = await navigator.storage.getDirectory();
      const fh = await root.getFileHandle(name, { create: true });
      const w = await fh.createWritable();
      await w.write(bytes);
      await w.close();
      return true;
    } catch (e) {
      return false; // quota/eviction/permission -> silently skip (cache is optional)
    }
  }

  function b64ToBytes(b64) {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  // preload: for each planned index, fetch its cached blob from OPFS and mount it
  // into the in-memory fs at the cache path, so the next query's index build
  // loads it instead of scanning. cachePlan is n1k1OpenDir's .cachePlan.
  async function preload(cachePlan) {
    if (!available || !Array.isArray(cachePlan) || !cachePlan.length) return 0;
    let hits = 0;
    for (const entry of cachePlan) {
      const bytes = await get(keyFor(entry.path));
      if (bytes && bytes.length) {
        globalThis.n1k1MountFile(entry.path, bytes);
        hits++;
      }
    }
    return hits;
  }

  // persist: drain the engine's freshly-built index blobs and write them to OPFS
  // for next time. No-op (but still drains) where OPFS is unavailable.
  async function persist() {
    let blobs;
    try { blobs = JSON.parse(globalThis.n1k1TakeIndexBlobs()); }
    catch (e) { return 0; }
    const paths = Object.keys(blobs || {});
    if (!available || !paths.length) return 0;
    let saved = 0;
    for (const path of paths) {
      if (await put(keyFor(path), b64ToBytes(blobs[path]))) saved++;
    }
    return saved;
  }

  // ---- Source persistence: remember the last user-loaded dataset -------------
  // Persist the mounted datastore tree so a page reload can restore it without
  // re-dropping. Single slot (the most recent). Best-effort + evictable, like
  // the index cache. Payload: { label, mount, tree }.
  const SOURCE_KEY = "source.json";
  async function saveSource(payload) {
    if (!available) return false;
    try { return await put(SOURCE_KEY, new TextEncoder().encode(JSON.stringify(payload))); }
    catch (e) { return false; } // quota/serialize failure -> skip (persistence is optional)
  }
  async function loadSource() {
    if (!available) return null;
    const bytes = await get(SOURCE_KEY);
    if (!bytes || !bytes.length) return null;
    try { return JSON.parse(new TextDecoder().decode(bytes)); } catch (e) { return null; }
  }
  async function forgetSource() {
    if (!available) return;
    try {
      const root = await navigator.storage.getDirectory();
      await root.removeEntry(SOURCE_KEY);
    } catch (e) { /* absent already / unsupported */ }
  }

  g.n1k1OPFS = { available, preload, persist, saveSource, loadSource, forgetSource };
})(typeof globalThis !== "undefined" ? globalThis : this);
