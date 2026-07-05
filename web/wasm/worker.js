//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License").
//
// Web Worker that hosts the n1k1 engine off the main thread. The UI thread stays
// responsive during a query, and live per-operator stats stream back mid-query
// (the engine's OnStats -> globalThis.n1k1EmitStats -> postMessage, while this
// worker thread is busy in the synchronous query; the main thread paints them).
//
// Protocol: the page posts {id, op, args}; we reply {id, ok, result|error}.
// Unsolicited {type:"ready"} (once booted) and {type:"stats", snapshot} (during
// a query) are also posted. Data-mounting, ingestion and OPFS all run here,
// co-located with the wasm, so the main thread only ships SQL and file bytes and
// receives rows/stats. See index.html's WorkerEngine for the client side.
//
// This is a classic (non-module) worker so importScripts + wasm_exec.js work.

// Paths are relative to this file (web/wasm/): the wasm and wasm_exec live one
// level up (web/), the JS helpers alongside.
importScripts("../wasm_exec.js", "fs_mem.js", "ingest.js", "opfs.js", "../samples.js");

// The engine calls these during a query; forward to the UI thread. Stats are
// throttled per-op counter snapshots; rows are JSON-array batches streamed as
// they're produced (so the page renders progressively without holding them all).
self.n1k1EmitStats = function (snapshotJSON) {
  self.postMessage({ type: "stats", snapshot: snapshotJSON });
};
self.n1k1EmitRows = function (batchJSON) {
  self.postMessage({ type: "rows", batch: batchJSON });
};

// Mount the built-in sample, then boot the wasm (it opens /n1k1data at start).
installN1k1FS("/n1k1data", DATASETS);

const go = new Go();
WebAssembly.instantiateStreaming(fetch("../n1k1.wasm"), go.importObject)
  .then((res) => {
    go.run(res.instance); // blocks in Go on select{}; sets globals as it runs
    (function waitReady() {
      if (self.n1k1Ready) self.postMessage({ type: "ready" });
      else if (self.n1k1InitError) self.postMessage({ type: "initError", error: self.n1k1InitError });
      else setTimeout(waitReady, 20);
    })();
  })
  .catch((e) => self.postMessage({ type: "initError", error: String(e) }));

// Higher-level ops (bundled to cut round-trips). Each returns a result object.
const ops = {
  // Open the built-in sample datastore.
  async openBuiltin() {
    const resp = JSON.parse(self.n1k1OpenDir("/n1k1data"));
    if (resp.ok) await self.n1k1OPFS.preload(resp.cachePlan);
    return resp;
  },

  // Ingest dropped/picked files, mount, and open. files: [{name, bytes}].
  async loadFiles({ files, mount, label }) {
    const { tree, stats } = await self.n1k1Ingest.filesToDatastore(files);
    if (!stats.keyspaces) return { ok: false, stats, error: "no documents found" };
    self.n1k1MountTree(mount, tree);
    const resp = JSON.parse(self.n1k1OpenDir(mount));
    if (resp.ok) {
      await self.n1k1OPFS.preload(resp.cachePlan);
      self.n1k1OPFS.saveSource({ label: label || "dataset", mount, tree }); // persist (best-effort)
    }
    return { ...resp, stats };
  },

  // Mount an already-built tree (e.g. the folder picker's keyspace map) + open.
  async openTree({ mount, tree, label }) {
    self.n1k1MountTree(mount, tree);
    const resp = JSON.parse(self.n1k1OpenDir(mount));
    if (resp.ok) {
      await self.n1k1OPFS.preload(resp.cachePlan);
      self.n1k1OPFS.saveSource({ label: label || "folder", mount, tree });
    }
    return resp;
  },

  // Restore the last user dataset persisted in OPFS (if any) and open it.
  async restoreSource() {
    const src = await self.n1k1OPFS.loadSource();
    if (!src || !src.tree || !src.mount) return { ok: true, source: false };
    self.n1k1MountTree(src.mount, src.tree);
    const resp = JSON.parse(self.n1k1OpenDir(src.mount));
    if (resp.ok) await self.n1k1OPFS.preload(resp.cachePlan);
    return { ...resp, source: true, label: src.label || "restored dataset" };
  },

  // Forget the persisted dataset (back to just the built-in sample next time).
  async forgetSource() {
    await self.n1k1OPFS.forgetSource();
    return { ok: true };
  },

  // Run a query; persist any freshly-built index blobs afterward.
  async query({ sql }) {
    const result = JSON.parse(self.n1k1RunQuery(sql));
    self.n1k1OPFS.persist(); // fire-and-forget
    return result;
  },
};

self.onmessage = async (e) => {
  const { id, op, args } = e.data || {};
  if (id == null || !ops[op]) return;
  try {
    self.postMessage({ id, ok: true, result: await ops[op](args || {}) });
  } catch (err) {
    self.postMessage({ id, ok: false, error: String(err && err.message ? err.message : err) });
  }
};
