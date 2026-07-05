//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License").
//
// End-to-end smoke test: load the built n1k1.wasm, mount data through the fs
// shim, and run real SQL++ queries — including the drag-drop ingestion path.
// Skips (does not fail) when web/n1k1.wasm hasn't been built yet.
//
//   sh web/wasm/build.sh && node --test web/wasm/

import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import url from "node:url";
import zlib from "node:zlib";

const webDir = path.resolve(path.dirname(url.fileURLToPath(import.meta.url)), "..");
const wasmPath = path.join(webDir, "n1k1.wasm");

if (!fs.existsSync(wasmPath)) {
  test("e2e (needs a built n1k1.wasm — run web/wasm/build.sh)", { skip: true }, () => {});
} else {
  // ---- one-time engine boot, shared by the subtests below -------------------
  const samplesSrc = fs.readFileSync(path.join(webDir, "samples.js"), "utf8");
  const { DATASETS, SAMPLE_QUERIES } =
    new Function(samplesSrc + "\nreturn { DATASETS, SAMPLE_QUERIES };")();

  await import(url.pathToFileURL(path.join(webDir, "wasm/fs_mem.js")).href);
  await import(url.pathToFileURL(path.join(webDir, "wasm/ingest.js")).href);
  globalThis.installN1k1FS("/n1k1data", DATASETS);

  await import(url.pathToFileURL(path.join(webDir, "wasm_exec.js")).href);
  const go = new globalThis.Go();
  const { instance } = await WebAssembly.instantiate(fs.readFileSync(wasmPath), go.importObject);
  go.run(instance); // blocks in Go on select{}; sets globals as it runs
  for (let i = 0; i < 100 && !globalThis.n1k1Ready; i++) await new Promise((r) => setTimeout(r, 20));
  const run = (sql) => JSON.parse(globalThis.n1k1RunQuery(sql));

  test("engine becomes ready", () => {
    assert.ok(globalThis.n1k1Ready, globalThis.n1k1InitError || "not ready");
  });

  test("all curated sample queries succeed", () => {
    for (const { label, sql } of SAMPLE_QUERIES) {
      const r = run(sql);
      assert.ok(r.ok, `${label}: ${r.error}`);
    }
  });

  test("aggregate + join over the built-in sample", () => {
    const r = run("SELECT bw.country, COUNT(*) AS n FROM beers b JOIN breweries bw ON KEYS b.brewery_id GROUP BY bw.country ORDER BY n DESC");
    assert.ok(r.ok);
    assert.equal(r.rows[0].country, "United States");
  });

  test("n1k1OpenDir lists the built-in keyspaces", () => {
    const r = JSON.parse(globalThis.n1k1OpenDir("/n1k1data"));
    assert.ok(r.ok);
    assert.deepEqual(r.keyspaces.sort(), ["beers", "breweries"]);
  });

  test("in-memory secondary index yields an IndexScan (browser build)", () => {
    // The built-in sample ships a .n1k1/catalog.json with a gsi index on abv;
    // the wasm build has no bbolt, so this exercises idx_mem.go end-to-end.
    const ex = run("EXPLAIN SELECT b.name FROM beers b WHERE b.abv >= 7");
    assert.ok(ex.ok, ex.error);
    const plan = JSON.stringify(ex.rows[0]);
    assert.ok(plan.includes("beers_by_abv"), "mem index not chosen:\n" + plan);
    assert.ok(plan.includes("IndexScan"), "expected an IndexScan operator:\n" + plan);
    // and it returns the right rows (7.0, 7.0, 7.0, 10, 10 ... abv>=7 in the sample)
    const q = run("SELECT COUNT(*) AS n FROM beers b WHERE b.abv >= 7");
    assert.ok(q.ok && q.rows[0].n >= 1, JSON.stringify(q.rows));
  });

  test("OPFS wiring: openDir returns a cache plan; built blobs drain", () => {
    // The Go side of OPFS caching (the browser opfsGet/opfsPut can't run in
    // node). openDir advertises what to cache; a built index queues its blob
    // (the wasm fs write fails) for the host to persist.
    const r = JSON.parse(globalThis.n1k1OpenDir("/n1k1data"));
    assert.ok(Array.isArray(r.cachePlan) && r.cachePlan.length >= 1, "cachePlan: " + JSON.stringify(r.cachePlan));
    assert.ok(r.cachePlan[0].path && r.cachePlan[0].sig, "plan entry has path+sig");
    // A prior indexed query built the mem index(es); their blobs are queued.
    const blobs = JSON.parse(globalThis.n1k1TakeIndexBlobs());
    assert.ok(Object.keys(blobs).length >= 1, "expected queued index blob(s)");
    // decodes as base64
    assert.doesNotThrow(() => Buffer.from(Object.values(blobs)[0], "base64"));
    // draining clears them
    assert.equal(Object.keys(JSON.parse(globalThis.n1k1TakeIndexBlobs())).length, 0);
  });

  test("streaming: n1k1EmitRows receives batches; result omits rows", () => {
    // Register the row-emit hook (the worker does this) and verify the Go side
    // streams rows to it instead of accumulating them in the result.
    const collected = [];
    globalThis.n1k1EmitRows = (batchJSON) => { for (const r of JSON.parse(batchJSON)) collected.push(r); };
    try {
      const r = JSON.parse(globalThis.n1k1RunQuery("SELECT b.name FROM beers b ORDER BY b.name"));
      assert.ok(r.ok, r.error);
      assert.equal(r.streamed, true, "should report streamed");
      assert.ok(r.rows == null, "streamed result must not carry rows");
      assert.equal(r.count, 10, "count reflects all rows");
      assert.equal(collected.length, 10, "all rows arrived via n1k1EmitRows");
      assert.ok(collected[0] && typeof collected[0].name === "string", JSON.stringify(collected[0]));
    } finally {
      delete globalThis.n1k1EmitRows; // don't affect other tests
    }
  });

  test("ingestion → mount → open → query (tar.gz)", async () => {
    const enc = new TextEncoder();
    const tarBlocks = [];
    for (const [name, obj] of [["shops/s1.json", { id: "s1", city: "SF" }], ["shops/s2.json", { id: "s2", city: "NY" }]]) {
      const data = enc.encode(JSON.stringify(obj));
      const h = new Uint8Array(512);
      enc.encodeInto(name, h.subarray(0, 100));
      enc.encodeInto(data.length.toString(8).padStart(11, "0") + " ", h.subarray(124, 136));
      h[156] = "0".charCodeAt(0);
      tarBlocks.push(h);
      const pad = new Uint8Array(512); pad.set(data); tarBlocks.push(pad);
    }
    tarBlocks.push(new Uint8Array(1024));
    const tarU8 = new Uint8Array(tarBlocks.reduce((n, b) => n + b.length, 0));
    let o = 0; for (const b of tarBlocks) { tarU8.set(b, o); o += b.length; }
    const tarGz = new Uint8Array(zlib.gzipSync(Buffer.from(tarU8)));

    const { tree, stats } = await globalThis.n1k1Ingest.filesToDatastore([{ name: "shops.tar.gz", bytes: tarGz }]);
    assert.equal(stats.docs, 2);
    globalThis.n1k1MountTree("/e2e-drop", tree);
    const opened = JSON.parse(globalThis.n1k1OpenDir("/e2e-drop"));
    assert.ok(opened.ok && opened.keyspaces.includes("shops"));
    const q = run("SELECT COUNT(*) AS n FROM shops");
    assert.ok(q.ok && q.rows[0].n === 2, JSON.stringify(q.rows));

    // switching back to the built-in sample still works
    JSON.parse(globalThis.n1k1OpenDir("/n1k1data"));
    assert.equal(run("SELECT COUNT(*) AS n FROM beers").rows[0].n, 10);
  });
}
