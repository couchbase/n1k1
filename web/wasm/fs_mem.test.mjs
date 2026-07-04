//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License").
//
// Unit tests for wasm/fs_mem.js — the in-memory read-only filesystem the engine
// reads through under GOOS=js. Drives the node-style callback API directly.
//
//   node --test web/wasm/

import { test } from "node:test";
import assert from "node:assert/strict";
import "./fs_mem.js";

// Promisify the callback-style fs methods for assertions.
const call = (fn, ...args) =>
  new Promise((resolve, reject) => fn(...args, (err, res) => (err ? reject(err) : resolve(res))));

function freshFS() {
  return globalThis.installN1k1FS("/data", {
    default: {
      beers: { "a.json": '{"n":1}', "b.json": '{"n":22}' },
      breweries: { "x.json": '{"c":"SF"}' },
    },
  });
}

test("readdir lists namespaces, keyspaces, and docs (sorted)", async () => {
  const fs = freshFS();
  assert.deepEqual(await call(fs.readdir.bind(fs), "/data"), ["default"]);
  assert.deepEqual(await call(fs.readdir.bind(fs), "/data/default"), ["beers", "breweries"]);
  assert.deepEqual(await call(fs.readdir.bind(fs), "/data/default/beers"), ["a.json", "b.json"]);
});

test("stat distinguishes directories from files", async () => {
  const fs = freshFS();
  const dir = await call(fs.stat.bind(fs), "/data/default/beers");
  assert.ok(dir.isDirectory() && !dir.isFile());
  const file = await call(fs.stat.bind(fs), "/data/default/beers/b.json");
  assert.ok(file.isFile() && !file.isDirectory());
  assert.equal(file.size, '{"n":22}'.length);
});

test("stat on a missing path rejects with ENOENT", async () => {
  const fs = freshFS();
  await assert.rejects(() => call(fs.stat.bind(fs), "/data/nope"), (e) => e.code === "ENOENT");
});

test("open/read/close returns file bytes", async () => {
  const fs = freshFS();
  const fd = await call(fs.open.bind(fs), "/data/default/beers/a.json", 0, 0);
  const buf = new Uint8Array(64);
  const n = await call(fs.read.bind(fs), fd, buf, 0, 64, 0);
  assert.equal(new TextDecoder().decode(buf.subarray(0, n)), '{"n":1}');
  await call(fs.close.bind(fs), fd);
});

test("read honors position=null sequential cursor", async () => {
  const fs = freshFS();
  const fd = await call(fs.open.bind(fs), "/data/default/beers/b.json", 0, 0);
  const b1 = new Uint8Array(3), b2 = new Uint8Array(16);
  const n1 = await call(fs.read.bind(fs), fd, b1, 0, 3, null);
  const n2 = await call(fs.read.bind(fs), fd, b2, 0, 16, null);
  const s = new TextDecoder().decode(b1.subarray(0, n1)) + new TextDecoder().decode(b2.subarray(0, n2));
  assert.equal(s, '{"n":22}');
});

test("path normalization collapses . and ..", async () => {
  const fs = freshFS();
  const st = await call(fs.stat.bind(fs), "/data/./default/../default/beers");
  assert.ok(st.isDirectory());
});

test("n1k1MountTree merges a second tree alongside the first", async () => {
  const fs = freshFS();
  globalThis.n1k1MountTree("/more", { default: { extra: { "e.json": "{}" } } });
  assert.deepEqual(await call(fs.readdir.bind(fs), "/more/default"), ["extra"]);
  // original mount still intact
  assert.deepEqual(await call(fs.readdir.bind(fs), "/data/default"), ["beers", "breweries"]);
});

test("writes are ENOSYS (read-only fs)", async () => {
  const fs = freshFS();
  await assert.rejects(() => call(fs.mkdir.bind(fs), "/data/new", 0o755), (e) => e.code === "ENOSYS");
});
