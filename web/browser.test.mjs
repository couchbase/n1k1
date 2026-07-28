//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License").
//
// Headless-browser tests for the n1k1 SQL++ playground (web/index.html) -- the parts the
// dep-free node suite (web/wasm/*.test.mjs) can't reach: the DOM + Web Worker + wasm + the
// "Try It Now" URL-hash flow. Run with a real browser via Playwright (chromium-headless-shell),
// since jsdom has no Web Worker / WebAssembly. This is the regression guard for the exact class
// of bug where a shared query got clobbered by the sample on load.
//
//   Prereqs: web/n1k1.wasm built (sh web/wasm/build.sh) + `npm ci` in web/ + a chromium
//            (npx playwright install --with-deps chromium). Run: `npm test` in web/, or
//            sh web/wasm/browser-test.sh. See web/README.md.
//
// Tests run serially (--test-concurrency=1 in package.json): three chromium pages each
// compiling a ~70MB wasm in parallel starves the cold first load past its ready timeout.

import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join, normalize } from "node:path";
import { chromium } from "playwright";

const WEB = dirname(fileURLToPath(import.meta.url));
const MIME = { ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript",
  ".wasm": "application/wasm", ".json": "application/json" };

let server, browser, base;

before(async () => {
  // Minimal static server for web/ -- crucially serving .wasm as application/wasm so
  // WebAssembly.instantiateStreaming works (the same MIME GitHub Pages sends).
  server = http.createServer(async (req, res) => {
    try {
      const rel = normalize(decodeURIComponent(req.url.split("?")[0].split("#")[0])).replace(/^(\.\.[/\\])+/, "");
      const path = join(WEB, rel === "/" ? "index.html" : rel);
      const body = await readFile(path);
      const ext = path.slice(path.lastIndexOf("."));
      res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream" });
      res.end(body);
    } catch { res.writeHead(404); res.end("not found"); }
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  base = `http://127.0.0.1:${server.address().port}/`;
  browser = await chromium.launch();
});

after(async () => { await browser?.close(); server?.close(); });

// Open the playground at `hash`, fail on any page error, and return the page once the
// engine is ready (the Run button un-disables). 30s: the cold wasm compile is slow.
async function open(hash = "") {
  const page = await browser.newPage();
  const errors = [];
  page.on("pageerror", (e) => errors.push(String(e)));
  page.errors = errors;
  await page.goto(base + hash);
  await page.waitForFunction(() => !document.getElementById("run").disabled, { timeout: 30000 });
  return page;
}

const editor = (p) => p.$eval("#sql", (el) => el.value);

// Wait for a query to finish rendering, then return its result rows (parsed from #jsonView,
// which always holds the full JSON regardless of the table/JSON view toggle). #resultMeta
// reads "N row(s) ..." on success and "error" on failure -- the reliable done-signal, unlike
// scraping body.innerText for cell contents. Asserts the engine reported no error.
async function result(page) {
  await page.waitForFunction(
    () => { const m = document.getElementById("resultMeta"); return m && m.textContent.trim() !== ""; },
    { timeout: 20000 });
  const meta = await page.$eval("#resultMeta", (el) => el.textContent);
  const status = await page.$eval("#status", (el) => el.textContent);
  assert.ok(!/error/i.test(meta), `query errored -- status: ${status}`);
  assert.match(meta, /\brows?\b/, `expected a row count, got: ${meta}`);
  return JSON.parse(await page.$eval("#jsonView", (el) => el.textContent || "[]"));
}

test("playground boots, mounts the sample, and a query returns rows", async () => {
  const page = await open();
  // Boot async-populates #sql with a default example AFTER the Run button enables; wait for
  // that to settle before filling, else it clobbers our query (the same ordering hazard the
  // "Try It Now" tests below guard against).
  await page.waitForFunction(() => document.getElementById("sql").value.length > 0, { timeout: 10000 });
  await page.fill("#sql", "SELECT COUNT(*) AS n FROM beers");
  await page.click("#run");
  const rows = await result(page);
  assert.equal(rows.length, 1);
  assert.ok(rows[0].n > 0, `count should be positive, got ${JSON.stringify(rows[0])}`);
  assert.deepEqual(page.errors, []);
  await page.close();
});

test("Try It Now: a self-contained shared query wins over the sample + auto-runs", async () => {
  const sql = 'WITH doc AS ({"nums":[1,2,5,3,5,3,1]})\nSELECT ARRAY_DISTINCT(doc.nums) AS u\nFROM doc';
  const page = await open("#sql=" + encodeURIComponent(sql) + "&title=" + encodeURIComponent("Unique / dedup"));
  // The regression: the editor must hold the SHARED query, not the sample (FROM beers).
  await page.waitForFunction(() => document.getElementById("sql").value.includes("ARRAY_DISTINCT"), { timeout: 10000 });
  const val = await editor(page);
  assert.ok(val.startsWith("WITH doc AS"), `editor: ${val.slice(0, 40)}`);
  assert.ok(!/FROM beers/.test(val), "editor still shows the sample query");
  assert.equal(await page.$eval("#sharedTitle", (el) => el.textContent), "Unique / dedup");
  // Auto-run produced the dedup result [1,2,5,3].
  const rows = await result(page);
  assert.deepEqual(rows, [{ u: [1, 2, 5, 3] }]);
  assert.deepEqual(page.errors, []);
  await page.close();
});

test("Try It Now: a shared dataset mounts under FROM", async () => {
  const sql = "SELECT o.id, o.total FROM orders o WHERE o.total >= 10 ORDER BY o.id";
  const data = { orders: { "o1.json": '{"id":"o1","total":5}', "o2.json": '{"id":"o2","total":22}' } };
  const hash = "#sql=" + encodeURIComponent(sql) + "&data=" + encodeURIComponent(JSON.stringify(data));
  const page = await open(hash);
  await page.waitForFunction(() => document.getElementById("sql").value.includes("FROM orders"), { timeout: 10000 });
  // The shared data mounted + queried: o2 (total 22) matches, o1 (5) is filtered out.
  const rows = await result(page);
  assert.deepEqual(rows, [{ id: "o2", total: 22 }]);
  assert.deepEqual(page.errors, []);
  await page.close();
});
