#!/bin/sh
#  Copyright (c) 2026 Couchbase, Inc.
#  Licensed under the Apache License, Version 2.0 (the "License").
#
# Runs the headless-browser tests for the SQL++ playground (web/browser.test.mjs) via
# Playwright. Builds the wasm if missing, ensures node deps + a chromium are present, then
# runs the suite. Complements the dep-free node tests (web/wasm/test.sh), which cover the
# engine/fs/ingest but not the browser DOM + Web Worker + "Try It Now" hash flow.
#
# Usage:  sh web/wasm/browser-test.sh        (from the repo root)
set -e

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"

[ -f web/n1k1.wasm ] || sh web/wasm/build.sh

cd web
[ -d node_modules/playwright ] || npm ci --no-audit --no-fund
# Install just the headless chromium shell if it's not cached.
npx playwright install chromium >/dev/null 2>&1 || npx playwright install chromium
npm test
