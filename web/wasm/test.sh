#!/bin/sh
#  Copyright (c) 2026 Couchbase, Inc.
#  Licensed under the Apache License, Version 2.0 (the "License").
#
# Run the web demo's JavaScript tests with Node's built-in runner.
# e2e.test.mjs skips unless web/n1k1.wasm has been built (web/wasm/build.sh).
#
#   sh web/wasm/test.sh
set -e
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
exec node --test "$ROOT/web/wasm/"*.test.mjs
