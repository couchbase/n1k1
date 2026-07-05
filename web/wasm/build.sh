#!/bin/sh
#  Copyright (c) 2026 Couchbase, Inc.
#  Licensed under the Apache License, Version 2.0 (the "License").
#
# Builds the n1k1 WebAssembly demo: web/n1k1.wasm + web/wasm_exec.js.
#
# The engine is pure Go, so it cross-compiles to GOOS=js/GOARCH=wasm -- with two
# kinds of patch, both because a browser has no real syscalls:
#
#   1. Build-tag guards. glue/idx_si.go (bbolt GSI) and glue/idx_fts.go (bleve
#      FTS) use mmap/flock; they're excluded under `//go:build !wasm` and
#      glue/idx_wasm.go supplies the few symbols the core still references. No
#      external state -- committed to the tree.
#
#   2. Local dependency patches (this script). A handful of dependency files
#      call syscalls GOOS=js lacks (rusage, rlimit, mmap). We copy those modules
#      to a scratch dir, add js-tagged stubs, and point `go.mod` replaces at the
#      copies. These edits are host-specific and UNCOMMITTED (like the EE stubs
#      in DESIGN-testing.md). Re-run this script to (re)create them.
#
# Usage:  sh web/wasm/build.sh        (run from the repo root)
set -e

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"

SCRATCH=${N1K1_WASM_SCRATCH:-"$ROOT/.wasm-mods"}
OVERLAY="$ROOT/web/wasm/overlay"

echo ">> scratch module dir: $SCRATCH"
mkdir -p "$SCRATCH"

# --- 1. Local patched copy of the couchbase/query fork (rlimit/rusage/signals) -
QUERY_SRC=$(go list -m -f '{{.Dir}}' github.com/couchbase/query)
QUERY_DST="$SCRATCH/query"
if [ ! -d "$QUERY_DST" ]; then
  echo ">> copying $QUERY_SRC -> $QUERY_DST (large; one time)"
  cp -R "$QUERY_SRC" "$QUERY_DST"
  chmod -R u+w "$QUERY_DST"
  # Narrow the unix-only files so GOOS=js falls through to our js stubs.
  sed -i.bak -e 's|^//go:build !windows$|//go:build !windows \&\& !js|' "$QUERY_DST/logging/platform.go"
  sed -i.bak -e 's|^//go:build !windows$|//go:build !windows \&\& !js|' "$QUERY_DST/util/signals.go"
  sed -i.bak -e 's|^//go:build !windows && !solaris$|//go:build !windows \&\& !solaris \&\& !js|' "$QUERY_DST/util/cpu_times.go"
  rm -f "$QUERY_DST"/logging/platform.go.bak "$QUERY_DST"/util/signals.go.bak "$QUERY_DST"/util/cpu_times.go.bak
  cp "$OVERLAY/logging_js.go" "$QUERY_DST/logging/platform_js.go"
  cp "$OVERLAY/util_signals_js.go"     "$QUERY_DST/util/signals_js.go"
  cp "$OVERLAY/util_cpu_js.go"   "$QUERY_DST/util/cpu_times_js.go"
fi

# --- 2. Local patched copy of edsrzf/mmap-go (add a js implementation) ---------
MMAP_SRC=$(go list -m -f '{{.Dir}}' github.com/edsrzf/mmap-go)
MMAP_DST="$SCRATCH/mmap-go"
if [ ! -d "$MMAP_DST" ]; then
  echo ">> copying $MMAP_SRC -> $MMAP_DST"
  cp -R "$MMAP_SRC" "$MMAP_DST"
  chmod -R u+w "$MMAP_DST"
  cp "$OVERLAY/mmap_js.go" "$MMAP_DST/mmap_js.go"
fi

# --- 3. Point go.mod replaces at the local copies (uncommitted) ----------------
go mod edit -replace "github.com/couchbase/query=$QUERY_DST"
go mod edit -replace "github.com/edsrzf/mmap-go=$MMAP_DST"

# --- 4. Build the wasm binary + ship wasm_exec.js ------------------------------
echo ">> building web/n1k1.wasm"
GOOS=js GOARCH=wasm CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' \
  go build -tags n1ql -ldflags="-s -w" -o "$ROOT/web/n1k1.wasm" ./web/wasm/

cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" "$ROOT/web/wasm_exec.js"

echo ">> done:"
ls -lh "$ROOT/web/n1k1.wasm" "$ROOT/web/wasm_exec.js"
echo ">> serve with:  (cd web && python3 -m http.server 8080)  then open http://localhost:8080/"
