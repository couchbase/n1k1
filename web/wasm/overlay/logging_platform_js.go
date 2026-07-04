//go:build !windows

// Overlay replacement for couchbase/query/logging/platform.go used ONLY by the
// WASM demo build (see web/wasm/build.sh -overlay). The upstream file calls
// syscall.Getrlimit/Setrlimit(RLIMIT_CORE), which don't exist under GOOS=js, so
// the browser build can't compile it. setCoreLimit only raises the core-dump
// size limit -- meaningless in a browser -- so a no-op is a faithful stub.

package logging

// setCoreLimit is a no-op under GOOS=js (wasm has no core-dump rlimit).
func setCoreLimit() {}
