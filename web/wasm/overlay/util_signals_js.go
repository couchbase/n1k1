//go:build js

// WASM demo stub for couchbase/query/util (see web/wasm/build.sh). GOOS=js has
// no syscall.SIGCONT; this const only feeds a signal-handler registration that
// never fires in a browser.

package util

import "syscall"

const SIGCONT = syscall.Signal(0x13)
