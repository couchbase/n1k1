//go:build js

// WASM demo stub for couchbase/query/util (see web/wasm/build.sh). GOOS=js lacks
// syscall.Getrusage; CpuTimes only feeds optional profiling counters, so zeros
// are a faithful stand-in in the browser.

package util

func CpuTimes() (int64, int64) { return 0, 0 }
