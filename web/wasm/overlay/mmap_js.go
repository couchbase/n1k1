//go:build js

// Added to github.com/edsrzf/mmap-go via the WASM demo's -overlay (see
// web/wasm/build.sh). The package's mmap()/flush()/lock()/unlock()/unmap()
// helpers are defined per-OS in mmap_unix.go and mmap_windows.go; GOOS=js
// matches neither, so they're undefined and the package won't link under wasm.
//
// There is no real mmap in a browser. This provides an in-memory stand-in so
// the package links. It is only reached if rhmap/store spills to its mmap'd
// backing file, which the demo's small in-memory datasets never trigger; if it
// ever were, the returned buffer is a plain anonymous slice (not file-backed),
// which is why this is a demo-only shim and not a general js port.

package mmap

func mmap(length int, inprot, inflags, fd uintptr, off int64) ([]byte, error) {
	return make([]byte, length), nil
}

func (m MMap) flush() error  { return nil }
func (m MMap) lock() error   { return nil }
func (m MMap) unlock() error { return nil }
func (m MMap) unmap() error  { return nil }
