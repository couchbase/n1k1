# Done

Gist only -- details live in commit messages, README, and code comments.

## 2026/06 -- build modernization (dusted off after ~5 years)
- Go 1.17 -> 1.25; GOTOOLCHAIN=auto fetches it.
- go mod: dropped local-symlink replaces, pinned ~25 couchbase modules
  to real versions. No more dependency on a local couchbase-server tree.
  - Recipe: GOPRIVATE=github.com/couchbase/* + replace => realversion to
    kill the v0.0.0-00010101 placeholders. cbauth/gomemcached/cbft @ master.
- Core (root pkg, base/, intermed/, cmd/) builds + vets + tests clean.
  - Fixed real go vet issues (op_scan.go Errorf, op_window.go self-assign).
- N1QL engine layer (glue/ + test/) gated behind `//go:build n1ql`, so the
  default build is green. tmp/easy-to-read gated `//go:build ignore`.
- Makefile: default = core flow; `make n1ql` = deferred engine build.
- README: documents the whole recipe + the don't-`go mod tidy` warning.
