default: run_intermed_build

# Target run_intermed_build builds the intermed_build tool, regenerates
# the intermed/ code, and runs the self-contained core unit tests. This
# is the useful day-to-day development target.
#
# The N1QL-engine integration (glue/ + test/) is gated behind the "n1ql"
# build tag and is NOT exercised here -- see the n1ql target below and
# the "Building & testing" section of the README.
run_intermed_build:
	go vet ./...
	go test -v ./base
	go test .
	go build ./cmd/intermed_build/
	./intermed_build
	go fmt ./...
	go test ./...
	go fmt ./...

# ------------------------------------------------------------------
# Testing targets. Core targets (test/build) are self-contained. The n1ql /
# glue targets exercise the N1QL-engine layer (glue/ + test/), build pure-Go
# (CGO_ENABLED=0), and need the patched query fork -- see patches/README.md.

.PHONY: test build build-glue test-glue test-filestore test-all

# test runs the self-contained core build + vet + unit tests (no external setup).
test: build
	go vet ./...
	go test ./...

# build builds the self-contained core packages.
build:
	go build ./...

# build-glue builds the N1QL-engine layer (glue/ + test/) pure-Go.
build-glue:
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go build -tags n1ql ./glue/... ./test/...

# test-glue runs the glue package unit tests (N1QL engine layer).
test-glue: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./glue

# test-filestore runs just the upstream couchbase/query "filestore" conformance
# corpus (600+ cases under test/filestore/) verbosely: a summary, a grouped
# table of expected non-pass cases, and any unexpected regressions.
test-filestore: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v -run TestFilestoreCases ./test

# test-all runs the whole N1QL-engine layer (glue/ + test/, includes filestore).
test-all: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./glue ./test

# Target easy-to-read parses source code files and generates
# versions that are easier to read in a tmp subdirectory.
easy-to-read:
	mkdir -p ./tmp/easy-to-read
	rm -f ./tmp/easy-to-read/*.go
	for f in ./*.go; do \
       sed -e 's/[Ll]z//g' $$f | \
       sed -e 's/ \/\/ !//g' | \
       sed -e 's/ \/\/ <== .*//g' > ./tmp/easy-to-read/$$f; \
    done
	go fmt ./tmp/easy-to-read

# Target cloc emits lines of code stats.
cloc:
	find . | grep go | grep -v test | grep -v generated | grep -v tmp | \
       xargs cloc --by-file

# Target benchmark-expr-eq runs microbenchmarks on expression eq. These live in
# test/ (which uses glue/), so they need the n1ql tag + the ../n1k1-query fork
# and build pure-Go (CGO_ENABLED=0) -- see patches/README.md.
benchmark-expr-eq:
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -v -tags n1ql -bench=InterpExprStr -benchmem ./test
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -v -tags n1ql -bench=InterpExprEq -benchmem ./test
