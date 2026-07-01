# `make` (no args) runs the full local test sweep -- see the `test` target below.
default: test

# Target run_intermed_build builds the intermed_build tool, regenerates
# the intermed/ code, and runs the self-contained core unit tests. A quick
# core-only dev loop (it was the old `make` default; the default is now `test`).
#
# The N1QL-engine integration (glue/ + test/) is gated behind the "n1ql"
# build tag and is NOT exercised here -- see the n1ql target below and
# the "Building & testing" section of the README.
run_intermed_build:
	go vet ./...
	go test -v ./base
	go test ./engine
	go build ./cmd/intermed_build/
	./intermed_build
	go fmt ./...
	go test ./...
	go fmt ./...

# ------------------------------------------------------------------
# Testing targets. Core targets (test/build) are self-contained. The n1ql /
# glue targets exercise the N1QL-engine layer (glue/ + test/), build pure-Go
# (CGO_ENABLED=0), and need the patched query fork -- see glue/patches/README.md.

.PHONY: test test-core build intermed build-glue test-glue test-suite test-gsi test-compiler test-all cli install-cli

# test (the default `make` target) is the full local test sweep: the core unit
# tests, the glue N1QL-engine tests, the SQL++ conformance suite, and the
# compiler -- but NOT the larger data-backed gsi suite (see test-all). ~1 minute.
test: test-core test-glue test-suite test-compiler

# test-all is the exhaustive sweep: everything in `test` PLUS the data-backed gsi
# suites via ./test/... (TestGsiSuiteCases + TestGsiSuiteWithCompiler + the
# generated gsi islands). SLOW -- roughly 3-5 minutes total: the gsi corpus
# primary-scans large keyspaces with no index, so each gsi pass alone is ~40-55s.
test-all: test test-gsi

# cli builds the n1k1 command-line tool: a single pure-Go binary (CGO off) that
# runs SQL++ over a file datastore. See cmd/n1k1 and DESIGN-cli.md.
cli: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go build -tags n1ql -o n1k1 ./cmd/n1k1
	@echo 'built ./n1k1 -- try:  ./n1k1 ./test/suite/json   (or:  ./n1k1 -c "SELECT 1+1" .)'

# install-cli installs the n1k1 binary into $(GOBIN) (or $(GOPATH)/bin).
install-cli:
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go install -tags n1ql ./cmd/n1k1

# test-core runs the self-contained core build + vet + unit tests (no external
# setup, no n1ql tag).
test-core: build
	go vet ./...
	go test ./...

# build builds the self-contained core packages.
build:
	go build ./...

# intermed regenerates the gitignored intermed/ package (the compiled-query
# codegen) from engine/*.go -- a prerequisite for building the n1ql/glue layer
# (test/emit imports it). Formerly done by the old default target.
intermed:
	go build ./cmd/intermed_build/
	./intermed_build

# build-glue builds the N1QL-engine layer (glue/ + test/) pure-Go. Regenerates
# intermed/ first so a fresh checkout (where intermed/*.go is gitignored) builds.
build-glue: intermed
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go build -tags n1ql ./glue/... ./test/...

# test-glue runs the glue package unit tests (N1QL engine layer).
test-glue: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v ./glue

# test-suite runs just the SQL++ conformance suite (the upstream couchbase/query
# corpus, 600+ cases under test/suite/) verbosely: a summary, the full SQL++ of
# unsupported queries, exotic-case snippets, a grouped table of expected non-pass
# cases, and any unexpected regressions.
test-suite: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v -run TestSuiteCases ./test

# test-compiler exercises the n1k1 *compiler* end to end. The first step runs
# the two generators -- TestCasesSimpleWithCompiler (hand-built TestCasesSimple
# Op trees) and TestSuiteWithCompiler (Op trees the glue layer derives from real
# SQL++ conformance-suite queries) -- which emit Go source into test/tmp/
# (gitignored). The middle step `go fmt`s that generated source so it's readable
# (the emitter doesn't indent); it's run from inside test/tmp because `go fmt
# ./test/tmp` from the module root would trigger go.sum/module verification.
# The last step compiles and runs the generated package, whose TestGeneratedN /
# TestGeneratedFS_N funcs execute the *compiled* query and compare its results.
# The steps MUST stay ordered so ./test/tmp never compiles a stale copy.
test-compiler: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run 'TestCasesSimpleWithCompiler|TestSuiteWithCompiler|TestQueryCasesWithCompiler' ./test
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./test/tmp

# test-gsi runs the data-backed gsi corpus (test/suite/json-gsi; see
# DESIGN-testing.md): the interpreter suite (verbose -- prints the summary +
# grouped expected-non-pass table), then the compiler-generation pass, then the
# generated gsi islands in test/tmp. SLOW -- ~2-3 minutes: the gsi keyspaces are
# large and there's no index, so every pass is a full primary scan.
test-gsi: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v -run TestGsiSuiteCases ./test
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run TestGsiSuiteWithCompiler ./test
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./test/tmp

# Target easy-to-read parses source code files and generates
# versions that are easier to read in a tmp subdirectory.
easy-to-read:
	mkdir -p ./tmp/easy-to-read
	rm -f ./tmp/easy-to-read/*.go
	for f in engine/*.go; do \
       sed -e 's/[Ll]z//g' $$f | \
       sed -e 's/ \/\/ !//g' | \
       sed -e 's/ \/\/ <== .*//g' > ./tmp/easy-to-read/$$(basename $$f); \
    done
	go fmt ./tmp/easy-to-read

# Target cloc emits lines of code stats.
cloc:
	find . | grep go | grep -v test | grep -v generated | grep -v tmp | \
       xargs cloc --by-file

# Target benchmark-expr-eq runs microbenchmarks on expression eq. These live in
# test/ (which uses glue/), so they need the n1ql tag + the ../n1k1-query fork
# and build pure-Go (CGO_ENABLED=0) -- see glue/patches/README.md.
benchmark-expr-eq:
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -v -tags n1ql -bench=InterpExprStr -benchmem ./test
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -v -tags n1ql -bench=InterpExprEq -benchmem ./test

# bench runs the Phase-1 engine benchmarks (see DESIGN-benchmark.md +
# test/benchmark/README.md): scan/filter/project throughput + allocs, and
# GROUP BY across the spill point. Pure-Go (CGO_ENABLED=0), n1ql-gated.
bench: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run=xxx -bench=. -benchmem ./test/benchmark

# bench-spill pins the distinct-key cardinality at which GROUP BY's rhmap/store
# grows its metadata slots onto disk (a "*_slots_*" temp file).
bench-spill: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run TestSpillPoint -v ./test/benchmark

# bench-compiler is Phase 2: interpreted vs compiled. TestGenerateBenchmarks
# emits paired BenchmarkInterp_X / BenchmarkCompiled_X funcs into test/tmp (the
# latter is the operators fused inline), then they run side by side -- the diff
# is the compilation payoff (fewer allocs from fusion + lifted-var reuse).
bench-compiler: build-glue
	rm -f test/tmp/*.go
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run TestGenerateBenchmarks ./test/benchmark
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run=xxx -bench=Benchmark -benchmem -benchtime=30s ./test/tmp
