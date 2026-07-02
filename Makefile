default: test

.PHONY: test test-core test-n1ql test-n1ql-all test-glue test-suite test-suite-gsi test-compiler test-all cli install-cli build build-glue build-intermed

# VERSION is `git describe` of the source tree at build time, injected into the
# CLI via -ldflags so `n1k1 -version` reports it. Falls back to "dev" outside a
# git checkout. (Dependency SHAs come from the embedded build info at runtime --
# no `go mod tidy` needed, so the go.mod `replace` pins stay untouched.)
VERSION := $(shell git describe --long --tags --always --dirty 2>/dev/null || echo dev)
VERSION_LDFLAGS := -X main.version=$(VERSION)

# cli builds the n1k1 command-line tool: a single pure-Go binary (CGO off) that
# runs SQL++ queries over local files. See cmd/n1k1 and DESIGN-cli.md.
cli: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go build -tags n1ql -ldflags "$(VERSION_LDFLAGS)" -o n1k1 ./cmd/n1k1
	@echo 'built ./n1k1 -- try: ./n1k1 ./test/suite/json (or: ./n1k1 -c "SELECT 1+1" .)'

# install-cli installs the n1k1 binary into $(GOBIN) (or $(GOPATH)/bin).
install-cli:
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go install -tags n1ql -ldflags "$(VERSION_LDFLAGS)" ./cmd/n1k1

# build builds the self-contained core packages.
build:
	go build ./...

# build-glue builds the n1ql-engine (cbq-engine) integrations (glue/ +
# test/) pure-Go. Regenerates intermed/ first so a fresh checkout
# (where intermed/*.go is gitignored) builds.
build-glue: build-intermed
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go build -tags n1ql ./glue/... ./test/...

# intermed regenerates the gitignored intermed/ package (the compiled-query
# codegen) from engine/*.go -- a prerequisite for building the glue/ layer.
build-intermed:
	go build ./cmd/intermed_build/
	./intermed_build

# test is a fast local sweep: the self-contained core (test-core) plus every
# n1ql-tagged test EXCEPT the slow gsi corpus (test-n1ql).
test: test-core test-n1ql

# test-all is the full sweep -- many minutes total. Like test, but ALSO runs the
# slow gsi corpus, both interpreter and compiler (test-n1ql-all supersedes
# test-n1ql). Every test runs exactly once -- no target reruns another's tests.
test-all: test-core test-n1ql-all

# test-n1ql runs every n1ql-tagged test EXCEPT the slow gsi corpus: the ./glue
# unit tests and the WHOLE ./test package (no -run filter, so nothing is missed
# -- interpreter suites, the WithCompiler generators, and every unit test), then
# compiles+runs the generated code the generators emit into test/tmp. The gsi
# corpus (TestGsiSuite*) and its generated funcs (TestGeneratedGsiFS*) are
# skipped -- test-all covers them via test-n1ql-all.
test-n1ql: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -skip TestGsiSuite ./test
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -skip TestGeneratedGsiFS ./test/tmp

# test-n1ql-all is test-n1ql plus the slow gsi corpus: the whole ./test package
# (incl. TestGsiSuiteCases + TestGsiSuiteWithCompiler) and ALL the generated
# code. SLOW: several minutes (every gsi pass is a full primary scan -- TODO).
test-n1ql-all: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./test
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./test/tmp

# test-core runs the self-contained core build + vet + tests (no external
# setup, no n1ql tag).
test-core: build
	go vet ./...
	go test ./...

# test-glue runs the glue package unit tests.
test-glue: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v ./glue

# test-suite runs the main SQL++ conformance suite (based on the
# upstream couchbase/query corpus, 600+ cases under test/suite/)
# verbosely: a summary, the full SQL++ of unsupported queries,
# exotic-case snippets, a grouped table of expected non-pass cases,
# and any unexpected regressions.
test-suite: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v -run TestSuiteCases ./test

# test-compiler exercises the n1k1 *compiler* end to end. The first
# step runs the two generators -- TestCasesSimpleWithCompiler
# (hand-built TestCasesSimple Op trees) and TestSuiteWithCompiler (Op
# trees the glue layer derives from real SQL++ conformance-suite
# queries) -- which emit Go source into test/tmp/ (gitignored). The last
# step compiles and runs the generated package, whose TestGeneratedN /
# TestGeneratedFS_N funcs execute the *compiled* query and compare its
# results. The steps MUST stay ordered so ./test/tmp never compiles a
# stale copy.
test-compiler: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run 'TestCasesSimpleWithCompiler|TestSuiteWithCompiler|TestQueryCasesWithCompiler' ./test
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./test/tmp

# test-suite-gsi runs the data-backed gsi corpus (test/suite/json-gsi; it is NOT
# testing GSI, but just is based on data and test cases from the gsi corpus; see
# DESIGN-testing.md): the interpreter suite.
#
# SLOW: several minutes, as every pass is currently a full primary scan (TODO).
test-suite-gsi: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v -run TestGsiSuiteCases ./test
	echo test-suite-gsi - BEWARE / SLOW, test-suite-gsi can take minutes!
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run TestGsiSuiteWithCompiler ./test
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./test/tmp

# easy-to-read cleanses generated source code files to be easier to read.
easy-to-read:
	mkdir -p ./tmp/easy-to-read
	rm -f ./tmp/easy-to-read/*.go
	for f in engine/*.go; do \
       sed -e 's/[Ll]z//g' $$f | \
       sed -e 's/ \/\/ !//g' | \
       sed -e 's/ \/\/ <== .*//g' > ./tmp/easy-to-read/$$(basename $$f); \
    done
	go fmt ./tmp/easy-to-read

# cloc emits lines of code stats.
cloc:
	find . | grep go | grep -v test | grep -v generated | grep -v tmp | grep -v claude | \
       xargs cloc --by-file

# ------------------------------------------------------------------

# bench runs basic engine benchmarks (see DESIGN-benchmark.md +
# test/benchmark/README.md).
bench: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run=xxx -bench=. -benchmem ./test/benchmark

# bench-spill determines the point at which rhmap/store begins to spill to disk.
bench-spill: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run TestSpillPoint -v ./test/benchmark

# bench-compiler compares interpreted vs compiled. TestGenerateBenchmarks
# emits paired BenchmarkInterp_X / BenchmarkCompiled_X funcs into test/tmp.
bench-compiler: build-glue
	rm -f test/tmp/*.go
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run TestGenerateBenchmarks ./test/benchmark
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -run=xxx -bench=Benchmark -benchmem -benchtime=30s ./test/tmp

# benchmark-expr-eq runs microbenchmarks on expression eq.
benchmark-expr-eq:
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -v -tags n1ql -bench=InterpExprStr -benchmem ./test
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -v -tags n1ql -bench=InterpExprEq -benchmem ./test

# ------------------------------------------------------------------

# run_intermed_build produces the intermed_build tool, then invokes it
# to regenerate the intermed/ code, and runs related unit tests.
run_intermed_build:
	go vet ./...
	go test -v ./base
	go test ./engine
	go build ./cmd/intermed_build/
	./intermed_build
	go fmt ./...
	go test ./...
	go fmt ./...

