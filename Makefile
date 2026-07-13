default: test

.PHONY: test test-all test-core test-glue test-compiler test-suite test-suite-all rules-test cli install-cli build build-glue build-intermed run-intermed-build

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

# build builds the self-contained core packages. Regenerates intermed/ first:
# intermed/*.go is gitignored and generated from engine/*.go, so `go build ./...`
# would otherwise compile a stale (or absent) generated file and fail whenever a
# base symbol it references changes.
build: build-intermed
	go build ./...

# build-glue builds the n1ql-engine (cbq-engine) integrations (glue/ +
# test/) pure-Go. Regenerates intermed/ first so a fresh checkout
# (where intermed/*.go is gitignored) builds. Also ensures test/tmp/
# exists: it's gitignored (absent on a fresh checkout), but the
# *WithCompiler generators WriteFile their emitted Go source into it and
# won't create the parent dir, so the suite/compiler targets need it here.
build-glue: build-intermed
	mkdir -p test/tmp
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go build -tags n1ql ./glue/... ./test/...

# intermed regenerates the gitignored intermed/ package (the compiled-query
# codegen) from engine/*.go -- a prerequisite for building the glue/ layer.
build-intermed:
	go build ./cmd/intermed_build/
	./intermed_build

# test is a fast & local.
test: test-core test-suite

# test-all is the full sweep -- many minutes total.
test-all: test-core test-glue test-cli test-suite-all

# test-core runs the self-contained core build + vet + tests (no external
# setup, no n1ql tag).
test-core: build
	go vet ./...
	go test -v ./...

# test-glue runs the glue package unit tests.
test-glue: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v ./glue

# test-cli runs the CLI (cmd/n1k1) tests. Every cmd/n1k1 file is //go:build n1ql,
# so test-core's untagged `go test ./...` SILENTLY skips the package (build
# constraints exclude all files -- it isn't even listed). Run them explicitly with
# the tag so CLI regressions (flags, dot-commands, .prepare) are actually caught.
test-cli: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v ./cmd/n1k1

# test-compiler exercises the n1k1 *compiler* end to end. The first
# step runs the two generators -- TestCasesSimpleWithCompiler
# (hand-built TestCasesSimple Op trees) and TestSuiteWithCompiler (Op
# trees the glue layer derives from real SQL++ conformance-suite
# queries) -- which emit Go source into test/tmp/ (gitignored). The last
# step compiles and runs the generated package, whose TestGeneratedN /
# TestGeneratedFS_N funcs execute the *compiled* query and compare its
# results. The steps MUST stay ordered so ./test/tmp never compiles a
# stale copy.
#
# It does NOT regenerate the data-backed gsi cases (TestGsiSuiteWithCompiler),
# so the run SKIPS TestGeneratedGsiFS_* -- otherwise it would exercise a stale
# gsi generated file against the current runtime and report phantom failures.
# The gsi compiled differential is regenerated + run by test-suite-all.
test-compiler: build-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v -run 'TestCasesSimpleWithCompiler|TestSuiteWithCompiler|TestQueryCasesWithCompiler' ./test
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -skip TestGeneratedGsiFS ./test/tmp

# test-suite runs the main SQL++ conformance suite (based on the
# upstream couchbase/query corpus.
test-suite: test-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v -skip 'TestCasesSimpleWithCompiler|TestSuiteWithCompiler|TestQueryCasesWithCompiler|TestGsiSuite' ./test
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -skip TestGeneratedGsiFS ./test/tmp

# test-suite-all runs the data-backed gsi corpus; this is NOT testing GSI, but is
# based on data and test cases originally from the gsi corpus; see DESIGN-testing.md.
test-suite-all: test-glue
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v ./test
	cd test/tmp && go fmt
	CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./test/tmp

# rules-test runs the PREPARE++ phase-7 GOLDEN-FIXTURE runner (`.multi test`) over the
# example recipe corpus (cmd/n1k1/testdata/detectors), failing the build on ANY recipe
# FAIL -- the CI pattern a real detector-recipe repo copies (it drives the built binary,
# exactly as an ops pipeline would). `.multi test` is HERMETIC: it builds its own temp
# fixture keyspaces per recipe, so the datastore dir argument is unused (the corpus dir
# is passed only to satisfy the CLI's startup open; it is never queried). Kept STANDALONE
# rather than wired into test-all: test-all is a pure `go test` sweep, whereas this drives
# the built ./n1k1 binary; the same logic is also covered by `go test` in test-cli
# (cmd/n1k1/rules_test.go's TestRulesTest*). The command exits non-zero on any FAIL
# (cli sawError -> exit 1), which fails this make target.
rules-test: cli
	./n1k1 -c '.multi test --corpus cmd/n1k1/testdata/detectors' cmd/n1k1/testdata/detectors

# ------------------------------------------------------------------

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

# ------------------------------------------------------------------

# cloc emits lines of code stats.
cloc:
	find . | grep go | grep -v test | grep -v generated | grep -v tmp | grep -v wasm-mods | grep -v claude | \
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
run-intermed-build:
	go vet ./...
	go test -v ./base
	go test ./engine
	go build ./cmd/intermed_build/
	./intermed_build
	go fmt ./...
	go test ./...
	go fmt ./...

