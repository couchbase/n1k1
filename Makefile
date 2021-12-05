default: run_intermed_build

# Target run_intermed_build builds the intermed_build tool and runs
# unit tests, and is a useful target during development.
run_intermed_build:
	go test -v ./base
	go test -v ./glue
	go test .
	go build ./cmd/intermed_build/
	./intermed_build
	go fmt ./...
	go test ./test/.
	go fmt ./...
	go test -v ./test/...
	go fmt ./...

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

# Target benchmark-expr-eq runs microbenchmarks on expression eq.
benchmark-expr-eq:
	go test -v -bench=InterpExprStr -benchmem ./test
	go test -v -bench=InterpExprEq -benchmem ./test
