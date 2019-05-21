default: run_intermed_build

run_intermed_build:
	go test -v ./base
	go test .
	go build ./cmd/intermed_build/
	./intermed_build
	go fmt ./...
	go test ./test/.
	go fmt ./...
	go test -v ./test/...
	go fmt ./...

# Convert the source files into easier-to-read versions.
easy-to-read:
	mkdir -p ./tmp/easy-to-read
	rm -f ./tmp/easy-to-read/*.go
	for f in ./*.go; do \
       sed -e 's/[Ll]z//g' $$f | \
       sed -e 's/ \/\/ !//g' | \
       sed -e 's/ \/\/ <== .*//g' > ./tmp/easy-to-read/$$f; \
    done
	go fmt ./tmp/easy-to-read

cloc:
	find . | grep go | grep -v test | grep -v generated | grep -v tmp | \
       xargs cloc --by-file

benchmark-expr-eq:
	go test -bench=InterpExprStr -benchmem ./test
	go test -bench=InterpExprEq -benchmem ./test
