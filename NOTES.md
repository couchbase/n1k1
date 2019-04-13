To generate a compiler...

First, preprocess the *.go files by translating any lazy lines that
are inside a func body into a printf's.  Non-lazy vars are turned into
printf'ed placeholder vars.

The compiler is then executed with a query, where the captured output
the query compiled to code.

ISSUES...
- SIMD optimizations possible?
- batching optimizations?
- col versus row optimizations?
- multi-threading optimizations?
- multiple types?
- leverage types learned during expression processing?

DEV...
- go test ./... && go build ./cmd/n1k1_build/ && ./n1k1_build