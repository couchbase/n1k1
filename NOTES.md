First, preprocess the engine.go file by translating any lazy lines
that are inside a func body into a printf's.  Non-lazy vars are turned
into printf'ed placeholder vars.

Then, execute the translated file with a query, capturing output
which is the compiled result.

ISSUES...
- SIMD optimizations possible?
- batching optimizations?
- col versus row optimizations?
- multi-threading optimizations?
- Val becomes []byte instead of string
- multiple types?
- JSON support?
- avoiding memcpy's
