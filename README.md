n1k1 is an experimental query compiler and execution engine for N1QL.

-------------------------------------------------------
DEV SHORTCUT...

    go test . && go build ./cmd/intermed_build/ && ./intermed_build && go test ./... && go test -v ./... && go fmt ./...

-------------------------------------------------------
The way n1k1 works and how intermed_build generates a N1QL compiler...

- 1: First, take a look at the n1k1/*.go files.  You'll see a simple,
bare-bones "N1QL" query-plan interpreter.  It basically recursively
walks down through a query-plan tree, and executes it by pushing (or
yield()'ing) data records from leaf nodes (e.g., a scan) up to higher
nodes in the query-plan tree.

- 1.1: As part of that, you'll also see some variables and functions
that follow a naming convention with "lazy" in their names.  That's a
marker that tells us whether some variables are late-bound (they need
actual data records), versus other variables that are not late-bound
(they only need "early" information from the query-plan).

- 1.2: Of note, the n1k1/*.go files are written in a careful subset of
golang.  It's legal golang code, but it follows more rules and
conventions (like the "lazy" names) to make parsing by n1k1's
intermed_build tool easy.

- 2: The intermed_build tool parses the "lazy" conventions and other
markers (e.g., special code comments) from the n1k1/*.go source code
in order to translate that interpreter code into a intermediary,
helper package, called n1k1/intermed.

- 2.1: The n1k1/intermed package will be used later by the final n1k1
compiler.

- 2.2: The way intermed_build works is that it processes the n1k1/*.go
source files line-by-line, and translates any "lazy" lines into a
printf's.  Non-lazy expressions are turned into printf'ed placeholder
vars.  Non-lazy lines are emitted as-is, as they are "early bound".

- 3: Finally, the n1k1 compiler, which uses that generated
n1k1/intermed package, will take the user's input of a N1QL query-plan
and will emit *.go code (or possibly other languages) that can
efficiently execute that query-plan.  The user's input query-plan is
the same query-plan tree that's used by the interpreter from step #1
above, as all this was "born" originally from that interpreter.

------------------------------------------
TODO...
- conversion of real N1QL query-plan into n1k1 query-plan
- SIMD optimizations possible?
- batching (or staging) optimizations?
- prefetching optimizations?
- lifting vars to avoid local func calls
- the yield callback might return slice that next yield
  can place data into, to avoid append-copying items?
- col versus row optimizations?
- multi-threading optimizations?
- multiple types?
- types learned during expression processing?
- positional fields versus access to the full record?
- perhaps the 0'th field might represent the full record?
- emit other languages?
- early stop when an error or LIMIT is reached?
- early stop when processing is canceled?
- compiled accessor(s) to a given JSON-path in a raw []byte value?
- learnings from SIMD-json tricks?

ISSUES...
- outer joins when one of the tables is empty incorrectly
  does not produce results from the non-empty table.
