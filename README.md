n1k1 is an experimental query compiler and execution engine for N1QL.

-------------------------------------------------------
DEV SHORTCUT...

    go test . && go build ./cmd/n1k1_build/ && ./n1k1_build && go test ./...

-------------------------------------------------------
The way n1k1 works and how n1k1_build generates a N1QL compiler...

1. First, take a look at the n1k1/*.go files.  You'll see a simple,
bare-bones "N1QL" query-plan interpreter.  It basically recursively
walks down through a query-plan tree, and executes it by pushing (or
yield()'ing) data records from leaf nodes (e.g., a scan) up to higher
nodes in the query-plan tree.

As part of that, you'll also see some variables and functions that
follow a naming convention with "lazy" in their names.  That's a
marker that tells us whether some variables are late-bound (they need
actual data records), versus other variables that are not late-bound
(they need "early" information from a query-plan).

Of note, the n1k1/*.go files are written in a careful subset of
golang.  It's legal golang code, but it follows more rules and
conventions (like the "lazy" names) to make parsing by the n1k1_build
tool easy.

2. The n1k1_build tool uses the "lazy" conventions and other markers
(e.g., special code comments) in order to translate the code of the
basic n1k1/*.go interpreter into a "n1k1_build" program.

2.1. The n1k1_build program, when run, generates a golang package
(n1k1/n1k1_compiler) that'll be used later by the final n1k1 compiler.

2.2. The way n1k1_build works is that it processes the n1k1/*.go
source files line-by-line, and translates any "lazy" lines into a
printf's.  Non-lazy expressions are turned into printf'ed placeholder
vars.  Non-lazy lines are emitted as-is, as they are "early bound".

3. Finally, the n1k1 compiler, which uses that generated
n1k1/n1k1_compiler package, will takes user input of a N1QL query-plan
(the same query-plan used by the interpreter in step #1 above).  And,
the n1k1 compiler can emit *.go code that can efficiently execute that
query-plan.

------------------------------------------
TODO...
- conversion of real N1QL query-plan into n1k1 query-plan
- SIMD optimizations possible?
- batching optimizations?
- col versus row optimizations?
- multi-threading optimizations?
- multiple types?
- types learned during expression processing?
- positional fields versus access to the full record?
- perhaps the 0'th field might represent the full record?
