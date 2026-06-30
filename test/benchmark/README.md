# test/benchmark/ — do DESIGN.md's perf techniques work?

Phase 1 of `../../DESIGN-benchmark.md`: in-process, pure-Go benchmarks over
synthetic local data that measure the claims in `../../DESIGN.md`. Each runs the
engine with a **no-op yield** (count rows, discard bytes), so the number is the
engine, not output formatting.

## Run

    make bench          # all engine benchmarks, with -benchmem
    make bench-spill    # pin where GROUP BY spills to disk (verbose)

    # directly:
    CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' \
      go test -tags n1ql -run=xxx -bench=. -benchmem ./test/benchmark

Compare runs with `benchstat` (save `-bench` output to files, then
`benchstat old.txt new.txt`).

## Claim → benchmark

| DESIGN.md claim | Benchmark | Signal |
|---|---|---|
| garbage avoidance / `[]byte` / no boxing | `BenchmarkScan`, `ScanFilter`, `ScanFilterProject` | allocs/op **flat** as rows grow 1K→1M |
| push-based per-row cost | same (rows/s) | throughput holds at scale |
| static-param expr optimization | `ScanFilter` (eq) + `test/BenchmarkInterpExprEq` vs `ExprStr` | low, flat allocs |
| rhmap spill-to-disk + graceful degradation | `TestSpillPoint`, `BenchmarkGroupBy` | spill onset; throughput holds across it |

(Lower-level micro-benchmarks for canonical JSON, value compare, parse, boxing
already live in `base/` and `test/`.)

## Findings so far

Indicative numbers (apple-silicon, `-benchtime=30x`; trends matter, not exact ns):

- **Garbage avoidance — holds.** allocs/op is constant as row count scales 1000×:
  scan **6**, scan+filter **18**, scan+filter+project **37** allocs/op at 1K *and*
  1M rows. The fixed count is pipeline setup; per-row allocation is ~zero.
- **Throughput:** raw scan ~500M rows/s, +filter ~12M, +project ~5M rows/s.
- **Spill point ≈ 4000–5000 distinct keys.** GROUP BY's rhmap/store keeps its
  metadata slots in memory until ~4000 distinct keys (rhmap `StartSize=5303`,
  but `Grow` fires a touch earlier on load factor / MaxDistance), then grows to
  an mmap'd `*_slots_*` file. Above it, temp bytes grow ~linearly (~80 B/key:
  ~4MB at 64K keys, ~20MB at 256K).
- **Graceful degradation — holds.** GROUP BY throughput barely moves across the
  spill boundary: ~4.5M rows/s at 1000 distinct (in-memory) vs ~4.2M at 64000
  (spilled) — ~6% slower while paging to disk, not a cliff.

## Phase 2: interpreted vs compiled

`make bench-compiler` generates, for a fixed query set, paired
`BenchmarkInterp_X` (engine.ExecOp over a baked Op tree) and
`BenchmarkCompiled_X` (the operators fused inline as compiler-generated Go) into
`test/tmp`, run side by side. (Generator: `test/bench_compiler_test.go`'s
`TestGenerateBenchmarks`.)

Each op runs ~5s (30M rows; `bench-compiler` uses `-benchtime=30s`, so per-op
fixed setup is negligible and the delta is the per-row codepath). Findings,
stable across repeats:

| query (30M rows/op) | interpreted | compiled | |
|---|---|---|---|
| ScanFilterProject | ~5.60s, 35 allocs/op | ~5.77s, 21 allocs/op | ~3% slower, **40% fewer allocs** |
| GroupBy | ~4.28s, 174 allocs/op | ~3.74s, ~150 allocs/op | **~13% faster**, ~15% fewer allocs |

- **Fusion + lifted-var reuse cut allocations** (fewer closures / reused
  buffers) on both — the DESIGN.md claim holds on the allocation axis. (allocs/op
  also held ~flat from 100K → 30M rows: 35 vs 34 for the pipeline — garbage
  avoidance again, at 300x scale.)
- **Wall-time is shape-dependent.** GROUP BY (aggregation is function-call-heavy
  per row) gets a clear ~13% from fusion. The scan→filter→project pipeline is
  parse-bound (jsonparser field extraction dominates), so eliminating per-op
  call overhead is marginal there (even slightly slower). Useful signal: for
  scan/filter/project, optimize parsing; fusion pays most for call-heavy ops.

## Not here yet

- **vs couchbase/query** — Phase 3 is **blocked in a stock dev env** (no
  buildable server source tree). The prebuilt `cbq-engine` from Couchbase
  Server.app can't run standalone (it blocks forever on the cbauth/metakv
  settings notifier), an in-process race is impossible (the cgo `query/execution`
  deps were pruned by the pure-Go decouple), and a from-source patched build
  needs the full server module manifest. The blockers, the one-line patch that
  makes a from-source `cbq-engine` run standalone, and a turnkey run recipe are
  documented in `../../DESIGN-benchmark.md` §10. Phase 1/2 stand alone as the
  perf story.

## Notes

- `gen.go` makes one corpus-like "contact" doc shape; the `g` field is a
  tunable-cardinality grouping key.
- Scan `reps` cheaply amplifies row count without growing the source string
  (used for throughput/garbage scaling); distinct cardinality (for spill) comes
  from distinct `g` values, which `reps` does not multiply.
