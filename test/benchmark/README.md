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

Findings (100K rows/op; stable across `-count=2`):

| query | interpreted | compiled | |
|---|---|---|---|
| ScanFilterProject | ~18.5ms, 34 allocs/op | ~19.1ms, 20 allocs/op | ~3% slower, **41% fewer allocs** |
| GroupBy | ~13.8ms, 42 allocs/op | ~12.7ms, 29 allocs/op | ~8% faster, **31% fewer allocs** |

- **Fusion + lifted-var reuse cut allocations** by ~30–40% (fewer closures /
  reused buffers) — the DESIGN.md claim holds on the allocation axis.
- **Wall-time is roughly equal** at these shapes/scales: removing per-op
  function-call overhead is swamped by the per-row cost of JSON parsing and
  value handling. Useful signal: to go faster, optimize parsing, not just fuse.

## Not here yet

- **vs couchbase/query** — Phase 3: a standalone cbq-engine over the same JSON
  dir (`-datastore dir:...`), or a fork-patched in-process timing hook.

## Notes

- `gen.go` makes one corpus-like "contact" doc shape; the `g` field is a
  tunable-cardinality grouping key.
- Scan `reps` cheaply amplifies row count without growing the source string
  (used for throughput/garbage scaling); distinct cardinality (for spill) comes
  from distinct `g` values, which `reps` does not multiply.
