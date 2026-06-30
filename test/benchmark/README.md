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

## Not here yet (later phases)

- **Compiled queries** — Phase 2: run the same query interpreted vs the
  compiler-generated Go, to measure operator fusion / lifted-var reuse.
- **vs couchbase/query** — Phase 3: a standalone cbq-engine over the same JSON
  dir (`-datastore dir:...`), or a fork-patched in-process timing hook.

## Notes

- `gen.go` makes one corpus-like "contact" doc shape; the `g` field is a
  tunable-cardinality grouping key.
- Scan `reps` cheaply amplifies row count without growing the source string
  (used for throughput/garbage scaling); distinct cardinality (for spill) comes
  from distinct `g` values, which `reps` does not multiply.
