# DESIGN-col-spike

Throwaway "ceiling spike" for `DESIGN-col.md` § *Proposed approach*, Step 2:
does fixed-width columnar beat n1k1's row-at-a-time JSON path, in **pure Go /
no SIMD**, on Apple Silicon (arm64)?

Self-contained module (own `go.mod`, only dep is `buger/jsonparser` — the same
decoder n1k1's row path uses, so the baseline is faithful). Nested module: the
parent n1k1 build ignores it.

```sh
cd DESIGN-col-spike
GOTOOLCHAIN=local GOPROXY=off go test -bench=. -benchmem -benchtime=150ms
```

Benchmarks:
- `Sum_RowJSON_narrow` — n1k1 today: whole JSON doc per record, `GetFloat` per row.
- `Sum_ColJSONArray` — encoding 1: one `[v0,v1,…]` JSON array, parsed per value.
- `Sum_ColFixedWidth` — encoding 2: one `[]byte` of N little-endian float64s.
- `Sum_NativeSlice` — theoretical ceiling: a real `[]float64`.
- `Sum_RowJSON_byWidth` — sweeps doc width (fields/doc); the "vertical stripe" axis.
- `Filter_{RowJSON,ColFixedWidth}` — `price > 500` count.

Results and analysis: see `DESIGN-col.md` § *Spike results*. Headline: fixed-width
is ~44× the row path (at the native-`[]float64` ceiling) with no SIMD, and the
advantage grows to ~730× as documents widen — the win is *skipping JSON parsing*
and *touching only one column stripe*, not vector width.
