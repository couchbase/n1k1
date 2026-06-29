# TODO

Gist only -- see README "Building the N1QL engine layer" for specifics.

## Next: revive the N1QL engine layer (glue/ + test/, behind `-tags n1ql`)
Go deps resolve fully; `make n1ql` stops at two cgo native libs. Pick one:
- [ ] **Wire native libs**: build libsigar (from couchbase-server `sigar/`)
      + OpenSSL 3 (brew), set CGO_CFLAGS/LDFLAGS. Faithful but machine-bound.
- [ ] **Decouple** (preferred): make glue/ import only the pure-Go query pkgs
      (value, expression, parser, algebra, plan/planner); drop server +
      datastore/system so no cgo at all. More work, far more portable.

## After the engine compiles again
- [ ] Un-gate test/ and see what passes vs the new query version.
- [ ] Revisit the pre-existing SKIP tests: UNNEST + array-as-FROM
      (broke during the CB 6.5 -> 7 upgrade in 2021). `git grep SKIP`.
- [ ] Re-pin couchbase modules to ONE consistent manifest snapshot rather
      than per-module @master (current pins are ~contemporaneous, not exact).

## Housekeeping (low priority)
- [ ] Drop vestigial tmp/ symlinks + committed cpu.pprof / intermed_build.
- [ ] Consider deleting tmp/easy-to-read (regenerable via `make easy-to-read`).
