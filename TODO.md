# TODO

Gist only -- see README "Building the N1QL engine layer" for specifics.

## Next: revive the N1QL engine layer (glue/ + test/, behind `-tags n1ql`)

Investigated 2026/06. Two distinct blockers -- cgo is NOT the hard one:

- cgo (sigar, OpenSSL 3): SOLVED/easy. A prebuilt libsigar.dylib ships in
  "Couchbase Server.app" + openssl@3 via brew. Wiring CGO_CFLAGS/LDFLAGS
  (see below) makes every cgo error vanish. No need to patch/stub cgo.
- **goyacc parser-gen gap (the real blocker)**: query/parser/n1ql ships the
  grammar (n1ql.y) but NOT the generated parser (yyParse/yySymType). Query's
  build.sh runs goyacc at build time and gitignores the output, so `go get`
  never produces it, and you can't generate into the read-only module cache.
- minor: cbft<->cbgt version drift (bump cbgt@master too) -- the usual
  "tags lag HEAD" chase.

CGO recipe that works on this machine:
  CGO_CFLAGS="-I<cb-src>/sigar/include -I$(brew --prefix openssl@3)/include"
  CGO_LDFLAGS="-L<Couchbase Server.app>/Contents/Resources/couchbase-core/lib \
               -lsigar -L$(brew --prefix openssl@3)/lib"

How to source a buildable query (solves the parser-gen gap) -- pick one:
- [ ] **Fast proof**: replace query => local cb checkout, run its build.sh
      once to gen the parser, build with CGO flags. Re-couples locally and
      drops the pinned-versions property, but proves n1k1 vs modern query now.
- [ ] **Reproducible** (keeps pinned versions): fork query (+cbft) at the
      pinned SHA, commit the generated parser, replace => fork. More setup;
      stays pure `go get`. (Also the place to stub cgo if ever wanted -- but
      cgo is easy, so don't.)

## After the engine compiles again
- [ ] Un-gate test/ and see what passes vs the new query version.
- [ ] Revisit the pre-existing SKIP tests: UNNEST + array-as-FROM
      (broke during the CB 6.5 -> 7 upgrade in 2021). `git grep SKIP`.
- [ ] Re-pin couchbase modules to ONE consistent manifest snapshot rather
      than per-module @master (current pins are ~contemporaneous, not exact).

## Housekeeping (low priority)
- [ ] Drop vestigial tmp/ symlinks + committed cpu.pprof / intermed_build.
- [ ] Consider deleting tmp/easy-to-read (regenerable via `make easy-to-read`).
