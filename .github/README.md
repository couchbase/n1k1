# CI / CD

Three workflows, plus one composite action they share. **No secrets required** —
every module the build actually reaches is public, so CI works out of the box,
including on pull requests from forks.

| Workflow | Trigger | What it does |
|---|---|---|
| [`ci.yml`](workflows/ci.yml) | push to `master`, every PR, nightly | regenerates the recipes doc and fails if it drifted; builds + tests on Linux/macOS/Windows; runs every SQL++ recipe; runs the conformance suite (not on PRs) |
| [`release.yml`](workflows/release.yml) | tag `v*` (or manual) | cross-builds the CLI for 6 targets, packages + checksums them, publishes a GitHub Release |
| [`pages.yml`](workflows/pages.yml) | push to `master` touching `docs/recipes.*` | publishes the SQL++ recipes doc to GitHub Pages |

## One-time setup

Only one step, and only for the docs site:

> Settings → Pages → Build and deployment → Source = **GitHub Actions**

Then run the `Pages (SQL++ recipes)` workflow (or push a `docs/recipes.*` change).
The site is published at `https://couchbase.github.io/n1k1/`, with the recipes page
as the site root. **This is a public page** (the repository is public).

## Local equivalent

CI runs the same recipe a developer does:

```sh
make bootstrap       # stub the placeholder EE modules, regen intermed/, mkdir test/tmp
make test            # core + conformance suite
make recipes         # regenerate docs/recipes.{md,html}
make recipes-check   # run every SQL++ example in the doc (needs ./n1k1)
```

`make bootstrap` appends **machine-local** `replace` lines to `go.mod`; run
`git checkout go.mod` before committing or rebasing. See
[`docs/design/DESIGN-testing.md`](../docs/design/DESIGN-testing.md).

## Notes

- **Why a bootstrap step exists.** `go.mod` requires seven placeholder EE modules
  pinned at the `v0.0.0-00010101…` non-version, whose `go.mod` files live only in
  Couchbase's internal repo-sync tree. Nothing in the CE build imports them, but the
  module-graph loader still demands each one, so `scripts/bootstrap.sh` points every
  one at an empty local stub module.
- **One private module is tolerated by pruning.** `github.com/couchbase/cbftx` is
  private and *indirect*, and no package the build reaches imports it, so Go's module
  graph pruning never needs its `go.mod`. Verified: with `cbftx` absent from the
  module cache and `GOPROXY=off`, the core build, the n1ql build, `go vet`, and all
  test binaries still compile. The consequence: **do not add `go mod tidy` or
  `go list -m all` to CI** — those load the *full* graph and would need a credential.
- **The first run on a cold cache is slow.** `github.com/couchbase/query` resolves to
  the `n1k1-query` fork — the whole cbq engine, a large module that takes minutes to
  download. `actions/setup-go` caching (keyed on `go.sum`) is enabled in the composite
  action, so subsequent runs restore it instead of re-downloading.
- **`CGO_ENABLED=0` everywhere.** n1k1 is pure Go; this keeps the FAISS/cgo paths
  dark and lets all six release targets cross-compile from one Linux runner.
- **No `-race` gate.** A known race remains in the cbq fork's global
  `_COVERING_ENTRY_POOL` (`util.FastPool`) during concurrent planning; n1k1's own
  engine is race-clean. See `docs/design/DESIGN-concurrency.md`.
- **The conformance suite does not gate PRs** (it takes many minutes). It runs on
  `master`, nightly, and on demand.
- **All three OS legs are blocking**, with one narrow Windows exception: the
  `test (glue)` and `test (CLI)` steps are `continue-on-error` there, because those
  suites are not ported to Windows yet (22 failures across four unrelated causes — see
  "Windows port" in [`TODO.md`](../TODO.md)). Everything else still gates on Windows:
  bootstrap, vet, the core build, **the core tests**, and that glue/test/cmd compile.
  CI found a real `globMatch` separator bug this way, which is now fixed.
- **Step order in the suite job is load-bearing:** the generators emit Go source into
  `test/tmp/`, which is then compiled and run as the compiled-mode differential.
  Reordering would test a stale generated package.
