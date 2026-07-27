# CI / CD

Three workflows, plus one composite action they share.

| Workflow | Trigger | Needs a secret? | What it does |
|---|---|---|---|
| [`ci.yml`](workflows/ci.yml) | push to `master`, every PR, nightly | partly | `docs` gate always; build + test on Linux/macOS/Windows, the recipes checker, and the conformance suite when the fork token is available |
| [`release.yml`](workflows/release.yml) | tag `v*` (or manual) | yes | cross-builds the CLI for 6 targets, packages + checksums them, publishes a GitHub Release |
| [`pages.yml`](workflows/pages.yml) | push to `master` touching `docs/recipes.*` | **no** | publishes the SQL++ recipes doc to GitHub Pages |

## One-time setup

### 1. `CBQ_FORK_TOKEN` secret — required for every Go job

`go.mod` has `github.com/couchbase/query` as a **direct** require and replaces it
with **`github.com/couchbase/n1k1-query`**, which is a **private** repository:

```
replace github.com/couchbase/query => github.com/couchbase/n1k1-query v0.0.0-…
```

`proxy.golang.org` cannot serve a private module (it returns 404), and because the
module is a *direct* require its `go.mod` must be loaded to compute the build list.
So **no Go command works without a credential** — not even `go build ./...` on the
untagged core.

Add a token with **read access to `couchbase/n1k1-query`**:

> Settings → Secrets and variables → Actions → New repository secret
> Name: `CBQ_FORK_TOKEN`
> Value: a fine-grained PAT (Contents: Read-only on `couchbase/n1k1-query`), or a
> GitHub App installation token

Until it is set, the Go jobs **skip cleanly** (they do not fail) and annotate the
run explaining why. The `docs` job and Pages still run, so outside contributors and
fork PRs get useful signal.

> Secrets are never exposed to `pull_request` runs from forks. That is by design —
> a fork PR gets the `docs` gate only, and a maintainer's push to a branch in this
> repo exercises the full matrix.

### 2. Enable GitHub Pages

> Settings → Pages → Build and deployment → Source = **GitHub Actions**

Then run the `Pages (SQL++ recipes)` workflow (or push a `docs/recipes.*` change).
The site is published at `https://couchbase.github.io/n1k1/` — the recipes page is
the site root. **This is a public page** (the repository is public).

## Local equivalent

CI runs the same recipe a developer does:

```sh
make bootstrap      # stub the placeholder EE modules, regen intermed/, mkdir test/tmp
make test           # core + conformance suite
make recipes        # regenerate docs/recipes.{md,html}
make recipes-check   # run every SQL++ example in the doc (needs ./n1k1)
```

`make bootstrap` appends **machine-local** `replace` lines to `go.mod`; run
`git checkout go.mod` before committing or rebasing. See
[`docs/design/DESIGN-testing.md`](../docs/design/DESIGN-testing.md).

## Notes

- **`CGO_ENABLED=0` everywhere.** n1k1 is pure Go; this keeps the FAISS/cgo paths
  dark and lets all six release targets cross-compile from one Linux runner.
- **No `-race` gate.** A known race remains in the cbq fork's global
  `_COVERING_ENTRY_POOL` (`util.FastPool`) during concurrent planning; n1k1's own
  engine is race-clean. See `docs/design/DESIGN-concurrency.md`.
- **The conformance suite does not gate PRs** (it takes many minutes). It runs on
  `master`, nightly, and on demand.
- **Step order in the suite job is load-bearing:** the generators emit Go source
  into `test/tmp/`, which is then compiled and run as the compiled-mode
  differential. Reordering would test a stale generated package.
