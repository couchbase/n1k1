#!/usr/bin/env bash
#
# bootstrap.sh -- prepare a fresh checkout / worktree / CI runner so that the
# n1ql-tagged packages (glue/, test/, cmd/n1k1) can build.
#
# See docs/design/DESIGN-testing.md "Building the gsi suite in a fresh worktree".
#
# Why this is needed: go.mod requires several placeholder EE modules pinned at the
# v0.0.0-00010101000000-000000000000 non-version, whose go.mod files exist only in
# Couchbase's internal repo-sync tree. Nothing in the CE build imports them, but
# the module-graph loader still demands a go.mod for each -- so we point every one
# at an empty local stub module via a `replace` directive.
#
# The stub replaces are MACHINE-LOCAL and must never be committed: they are
# appended to go.mod here, and .ee-stubs/ is gitignored. Run `git checkout go.mod`
# before committing or rebasing.
#
# Idempotent: safe to re-run.
#
set -euo pipefail

cd "$(dirname "$0")/.."

STUBS=".ee-stubs"   # gitignored; RELATIVE so the replace paths stay portable
                    # across Linux/macOS/Windows runners (an absolute $RUNNER_TEMP
                    # path is a bash-style path that the Windows go.exe rejects).

if grep -q "=> ./$STUBS/" go.mod; then
  echo "bootstrap: EE stub replaces already in go.mod -- skipping"
else
  # Capture the module list BEFORE appending (we are about to write to go.mod,
  # so a `grep go.mod | while read` pipeline would race with its own output).
  mods=$(grep -E '00010101000000-000000000000' go.mod | awk '{print $1}' | sort -u)
  gover=$(awk '/^go [0-9]/{print $2; exit}' go.mod)

  if [ -z "$mods" ]; then
    echo "bootstrap: no placeholder EE requires found -- nothing to stub"
  else
    {
      echo ""
      echo "// --- local EE stubs, appended by scripts/bootstrap.sh -- DO NOT COMMIT ---"
      for m in $mods; do
        d="$STUBS/$m"
        mkdir -p "$d"
        # A stub module needs only a module path and a go version -- no .go files.
        printf 'module %s\n\ngo %s\n' "$m" "$gover" > "$d/go.mod"
        echo "replace $m => ./$d"
      done
    } >> go.mod
    echo "bootstrap: stubbed $(echo "$mods" | wc -w | tr -d ' ') placeholder EE modules"
  fi
fi

# test/tmp/ is gitignored (absent on a fresh checkout), but the *WithCompiler
# generators WriteFile their emitted Go source into it and won't create it.
mkdir -p test/tmp

# intermed/ is gitignored and generated from engine/*.go, so a fresh checkout has
# no (or a stale) copy. `go run` keeps this portable -- `go build -o intermed_build`
# would need an .exe suffix on Windows.
if ! out=$(go run ./cmd/intermed_build/ 2>&1); then
  printf '%s\n' "$out" >&2
  echo "bootstrap: intermed_build FAILED" >&2
  exit 1
fi
echo "bootstrap: regenerated intermed/"

echo "bootstrap: done -- try: CGO_ENABLED=0 go build -tags n1ql ./glue/... ./test/..."
