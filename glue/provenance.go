//go:build n1ql

//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package glue

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/couchbase/n1k1/base"
)

// Provenance identifies the n1k1 source a build/codegen was made against, for
// reproducibility and for stamping generated code. It matters once compiled
// PREPARE bundles subsets of n1k1's own source (engine/base, and eventually the
// datastore runtime) to `go build` a self-contained program: a finding or an
// artifact can then be traced back to the exact source it was produced from --
// including UNCOMMITTED dev edits, so codegen reflects work-in-progress without a
// commit.
//
// SHA is the git HEAD commit; Dirty flags uncommitted changes; Diff is the actual
// uncommitted patch. Diff is captured only from a live source repo (ProvenanceCapture
// with a git srcDir); the build-info fallback records SHA + Dirty from the binary's
// embedded VCS stamp but cannot recover the diff (the Go toolchain doesn't embed it).
type Provenance struct {
	SHA       string // git HEAD commit (hex), or "" if unknown
	Dirty     bool   // the working tree had uncommitted changes at capture
	Diff      string // `git diff HEAD` (staged + unstaged), when captured from a live repo; else ""
	GoVersion string // the Go runtime/toolchain version
	Source    string // where the fields came from: "build-info" or "git:<dir>"
}

// ProvenanceSourceDir returns the n1k1 source directory to capture live provenance
// from -- the N1K1_SRC env var. Empty means "use the binary's build info" (SHA +
// dirty flag, but no diff). Set N1K1_SRC to the n1k1 checkout to capture the exact
// working-tree state (including uncommitted edits) for codegen.
func ProvenanceSourceDir() string {
	return os.Getenv(base.DefEnv("N1K1_SRC", "n1k1 source dir for codegen provenance (git SHA + uncommitted diff)"))
}

// ProvenanceCapture records n1k1's source provenance. It prefers a live git repo at
// srcDir (git HEAD + porcelain status + `git diff HEAD`, so uncommitted dev edits
// are captured), and falls back to the binary's embedded build info (vcs.revision /
// vcs.modified, no diff) when srcDir is empty or not a usable git repo. Never
// errors: unknown fields are left zero.
func ProvenanceCapture(srcDir string) Provenance {
	if srcDir != "" {
		if g, ok := provenanceFromGit(srcDir); ok {
			return g
		}
	}
	return provenanceFromBuildInfo()
}

func provenanceFromBuildInfo() Provenance {
	p := Provenance{GoVersion: runtime.Version(), Source: "build-info"}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return p
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			p.SHA = s.Value
		case "vcs.modified":
			p.Dirty = s.Value == "true"
		}
	}
	return p
}

func provenanceFromGit(dir string) (Provenance, bool) {
	sha, err := gitOutput(dir, "rev-parse", "HEAD")
	if err != nil {
		return Provenance{}, false // not a git repo, or git unavailable
	}
	p := Provenance{
		SHA:       strings.TrimSpace(sha),
		GoVersion: runtime.Version(),
		Source:    "git:" + dir,
	}
	if st, err := gitOutput(dir, "status", "--porcelain"); err == nil && strings.TrimSpace(st) != "" {
		p.Dirty = true
		// The full working-tree diff vs HEAD (staged + unstaged) -- the uncommitted
		// edits, so codegen can reproduce dev work-in-progress without a commit.
		if d, err := gitOutput(dir, "diff", "HEAD"); err == nil {
			p.Diff = d
		}
	}
	return p, true
}

func gitOutput(dir string, args ...string) (string, error) {
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).Output()
	return string(out), err
}

// Stamp renders a one-line provenance summary for a generated-code header comment
// (the full Diff is kept in the struct, not the stamp -- it can be large).
func (p Provenance) Stamp() string {
	sha := p.SHA
	if sha == "" {
		sha = "unknown"
	} else if len(sha) > 12 {
		sha = sha[:12]
	}
	s := "n1k1 source " + sha
	if p.Dirty {
		s += " +uncommitted"
		if p.Diff != "" {
			s += fmt.Sprintf(" (diff %dB)", len(p.Diff))
		}
	}
	if p.GoVersion != "" {
		s += " · " + p.GoVersion
	}
	return s
}
