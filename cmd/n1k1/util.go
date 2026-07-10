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

// small cli helpers (string/tty/format utilities).
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/term"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

// ---- helpers --------------------------------------------------------------

func splitFirst(s string) (head, tail string) {
	s = strings.TrimSpace(s)
	if i := strings.IndexAny(s, " \t"); i >= 0 {
		return s[:i], strings.TrimSpace(s[i+1:])
	}
	return s, ""
}

// tidyMsg collapses runs of two-or-more spaces to a single space, cleaning up
// fork error strings before display -- e.g. couchbase/query renders a file
// datastore error as "Error in file datastore  - cause: ..." with a doubled space
// where its (empty) message slot would go.
func tidyMsg(s string) string {
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	return s
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// verboseLevel is the value of the -v / -verbose flag: a diagnostics level
// (0=off, 1=show query plans, 2=+timing). It behaves like a boolean flag so a
// bare -v works and repeats accumulate (-v -v -v -> 3), while -v=on|off|debug|<n>
// sets an explicit level. normalizeArgs rewrites the space form (-v <level>)
// into the =-form before parsing so both spellings work.
type verboseLevel int

func (v *verboseLevel) String() string {
	if v == nil {
		return "0"
	}
	return strconv.Itoa(int(*v))
}

func (v *verboseLevel) Set(s string) error {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "true": // bare -v (or a repeat): raise the level by one
		*v++
	case "on":
		*v = 1
	case "off", "false":
		*v = 0
	case "debug":
		*v = 2
	default:
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 0 {
			return fmt.Errorf("want on|off|debug|<n>, got %q", s)
		}
		*v = verboseLevel(n)
	}
	return nil
}

// IsBoolFlag lets -v be given bare (no argument) and repeated (-v -v -v).
func (v *verboseLevel) IsBoolFlag() bool { return true }

// statsModeFlag parses the -stats flag into a mode constant (off|on|final). It
// behaves like a boolean flag so a bare -stats means "on" (Set("true")), while
// -stats=off / -stats=final set an explicit mode. normalizeArgs also accepts the space
// form -stats final (rewriting to =-form when the next arg is a mode token).
type statsModeFlag struct{ p *string }

func (f statsModeFlag) String() string {
	if f.p == nil {
		return statsOff
	}
	return *f.p
}

func (f statsModeFlag) Set(s string) error {
	m, err := parseStatsMode(s)
	if err != nil {
		return err
	}
	*f.p = m
	return nil
}

func (f statsModeFlag) IsBoolFlag() bool { return true }

// prepareLevelFlag is the -prepare=<level> flag: the MAX preparation level n1k1
// will take a statement to (a ceiling), interpreted|data|full. Bare -prepare
// (flag value "true") means full -- compile as far as the statement allows.
// normalizeArgs also accepts the space form (-prepare full), like -stats.
type prepareLevelFlag struct{ p *glue.PrepareLevel }

func (f prepareLevelFlag) String() string {
	if f.p == nil {
		return glue.PrepareInterpreted.String()
	}
	return f.p.String()
}

func (f prepareLevelFlag) Set(s string) error {
	if s == "true" { // bare -prepare -> the maximum level
		s = "full"
	}
	l, err := glue.PrepareLevelParse(s)
	if err != nil {
		return err
	}
	*f.p = l
	return nil
}

func (f prepareLevelFlag) IsBoolFlag() bool { return true }

// isPrepareLevelToken reports whether s is a single-word level name that `.prepare
// <arg>` should treat as setting the ceiling (rather than as a one-shot statement
// to inspect). None of these are valid statements on their own, so the overlap is
// harmless.
func isPrepareLevelToken(s string) bool {
	if strings.ContainsAny(s, " \t\n") {
		return false
	}
	switch strings.ToLower(s) {
	case "interpreted", "interp", "data", "full", "on", "off":
		return true
	}
	return false
}

// isVerboseLevelToken reports whether s is a value -v/-verbose accepts as its
// level, so the space form "-v <level>" can be rewritten to "-v=<level>".
func isVerboseLevelToken(s string) bool {
	switch strings.ToLower(s) {
	case "on", "off", "debug":
		return true
	}
	n, err := strconv.Atoi(s)
	return err == nil && n >= 0
}

// boolValueFlags are the bool-like flags that ALSO take an explicit value: bare (-v,
// -stats, -prepare) toggles a default, and an explicit value tunes it. Go's flag
// package makes a bool-like flag ignore the next token (so "-stats final" leaves
// "final" as a positional -> "cannot open datastore final"), so normalizeArgs rewrites
// the space form to the =-form when -- and only when -- the next token is a recognized
// VALUE for that flag. A non-value next token (a dir, another flag) leaves the flag
// bare, preserving the toggle. Keyed by every accepted flag spelling. (IDEA-0013.)
var boolValueFlags = map[string]func(string) bool{
	"-v":        isVerboseLevelToken,
	"-verbose":  isVerboseLevelToken,
	"--verbose": isVerboseLevelToken,
	"-stats":    isStatsModeToken,
	"--stats":   isStatsModeToken,
	"-prepare":  isPrepareLevelToken,
	"--prepare": isPrepareLevelToken,
}

// normalizeArgs rewrites the space form of a bool-like valued flag ("-v 3",
// "-stats final", "-prepare full") into the =-form ("-v=3") the flag package needs,
// so both spellings work. A bare flag not followed by a recognized value token --
// including repeats (-v -v -v) and "-stats <dir>" -- is left untouched. Tokens after
// a "--" end-of-flags marker pass through as-is.
func normalizeArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" { // everything after is positional
			return append(out, args[i:]...)
		}
		if isValueTok, ok := boolValueFlags[a]; ok &&
			i+1 < len(args) && isValueTok(args[i+1]) {
			out = append(out, a+"="+args[i+1])
			i++ // consume the value token
			continue
		}
		out = append(out, a)
	}
	return out
}

// isStatsModeToken reports whether s is a value -stats accepts (so the space form
// "-stats final" can be rewritten); a bare "-stats" or "-stats <dir>" is not one.
func isStatsModeToken(s string) bool {
	if s == "" {
		return false
	}
	_, err := parseStatsMode(s)
	return err == nil
}

// verboseName describes a verbose level for status output.
func verboseName(n int) string {
	switch {
	case n <= 0:
		return "off (0)"
	case n == 1:
		return "on (1): info level"
	default:
		return fmt.Sprintf("on (%d): debug level", n)
	}
}

// terminalWidth reports the current output terminal's column count for auto
// box-width fitting, or 0 when it can't be determined (e.g. output is a pipe or
// a redirected file). Falls back to the COLUMNS env var when the ioctl fails.
func (c *cli) terminalWidth() int {
	if f, ok := c.out.(*os.File); ok {
		if w, _, err := term.GetSize(int(f.Fd())); err == nil && w > 0 {
			return w
		}
	}
	if s := strings.TrimSpace(os.Getenv(base.DefEnv("COLUMNS", "terminal output width"))); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

// maxRowsDesc describes the current .maxrows setting for status messages.
func (c *cli) maxRowsDesc() string {
	switch {
	case c.maxRows == 0:
		return "0 (all rows)"
	case c.maxRows < 0:
		return fmt.Sprintf("%d (last %d rows)", c.maxRows, -c.maxRows)
	default:
		return fmt.Sprintf("%d (head+tail)", c.maxRows)
	}
}

// maxWidthDesc describes the current .maxwidth setting for status messages.
func (c *cli) maxWidthDesc() string {
	switch {
	case c.maxWidth < 0:
		return "auto (fit terminal)"
	case c.maxWidth == 0:
		return "0 (uncapped)"
	default:
		return fmt.Sprintf("%d", c.maxWidth)
	}
}

func isTTY(f *os.File) bool {
	st, err := f.Stat()
	if err != nil {
		return false
	}
	return st.Mode()&os.ModeCharDevice != 0
}
