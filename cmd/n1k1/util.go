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

// terminalWidth reports the current output terminal's column count for auto
// box-width fitting, or 0 when it can't be determined (e.g. output is a pipe or
// a redirected file). Falls back to the COLUMNS env var when the ioctl fails.
func (c *cli) terminalWidth() int {
	if f, ok := c.out.(*os.File); ok {
		if w, _, err := term.GetSize(int(f.Fd())); err == nil && w > 0 {
			return w
		}
	}
	if s := strings.TrimSpace(os.Getenv("COLUMNS")); s != "" {
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
