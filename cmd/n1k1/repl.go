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

// cli input loops: the REPL, batch/piped input, file reading, and line buffering.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/peterh/liner"

	"github.com/couchbase/n1k1/cmd"
)

// ---- input loops ----------------------------------------------------------

func (c *cli) repl() {
	fmt.Fprintf(c.stderr, "%s%s — SQL++. Type %s for commands, %s to exit.\n",
		c.icon("🔎 "), c.style.Cyan(c.prog), c.style.Bold(".help"), c.style.Bold(".quit"))

	// Show the flattened keyspaces + copy-pasteable examples up front, so it's
	// clear what's queryable (and how the datastore dir was flattened).
	c.printKeyspaces(c.stderr)

	ln := liner.NewLiner()
	defer ln.Close()
	ln.SetCtrlCAborts(true) // Ctrl-C aborts the current line, not the process

	hist := historyPath()
	loadHistory(ln, hist)
	defer saveHistory(ln, hist)

	for {
		prompt := c.prog + "> "
		if c.buf.Len() > 0 {
			prompt = "  ...> " // continuing an unterminated statement
		}

		line, err := ln.Prompt(prompt)
		switch err {
		case liner.ErrPromptAborted: // Ctrl-C: discard the in-progress buffer
			c.buf.Reset()
			continue
		case io.EOF: // Ctrl-D
			fmt.Fprintln(c.stderr)
			return
		case nil:
		default:
			fmt.Fprintf(c.stderr, "input error: %v\n", err)
			return
		}

		if strings.TrimSpace(line) != "" {
			ln.AppendHistory(line)
		}
		if c.feed(line) {
			return // .quit / .exit
		}
	}
}

// historyPath is where the REPL persists line history (~/.<prog>_history).
func historyPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, "."+prog+"_history")
}

func loadHistory(ln *liner.State, path string) {
	if path == "" {
		return
	}
	if f, err := os.Open(path); err == nil {
		ln.ReadHistory(f)
		f.Close()
	}
}

func saveHistory(ln *liner.State, path string) {
	if path == "" {
		return
	}
	if f, err := os.Create(path); err == nil {
		ln.WriteHistory(f)
		f.Close()
	}
}

// batch runs piped/redirected input: every statement, plus a trailing one
// without a final ';'.
func (c *cli) batch(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		if c.feed(sc.Text()) {
			return
		}
	}
	c.flush()
}

// readFile runs a file of statements / dot-commands (used by -f, .read and the
// init file). Returns false if the file can't be read.
func (c *cli) readFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: cannot read %q: %v\n", c.prog, path, err)
		return false
	}
	defer f.Close()
	c.batch(f)
	return true
}

// feed processes one input line. When the line starts a dot-command (and no SQL
// is buffered) it runs immediately; otherwise it accumulates until a top-level
// ';' completes one or more statements. Returns true to quit.
func (c *cli) feed(line string) bool {
	if c.buf.Len() == 0 {
		t := strings.TrimSpace(line)
		if t == "" {
			return false
		}
		if strings.HasPrefix(t, ".") {
			return c.dot(t)
		}
	}

	c.buf.WriteString(line)
	c.buf.WriteByte('\n')

	stmts, rest := cmd.SplitStatements(c.buf.String())
	for _, s := range stmts {
		c.exec(s)
	}
	c.buf.Reset()
	// Only carry a non-blank remainder forward; a trailing newline must not keep
	// the buffer "non-empty" (that would hide the next line's dot-command).
	if strings.TrimSpace(rest) != "" {
		c.buf.WriteString(rest)
	}
	return false
}

// flush runs any buffered text as a final (unterminated) statement.
func (c *cli) flush() {
	if strings.TrimSpace(c.buf.String()) != "" {
		c.exec(c.buf.String())
	}
	c.buf.Reset()
}
