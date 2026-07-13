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
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"

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
	ln.SetCtrlCAborts(true) // Ctrl-C at the prompt aborts the current line (ErrPromptAborted)

	hist := historyPath()
	loadHistory(ln, hist)
	defer saveHistory(ln, hist)

	// Ctrl-C DURING a running query: liner holds the terminal in raw mode only while
	// Prompt() is reading, so SIGINT reaches this handler only mid-query. First press
	// cooperatively halts the query (keeps the session); a second press (query not
	// stopping) force-quits. Ctrl-C AT the prompt is handled by liner below (ErrPrompt
	// Aborted), where a double press exits. interruptN is reset per query in c.exec.
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)
	go func() {
		for range sigCh {
			if atomic.AddInt32(&c.interruptN, 1) >= 2 {
				fmt.Fprintln(c.stderr, "\n^C — force quit")
				saveHistory(ln, hist) // best-effort: os.Exit skips deferred cleanup
				os.Exit(130)
			}
			c.sess.Interrupt()
			fmt.Fprintln(c.stderr, "\n^C — interrupting (Ctrl-C again to force-quit)")
		}
	}()

	promptCtrlC := false // armed by a Ctrl-C at an empty prompt; a second one exits
	for {
		prompt := c.prog + "> "
		if c.buf.Len() > 0 {
			prompt = "  ...> " // continuing an unterminated statement
		}

		line, err := ln.Prompt(prompt)
		switch err {
		case liner.ErrPromptAborted: // Ctrl-C at the prompt
			if c.buf.Len() > 0 {
				c.buf.Reset() // discard a partial statement; not an exit intent
				promptCtrlC = false
				continue
			}
			if promptCtrlC { // second consecutive Ctrl-C at an empty prompt -> exit
				fmt.Fprintln(c.stderr)
				return
			}
			promptCtrlC = true
			fmt.Fprintln(c.stderr, c.style.Dim("(^C — press Ctrl-C again, or Ctrl-D, to exit)"))
			continue
		case io.EOF: // Ctrl-D
			fmt.Fprintln(c.stderr)
			return
		case nil:
			promptCtrlC = false // any real input disarms the double-Ctrl-C exit
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
// ';' completes one or more statements. Blank lines and comment-only lines are
// skipped when nothing is buffered (so a leading comment doesn't hide the next
// dot-command); with `.echo on` each non-blank line is echoed as it's read.
// Returns true to quit.
func (c *cli) feed(line string) bool {
	if c.echo && strings.TrimSpace(line) != "" {
		fmt.Fprintln(c.stderr, c.style.Dim(line))
	}

	if c.buf.Len() == 0 {
		// Dot-commands run immediately -- only at statement start.
		if t := strings.TrimLeft(line, " \t"); strings.HasPrefix(t, ".") {
			quit := c.dot(strings.TrimSpace(line))
			if c.failed {
				c.sawError = true // latch for the non-interactive exit code (e.g. .multi test FAIL)
			}
			return quit
		}
		// Skip blank/comment-only lines (sqlite/duckdb: nothing to execute yet).
		if cmd.IsBlankOrComment(line) {
			return false
		}
	}

	c.buf.WriteString(line)
	c.buf.WriteByte('\n')

	stmts, rest := cmd.SplitStatements(c.buf.String())
	for _, s := range stmts {
		c.exec(s)
		if c.failed {
			c.sawError = true // latch for the non-interactive exit code
		}
		if c.bail && c.failed { // .bail on: stop the input loop on first error
			c.buf.Reset()
			return true
		}
	}
	c.buf.Reset()
	// Carry the remainder forward only if it holds real SQL (or an unterminated
	// block comment); a trailing comment/whitespace must not keep the buffer
	// "non-empty" (that would hide the next line's dot-command).
	if !cmd.IsBlankOrComment(rest) {
		c.buf.WriteString(rest)
	}
	return false
}

// flush runs any buffered text as a final (unterminated) statement.
func (c *cli) flush() {
	if !cmd.IsBlankOrComment(c.buf.String()) {
		c.exec(c.buf.String())
		if c.failed {
			c.sawError = true // latch for the non-interactive exit code
		}
	}
	c.buf.Reset()
}
