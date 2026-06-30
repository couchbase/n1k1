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

// Command n1k1 is a single-binary CLI + REPL for running SQL++/N1QL with n1k1's
// engine over a file datastore (a directory of JSON docs). See DESIGN-cli.md.
//
//	n1k1 [flags] [datastore-dir]
//	n1k1 ./test/suite/json          # REPL over that datastore
//	n1k1 -c "SELECT 1+1"            # one-shot
//	echo "SELECT ..." | n1k1 dir    # stdin pipe (batch)
//	n1k1 -f script.n1ql dir         # run a file of ;-separated statements
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

func main() {
	var (
		cFlag      = flag.String("c", "", "run one statement and exit")
		fFlag      = flag.String("f", "", "run statements from a file and exit")
		nsFlag     = flag.String("ns", "default", "datastore namespace")
		modeFlag   = flag.String("mode", "", "output mode: "+strings.Join(outputModes, "|")+" (default box at a TTY, else jsonlines)")
		timerFlag  = flag.Bool("timer", false, "print row count + elapsed after each statement")
		vFlag      = flag.Bool("v", false, "verbose: show unsupported reasons / plan on error")
		initFlag   = flag.String("init", "", "run dot-commands/SQL from this file at startup (default ~/.n1k1rc)")
		noInitFlag = flag.Bool("no-init", false, "skip the startup init file")
	)
	flag.Usage = usage
	flag.Parse()

	dir := "."
	if args := flag.Args(); len(args) > 0 {
		dir = args[0]
	}

	sess, err := glue.OpenSession(dir, *nsFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "n1k1: cannot open datastore %q: %v\n", dir, err)
		os.Exit(1)
	}

	stdinIsTTY := isTTY(os.Stdin)

	mode := *modeFlag
	if mode == "" {
		if stdinIsTTY && *cFlag == "" && *fFlag == "" {
			mode = "box"
		} else {
			mode = "jsonlines"
		}
	}
	if !validMode(mode) {
		fmt.Fprintf(os.Stderr, "n1k1: unknown -mode %q (want %s)\n", mode, strings.Join(outputModes, "|"))
		os.Exit(2)
	}

	c := &cli{
		sess:     sess,
		dir:      dir,
		ns:       *nsFlag,
		mode:     mode,
		timer:    *timerFlag,
		verbose:  *vFlag,
		maxRows:  0,
		maxWidth: 50,
		listSep:  "|",
		out:      os.Stdout,
		stderr:   os.Stderr,
	}

	// Startup init file (dot-commands / SQL), unless suppressed.
	if !*noInitFlag {
		initFile := *initFlag
		if initFile == "" {
			if home, e := os.UserHomeDir(); e == nil {
				initFile = filepath.Join(home, ".n1k1rc")
			}
		}
		if initFile != "" {
			if _, e := os.Stat(initFile); e == nil {
				c.readFile(initFile)
			}
		}
	}

	switch {
	case *cFlag != "":
		c.feed(*cFlag)
		c.flush()
	case *fFlag != "":
		if !c.readFile(*fFlag) {
			os.Exit(1)
		}
	case !stdinIsTTY:
		c.batch(os.Stdin) // piped input
	default:
		c.repl()
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `n1k1 -- SQL++/N1QL over a file datastore (a directory of JSON docs)

usage: n1k1 [flags] [datastore-dir]

  n1k1 ./test/suite/json        REPL over that datastore
  n1k1 -c "SELECT 1+1"          run one statement and exit
  echo "SELECT ..." | n1k1 dir  stdin pipe (batch mode)
  n1k1 -f script.n1ql dir       run a file of ;-separated statements

flags:
`)
	flag.PrintDefaults()
}

// cli holds front-end state; all engine work goes through c.sess (glue.Session).
type cli struct {
	sess *glue.Session
	dir  string
	ns   string

	mode     string
	timer    bool
	verbose  bool
	explain  bool
	maxRows  int // box: 0 = show all
	maxWidth int // box: per-column cap, 0 = uncapped
	listSep  string

	out     io.Writer // result destination (stdout, or a .output file)
	outFile *os.File  // non-nil when .output redirected to a file
	stderr  io.Writer

	buf strings.Builder // REPL/batch statement accumulator
}

// ---- input loops ----------------------------------------------------------

func (c *cli) repl() {
	fmt.Fprintf(c.stderr, "n1k1 -- SQL++ over %s (namespace %q). Type .help for commands, .quit to exit.\n", c.dir, c.ns)
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	c.prompt()
	for sc.Scan() {
		if c.feed(sc.Text()) {
			return // .quit / .exit
		}
		c.prompt()
	}
	fmt.Fprintln(c.stderr) // newline after Ctrl-D
}

func (c *cli) prompt() {
	if c.buf.Len() == 0 {
		fmt.Fprint(c.stderr, "n1k1> ")
	} else {
		fmt.Fprint(c.stderr, "  ...> ")
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
		fmt.Fprintf(c.stderr, "n1k1: cannot read %q: %v\n", path, err)
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

	stmts, rest := splitStatements(c.buf.String())
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

// ---- statement execution --------------------------------------------------

func (c *cli) exec(stmt string) {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return
	}

	res, err := c.sess.Run(stmt)
	if err != nil {
		var unsup *glue.ErrUnsupported
		if errors.As(err, &unsup) {
			fmt.Fprintf(c.stderr, "Error (unsupported): %s\n", unsup.Reason)
		} else {
			fmt.Fprintf(c.stderr, "Error: %s\n", err)
		}
		return
	}

	if c.explain {
		fmt.Fprintln(c.stderr, "plan:")
		printPlan(c.stderr, res.Plan, 1)
	}

	c.renderResult(res)

	for _, w := range res.Warnings {
		fmt.Fprintf(c.stderr, "Warning: %s\n", w.Error())
	}
}

func (c *cli) renderResult(res *glue.Result) {
	switch c.mode {
	case "jsonlines":
		renderJSONLines(c.out, res.Rows)
	case "json":
		renderJSON(c.out, res.Rows)
	case "csv":
		renderCSV(c.out, res.Rows)
	case "markdown":
		renderMarkdown(c.out, res.Rows)
	case "line":
		renderLine(c.out, res.Rows)
	case "list":
		renderList(c.out, res.Rows, c.listSep)
	default: // "box"
		elapsed := ""
		if c.timer {
			elapsed = res.Elapsed.String()
		}
		renderBox(c.out, res.Rows, c.maxWidth, c.maxRows, elapsed)
		return // box prints its own row-count/elapsed footer
	}

	if c.timer {
		fmt.Fprintf(c.stderr, "%d row(s) in %s\n", len(res.Rows), res.Elapsed)
	}
}

// ---- dot-commands ---------------------------------------------------------

// dot handles a meta command line (it always starts with '.'). Returns true to
// quit the REPL.
func (c *cli) dot(line string) bool {
	cmd, arg := splitFirst(line)
	switch cmd {
	case ".quit", ".exit":
		return true
	case ".help":
		c.printHelp()
	case ".open":
		c.cmdOpen(arg)
	case ".tables", ".keyspaces":
		c.cmdKeyspaces()
	case ".schema":
		c.cmdSchema(arg)
	case ".mode":
		if validMode(arg) {
			c.mode = arg
		} else {
			fmt.Fprintf(c.stderr, "modes: %s\n", strings.Join(outputModes, " "))
		}
	case ".timer":
		c.timer = (arg == "on")
	case ".explain":
		c.explain = !c.explain
		fmt.Fprintf(c.stderr, "explain %s\n", onOff(c.explain))
	case ".maxrows":
		if n, err := strconv.Atoi(arg); err == nil {
			c.maxRows = n
		}
	case ".maxwidth":
		if n, err := strconv.Atoi(arg); err == nil {
			c.maxWidth = n
		}
	case ".read":
		c.readFile(arg)
	case ".output":
		c.cmdOutput(arg)
	default:
		fmt.Fprintf(c.stderr, "unknown command %q -- try .help\n", cmd)
	}
	return false
}

func (c *cli) printHelp() {
	fmt.Fprint(c.stderr, `.help                 show this help
.open <dir>           open a different file datastore directory
.tables / .keyspaces  list keyspaces in the namespace
.schema [<keyspace>]  sampled shape (keys + JSON types) of a keyspace
.mode <m>             output mode: `+strings.Join(outputModes, " ")+`
.timer on|off         toggle elapsed-time reporting
.explain              toggle printing the converted n1k1 plan per query
.maxrows <n>          box: cap rows shown (0 = all)
.maxwidth <n>         box: cap column width (0 = uncapped)
.read <file>          run statements/dot-commands from a file
.output [<file>]      send results to a file, or back to stdout if omitted
.quit / .exit         leave

Statements are SQL++; terminate with ';'. Keyspaces are queried as
<namespace>:<keyspace>, e.g.  SELECT * FROM `+c.ns+`:orders LIMIT 5;
`)
}

func (c *cli) cmdOpen(dir string) {
	if dir == "" {
		fmt.Fprintln(c.stderr, "usage: .open <dir>")
		return
	}
	sess, err := glue.OpenSession(dir, c.ns)
	if err != nil {
		fmt.Fprintf(c.stderr, "cannot open %q: %v\n", dir, err)
		return
	}
	c.sess, c.dir = sess, dir
	fmt.Fprintf(c.stderr, "opened %s\n", dir)
}

func (c *cli) cmdKeyspaces() {
	names, err := c.keyspaceNames()
	if err != nil {
		fmt.Fprintf(c.stderr, "%v\n", err)
		return
	}
	if len(names) == 0 {
		fmt.Fprintf(c.stderr, "(no keyspaces under %s/%s)\n", c.dir, c.ns)
		return
	}
	for _, n := range names {
		fmt.Fprintln(c.out, n)
	}
}

func (c *cli) keyspaceNames() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(c.dir, c.ns))
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func (c *cli) cmdSchema(keyspace string) {
	kss := []string{keyspace}
	if keyspace == "" {
		names, err := c.keyspaceNames()
		if err != nil {
			fmt.Fprintf(c.stderr, "%v\n", err)
			return
		}
		kss = names
	}
	for _, ks := range kss {
		shape, n := sampleSchema(filepath.Join(c.dir, c.ns, ks), 50)
		fmt.Fprintf(c.out, "%s  (sampled %d docs):\n", ks, n)
		keys := make([]string, 0, len(shape))
		for k := range shape {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			types := shape[k]
			sort.Strings(types)
			fmt.Fprintf(c.out, "  %-20s %s\n", k, strings.Join(types, "|"))
		}
	}
}

func (c *cli) cmdOutput(path string) {
	if c.outFile != nil {
		c.outFile.Close()
		c.outFile = nil
	}
	if path == "" {
		c.out = os.Stdout
		return
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(c.stderr, "cannot create %q: %v\n", path, err)
		return
	}
	c.outFile, c.out = f, f
}

// ---- helpers --------------------------------------------------------------

// splitStatements splits SQL text on top-level ';' (ignoring ';' inside ' " or
// ` quotes), returning the complete statements and any trailing remainder.
func splitStatements(s string) (stmts []string, rest string) {
	var start int
	var quote rune
	for i, r := range s {
		if quote != 0 {
			if r == quote {
				quote = 0
			}
			continue
		}
		switch r {
		case '\'', '"', '`':
			quote = r
		case ';':
			stmts = append(stmts, s[start:i])
			start = i + 1
		}
	}
	return stmts, s[start:]
}

func splitFirst(s string) (head, tail string) {
	s = strings.TrimSpace(s)
	if i := strings.IndexAny(s, " \t"); i >= 0 {
		return s[:i], strings.TrimSpace(s[i+1:])
	}
	return s, ""
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func isTTY(f *os.File) bool {
	st, err := f.Stat()
	if err != nil {
		return false
	}
	return st.Mode()&os.ModeCharDevice != 0
}

// sampleSchema reads up to limit *.json docs from a keyspace dir and returns a
// map of top-level key -> observed JSON type names, plus how many docs sampled.
func sampleSchema(dir string, limit int) (map[string][]string, int) {
	shape := map[string]map[string]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return map[string][]string{}, 0
	}
	n := 0
	for _, e := range entries {
		if n >= limit || e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var m map[string]interface{}
		if json.Unmarshal(b, &m) != nil {
			continue
		}
		n++
		for k, v := range m {
			if shape[k] == nil {
				shape[k] = map[string]bool{}
			}
			shape[k][jsonType(v)] = true
		}
	}
	out := make(map[string][]string, len(shape))
	for k, set := range shape {
		for t := range set {
			out[k] = append(out[k], t)
		}
	}
	return out, n
}

func jsonType(v interface{}) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case float64:
		return "number"
	case string:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "unknown"
	}
}

// printPlan prints the converted n1k1 op tree, one node per line, indented.
func printPlan(w io.Writer, op *base.Op, depth int) {
	if op == nil {
		return
	}
	fmt.Fprintf(w, "%s%s %v\n", strings.Repeat("  ", depth), op.Kind, op.Labels)
	for _, ch := range op.Children {
		printPlan(w, ch, depth+1)
	}
}
