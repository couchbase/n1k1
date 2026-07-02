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

	"github.com/peterh/liner"
	"golang.org/x/term"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/cmd"
	"github.com/couchbase/n1k1/glue"
	"github.com/couchbase/n1k1/records"
)

// prog is the command's short name, derived from how the binary was invoked
// (e.g. the base name of a symlink pointing at it), so an aliased install
// presents itself under that alias in usage, prompts and messages. Defaults
// to "n1k1".
var prog = progName()

// progName returns the base name of os.Args[0], falling back to "n1k1" when
// the invocation path is empty or degenerate (".", "/").
func progName() string {
	if len(os.Args) > 0 {
		if b := filepath.Base(os.Args[0]); b != "" && b != "." && b != string(filepath.Separator) {
			return b
		}
	}
	return "n1k1"
}

func main() {
	var (
		cFlag     = flag.String("c", "", "run statements and exit")
		fFlag     = flag.String("f", "", "run statements from a file and exit")
		nsFlag    = flag.String("ns", "default", "datastore namespace")
		modeFlag  = flag.String("mode", "", "output mode: "+strings.Join(cmd.OutputModes, "|")+" (append |pretty to indent JSON; default box|pretty at a TTY, else jsonlines)")
		timerFlag = flag.Bool("timer", false, "print row count + elapsed after each statement")
		vFlag     = flag.Bool("v", false, "verbose: show more info on errors")
		initFlag  = flag.String("init", "", "startup file of dot-commands/SQL++ (default ~/."+prog+"rc; use \"\", \"-\" or \"none\" to skip)")
		scanFlag  = flag.String("scan", "", "restrict file discovery to a comma-separated set (all|json|jsonl|csv|tsv|extract|gzip|recurse); empty or 'all' = everything")
		metaFlag  = flag.String("meta", "auto", "add a _meta sub-object (path/name/ext/size/mtime) to records: on|off|auto (auto = extracted docs only)")
		verFlag   = flag.Bool("version", false, "print version + build info (incl. dependency SHAs) and exit")
		indexFlag = flag.String("index", "lazy", "secondary index (.n1k1/catalog.json) build mode: eager|lazy|off")
	)
	flag.Usage = usage
	flag.Parse()

	// -version works without a datastore, so handle it before opening a session.
	if *verFlag {
		printVersion(os.Stdout)
		return
	}

	// -index selects when catalog-declared secondary indexes are built (lazy on
	// first use, eager up front, or off = ignore the catalog). See DESIGN-indexing.md.
	switch *indexFlag {
	case "eager", "lazy", "off":
		glue.SecondaryIndexMode = *indexFlag
	default:
		fmt.Fprintf(os.Stderr, "%s: bad -index %q (want eager|lazy|off)\n", prog, *indexFlag)
		os.Exit(2)
	}

	// -scan locks down which formats/layouts/compression n1k1 will scan, so a
	// tree with subdirs/formats the user doesn't want considered can be excluded.
	scanOpts, serr := records.ParseModes(*scanFlag)
	if serr != nil {
		fmt.Fprintf(os.Stderr, "%s: bad -scan: %v\n", prog, serr)
		os.Exit(2)
	}
	// -meta controls per-file metadata injection (_meta).
	mm, merr := records.ParseMetaMode(*metaFlag)
	if merr != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", prog, merr)
		os.Exit(2)
	}
	scanOpts.Meta = mm
	glue.ScanWalkOptions = scanOpts

	dir := "."
	if args := flag.Args(); len(args) > 0 {
		dir = args[0]
	}

	sess, err := glue.OpenSession(dir, *nsFlag)
	if err != nil {
		// No datastore at dir (e.g. it doesn't exist). Keep running with an empty
		// one so the user can still evaluate expressions (SELECT 1+2;) and .open a
		// real datastore later, rather than exiting.
		fmt.Fprintf(os.Stderr, "%s: no datastore at %q (%v); starting empty\n", prog, dir, err)
		empty, e2 := os.MkdirTemp("", "n1k1-empty-")
		if e2 != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", prog, e2)
			os.Exit(1)
		}
		defer os.RemoveAll(empty)
		if sess, err = glue.OpenSession(empty, *nsFlag); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", prog, err)
			os.Exit(1)
		}
		dir = ""
	}

	stdinIsTTY := isTTY(os.Stdin)

	mode := *modeFlag
	if mode == "" {
		if stdinIsTTY && *cFlag == "" && *fFlag == "" {
			mode = "box|pretty" // interactive: pretty-print nested JSON by default
		} else {
			mode = "jsonlines" // pipes/-c/-f: stay compact (one JSON per line)
		}
	}
	if !cmd.ValidMode(mode) {
		fmt.Fprintf(os.Stderr, "%s: unknown -mode %q (want %s)\n", prog, mode, strings.Join(cmd.OutputModes, "|"))
		os.Exit(2)
	}

	// Colors/emojis only for an interactive stdout, and honoring NO_COLOR.
	fancy := isTTY(os.Stdout) && os.Getenv("NO_COLOR") == ""

	c := &cli{
		prog:      prog,
		sess:      sess,
		dir:       dir,
		ns:        *nsFlag,
		mode:      mode,
		indexMode: *indexFlag,
		timer:     *timerFlag,
		verbose:   *vFlag,
		maxRows:   0,
		maxWidth:  -1,
		listSep:   "|",
		out:       os.Stdout,
		stderr:    os.Stderr,
		fancyTTY:  fancy,
		style:     cmd.Style{On: fancy},
	}

	c.eagerBuildIndexes() // -index=eager: build all catalog indexes up front

	// Startup init file (dot-commands / SQL). If -init was not given, use the
	// default ~/.<prog>rc; if given, the value names a file, or "", "-" or "none"
	// to skip. (flag.Visit distinguishes "not given" from an explicit -init "".)
	initGiven := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "init" {
			initGiven = true
		}
	})
	initFile := ""
	if !initGiven {
		if home, e := os.UserHomeDir(); e == nil {
			initFile = filepath.Join(home, "."+prog+"rc")
		}
	} else {
		switch strings.ToLower(strings.TrimSpace(*initFlag)) {
		case "", "-", "none", "skip":
			initFile = "" // explicit skip
		default:
			initFile = *initFlag
		}
	}
	if initFile != "" {
		if _, e := os.Stat(initFile); e == nil {
			c.readFile(initFile)
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
	fmt.Fprintf(os.Stderr, `%[1]s -- SQL++ for local files (json, jsonl, csv, gz, and more)

usage: %[1]s [flags] [datastore-dir | file]

  # a single file -- the keyspace is the filename minus its extension:
  %[1]s -c "SELECT * FROM events LIMIT 5"                   events.jsonl
  %[1]s -c "SELECT COUNT(*) AS n FROM access"               access.ndjson.gz
  %[1]s -c "SELECT city, SUM(amt) FROM sales GROUP BY city" path/to/sales.csv

  # a directory tree of files (flat, <ns>/<keyspace>/, or nested subdirs):
  %[1]s ./test/suite/json
  %[1]s -c "SELECT * FROM invoices WHERE total > 5" path/to/biz-datastore-dir

  # statements from -c, a file, or stdin:
  %[1]s -c "SELECT 1+1"
  %[1]s -f script.sql++ path/to/datastore-dir
  echo "SELECT 1+1" | %[1]s

flags:
`, prog)
	flag.PrintDefaults()
}

// cli holds front-end state; all engine work goes through c.sess (glue.Session).
type cli struct {
	prog string // short command name (from how the binary was invoked)
	sess *glue.Session
	dir  string
	ns   string

	mode      string
	indexMode string // -index: eager|lazy|off (drives eager build on open)
	timer     bool
	verbose   bool
	explain   bool
	maxRows   int // box: 0 = all; >0 = head+tail; <0 = last |n| rows
	maxWidth  int // box: per-column cap; 0 = uncapped; <0 = auto (fit terminal)
	listSep   string

	out     io.Writer // result destination (stdout, or a .output file)
	outFile *os.File  // non-nil when .output redirected to a file
	stderr  io.Writer

	fancyTTY bool      // stdout is an interactive TTY (drives colors/emojis)
	style    cmd.Style // ANSI styling for the box renderer

	buf strings.Builder // REPL/batch statement accumulator
}

// icon returns the given emoji marker only in interactive (fancy) mode, so
// piped/redirected output and dumb terminals stay plain.
func (c *cli) icon(s string) string {
	if c.fancyTTY {
		return s
	}
	return ""
}

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
			fmt.Fprintf(c.stderr, "%s%s\n", c.icon("🚧 "), c.style.Yellow("Unsupported: "+unsup.Reason))
		} else {
			fmt.Fprintf(c.stderr, "%s%s\n", c.icon("✗ "), c.style.Red("Error: "+err.Error()))
			// Point a caret at the offending column when the parser gives one.
			fmt.Fprint(c.stderr, errorCaret(stmt, err.Error(), c.style))
		}
		return
	}

	if c.explain {
		fmt.Fprintln(c.stderr, c.style.Dim("plan:"))
		printPlan(c.stderr, res.Plan, 1)
	}

	c.renderResult(res)

	for _, w := range res.Warnings {
		fmt.Fprintf(c.stderr, "%s%s\n", c.icon("⚠️  "), c.style.Yellow("Warning: "+w.Error()))
	}
}

func (c *cli) renderResult(res *glue.Result) {
	// A "|pretty" / "-pretty" suffix on the mode 2-space-indents JSON values.
	base, pretty, _ := cmd.ParseMode(c.mode)
	switch base {
	case "jsonlines":
		cmd.RenderJSONLines(c.out, res.Rows, pretty)
	case "json":
		cmd.RenderJSON(c.out, res.Rows, pretty)
	case "csv":
		cmd.RenderCSV(c.out, res.Rows, pretty)
	case "markdown":
		cmd.RenderMarkdown(c.out, res.Rows, pretty)
	case "line":
		cmd.RenderLine(c.out, res.Rows, pretty)
	case "list":
		cmd.RenderList(c.out, res.Rows, c.listSep, pretty)
	default: // "box"
		elapsed := ""
		if c.timer {
			elapsed = c.icon("⏱ ") + res.Elapsed.String()
		}
		termWidth := 0
		if c.maxWidth < 0 { // auto: fit the box to the terminal's width
			termWidth = c.terminalWidth()
		}
		cmd.RenderBox(c.out, res.Rows, c.maxWidth, c.maxRows, termWidth, elapsed, c.style, pretty)
		return // box prints its own row-count/elapsed footer
	}

	if c.timer {
		fmt.Fprintf(c.stderr, "%s%d row(s) in %s\n", c.icon("⏱ "), len(res.Rows), res.Elapsed)
	}
}

// ---- dot-commands ---------------------------------------------------------

// dot handles a meta command line (it always starts with '.'). Returns true to
// quit the REPL.
func (c *cli) dot(line string) bool {
	name, arg := splitFirst(line)
	switch name {
	case ".quit", ".exit":
		return true
	case ".help":
		c.printHelp()
	case ".version":
		printVersion(c.stderr)
	case ".open":
		c.cmdOpen(arg)
	case ".tables", ".keyspaces":
		c.cmdKeyspaces()
	case ".indexes", ".index":
		c.cmdIndexes()
	case ".schema":
		c.cmdSchema(arg)
	case ".mode":
		if cmd.ValidMode(arg) {
			c.mode = arg
		} else {
			fmt.Fprintf(c.stderr, "modes: %s\n", strings.Join(cmd.OutputModes, " "))
		}
	case ".timer":
		switch strings.ToLower(arg) {
		case "":
			fmt.Fprintf(c.stderr, "timer %s\n", onOff(c.timer))
		case "on":
			c.timer = true
		case "off":
			c.timer = false
		default:
			fmt.Fprintf(c.stderr, "usage: .timer [on|off] (currently %s)\n", onOff(c.timer))
		}
	case ".meta":
		// Toggle/check the _meta sub-object (path/name/ext/size/mtime). The engine
		// reads glue.ScanWalkOptions.Meta per query, so mutating it here takes
		// effect for subsequent statements.
		switch a := strings.ToLower(strings.TrimSpace(arg)); a {
		case "":
			fmt.Fprintf(c.stderr, "meta %s\n", glue.ScanWalkOptions.Meta)
		default:
			mm, err := records.ParseMetaMode(a)
			if err != nil {
				fmt.Fprintf(c.stderr, "usage: .meta [on|off|auto]  (currently %s)\n", glue.ScanWalkOptions.Meta)
			} else {
				glue.ScanWalkOptions.Meta = mm
				fmt.Fprintf(c.stderr, "meta %s\n", glue.ScanWalkOptions.Meta)
			}
		}
	case ".explain":
		c.explain = !c.explain
		fmt.Fprintf(c.stderr, "explain %s\n", onOff(c.explain))
	case ".maxrows":
		if arg == "" {
			fmt.Fprintf(c.stderr, "maxrows %s\n", c.maxRowsDesc())
		} else if n, err := strconv.Atoi(arg); err == nil {
			c.maxRows = n
			fmt.Fprintf(c.stderr, "maxrows %s\n", c.maxRowsDesc())
		} else {
			fmt.Fprintf(c.stderr, "usage: .maxrows <n>  (0 = all; negative = last |n| rows)\n")
		}
	case ".maxwidth":
		if arg == "" {
			fmt.Fprintf(c.stderr, "maxwidth %s\n", c.maxWidthDesc())
		} else if strings.EqualFold(arg, "auto") {
			c.maxWidth = -1
			fmt.Fprintf(c.stderr, "maxwidth %s\n", c.maxWidthDesc())
		} else if n, err := strconv.Atoi(arg); err == nil && n >= 0 {
			c.maxWidth = n
			fmt.Fprintf(c.stderr, "maxwidth %s\n", c.maxWidthDesc())
		} else {
			fmt.Fprintf(c.stderr, "usage: .maxwidth <n|auto>  (0 = uncapped; auto = fit terminal)\n")
		}
	case ".read":
		c.readFile(arg)
	case ".output":
		c.cmdOutput(arg)
	default:
		fmt.Fprintf(c.stderr, "unknown command %q -- try .help\n", name)
	}
	return false
}

func (c *cli) printHelp() {
	fmt.Fprint(c.stderr, `.help                 show this help
.open <dir>           open a different file datastore directory
.tables / .keyspaces  list keyspaces (with a copy-paste example each)
.indexes              list secondary indexes (.n1k1/catalog.json) with keys + stats
.schema [<keyspace>]  sampled shape (keys + JSON types) of a keyspace
.mode <m>             output mode (append |pretty to indent JSON): `+strings.Join(cmd.OutputModes, " ")+`
.meta [on|off|auto]   add a _meta sub-object to records (no arg shows the current setting)
.timer [on|off]       elapsed-time reporting (no arg shows the current setting)
.explain              toggle printing EXPLAIN PLAN per query
.maxrows <n>          box: cap rows shown (0 = all; negative = last |n| rows)
.maxwidth <n|auto>    box: cap column width (0 = uncapped; auto = fit terminal)
.read <file>          run statements/dot-commands from a file
.output [<file>]      send results to a file, or back to stdout if omitted
.version              show version + build info (incl. dependency SHAs)
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
	c.eagerBuildIndexes() // re-apply -index=eager to the newly opened datastore
	fmt.Fprintf(c.stderr, "opened %s\n", dir)
}

// eagerBuildIndexes builds all catalog-declared secondary indexes now when
// -index=eager, so the first query over the datastore pays no build cost. No-op
// in lazy/off mode or when the datastore has no indexes.
func (c *cli) eagerBuildIndexes() {
	if c.indexMode != "eager" || c.sess == nil || c.sess.Store == nil {
		return
	}
	if err := glue.EagerBuildSecondaryIndexes(c.sess.Store.Datastore); err != nil {
		fmt.Fprintf(c.stderr, "%s: eager index build: %v\n", c.prog, err)
	}
}

// cmdIndexes implements .indexes: list each declared secondary index with its
// keys, WHERE, and (once built) entry count and on-disk size. It opens/builds any
// not-yet-built index to report live stats.
func (c *cli) cmdIndexes() {
	if c.sess == nil || c.sess.Store == nil {
		fmt.Fprintln(c.stderr, "no datastore open")
		return
	}
	infos := glue.SecondaryIndexInfos(c.sess.Store.Datastore)
	if len(infos) == 0 {
		if c.indexMode == "off" {
			fmt.Fprintln(c.stderr, "secondary indexes disabled (-index=off)")
		} else {
			fmt.Fprintln(c.stderr, "no secondary indexes (declare them in .n1k1/catalog.json)")
		}
		return
	}
	for _, ix := range infos {
		name := ix.Namespace + ":" + ix.Keyspace + "." + ix.Name
		keys := "(" + strings.Join(ix.Keys, ", ") + ")"
		if ix.Where != "" {
			keys += " WHERE " + ix.Where
		}
		status := "not built"
		if ix.Built {
			status = fmt.Sprintf("%d entries, %s", ix.Entries, humanBytes(ix.SizeBytes))
		} else if ix.Err != "" {
			status = ix.Err
		}
		fmt.Fprintf(c.out, "%s %s  [%s]\n", name, keys, status)
	}
}

// humanBytes renders a byte count compactly (e.g. 4.0K, 1.2M).
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%c", float64(n)/float64(div), "KMGT"[exp])
}

func (c *cli) cmdKeyspaces() {
	c.printKeyspaces(c.out)
}

// keyspaceNames lists the keyspaces the datastore actually exposes in the
// current namespace -- via the datastore interface, not the raw filesystem, so
// the listing reflects n1k1's flattening (e.g. a synthetic flat-root keyspace,
// or later catalog-defined keyspaces), not just literal subdirectories.
func (c *cli) keyspaceNames() ([]string, error) {
	ns, nerr := c.sess.Store.Datastore.NamespaceByName(c.ns)
	if nerr != nil {
		// Namespace missing usually just means an empty datastore -- report it as
		// "no keyspaces" (empty list) rather than an error, so the caller can show
		// a friendly hint instead of a scary message.
		return nil, nil
	}
	names, kerr := ns.KeyspaceNames()
	if kerr != nil {
		return nil, fmt.Errorf("listing keyspaces: %v", kerr)
	}
	sort.Strings(names)
	return names, nil
}

// needsBackticks reports whether a keyspace/identifier name must be wrapped in
// backticks to parse as SQL++. An unquoted identifier is [A-Za-z_][A-Za-z0-9_]*;
// anything else -- empty, a leading digit, or a '.', '-', space, etc. -- needs
// quoting. Filesystem-derived keyspace names (a flat-root basename or single-file
// stem like "2026-01") routinely hit this. Backticks around an already-valid
// identifier are harmless, so being conservative here is safe.
func needsBackticks(name string) bool {
	if name == "" {
		return true
	}
	for i, r := range name {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
			// always allowed
		case r >= '0' && r <= '9':
			if i == 0 {
				return true // an unquoted identifier can't start with a digit
			}
		default:
			return true // any other rune (., -, space, :, ...) forces backticks
		}
	}
	return false
}

// quoteIdent wraps name in backticks when SQL++ parsing requires it, escaping an
// embedded backtick by doubling it. A valid bare identifier is returned unchanged.
func quoteIdent(name string) string {
	if !needsBackticks(name) {
		return name
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// exampleFor returns a copy-pasteable SQL++ example for a keyspace, varying the
// template by position so a multi-keyspace listing shows a mix. The keyspace is
// backticked when SQL++ parsing requires it, so the sample is paste-ready.
func exampleFor(ks string, i int) string {
	tmpls := []string{
		"SELECT COUNT(*) FROM %s;",
		"SELECT * FROM %s LIMIT 3;",
		"SELECT * FROM %s LIMIT 5;",
	}
	return fmt.Sprintf(tmpls[i%len(tmpls)], quoteIdent(ks))
}

// printKeyspaces writes the keyspace listing (with a copy-pasteable example per
// keyspace) to w. Shown at interactive startup and by .tables/.keyspaces.
func (c *cli) printKeyspaces(w io.Writer) {
	names, err := c.keyspaceNames()
	if err != nil {
		fmt.Fprintf(w, "%v\n", err)
		return
	}
	if len(names) == 0 {
		fmt.Fprintf(w, "%sNo keyspaces here yet — you can still evaluate expressions, e.g. %s\n",
			c.icon("💡 "), c.style.Cyan("SELECT 1+2;"))
		fmt.Fprintf(w, "   Point at data with %s (a dir of JSON, or <namespace>/<keyspace> subdirs).\n",
			c.style.Bold(".open <dir>"))
		return
	}
	// Display names backticked when SQL++ needs it (e.g. "2026-01"), so the
	// listed keyspace matches how it must be typed; pad on the displayed form.
	disp := make([]string, len(names))
	width := 0
	for i, n := range names {
		disp[i] = quoteIdent(n)
		if len(disp[i]) > width {
			width = len(disp[i])
		}
	}
	noun := "keyspaces"
	if len(names) == 1 {
		noun = "keyspace"
	}
	// The namespace is almost always "default"; only mention it when it isn't.
	nsNote := ""
	if c.ns != "default" {
		nsNote = " in namespace " + c.style.Bold(c.ns)
	}
	fmt.Fprintf(w, "%s%d %s%s — copy/paste to try:\n",
		c.icon("📚 "), len(names), noun, nsNote)
	for i, n := range names {
		pad := disp[i] + strings.Repeat(" ", width-len(disp[i]))
		fmt.Fprintf(w, "  %s   %s\n", c.style.Cyan(pad), c.style.Dim(exampleFor(n, i)))
	}
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
		c.style.On = c.fancyTTY // restore styling for the terminal
		return
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(c.stderr, "cannot create %q: %v\n", path, err)
		return
	}
	c.outFile, c.out = f, f
	c.style.On = false // never write ANSI codes to a file
}

// ---- helpers --------------------------------------------------------------

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
