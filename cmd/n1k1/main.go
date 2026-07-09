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
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"

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

// defaultNamespace is the only namespace n1k1's file datastore uses. Couchbase in
// practice only ever has "default", and every flat/single-file layout synthesizes
// its keyspaces into it, so the namespace isn't user-selectable; a rare classic
// multi-namespace directory tree can still be reached by qualifying a keyspace as
// "<ns>:<keyspace>" in SQL.
const defaultNamespace = "default"

func main() {
	var (
		cFlag       = flag.String("c", "", "run statements and exit")
		fFlag       = flag.String("f", "", "run statements from a file and exit")
		modeFlag    = flag.String("mode", "", "output mode: "+strings.Join(cmd.OutputModes, "|")+" (append |pretty to indent JSON; default box|pretty at a TTY, else jsonlines)")
		timerFlag   = flag.Bool("timer", false, "print row count + elapsed after each statement")
		echoFlag    = flag.Bool("echo", false, "echo each input line (SQL++ / dot-commands) as it's read; like .echo on (handy with -f)")
		initFlag    = flag.String("init", "", "startup file of dot-commands/SQL++ (default ~/."+prog+"rc; use \"\", \"-\" or \"none\" to skip)")
		formatsFlag = flag.String("formats", "", "restrict files scanned to a comma-separated set (all|json|jsonl|csv|tsv|extract|doc|text|image|video|gzip|recurse); empty or 'all' = everything")
		metaFlag    = flag.String("meta", "auto", "add a _meta sub-object (path/name/ext/size/mtime) to records: on|off|auto (auto = extracted docs only)")
		verFlag     = flag.Bool("version", false, "print version + build info (incl. dependency SHAs) and exit")
		cpuProfile  = flag.String("profile-cpu", "", "write a CPU profile to this file (go tool pprof)")
		memProfile  = flag.String("profile-mem", "", "write a memory/alloc profile to this file at exit (go tool pprof)")
		indexFlag   = flag.String("index", "lazy", "use catalog (secondary/FTS) indexes: "+
			"lazy (default; build each on first use) | eager (build all up front)"+
			" | off (ignore the catalog; always full-scan)")
	)
	// -ext / -extensions (synonyms): load extensions at startup. Repeatable
	// and comma-separated (a dir or a file each); kind auto-detected by file
	// extension (.js = JavaScript UDF). See the .extensions REPL command.
	var extPaths extPathsFlag
	flag.Var(&extPaths, "ext", "load extension(s) from a dir or file (repeatable; comma-separated ok); .js = JavaScript UDF. See .extensions")
	flag.Var(&extPaths, "extensions", "alias for -ext")
	// -verbose / -v (synonyms sharing one value): a diagnostics level. A bare
	// -verbose means on (level 1) and repeats accumulate (-v -v -v -> 3);
	// -verbose=on|off|debug|<n> sets an explicit level. normalizeVerbose lets the
	// space form (-verbose 3, -v on) work too. See .verbose in the REPL.
	var vLevel verboseLevel
	flag.Var(&vLevel, "verbose", "verbose level: bare -verbose = on (repeat to raise); -verbose=on|off|debug|<n> sets it (see .verbose)")
	flag.Var(&vLevel, "v", "alias for -verbose")

	// -stats: bare -stats (or -stats=on) = live footer; -stats=off = none;
	// -stats=final (alias end) = grand totals once at the end, no live footer (for
	// clean measurement without the live-render overhead). See .stats in the REPL.
	statsMode := statsOff
	flag.Var(statsModeFlag{&statsMode}, "stats",
		"per-operator counters + a runtime footer: on (live) | off | final (totals at end only); see .stats")

	// -prepare=<level>: the MAX level PREPARE may compile a statement to (a ceiling).
	// Default interpreted (cache op trees, no codegen); data/full opt into codegen.
	// PREPARE/EXECUTE work as SQL statements regardless; see .prepare and DESIGN-prepare.md.
	prepareLevel := glue.PrepareInterpreted
	flag.Var(prepareLevelFlag{&prepareLevel}, "prepare",
		"max prepare/codegen level: interpreted (default) | data | full; bare -prepare = full. See .prepare")

	flag.Usage = usage

	flag.CommandLine.Parse(normalizeVerbose(os.Args[1:]))

	// The sidecar dir (catalog.json, built indexes) is named after this binary --
	// ".n1k1" by default, ".<alias>" when invoked under a symlinked name.
	glue.SetSidecarName("." + prog)

	// -version works without a datastore, so handle it before opening a session.
	if *verFlag {
		printVersion(os.Stdout)
		return
	}

	base.LogLevel = int(vLevel)

	// -index selects when catalog-declared secondary indexes are built (lazy on
	// first use, eager up front, or off = ignore the catalog). See DESIGN-indexing.md.
	switch *indexFlag {
	case "eager", "lazy", "off":
		glue.SecondaryIndexMode = *indexFlag
	default:
		fmt.Fprintf(os.Stderr, "%s: bad -index %q (want eager|lazy|off)\n", prog, *indexFlag)
		os.Exit(2)
	}

	// -meta controls per-file metadata injection (_meta).
	mm, merr := records.ParseMetaMode(*metaFlag)
	if merr != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", prog, merr)
		os.Exit(2)
	}

	dir := "."
	explicit := false
	if args := flag.Args(); len(args) > 0 {
		dir = args[0]
		explicit = true
	}

	// -formats locks down which file formats/compression/recursion n1k1 will scan.
	// Precedence: an explicit -formats flag wins; else the datastore's persisted
	// formats (catalog.json, see CatalogSetFormats); else the flexible default.
	formatsGiven := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "formats" {
			formatsGiven = true
		}
	})

	formatsStr := *formatsFlag
	if !formatsGiven {
		if cf, cerr := glue.CatalogFormats(dir); cerr == nil {
			formatsStr = cf
		}
	}

	scanOpts, serr := records.ParseModes(formatsStr)
	if serr != nil {
		if formatsGiven {
			fmt.Fprintf(os.Stderr, "%s: bad -formats: %v\n", prog, serr)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "%s: ignoring bad formats in catalog.json (%v)\n", prog, serr)
		scanOpts = records.AllModes()
	}
	scanOpts.Meta = mm
	glue.ScanWalkOptions = scanOpts

	// Register extensions (JS UDFs + *.extract.js recipes) BEFORE opening the store:
	// resolveSession -> FileStore decides which files become keyspaces, and a
	// recipe-matched file is auto-exposed as a keyspace (glue/flat.go), so the recipes
	// must be registered first.
	if len(extPaths) > 0 {
		if _, lerr := loadExtensions(extPaths); lerr != nil {
			fmt.Fprintf(os.Stderr, "%s: -ext: %v\n", prog, lerr)
			os.Exit(1)
		}
	}

	sess, dir, cleanup, err := resolveSession(dir, explicit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", prog, err)
		os.Exit(1)
	}
	defer cleanup()
	if dir == "" { // fell back to an empty store (no path was given)
		fmt.Fprintf(os.Stderr, "%s: no datastore; starting empty — use %s\n",
			prog, ".open <dir>")
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
	fancy := isTTY(os.Stdout) &&
		os.Getenv(base.DefEnv("NO_COLOR", "disable colored terminal output")) == ""

	c := &cli{
		prog:         prog,
		sess:         sess,
		dir:          dir,
		mode:         mode,
		indexMode:    *indexFlag,
		timer:        *timerFlag,
		echo:         *echoFlag,
		prepareLevel: prepareLevel,
		statsMode:    statsMode,
		verbose:      int(vLevel),
		maxRows:      0,
		maxWidth:     -1,
		listSep:      "|",
		out:          os.Stdout,
		stderr:       os.Stderr,
		fancyTTY:     fancy,
		style:        cmd.Style{On: fancy},
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

	// -profile-cpu / -profile-mem: standard go-tool-pprof profiles around the run
	// below. The mem profile is written at exit (deferred, after a GC so inuse is
	// current); alloc_space in it is cumulative, which is what shows where a query's
	// allocations come from -- pair with -stats=final. (os.Exit paths skip these
	// defers, but the -c/-f/pipe/REPL run paths return normally.)
	if *cpuProfile != "" {
		f, ferr := os.Create(*cpuProfile)
		if ferr != nil {
			fmt.Fprintf(os.Stderr, "%s: profile-cpu: %v\n", prog, ferr)
			os.Exit(1)
		}
		defer f.Close()
		if serr := pprof.StartCPUProfile(f); serr != nil {
			fmt.Fprintf(os.Stderr, "%s: profile-cpu: %v\n", prog, serr)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}
	if *memProfile != "" {
		defer func() {
			f, ferr := os.Create(*memProfile)
			if ferr != nil {
				fmt.Fprintf(os.Stderr, "%s: profile-mem: %v\n", prog, ferr)
				return
			}
			defer f.Close()
			runtime.GC() // update in-use stats; alloc_space is cumulative regardless
			if werr := pprof.WriteHeapProfile(f); werr != nil {
				fmt.Fprintf(os.Stderr, "%s: profile-mem: %v\n", prog, werr)
			}
		}()
	}

	// A non-interactive run (-c / -f / a stdin pipe) signals failure via the process
	// exit code so a script or CI step (e.g. `make detect-test`, which drives
	// `-c '.detect test ...'`) fails the build on any statement / dot-command error.
	// c.sawError latches across the whole run (feed/flush/batch set it -- see repl.go);
	// reset it here so a failing startup init file doesn't poison the exit code. The
	// interactive REPL never exits non-zero on a query error (you just see the error).
	c.sawError = false
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
		return
	}
	if c.sawError {
		os.Exit(1)
	}
}

// resolveSession opens a Session for dir. Failing to open an *explicitly given*
// path -- a typo, a missing/unreadable directory or file, or a file that isn't a
// datastore -- is returned as an error so the caller exits non-zero; silently
// querying an empty store would let a bad path in a script "succeed". When no path
// was given (a bare REPL), an open failure instead falls back to a fresh empty
// store so the user can still evaluate expressions and `.open` a datastore later;
// in that case effDir is "" and cleanup removes the temp dir (it's a no-op
// otherwise). Callers should always `defer cleanup()`.
func resolveSession(dir string, explicit bool) (sess *glue.Session, effDir string, cleanup func(), err error) {
	if sess, err = glue.OpenSession(dir, defaultNamespace); err == nil {
		return sess, dir, func() {}, nil
	}
	if explicit {
		return nil, "", func() {}, fmt.Errorf("cannot open datastore %q: %s", dir, tidyMsg(err.Error()))
	}
	// No path was named: keep going with an empty store.
	empty, e2 := os.MkdirTemp("", "n1k1-empty-")
	if e2 != nil {
		return nil, "", func() {}, e2
	}
	if sess, err = glue.OpenSession(empty, defaultNamespace); err != nil {
		os.RemoveAll(empty)
		return nil, "", func() {}, err
	}
	return sess, "", func() { os.RemoveAll(empty) }, nil
}

func usage() {
	fmt.Fprintf(os.Stderr, `%[1]s -- SQL++ for local files (json, jsonl, csv, yaml, gz, and more)

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

	fmt.Fprintf(os.Stderr, "\nenv vars:\n")
	kMaxLen := 0
	for k, _ := range base.EnvAbout {
		if kMaxLen < len(k) {
			kMaxLen = len(k)
		}
	}
	var lines []string
	for k, about := range base.EnvAbout {
		lines = append(lines, fmt.Sprintf("  %s:%s %s\n", k,
			strings.Repeat(" ", kMaxLen-len(k)), about))
	}
	sort.Strings(lines)
	for _, line := range lines {
		fmt.Fprint(os.Stderr, line)
	}
}

// cli holds front-end state; all engine work goes through c.sess (glue.Session).
type cli struct {
	prog string // short command name (from how the binary was invoked)
	sess *glue.Session
	dir  string

	mode         string
	indexMode    string // -index: eager|lazy|off (drives eager build on open)
	timer        bool
	verbose      int // 0=off, 1=show query plans, 2=+timing (see .verbose)
	explain      bool
	prepareLevel glue.PrepareLevel // -prepare/.prepare: max compile level ceiling (interpreted|data|full)
	echo         bool              // echo each input line as it's read (see .echo)
	bail         bool              // stop the input loop on the first statement error (see .bail)
	failed       bool              // transient: the last exec'd statement / dot-command errored (drives .bail)
	sawError     bool              // latched: ANY statement / dot-command errored during a non-interactive run (drives the process exit code)
	statsMode    string            // "off" | "on" (live) | "final" (totals at end); see .stats, DESIGN-stats.md
	maxRows      int               // box: 0 = all; >0 = head+tail; <0 = last |n| rows
	maxWidth     int               // box: per-column cap; 0 = uncapped; <0 = auto (fit terminal)
	listSep      string

	out     io.Writer // result destination (stdout, or a .output file)
	outFile *os.File  // non-nil when .output redirected to a file
	outErr  error     // transient: last streaming-write error (e.g. a closed output pipe)
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
