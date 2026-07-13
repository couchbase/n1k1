package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/cmd"
)

var Keywords = map[string]bool{
	"var":    true,
	"func":   true,
	"return": true,
	"if":     true,
	"else":   true,
	"for":    true,
	"range":  true,
	"break":  true,
	"defer":  true,
	"select": true,
	"switch": true,
	"case":   true,
	"make":   true,
	"append": true,
	"copy":   true,
	"cap":    true,
	"len":    true,
	"nil":    true,
	"true":   true,
	"false":  true,
	"error":  true,
	"go":     true,
	"close":  true,

	"interface": true,
	"string":    true,
	"bool":      true,
	"byte":      true,
	"int":       true,
	"int64":     true,
	"uint64":    true,
}

// ---------------------------------------------------------------

// State represents the gen-compiler process as it walks through the
// lines of n1k1 source code to generate a query compiler.
type State struct {
	// Stack of line handlers with associated callback data.
	Handlers []*HandlerEntry

	Imports     map[string]bool
	ImportLines map[string]bool

	Indent string
}

func (s *State) Push(he *HandlerEntry) {
	s.Handlers = append(s.Handlers, he)
}

func (s *State) Pop() {
	s.Handlers = s.Handlers[0 : len(s.Handlers)-1]
}

func (s *State) Process(out []string, line string) ([]string, string) {
	curr := s.Handlers[len(s.Handlers)-1]

	return curr.Handler(s, curr, out, line)
}

type HandlerEntry struct {
	Handler Handler

	EndLine string

	Replacements map[string]string
}

// Handler represents a callback to process an incoming line.
type Handler func(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string)

// --------------------------------------------------------

func IntermedBuild(sourceDir, outDir string) error {
	log.Printf(" IntermedBuild, outDir: %s\n", outDir)

	state := &State{
		Handlers: []*HandlerEntry{
			&HandlerEntry{Handler: HandlerScanFile},
		},
		Imports:     map[string]bool{},
		ImportLines: map[string]bool{},
	}

	var outAll []string

	err := VisitSourceLines(sourceDir, outDir, nil,
		func(out []string, line string) ([]string, string, error) {
			if strings.HasPrefix(line, "package ") {
				return out, "", nil
			}

			if strings.Index(line, "// <== genCompiler:hide") > 0 {
				line = "// " + line
			}

			out, line = state.Process(out, line)

			return out, line, nil
		},
		func(fileName string, out []string) error {
			outAll = append(outAll, out...)

			return nil
		})
	if err != nil {
		return err
	}

	contents := []string{
		"package intermed\n",
	}

	var importLines []string
	for importLine := range state.ImportLines {
		importLines = append(importLines, importLine)
	}

	sort.Strings(importLines)

	contents = append(contents, "import (")
	contents = append(contents, importLines...)
	contents = append(contents, ")\n")

	contents = append(contents, `var Emit = fmt.Printf`)
	contents = append(contents, `var EmitLift = fmt.Printf`)
	contents = append(contents, `var EmitCaptured = func(path, pathItem, orig string) {}`)

	contents = append(contents, outAll...)

	return ioutil.WriteFile(outDir+"/generated_by_intermed_build.go",
		[]byte(strings.Join(contents, "\n")), 0644)
}

// --------------------------------------------------------

func HandlerScanFile(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string) {
	if line == "import (" {
		state.Push(&HandlerEntry{Handler: HandlerScanImports})

		return out, ""
	}

	if strings.HasPrefix(line, "func ") {
		state.Push(&HandlerEntry{Handler: HandlerScanTopLevelFuncSignature})

		return state.Process(out, line)
	}

	return out, line
}

func HandlerScanImports(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string) {
	if line == ")" {
		state.Pop()
	} else if len(line) > 0 {
		parts := strings.Split(strings.Split(line, `"`)[1], "/")

		state.Imports[parts[len(parts)-1]] = true

		state.ImportLines[line] = true
	}

	return out, ""
}

func HandlerScanTopLevelFuncSignature(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string) {
	if !strings.HasSuffix(line, " {") {
		return out, line
	}

	state.Pop()

	state.Push(&HandlerEntry{Handler: HandlerScanTopLevelFuncBody})

	return out, line
}

func HandlerScanTopLevelFuncBody(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string) {
	if len(line) > 0 && line[0] == '}' {
		state.Pop()

		return out, line
	}

	return EmitBlock(state, he, false, out, line)
}

// ---------------------------------------------------------------

func HandlerScanLzBlock(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string) {
	if line == he.EndLine {
		state.Pop()
	}

	return EmitBlock(state, he, true, out, line)
}

// ---------------------------------------------------------------

var LzRE = regexp.MustCompile(`[Ll]z`)

var SimpleExprRE = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9\._]+`)

// ArgTokenRE matches a positional placeholder token \x00<n>\x00 that both the
// varLift and SimpleExprRE passes plant where a live fmt arg belongs. It has no
// letters, so SimpleExprRE never matches it. A final left-to-right scan turns
// each token back into "%s" and collects its arg in the ORDER it appears on the
// line -- see EmitBlock.
var ArgTokenRE = regexp.MustCompile("\x00([0-9]+)\x00")

func EmitBlock(state *State, he *HandlerEntry, isLzBlock bool,
	out []string, line string) ([]string, string) {
	if !isLzBlock && strings.Index(line, "return ") > 0 {
		// Emit non-lz (e.g., top-level) return line as-is.
		return out, line
	}

	var emit = "Emit"

	var liveExprs []string

	liveExprsIgnore := map[string]bool{}

	// Positional arg tokens: the varLift and SimpleExprRE passes each plant a
	// \x00<n>\x00 token where a live fmt arg belongs, recording the Go expression
	// for that arg. A final scan (below) walks the line left-to-right, turning
	// each token into "%s" and appending its arg -- so a varLift (buffer) arg and
	// a SimpleExprRE (live-expr / func) arg on the SAME line interleave in the
	// right order. Appending per-pass instead mis-ordered them (all varLift args
	// ahead of all SimpleExprRE args regardless of on-line position).
	var tokenArgs []string
	mkToken := func(arg string) string {
		tok := "\x00" + strconv.Itoa(len(tokenArgs)) + "\x00"
		tokenArgs = append(tokenArgs, arg)
		return tok
	}

	lineLeftRight := strings.Split(line, "// ")
	if len(lineLeftRight) > 1 {
		if lineLeftRight[1] == "!lz" {
			return out, line
		}

		// Marker that allows expansion from a previously captured
		// output from EmitPop().
		//
		// Ex: lzFoo(lzVals) // <== emitCaptured: path pathItem
		if strings.HasPrefix(lineLeftRight[1], "<== emitCaptured: ") {
			rightParts := strings.Split(lineLeftRight[1], " ")

			pathVar := rightParts[2]
			pathItem := rightParts[3]

			line = `EmitCaptured(` +
				`fmt.Sprintf("%s", ` + pathVar + `), ` +
				pathItem + `, ` +
				fmt.Sprintf("%q", lineLeftRight[0]) + `)`

			return out, line
		}

		// Marker that allows variables to be lifted or hoisted
		// to the top of an EmitPush() stack.
		//
		// Ex: var lzFoo MyType // <== varLift: lzFoo by path
		// Ex: lzFoo = something // <== varLift: lzFoo by path
		// Ex: lzBar = lzFoo // <== varLift: lzFoo by path
		if strings.HasPrefix(lineLeftRight[1], "<== varLift: ") {
			rightParts := strings.Split(lineLeftRight[1], " ")

			varName := rightParts[2]
			suffix := rightParts[4]

			if he.Replacements == nil {
				he.Replacements = map[string]string{}
			}

			he.Replacements[varName] = suffix

			if strings.Index(lineLeftRight[0], "var ") > 0 {
				// Hoist the var declaration via EmitLift().
				emit = "EmitLift"
			}

			// Fall-through for usual processing of lz vars.
		}
	}

	for hei := len(state.Handlers) - 1; hei >= 0; hei-- {
		he := state.Handlers[hei]

		for varName, suffix := range he.Replacements {
			if strings.Index(lineLeftRight[0], varName) > 0 {
				// Every occurrence gets the SAME token; the final scan emits the
				// suffix once per occurrence (a var used twice on one line, e.g.
				// `lzBuf = f(lzBuf[:0])`, then correctly gets two `%s` args).
				tok := mkToken(suffix)

				lineLeftRight[0] = strings.Replace(lineLeftRight[0],
					varName, varName+tok, -1)

				liveExprsIgnore[suffix] = true
			}
		}
	}

	isLzLine := LzRE.MatchString(line)
	if !isLzLine && !isLzBlock {
		return out, line
	}

	if !isLzBlock {
		if strings.Index(line, " = func(") > 0 ||
			strings.Index(line, "switch ") > 0 ||
			strings.Index(line, "for ") > 0 ||
			strings.Index(line, "if ") > 0 {
			state.Push(&HandlerEntry{
				Handler: HandlerScanLzBlock,
				EndLine: SpacePrefix(line) + "}",
			})
		}
	}

	if strings.HasSuffix(line, " =") ||
		strings.HasSuffix(line, " :=") {
		// Eat 1st line of multi-line variable assignment.
		return out, ""
	}

	line = lineLeftRight[0] // Strips off line suffix comment.

	// Convert non-lz expressions into fmt placeholder.
	line = SimpleExprRE.ReplaceAllStringFunc(line,
		func(simpleExpr string) string {
			if Keywords[simpleExpr] || liveExprsIgnore[simpleExpr] {
				// A go-lang keyword.
				return simpleExpr
			}

			if state.Imports[strings.Split(simpleExpr, ".")[0]] {
				// Reference of an imported identfier.
				return simpleExpr
			}

			if LzRE.MatchString(simpleExpr) {
				// It's a lz expression, so it's not an alive
				// expression at compile-time.
				return simpleExpr
			}

			// Render live exprs via base.LzExprFmt rather than a raw %#v: it is
			// %#v for ints/bools/strings (unchanged) but emits a FUNC value by its
			// qualified name, so a harness can take a real func param (e.g.
			// base.StrCaseUpper) instead of an int op-code + switch.
			return mkToken("base.LzExprFmt(" + simpleExpr + ")")
		})

	// Turn positional tokens into %s in the order they appear, collecting args.
	line = ArgTokenRE.ReplaceAllStringFunc(line, func(tok string) string {
		idx, _ := strconv.Atoi(tok[1 : len(tok)-1])
		liveExprs = append(liveExprs, tokenArgs[idx])
		return "%s"
	})

	if strings.HasSuffix(line, "}") &&
		len(state.Indent) > 0 {
		state.Indent = state.Indent[:len(state.Indent)-1]
	}

	lineOrig := line

	line = emit + "(`" + state.Indent + strings.TrimSpace(line) + "\n`"

	if len(liveExprs) > 0 {
		line = line + ", " + strings.Join(liveExprs, ", ")
	}

	line = line + ")"

	if strings.HasSuffix(lineOrig, "{") {
		state.Indent = state.Indent + " "
	}

	return out, line
}

// ---------------------------------------------------------------

// SpacePrefix returns the whitespace prefix of the given line.
func SpacePrefix(line string) (prefix string) {
	for len(line) > 0 {
		if line[0] != ' ' && line[0] != '\t' {
			break
		}

		prefix = prefix + string(line[0])

		line = line[1:]
	}

	return prefix
}

// ---------------------------------------------------------------

var LzPrefixRE = regexp.MustCompile(`lz[A-Z]`)

func LzPrefixREFunc(lzX string) string {
	return strings.ToLower(lzX[len(lzX)-1:])
}

// ---------------------------------------------------------------

// expandLzRHS rewrites a single-line "!lzRHS" convenience form into the raw multi-
// line form the codegen expects, so engine source can stay one line per assignment
// instead of splitting the RHS onto its own "// !lz" line by hand:
//
//	LHS = RHS  // !lzRHS            ->  "LHS ="
//	                                    "\tRHS // !lz"
//
//	LHS := RHS // !lzRHS, via: V    ->  "V ="
//	                                    "\tRHS // !lz"
//	                                    "LHS := V"
//
// Returns (expandedLines, true) when the line carried a !lzRHS marker, else (nil,
// false). Indentation is preserved and the RHS is copied verbatim, so the emitted
// output is byte-identical to hand-writing the multi-line form. Each expanded line
// is then fed through the normal per-line processing (see VisitSourceLines).
func expandLzRHS(line string) ([]string, bool) {
	const marker = "// !lzRHS"
	mi := strings.Index(line, marker)
	if mi < 0 {
		return nil, false
	}
	// Never touch a comment line that merely mentions the marker (e.g. this doc).
	if strings.HasPrefix(strings.TrimLeft(line, " \t"), "//") {
		return nil, false
	}
	code := strings.TrimRight(line[:mi], " ")        // "<indent>LHS <op> RHS"
	tail := strings.TrimSpace(line[mi+len(marker):]) // "" or ", via: V"
	via := ""
	if t := strings.TrimSpace(strings.TrimPrefix(tail, ",")); strings.HasPrefix(t, "via:") {
		via = strings.TrimSpace(strings.TrimPrefix(t, "via:"))
	}
	indent := code[:len(code)-len(strings.TrimLeft(code, " \t"))]
	body := code[len(indent):] // "LHS <op> RHS"
	var lhs, op, rhs string
	if i := strings.Index(body, " := "); i >= 0 {
		lhs, op, rhs = body[:i], ":=", body[i+len(" := "):]
	} else if i := strings.Index(body, " = "); i >= 0 {
		lhs, op, rhs = body[:i], "=", body[i+len(" = "):]
	} else {
		return nil, false // no assignment operator -- not an lzRHS assignment
	}
	if via == "" {
		return []string{
			indent + lhs + " " + op,
			indent + "\t" + rhs + " // !lz",
		}, true
	}
	return []string{
		indent + via + " =",
		indent + "\t" + rhs + " // !lz",
		indent + lhs + " " + op + " " + via,
	}, true
}

func VisitSourceLines(sourceDir, outDir string,
	cbFileStart func(fileName string) error,
	cbFileLine func(out []string, line string) ([]string, string, error),
	cbFileEnd func(fileName string, out []string) error) error {
	sourcePackage := "package n1k1"

	outDirParts := strings.Split(outDir, "/")
	outPackage := "package " + outDirParts[len(outDirParts)-1]

	var out []string // Collected output or resulting lines.

	cb := func(kind, data string) (err error) {
		switch kind {
		case "fileStart":
			fileName := data

			log.Printf("  fileName: %s\n", fileName)

			out = nil

			if cbFileStart != nil {
				err = cbFileStart(fileName)
			}

		case "fileLine":
			// A one-line "// !lzRHS" assignment expands to the raw multi-line form
			// first; each resulting line then goes through the normal processing.
			srcLines, expanded := expandLzRHS(data)
			if !expanded {
				srcLines = []string{data}
			}

			for _, line := range srcLines {
				line = strings.Replace(line, sourcePackage, outPackage, -1)

				// Optional callback that can examine the incoming line,
				// and modify the line and/or the out state.
				if cbFileLine != nil {
					out, line, err = cbFileLine(out, line)
					if err != nil {
						break
					}
				}

				out = append(out, line)
			}

		case "fileEnd":
			fileName := data

			err = cbFileEnd(fileName, out)
		}

		return err
	}

	return cmd.VisitFiles(sourceDir, ".go", cb)
}
