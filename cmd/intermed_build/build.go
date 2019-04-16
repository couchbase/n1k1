package main

import (
	"io/ioutil"
	"log"
	"regexp"
	"sort"
	"strings"
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
	"switch": true,
	"case":   true,
	"make":   true,
	"append": true,
	"len":    true,
}

// ---------------------------------------------------------------

// State represents the gen-compiler process as it walks through the
// lines of n1k1 source code to generate a query compiler.
type State struct {
	// Stack of line handlers with associated callback data.
	Handlers []HandlerEntry

	Imports     map[string]bool
	ImportLines map[string]bool

	Indent string
}

func (s *State) Push(handler Handler, data string) {
	s.Handlers = append(s.Handlers, HandlerEntry{handler, data})
}

func (s *State) Pop() {
	s.Handlers = s.Handlers[0 : len(s.Handlers)-1]
}

func (s *State) Process(out []string, line string) ([]string, string) {
	curr := s.Handlers[len(s.Handlers)-1]

	return curr.Handler(s, curr.Data, out, line)
}

type HandlerEntry struct {
	Handler Handler
	Data    string
}

// Handler represents a callback to process an incoming line.
type Handler func(state *State, data string,
	out []string, line string) ([]string, string)

// --------------------------------------------------------

func IntermedBuild(sourceDir, outDir string) error {
	log.Printf(" BuildCompiler, outDir: %s\n", outDir)

	state := &State{
		Handlers: []HandlerEntry{
			HandlerEntry{HandlerScanFile, ""},
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

	contents = append(contents, outAll...)

	return ioutil.WriteFile(outDir+"/generated_by_intermed_build.go",
		[]byte(strings.Join(contents, "\n")), 0644)
}

// --------------------------------------------------------

func HandlerScanFile(state *State, data string,
	out []string, line string) ([]string, string) {
	if line == "import (" {
		state.Push(HandlerScanImports, "")

		return out, ""
	}

	if strings.HasPrefix(line, "func ") {
		state.Push(HandlerScanTopLevelFuncSignature, "")

		return state.Process(out, line)
	}

	return out, line
}

func HandlerScanImports(state *State, data string,
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

func HandlerScanTopLevelFuncSignature(state *State, data string,
	out []string, line string) ([]string, string) {
	if !strings.HasSuffix(line, " {") {
		return out, line
	}

	state.Pop()

	state.Push(HandlerScanTopLevelFuncBody, "")

	return out, line
}

func HandlerScanTopLevelFuncBody(state *State, data string,
	out []string, line string) ([]string, string) {
	if len(line) > 0 && line[0] == '}' {
		state.Pop()

		return out, line
	}

	return EmitBlock(state, false, out, line)
}

// ---------------------------------------------------------------

func HandlerScanLazyBlock(state *State, data string,
	out []string, line string) ([]string, string) {
	lineToEndBlock := data
	if lineToEndBlock == line {
		state.Pop()
	}

	return EmitBlock(state, true, out, line)
}

// ---------------------------------------------------------------

var LazyRE = regexp.MustCompile(`[Ll]azy`)

var SimpleExprRE = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9\._]+`)

func EmitBlock(state *State, isLazyBlock bool,
	out []string, line string) ([]string, string) {
	if !isLazyBlock && strings.Index(line, "return ") > 0 {
		// Emit non-lazy (e.g., top-level) return line as-is.
		return out, line
	}

	var emit = "Emit"

	var liveExprs []string

	liveExprsIgnore := map[string]bool{}

	lineLeftRight := strings.Split(line, "// ")
	if len(lineLeftRight) > 1 {
		if lineLeftRight[1] == "<== inlineOk" {
			return out, line
		}

		// Marker that allows variables to be lifted or hoisted
		// to the top of an EmitPush() scope.
		//
		// Ex: var lazyFoo MyType // <== varLift: lazyFoo by path
		// Ex: lazyFoo = something // <== varLift: lazyFoo by path
		// Ex: lazyBar = lazyFoo // <== varLift: lazyFoo by path
		if strings.HasPrefix(lineLeftRight[1], "<== varLift: ") {
			rightParts := strings.Split(lineLeftRight[1], " ")

			varName := rightParts[2]
			suffix := rightParts[4]

			lineLeftRight[0] = strings.Replace(lineLeftRight[0],
				varName, varName+"%s", -1)

			liveExprs = append(liveExprs, suffix)

			liveExprsIgnore[suffix] = true

			if strings.Index(lineLeftRight[0], "var ") > 0 {
				// Hoist the var declaration via EmitLift().
				emit = "EmitLift"
			}

			// Fall-through for usual processing of lazy vars.
		}
	}

	isLazyLine := LazyRE.MatchString(line)
	if !isLazyLine && !isLazyBlock {
		return out, line
	}

	if !isLazyBlock {
		if strings.Index(line, " = func(") > 0 ||
			strings.Index(line, "switch ") > 0 ||
			strings.Index(line, "for ") > 0 ||
			strings.Index(line, "if ") > 0 {
			state.Push(HandlerScanLazyBlock, SpacePrefix(line)+"}")
		}
	}

	if strings.HasSuffix(line, " =") {
		// Eat 1st line of multi-line variable assignment.
		return out, ""
	}

	line = lineLeftRight[0] // Strips off line suffix comment.

	// Convert non-lazy expressions into fmt placeholder.
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

			if LazyRE.MatchString(simpleExpr) {
				// It's a lazy expression, so it's not an alive
				// expression at compile-time.
				return simpleExpr
			}

			liveExprs = append(liveExprs, simpleExpr)

			return "%#v"
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

var LazyPrefixRE = regexp.MustCompile(`lazy[A-Z]`)

func LazyPrefixREFunc(lazyX string) string {
	return strings.ToLower(lazyX[len(lazyX)-1:])
}

// ---------------------------------------------------------------

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
			line := data

			line = strings.Replace(line, sourcePackage, outPackage, -1)

			// Optional callback that can examine the incoming line,
			// and modify the line and/or the out state.
			if cbFileLine != nil {
				out, line, err = cbFileLine(out, line)
			}

			out = append(out, line)

		case "fileEnd":
			fileName := data

			err = cbFileEnd(fileName, out)
		}

		return err
	}

	return VisitFiles(sourceDir, ".go", cb)
}
