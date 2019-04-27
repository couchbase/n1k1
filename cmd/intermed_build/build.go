package main

import (
	"fmt"
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
	"nil":    true,
	"error":  true,

	"interface": true,
	"bool":      true,
	"int":       true,
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
	log.Printf(" BuildCompiler, outDir: %s\n", outDir)

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

func EmitBlock(state *State, he *HandlerEntry, isLzBlock bool,
	out []string, line string) ([]string, string) {
	if !isLzBlock && strings.Index(line, "return ") > 0 {
		// Emit non-lz (e.g., top-level) return line as-is.
		return out, line
	}

	var emit = "Emit"

	var liveExprs []string

	liveExprsIgnore := map[string]bool{}

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
				lineLeftRight[0] = strings.Replace(lineLeftRight[0],
					varName, varName+"%s", -1)

				liveExprs = append(liveExprs, suffix)

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

var LzPrefixRE = regexp.MustCompile(`lz[A-Z]`)

func LzPrefixREFunc(lzX string) string {
	return strings.ToLower(lzX[len(lzX)-1:])
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
