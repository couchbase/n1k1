package cmd

import (
	"io/ioutil"
	"log"
	"regexp"
	"sort"
	"strings"
)

// State represents the gen-compiler process as it walks through the
// lines of n1k1 source code to generate a query compiler.
type State struct {
	// Stack of line handlers with associated callback data.
	Handlers []HandlerEntry

	ImportLines map[string]bool
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

func GenCompiler(sourceDir, outDir string) error {
	log.Printf(" GenCompiler, outDir: %s\n", outDir)

	state := &State{
		Handlers: []HandlerEntry{
			HandlerEntry{HandlerScanFile, ""},
		},
		ImportLines: map[string]bool{},
	}

	var outAll []string

	err := GenInterpMain(sourceDir, outDir, nil,
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
		},
		false, false)
	if err != nil {
		return err
	}

	contents := []string{
		"package n1k1_compiler\n",
	}

	var importLines []string
	for importLine := range state.ImportLines {
		importLines = append(importLines, importLine)
	}

	sort.Strings(importLines)

	contents = append(contents, "import (")
	contents = append(contents, importLines...)
	contents = append(contents, ")")

	contents = append(contents, outAll...)

	return ioutil.WriteFile(outDir+"/generated_by_n1k1_build.go",
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
	} else {
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

var LazyRE = regexp.MustCompile(`[Ll]azy`)

func HandlerScanTopLevelFuncBody(state *State, data string,
	out []string, line string) ([]string, string) {
	if len(line) > 0 && line[0] == '}' {
		state.Pop()

		return out, line
	}

	return EmitBlock(state, false, out, line)
}

func EmitBlock(state *State, isLazyBlock bool,
	out []string, line string) ([]string, string) {
	if !isLazyBlock && strings.Index(line, "return ") > 0 {
		// Emit non-lazy (e.g., top-level) return line as-is.
		return out, line
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

	line = strings.Split(line, "//")[0]

	var liveExprs []string

	line = SimpleExprRE.ReplaceAllStringFunc(line,
		func(simpleExpr string) string {
			if Keywords[simpleExpr] {
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

	line = "fmt.Printf(`" + line + "\n`"

    if len(liveExprs) > 0 {
		line = line + ", " + strings.Join(liveExprs, ", ")
	}

    line = line + ")"

	return out, line
}

var Keywords = map[string]bool{
	"var": true,
	"func": true,
	"return": true,
	"if": true,
	"else": true,
	"for": true,
	"range": true,
	"break": true,
	"defer": true,
	"switch": true,
	"case": true,
	"append": true,
	"len": true,
}

var SimpleExprRE = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9\._\[\]]+`)

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

func HandlerScanLazyBlock(state *State, data string,
	out []string, line string) ([]string, string) {
	lineToEndBlock := data
	if lineToEndBlock == line {
		state.Pop()
	}

	return EmitBlock(state, true, out, line)
}
