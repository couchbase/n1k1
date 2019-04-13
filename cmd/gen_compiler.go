package cmd

import (
	"log"
	"regexp"
	"strings"
)

// State represents the gen-compiler process as it walks through the
// lines of n1k1 source code to generate a query compiler.
type State struct {
	// Stack of line handlers with associated callback data.
	Handlers []HandlerEntry
}

func (s *State) Push(handler Handler, data string) {
	s.Handlers = append(s.Handlers, HandlerEntry{handler, data})
}

func (s *State) Pop() {
	s.Handlers = s.Handlers[0:len(s.Handlers)-1]
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
			HandlerEntry{HandlerScanForTopLevelFuncDeclStart, ""},
		},
	}

	return GenInterpMain(sourceDir, outDir,
		func(out []string, line string) ([]string, string) {
			return state.Process(out, line)
		}, false, false)
}

// --------------------------------------------------------

func HandlerScanForTopLevelFuncDeclStart(state *State, data string,
	out []string, line string) ([]string, string) {
	if !strings.HasPrefix(line, "func ") {
		return out, line
	}

	state.Push(HandlerScanForTopLevelFuncDeclEnd, "")

	return state.Process(out, line)
}

func HandlerScanForTopLevelFuncDeclEnd(state *State, data string,
	out []string, line string) ([]string, string) {
	if !strings.HasSuffix(line, " {") {
		return out, line
	}

	state.Pop()

	state.Push(HandlerForTopLevelFuncBody, "")

	return out, line
}

var LazyRE = regexp.MustCompile(`[Ll]azy`)

func HandlerForTopLevelFuncBody(state *State, data string,
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

	isLazyLine := LazyRE.Match([]byte(line))
	if !isLazyLine && !isLazyBlock {
		return out, line
	}

	if !isLazyBlock {
		if strings.Index(line, " = func(") > 0 ||
			strings.Index(line, "switch ") > 0 ||
			strings.Index(line, "for ") > 0 ||
			strings.Index(line, "if ") > 0 {
			state.Push(HandlerForLazyBlock, SpacePrefix(line) + "}")
		}
	}

	if strings.HasSuffix(line, " =") {
		// Eat 1st line of multi-line variable assignment.
		return out, ""
	}

	line = "fmt.Printf(`" + line + "\n`)"

	return out, line
}

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

func HandlerForLazyBlock(state *State, data string,
	out []string, line string) ([]string, string) {
	if strings.HasPrefix(line, data) {
		state.Pop()
	}

	return EmitBlock(state, true, out, line)
}
