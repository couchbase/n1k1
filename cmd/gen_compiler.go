package cmd

import (
	"log"
	"strings"
)

// State represents the gen-compiler process as it walks through the
// lines of n1k1 source code to generate a query compiler.
type State struct {
	Handlers []Handler // Stack of line handlers.
}

func (s *State) Push(h Handler) {
	s.Handlers = append(s.Handlers, h)
}

func (s *State) Pop() {
	s.Handlers = s.Handlers[0:len(s.Handlers)-1]
}

func (s *State) Process(out []string, line string) ([]string, string) {
	return s.Handlers[len(s.Handlers)-1](s, out, line)
}

// Handler represents a callback to process an incoming line.
type Handler func(state *State, out []string, line string) (
	[]string, string)

// --------------------------------------------------------

func GenCompiler(sourceDir, outDir string) error {
	log.Printf(" GenCompiler, outDir: %s\n", outDir)

	state := &State{Handlers: []Handler{ScanForTopLevelFunc}}

	return GenInterpMain(sourceDir, outDir,
		func(out []string, line string) ([]string, string) {
			return state.Process(out, line)
		})
}

// --------------------------------------------------------

func ScanForTopLevelFunc(state *State, out []string, line string) (
	[]string, string) {
	if !strings.HasPrefix(line, "func ") {
		return out, line
	}

	state.Push(ScanForEndOpenBrace)

	return state.Process(out, line)
}

func ScanForEndOpenBrace(state *State, out []string, line string) (
	[]string, string) {
	if !strings.HasSuffix(line, " {") {
		return out, line
	}

	state.Pop()

	state.Push(EmitTopLevelFuncBody)

	return out, line
}

func EmitTopLevelFuncBody(state *State, out []string, line string) (
	[]string, string) {
	if len(line) > 0 && line[0] == '}' {
		state.Pop()
	}

	return out, line
}
