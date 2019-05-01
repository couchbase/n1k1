package main

import (
	"io/ioutil"
	"log"
	"sort"
	"strings"

	"github.com/couchbase/n1k1/cmd"
)

// ISSUES...
//
// - bindings?
//
// - annotated values?
//
// - META / META SELF?
//
// - a function often knows its domain of output types,
//   which can be leveraged by the next applied function?
//
// - if a function is done with its output,
//   it can let the next function take ownership (mutate/append)?
//   But, perhaps this already happens -- see ArrayAppend / ArrayConcat?
//
// - if there's a sub-tree of functions doing math on numbers
//   then don't need to convert back/forth to Val
//   between each step?
//   And, don't need to check MISSING / NULL on every step?
//
// - can we tell if for-range loops are working over big arrays
//   or over just small, bounded (compile-time) args?
//   The answer helps choose which recycled pool to use?

// ---------------------------------------------------------------

// State represents the gen-compiler process as it walks through the
// lines of n1k1 source code to generate a query compiler.
type State struct {
	// Stack of line handlers with associated callback data.
	Handlers []*HandlerEntry

	Imports map[string]bool

	// Keyed by struct name, values are [category, funcAlias0, ...].
	// Ex: "Add" => ["Arithmetic", "add"]
	// Ex: "RegexpContains" => [
	//       "Regular expressions", "contains_regex", "contains_regexp"
	//     ]
	Funcs map[string][]string

	LastFuncCategory string
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
}

// Handler represents a callback to process an incoming line.
type Handler func(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string)

// --------------------------------------------------------

func ExprBuild(sourceDir, outDir string) error {
	log.Printf(" ExprBuild, outDir: %s\n", outDir)

	state := &State{
		Handlers: []*HandlerEntry{
			&HandlerEntry{Handler: HandlerScanFile},
		},
		Imports: map[string]bool{},
		Funcs:   map[string][]string{},
	}

	var outAll []string

	err := VisitSourceLines(sourceDir, outDir,
		func(fileName string) error { // Start of a new source file.
			return nil
		},
		func(out []string, line string) ([]string, string, error) {
			out, line = state.Process(out, line)

			return out, line, nil
		},
		func(fileName string, out []string) error {
			for _, line := range out {
				if len(line) > 0 {
					outAll = append(outAll, line)
				}
			}

			return nil
		})
	if err != nil {
		return err
	}

	contents := []string{
		"package expr\n",
	}

	contents = append(contents, outAll...)

	var names []string
	for name := range state.Funcs {
		names = append(names, name)
	}

	sort.Strings(names)

	contents = append(contents, "/*")

	for _, name := range names {
		aliases := state.Funcs[name]

		contents = append(contents,
			name+": ("+aliases[0]+") "+strings.Join(aliases[1:], ", "))
	}

	contents = append(contents, "*/")

	return ioutil.WriteFile(outDir+"/generated_by_expr_build.go",
		[]byte(strings.Join(contents, "\n")), 0644)
}

// --------------------------------------------------------

func HandlerScanFile(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string) {
	if line == "import (" {
		state.Push(&HandlerEntry{Handler: HandlerScanImports})

		return out, ""
	}

	if strings.HasPrefix(line, "func (this *") &&
		strings.Index(line, " Apply(") > 0 {
		state.Push(&HandlerEntry{Handler: HandlerScanTopLevelFuncSignature})

		return state.Process(out, line)
	}

	if strings.HasPrefix(line, "func (this *") &&
		strings.Index(line, " Evaluate(") > 0 {
		state.Push(&HandlerEntry{Handler: HandlerScanTopLevelFuncSignature})

		line = "\n// -----------------------------------------------------------------\n" + line

		return state.Process(out, line)
	}

	if line == "var _FUNCTIONS = map[string]Function{" {
		state.Push(&HandlerEntry{Handler: HandlerScanTopLevelFuncRegistry})

		return state.Process(out, line)
	}

	return out, ""
}

func HandlerScanImports(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string) {
	if line == ")" {
		state.Pop()
	} else if len(line) > 0 {
		parts := strings.Split(strings.Split(line, `"`)[1], "/")

		state.Imports[parts[len(parts)-1]] = true
	}

	return out, ""
}

func HandlerScanTopLevelFuncRegistry(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string) {
	line = strings.TrimSpace(line)

	if len(line) <= 0 {
		return out, ""
	}

	if line[0] == '}' {
		state.Pop()

		return out, ""
	}

	if strings.HasPrefix(line, "// ") {
		state.LastFuncCategory = line[3:]

		return out, ""
	}

	if strings.HasPrefix(line, `"`) {
		parts := strings.Split(line, " ")

		alias := parts[0]               // Ex: `"between":`
		alias = alias[1:]               // Ex: `between":`
		alias = alias[0 : len(alias)-2] // Ex: `between`

		name := parts[len(parts)-1]  // Ex: `&Between{}`,
		name = name[1:]              // Ex: `Between{},`
		name = name[0 : len(name)-3] // Ex: `Between`

		aliases := state.Funcs[name]

		if len(aliases) <= 0 {
			aliases = []string{state.LastFuncCategory}
		}

		aliases = append(aliases, alias)

		state.Funcs[name] = aliases
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

	return EmitBlock(state, he, out, line)
}

// ---------------------------------------------------------------

func EmitBlock(state *State, he *HandlerEntry, out []string,
	line string) ([]string, string) {
	return out, line
}

// ---------------------------------------------------------------

func VisitSourceLines(sourceDir, outDir string,
	cbFileStart func(fileName string) error,
	cbFileLine func(out []string, line string) ([]string, string, error),
	cbFileEnd func(fileName string, out []string) error) error {
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

	return cmd.VisitFiles(sourceDir, ".go", cb)
}
