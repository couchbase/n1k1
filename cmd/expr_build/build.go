package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/cmd"
)

// ISSUES...
//
// - bindings?
//
// - a function often knows its domain of output types,
//   which can be leveraged by the next applied function?
//
// - if a function is done with its output,
//   it can let the next function take ownership (mutate/append)?
//
// - if there's a sub-tree of functions doing math on numbers
//   then don't need to convert back/forth to Val
//   between each step?
//
// - don't need to check MISSING / NULL on every step?
//   Can jump or goto to the first thing that handles MISSING/NULL?
//
// - annotated values?
//   - handled by field name prefix, like '^'.
//
// - META / META SELF?
//   - handled by field name like '^beers.meta', '^brewery.meta'.
//
// - can we tell if for-range loops are working over big arrays
//   or over just small, bounded (compile-time) args?
//   The answer helps choose which recycled pool to use?
//
// - some functions never return an error, always returning nil for error.

// ---------------------------------------------------------------

type FuncInfo struct {
	Name     string   // Ex: "ArrayPosition".
	Registry []string // Ex: ["array_pos", "array_position"].

	// Ex: "this.UnaryEval(this, item, context)", "MULTILINE".
	EvaluateKind string

	ApplyParams  string // Ex: "arg value.Value".
	ApplyLines   []string
	ApplyReturns []string
}

func (fi *FuncInfo) Cleanse() {
	sort.Strings(fi.Registry)

	for i, line := range fi.ApplyLines {
		fi.ApplyLines[i] = strings.Replace(line, "\t", " ", -1)
	}
}

// ---------------------------------------------------------------

// State represents the gen-compiler process as it walks through the
// lines of n1k1 source code to generate a query compiler.
type State struct {
	// Stack of line handlers with associated callback data.
	Handlers []*HandlerEntry

	Imports map[string]bool

	// Keyed by func name.
	FuncsByName map[string]*FuncInfo

	// Keyed by func name, values are [category, funcAlias0, ...].
	// Ex: "Add" => ["Arithmetic", "add"]
	// Ex: "RegexpContains" => [
	//       "Regular expressions", "contains_regex", "contains_regexp"
	//     ]
	FuncsByRegistry map[string][]string

	TotFuncsEvaluate int
	TotFuncsApply    int

	// Keyed by kind of Evaluate(), value is func names.
	// Ex key: "this.UnaryEval(this, item, context)", "MULTILINE".
	FuncsByEvaluateKind map[string][]string

	// Keyed by params of Apply(), value is func names.
	// Ex key: "context Context, first, second value.Value".
	FuncsByApplyParams map[string][]string

	// Keyed by return snippets, value is func names.
	// Ex key: "value.NULL_VALUE, nil".
	FuncsByApplyReturn map[string][]string

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

// --------------------------------------------------------

type HandlerEntry struct {
	Handler Handler

	// Ex: "Evaluate", "Apply", etc.
	Kind string

	// Ex: "ArrayAppend", name of function, etc.
	Name string

	// Ex: function body lines, etc.
	Lines []string
}

// Handler represents a callback to process an incoming line.
type Handler func(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string)

// --------------------------------------------------------

var Dashes = "// -----------------------------------------"

// --------------------------------------------------------

func (s *State) FuncInfo(name string) *FuncInfo {
	rv := s.FuncsByName[name]
	if rv == nil {
		rv = &FuncInfo{Name: name}
		s.FuncsByName[name] = rv
	}

	return rv
}

// --------------------------------------------------------

func ExprBuild(sourceDir, outDir string) error {
	log.Printf(" ExprBuild, outDir: %s\n", outDir)

	state := &State{
		Handlers: []*HandlerEntry{
			&HandlerEntry{Handler: HandlerScanFile},
		},
		Imports:             map[string]bool{},
		FuncsByName:         map[string]*FuncInfo{},
		FuncsByRegistry:     map[string][]string{},
		FuncsByEvaluateKind: map[string][]string{},
		FuncsByApplyParams:  map[string][]string{},
		FuncsByApplyReturn:  map[string][]string{},
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

	// ------------------------------------------------

	contents := []string{
		"package expr",
	}

	contents = append(contents, outAll...)

	// ------------------------------------------------

	contents = append(contents, "\n"+Dashes)

	contents = append(contents,
		fmt.Sprintf("// TotFuncsEvaluate: %d", state.TotFuncsEvaluate))

	contents = append(contents,
		fmt.Sprintf("// TotFuncsApply: %d", state.TotFuncsApply))

	// ------------------------------------------------

	contents = append(contents, "\n"+Dashes)
	contents = append(contents, "// FuncsByName...\n")

	var names []string
	for name := range state.FuncsByName {
		names = append(names, name)
	}
	sort.Strings(names)

	contents = append(contents, "/* ("+
		strconv.Itoa(len(state.FuncsByName))+")")

	for _, name := range names {
		fi := state.FuncsByName[name]
		fi.Cleanse()

		jsonBytes, _ := json.MarshalIndent(fi, "", "  ")

		contents = append(contents, string(jsonBytes))
	}

	contents = append(contents, "*/")

	// ------------------------------------------------

	contents = append(contents, "\n"+Dashes)
	contents = append(contents, "// FuncsByRegistry...\n")

	names = names[:0]
	for name := range state.FuncsByRegistry {
		names = append(names, name)
	}
	sort.Strings(names)

	contents = append(contents, "/* ("+
		strconv.Itoa(len(state.FuncsByRegistry))+")")

	for _, name := range names {
		aliases := state.FuncsByRegistry[name]

		sort.Strings(aliases[1:])

		contents = append(contents,
			name+": ("+aliases[0]+") "+strings.Join(aliases[1:], ", "))
	}

	contents = append(contents, "*/")

	// ------------------------------------------------

	contents = append(contents, "\n"+Dashes)
	contents = append(contents, "// FuncsByEvaluateKind...\n")

	n := 0

	var evaluateKinds []string
	for evaluateKind, names := range state.FuncsByEvaluateKind {
		evaluateKinds = append(evaluateKinds, evaluateKind)
		n += len(names)
	}
	sort.Strings(evaluateKinds)

	contents = append(contents, "/* ("+strconv.Itoa(n)+")")

	for i, evaluateKind := range evaluateKinds {
		if i != 0 {
			contents = append(contents, "")
		}

		names := state.FuncsByEvaluateKind[evaluateKind]
		sort.Strings(names)

		contents = append(contents, evaluateKind+": "+
			strconv.Itoa(len(names)))

		for _, name := range names {
			contents = append(contents, "  "+name)
		}
	}

	contents = append(contents, "*/")

	// ------------------------------------------------

	contents = append(contents, "\n"+Dashes)
	contents = append(contents, "// FuncsByApplyParams...\n")

	n = 0

	var applyParamsAll []string
	for applyParams, names := range state.FuncsByApplyParams {
		applyParamsAll = append(applyParamsAll, applyParams)
		n += len(names)
	}
	sort.Strings(applyParamsAll)

	contents = append(contents, "/* ("+strconv.Itoa(n)+")")

	for i, applyParams := range applyParamsAll {
		if i != 0 {
			contents = append(contents, "")
		}

		names := state.FuncsByApplyParams[applyParams]
		sort.Strings(names)

		contents = append(contents, applyParams+": "+
			strconv.Itoa(len(names)))

		for _, name := range names {
			contents = append(contents, "  "+name)
		}
	}

	contents = append(contents, "*/")

	// ------------------------------------------------

	contents = append(contents, "\n"+Dashes)
	contents = append(contents, "// FuncsByApplyReturn...\n")

	var applyReturns []string
	for applyReturn := range state.FuncsByApplyReturn {
		applyReturns = append(applyReturns, applyReturn)
	}
	sort.Strings(applyReturns)

	contents = append(contents, "/*")

	for i, applyReturn := range applyReturns {
		if i != 0 {
			contents = append(contents, "")
		}

		names := state.FuncsByApplyReturn[applyReturn]
		sort.Strings(names)

		contents = append(contents, applyReturn+":"+
			strconv.Itoa(len(names)))

		for _, name := range names {
			contents = append(contents, "  "+name)
		}
	}

	contents = append(contents, "*/")

	// ------------------------------------------------

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

	// Ex: `func (gthis *ArrayAppend) Evaluate(item value.Value, context Context) (value.Value, error) {`
	if strings.HasPrefix(line, "func (this *") &&
		strings.Index(line, " Evaluate(") > 0 {
		state.TotFuncsEvaluate++

		if strings.Index(line, `(item value.Value, context Context) (`) < 0 {
			panic("Evaluate() has unexpected signature: " + line)
		}

		name := strings.TrimSpace(line)
		name = name[len("func (this *"):]
		name = strings.Split(name, ")")[0]

		state.Push(&HandlerEntry{
			Handler: HandlerScanTopLevelFuncSignature,
			Kind:    "Evaluate",
			Name:    name,
		})

		line = "\n" + Dashes + "\n" + line

		return state.Process(out, line)
	}

	// Ex: `func (this *ArrayAppend) Apply(context Context, args ...value.Value) (value.Value, error) {`
	if strings.HasPrefix(line, "func (this *") &&
		strings.Index(line, " Apply(") > 0 {
		state.TotFuncsApply++

		name := strings.TrimSpace(line)
		name = name[len("func (this *"):]
		name = strings.Split(name, ")")[0]

		if strings.Index(line, ") (") < 0 {
			panic("Apply() params are not single line: " + name)
		}

		applyParams := strings.TrimSpace(line)
		applyParams =
			applyParams[strings.Index(applyParams, "Apply(")+len("Apply("):]

		applyParams = strings.Split(applyParams, ") (")[0]

		if !strings.HasPrefix(applyParams, "context Context, ") {
			panic("Apply() params does not take context: " + name)
		}

		applyParams = applyParams[len("context Context, "):]

		state.FuncInfo(name).ApplyParams = applyParams

		state.FuncsByApplyParams[applyParams] =
			append(state.FuncsByApplyParams[applyParams], name)

		state.Push(&HandlerEntry{
			Handler: HandlerScanTopLevelFuncSignature,
			Kind:    "Apply",
			Name:    name,
		})

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

		aliases := state.FuncsByRegistry[name]

		if len(aliases) <= 0 {
			aliases = []string{state.LastFuncCategory}
		}

		aliases = append(aliases, alias)

		state.FuncsByRegistry[name] = aliases

		state.FuncInfo(name).Registry =
			append(state.FuncInfo(name).Registry, alias)
	}

	return out, ""
}

func HandlerScanTopLevelFuncSignature(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string) {
	if !strings.HasSuffix(line, " {") {
		return out, line
	}

	state.Pop()

	state.Push(&HandlerEntry{
		Handler: HandlerScanTopLevelFuncBody,
		Kind:    he.Kind,
		Name:    he.Name,
	})

	return out, line
}

func HandlerScanTopLevelFuncBody(state *State, he *HandlerEntry,
	out []string, line string) ([]string, string) {
	if len(line) > 0 && line[0] == '}' {
		state.Pop()

		if he.Kind == "Evaluate" {
			evaluateKind := "MULTILINE"

			if len(he.Lines) == 1 {
				// Ex: "return this.BinaryEval(this, item, context)"
				evaluateKind = strings.TrimSpace(he.Lines[0])
				evaluateKind = strings.Replace(evaluateKind,
					"return ", "", -1)
			}

			state.FuncInfo(he.Name).EvaluateKind = evaluateKind

			state.FuncsByEvaluateKind[evaluateKind] =
				append(state.FuncsByEvaluateKind[evaluateKind], he.Name)
		}

		return out, line
	}

	if he.Kind == "Apply" && len(line) > 0 {
		state.FuncInfo(he.Name).ApplyLines =
			append(state.FuncInfo(he.Name).ApplyLines, line)

		lineBody := strings.Split(line, "//")[0]

		r := strings.Index(lineBody, "return ")
		if r >= 0 {
			returnKind := lineBody[r+len("return "):]
			if strings.Index(returnKind, ", ") > 0 {
				state.FuncInfo(he.Name).ApplyReturns =
					append(state.FuncInfo(he.Name).ApplyReturns, returnKind)

				state.FuncsByApplyReturn[returnKind] =
					append(state.FuncsByApplyReturn[returnKind], he.Name)
			}
		}
	}

	he.Lines = append(he.Lines, line)

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
