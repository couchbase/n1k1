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

	Tags map[string]bool
}

func (fi *FuncInfo) Classify() {
	sort.Strings(fi.Registry)

	body := strings.Join(fi.ApplyLines, "\n")

	fi.Tags = map[string]bool{}

	if strings.HasPrefix(body, MissingNullOnArgs) {
		fi.Tags["shortCircuits:missing"] = true
	}

	if strings.HasPrefix(body, MissingNullOnArg) {
		fi.Tags["shortCircuits:missing"] = true
		fi.Tags["shortCircuits:null"] = true
	}

	if strings.HasPrefix(body, MissingOnArg) {
		fi.Tags["shortCircuits:missing"] = true
	}

	if strings.HasPrefix(body, MissingOnFirstSecond) {
		fi.Tags["shortCircuits:missing"] = true
	}

	if strings.HasPrefix(body, MissingOnFirstSecondThird) {
		fi.Tags["shortCircuits:missing"] = true
	}

	// ---------------------------------

	sort.Strings(fi.ApplyReturns)

	var dedupe []string

OUTER:
	for _, ar := range fi.ApplyReturns {
		if len(dedupe) == 0 || dedupe[len(dedupe)-1] != ar {
			dedupe = append(dedupe, ar)
		}

		if strings.Index(ar, "value.MISSING_VALUE") >= 0 {
			fi.Tags["returns:missing"] = true
		} else if strings.Index(ar, "value.NULL_VALUE") >= 0 {
			fi.Tags["returns:null"] = true
		} else if strings.Index(ar, "value.FALSE_VALUE") >= 0 ||
			strings.Index(ar, "value.TRUE_VALUE") >= 0 {
			fi.Tags["returns:bool"] = true
		} else if strings.HasPrefix(ar, "value.NewValue(math.") &&
			strings.HasSuffix(ar, "(arg.Actual().(float64))), nil") {
			fi.Tags["returns:number"] = true
		} else if strings.HasPrefix(ar, "value.AsNumberValue(") {
			fi.Tags["returns:number"] = true
		} else if strings.HasPrefix(ar, "value.") &&
			strings.HasSuffix(ar, "NUMBER, nil") {
			fi.Tags["returns:number"] = true
		} else if ar == "value.NewValue(str), nil" {
			fi.Tags["returns:string"] = true
		} else {
			if strings.HasPrefix(ar, "value.NewValue(") &&
				strings.HasSuffix(ar, "), nil") {
				// Ex: "value.NewValue(ra), nil".
				v := ar[len("value.NewValue("):]
				v = v[0 : len(v)-len("), nil")] // Ex: "ra".

				if strings.HasSuffix(v, ".String()") ||
					strings.HasPrefix(v, "timeToStr(") {
					fi.Tags["returns:string"] = true
					continue OUTER
				}

				vVar := "\t" + v + " := " // Ex: "ra := ".
				for _, line := range fi.ApplyLines {
					if strings.HasPrefix(line, vVar) {
						vInit := line[len(vVar):]
						if strings.HasPrefix(vInit, ArrayMake) {
							fi.Tags["returns:array"] = true
							continue OUTER
						} else if strings.HasSuffix(vInit, ".String()") {
							fi.Tags["returns:string"] = true
							continue OUTER
						} else if vInit == "value.NULL_VALUE" {
							fi.Tags["returns:null"] = true
							continue OUTER
						} else if vInit == "0.0" {
							fi.Tags["returns:number"] = true
							continue OUTER
						} else if strings.HasPrefix(vInit, "strings.Trim(") ||
							strings.HasPrefix(vInit, "strings.Replace(") ||
							strings.HasPrefix(vInit, "strings.ToUpper(") ||
							strings.HasPrefix(vInit, "strings.ToLower(") {
							fi.Tags["returns:string"] = true
							continue OUTER
						} else if strings.HasPrefix(vInit, "strings.Contains(") ||
							strings.HasPrefix(vInit, "source.Contains") {
							fi.Tags["returns:bool"] = true
							continue OUTER
						}

						break
					}
				}
			}

			fi.Tags["returns:other"] = true
		}
	}

	fi.ApplyReturns = dedupe

	if len(fi.ApplyReturns) == 3 {
		if fi.ApplyReturns[0] == "value.MISSING_VALUE, nil" &&
			fi.ApplyReturns[1] == "value.NULL_VALUE, nil" &&
			strings.HasPrefix(fi.ApplyReturns[2], "value.NewValue(math.") &&
			strings.HasSuffix(fi.ApplyReturns[2], "(arg.Actual().(float64))), nil") {
			fi.Tags["returns:missing,null,number"] = true
		}
	}

	// ---------------------------------

	for i, line := range fi.ApplyLines {
		fi.ApplyLines[i] = strings.Replace(line, "\t", " ", -1)
	}
}

// ---------------------------------------------------------------

const ArrayMake = "make([]interface{}, "

// ---------------------------------------------------------------

const MissingNullOnArgs = "\tfor _, arg := range args {\n\t\tif arg.Type() == value.MISSING {\n\t\t\treturn value.MISSING_VALUE, nil\n\t\t}\n\t}\n"

const MissingNullOnArg = "\tif arg.Type() == value.MISSING || arg.Type() == value.NULL {\n\t\treturn arg, nil\n\t}"

const MissingOnArg = "\tif arg.Type() == value.MISSING {\n\t\treturn value.MISSING_VALUE, nil\n\t}"

const MissingOnFirstSecond = "\tif first.Type() == value.MISSING || second.Type() == value.MISSING {\n\t\treturn value.MISSING_VALUE, nil\n\t}"

const MissingOnFirstSecondThird = "\tif first.Type() == value.MISSING || second.Type() == value.MISSING || third.Type() == value.MISSING {\n\t\treturn value.MISSING_VALUE, nil\n\t}"

// ---------------------------------------------------------------

// State represents the gen-compiler process as it walks through the
// lines of n1k1 source code to generate a query compiler.
type State struct {
	// Stack of line handlers with associated callback data.
	Handlers []*HandlerEntry

	Imports map[string]bool

	// Keyed by funcName.
	FuncsByName map[string]*FuncInfo

	// Keyed by funcName, values are [category, funcAlias0, ...].
	// Ex: "Add" => ["Arithmetic", "add"]
	// Ex: "RegexpContains" => [
	//       "Regular expressions", "contains_regex", "contains_regexp"
	//     ]
	FuncsByRegistry map[string][]string

	TotFuncsEvaluate int
	TotFuncsApply    int

	// Keyed by kind of Evaluate(), value is funcNames.
	// Ex key: "this.UnaryEval(this, item, context)", "MULTILINE".
	FuncsByEvaluateKind map[string][]string

	// Keyed by params of Apply(), value is funcNames.
	// Ex key: "context Context, first, second value.Value".
	FuncsByApplyParams map[string][]string

	// Keyed by return snippets, value is funcNames.
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

func (s *State) FuncInfo(funcName string) *FuncInfo {
	rv := s.FuncsByName[funcName]
	if rv == nil {
		rv = &FuncInfo{Name: funcName}
		s.FuncsByName[funcName] = rv
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

	var funcNames []string
	for funcName, fi := range state.FuncsByName {
		funcNames = append(funcNames, funcName)
		fi.Classify()
	}
	sort.Strings(funcNames)

	// ------------------------------------------------

	contents = append(contents, "\n"+Dashes)
	contents = append(contents, "// FuncsByName...\n")

	contents = append(contents, "/* ("+
		strconv.Itoa(len(state.FuncsByName))+")")

	for _, funcName := range funcNames {
		fi := state.FuncsByName[funcName]

		jsonBytes, _ := json.MarshalIndent(fi, "", "  ")

		contents = append(contents, string(jsonBytes))
	}

	contents = append(contents, "*/")

	// ------------------------------------------------

	contents = append(contents, "\n"+Dashes)
	contents = append(contents, "// FuncsByRegistry...\n")

	var funcNamesRegistered []string
	for funcNameRegistered := range state.FuncsByRegistry {
		funcNamesRegistered = append(funcNamesRegistered, funcNameRegistered)
	}
	sort.Strings(funcNamesRegistered)

	contents = append(contents, "/* ("+
		strconv.Itoa(len(state.FuncsByRegistry))+")")

	for _, funcNameRegistered := range funcNamesRegistered {
		aliases := state.FuncsByRegistry[funcNameRegistered]

		sort.Strings(aliases[1:])

		contents = append(contents,
			funcNameRegistered+
				": ("+aliases[0]+") "+strings.Join(aliases[1:], ", "))
	}

	contents = append(contents, "*/")

	// ------------------------------------------------

	contents = append(contents, "\n"+Dashes)
	contents = append(contents, "// FuncsByEvaluateKind...\n")

	n := 0

	var evaluateKinds []string
	for evaluateKind, funcNames := range state.FuncsByEvaluateKind {
		evaluateKinds = append(evaluateKinds, evaluateKind)
		n += len(funcNames)
	}
	sort.Strings(evaluateKinds)

	contents = append(contents, "/* ("+strconv.Itoa(n)+")")

	for i, evaluateKind := range evaluateKinds {
		if i != 0 {
			contents = append(contents, "")
		}

		funcNames := state.FuncsByEvaluateKind[evaluateKind]
		sort.Strings(funcNames)

		contents = append(contents, evaluateKind+": "+
			strconv.Itoa(len(funcNames)))

		for _, funcName := range funcNames {
			contents = append(contents, "  "+funcName)
		}
	}

	contents = append(contents, "*/")

	// ------------------------------------------------

	contents = append(contents, "\n"+Dashes)
	contents = append(contents, "// FuncsByApplyParams...\n")

	n = 0

	var applyParamsAll []string
	for applyParams, funcNames := range state.FuncsByApplyParams {
		applyParamsAll = append(applyParamsAll, applyParams)
		n += len(funcNames)
	}
	sort.Strings(applyParamsAll)

	contents = append(contents, "/* ("+strconv.Itoa(n)+")")

	for i, applyParams := range applyParamsAll {
		if i != 0 {
			contents = append(contents, "")
		}

		funcNames := state.FuncsByApplyParams[applyParams]
		sort.Strings(funcNames)

		contents = append(contents, applyParams+": "+
			strconv.Itoa(len(funcNames)))

		for _, funcName := range funcNames {
			contents = append(contents, "  "+funcName)
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

		funcNames := state.FuncsByApplyReturn[applyReturn]
		sort.Strings(funcNames)

		contents = append(contents, applyReturn+":"+
			strconv.Itoa(len(funcNames)))

		for _, funcName := range funcNames {
			contents = append(contents, "  "+funcName)
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

		funcName := strings.TrimSpace(line)
		funcName = funcName[len("func (this *"):]
		funcName = strings.Split(funcName, ")")[0]

		state.Push(&HandlerEntry{
			Handler: HandlerScanTopLevelFuncSignature,
			Kind:    "Evaluate",
			Name:    funcName,
		})

		line = "\n" + Dashes + "\n" + line

		return state.Process(out, line)
	}

	// Ex: `func (this *ArrayAppend) Apply(context Context, args ...value.Value) (value.Value, error) {`
	if strings.HasPrefix(line, "func (this *") &&
		strings.Index(line, " Apply(") > 0 {
		state.TotFuncsApply++

		funcName := strings.TrimSpace(line)
		funcName = funcName[len("func (this *"):]
		funcName = strings.Split(funcName, ")")[0]

		if strings.Index(line, ") (") < 0 {
			panic("Apply() params are not single line: " + funcName)
		}

		applyParams := strings.TrimSpace(line)
		applyParams =
			applyParams[strings.Index(applyParams, "Apply(")+len("Apply("):]

		applyParams = strings.Split(applyParams, ") (")[0]

		if !strings.HasPrefix(applyParams, "context Context, ") {
			panic("Apply() params does not take context: " + funcName)
		}

		applyParams = applyParams[len("context Context, "):]

		state.FuncInfo(funcName).ApplyParams = applyParams

		state.FuncsByApplyParams[applyParams] =
			append(state.FuncsByApplyParams[applyParams], funcName)

		state.Push(&HandlerEntry{
			Handler: HandlerScanTopLevelFuncSignature,
			Kind:    "Apply",
			Name:    funcName,
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

		funcName := parts[len(parts)-1]          // Ex: `&Between{}`,
		funcName = funcName[1:]                  // Ex: `Between{},`
		funcName = funcName[0 : len(funcName)-3] // Ex: `Between`

		aliases := state.FuncsByRegistry[funcName]

		if len(aliases) <= 0 {
			aliases = []string{state.LastFuncCategory}
		}

		aliases = append(aliases, alias)

		state.FuncsByRegistry[funcName] = aliases

		state.FuncInfo(funcName).Registry =
			append(state.FuncInfo(funcName).Registry, alias)
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
