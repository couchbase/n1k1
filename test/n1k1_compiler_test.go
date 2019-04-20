package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/intermed"
)

type Captured struct {
	Pos int
	Out []string
}

func TestCasesSimpleWithCompiler(t *testing.T) {
	tests := TestCasesSimple

	var testOuts [][]string

	for _, test := range tests {
		outStack := [][]string{nil}

		appendOut := func(at int, s string) int {
			out := outStack[at]
			pos := len(out)
			out = append(out, s)
			outStack[at] = out
			return pos
		}

		appendOuts := func(at int, ss []string) int {
			out := outStack[at]
			pos := len(out)
			out = append(out, ss...)
			outStack[at] = out
			return pos
		}

		intermed.Emit = func(format string, a ...interface{}) (
			n int, err error) {
			s := fmt.Sprintf(format, a...)
			s = strings.Replace(s, "LazyScope", "true", -1)

			appendOut(len(outStack)-1, s)

			return len(s), nil
		}

		intermed.EmitLift = func(format string, a ...interface{}) (
			n int, err error) {
			s := fmt.Sprintf(format, a...)
			s = strings.Replace(s, "LazyScope", "true", -1)

			appendOut(0, s)

			return len(s), nil
		}

		intermed.EmitPush = func(path, pathItem string) {
			outStack = append(outStack, nil)
		}

		emitPopsCaptured := map[string]Captured{}

		intermed.EmitPop = func(path, pathItem string) {
			out := outStack[len(outStack)-1]

			outStack[len(outStack)-1] = nil
			outStack = outStack[0 : len(outStack)-1]

			pos := appendOuts(len(outStack)-1, out)

			emitPopsCaptured[path+"_"+pathItem] = Captured{
				Pos: pos,
				Out: out,
			}
		}

		intermed.EmitCaptured = func(path, pathItem string) {
			captured, ok := emitPopsCaptured[path+"_"+pathItem]
			if !ok {
				panic(fmt.Sprintf("EmitCaptured does not exist,"+
					" path: %q, pathItem: %q,\n"+
					" emitPopsCaptured: %+v",
					path, pathItem, emitPopsCaptured))
			}

			capturedOut := captured.Out

			// Scan backwards until a func/closure.
			start := len(capturedOut) - 1
			for start >= 0 {
				if strings.Index(capturedOut[start], " func(") > 0 {
					break
				}

				start = start - 1
			}

			// Emit the body of the last func/closure.
			var out []string

			scopes := 0 // Count scope / braces of IF statements.

			for i := start + 1; i < len(capturedOut); i++ {
				trimmed := strings.TrimSpace(capturedOut[i])
				if len(trimmed) <= 0 {
					continue
				}

				if strings.HasPrefix(trimmed, "return ") { // Filter away return lines.
					continue
				}

				if strings.HasPrefix(trimmed, "if ") {
					scopes += 1
				}

				if strings.HasSuffix(trimmed, "}") { // Ignore IF close braces.
					if scopes > 0 {
						scopes -= 1
					} else {
						continue
					}
				}

				out = append(out, capturedOut[i])
			}

			// Collapse sibling lines that look like...
			//    a = foo
			//    bar := a
			// Into...
			//    bar := foo
			for i := 1; i < len(out); i++ {
				if len(out[i-1]) > 0 {
					prev := strings.Split(strings.TrimSpace(out[i-1]), " = ")
					if len(prev) == 2 {
						curr := strings.Split(strings.TrimSpace(out[i]), " := ")
						if len(curr) == 2 &&
							prev[0] == curr[1] {
							out[i-1] = curr[0] + " := " + prev[1]
							out[i] = ""
						}
					}
				}
			}

			appendOuts(len(outStack)-1, out)

			clearFuncLines(outStack[len(outStack)-1][captured.Pos : captured.Pos+len(captured.Out)])
		}

		intermed.ExecOperator(&test.o, nil, nil)

		if len(outStack) != 1 {
			panic("len(outStack) should be height 1")
		}

		out := outStack[len(outStack)-1]

		testOuts = append(testOuts, out)
	}

	c := []string{
		"package tmp",
		``,
		`import "bufio"`,
		`import "bytes"`,
		`import "strings"`,
		`import "reflect"`,
		`import "testing"`,
		`import "github.com/couchbase/n1k1/base"`,
		`import "github.com/couchbase/n1k1/test"`,
		``,
		`var LazyErrNil error`,
	}

	for testi, test := range tests {
		oj, _ := json.MarshalIndent(test.o, "// ", "  ")

		c = append(c, "// ------------------------------------------")
		c = append(c, "// "+test.about)
		c = append(c, "// "+string(oj))
		c = append(c, "//")
		c = append(c, fmt.Sprintf("func TestGenerated%d(t *testing.T) {", testi))

		c = append(c, `  lazyYieldVals, lazyYieldErr, returnYields :=`)
		c = append(c, fmt.Sprintf(`    test.MakeYieldCaptureFuncs(nil, %d, %q)`,
			testi, test.expectErr))
		c = append(c, "  _ = lazyYieldVals")
		c = append(c, "  _ = lazyYieldErr")
		c = append(c, "")

		c = append(c, testOuts[testi]...)
		c = append(c, "")

		c = append(c, `  yields := returnYields()`)

		c = append(c, fmt.Sprintf(`  expectYields := %#v`,
			test.expectYields))

		c = append(c, `
  if len(yields) != len(expectYields) ||
    !reflect.DeepEqual(yields, expectYields) {
    t.Fatalf("len(yields): %d, len(expectYields): %d,\n"+
             " yields: %+v, expectYields: %+v,\n"+
             " about: %s",
             len(yields), len(expectYields),
             yields, expectYields, `+
			fmt.Sprintf("%q", test.about)+`)
  }`)

		c = append(c, "}\n")
	}

	err := ioutil.WriteFile("./tmp/generated_by_n1k1_compiler_test.go",
		[]byte(strings.Join(c, "\n")), 0644)
	if err != nil {
		t.Fatal(err)
	}
}

func clearFuncLines(lines []string) {
	var stack []string // Tracks nesting of lines.

	for i := 0; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])

		clear := false

		// NOTE: Assumes func() and its start brace are on same line.
		if strings.Index(trimmed, " func(") > 0 {
			stack = append(stack, "func")
		} else if strings.HasSuffix(trimmed, "{") {
			stack = append(stack, "{")
		}

		for _, v := range stack {
			if v == "func" {
				clear = true
				break
			}
		}

		if strings.HasSuffix(trimmed, "}") {
			stack = stack[0 : len(stack)-1]
		}

		// Only keep "var foo = initializer" idioms of the lifted /
		// hosted variables.
		hasVar := strings.HasPrefix(trimmed, "var ")
		hasAssign := strings.Index(trimmed, " = ") > 0

		if !(hasVar && hasAssign) {
			clear = true
		}

		if clear {
			lines[i] = ""
		}
	}
}
