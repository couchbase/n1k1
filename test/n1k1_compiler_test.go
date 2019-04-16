package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/n1k1/intermed"
)

func TestCasesSimpleWithCompiler(t *testing.T) {
	tests := TestCasesSimple

	var testOuts [][]string

	for testi, test := range tests {
		var yields []base.Vals

		lazyYield := func(lazyVals base.Vals) {
			var lazyValsCopy base.Vals
			for _, v := range lazyVals {
				lazyValsCopy = append(lazyValsCopy, append(base.Val(nil), v...))
			}

			yields = append(yields, lazyValsCopy)
		}

		lazyYieldErr := func(err error) {
			if (test.expectErr != "") != (err != nil) {
				t.Fatalf("testi: %d, test: %+v,\n"+
					" got err: %v",
					testi, test, err)
			}
		}

		_ = lazyYield
		_ = lazyYieldErr

		outStack := [][]string{nil}

		appendOut := func(pos int, s string) {
			out := outStack[pos]
			out = append(out, s)
			outStack[pos] = out
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

		intermed.EmitPop = func(path, pathItem string) {
			out := outStack[len(outStack)-1]

			outStack = outStack[0 : len(outStack)-1]

			outTop := outStack[len(outStack)-1]
			outTop = append(outTop, out...)
			outStack[len(outStack)-1] = outTop
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
