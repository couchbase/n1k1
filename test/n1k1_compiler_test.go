package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	n1k1 "github.com/couchbase/n1k1/n1k1_compiler"

	"github.com/couchbase/n1k1/base"
)

func TestCasesSimpleWithCompiler(t *testing.T) {
	var testOuts [][]string

	for testi, test := range TestCasesSimple {
		var yields []base.LazyVals

		lazyYield := func(lazyVals base.LazyVals) {
			var lazyValsCopy base.LazyVals
			for _, v := range lazyVals {
				lazyValsCopy = append(lazyValsCopy, append(base.LazyVal(nil), v...))
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

		var out []string

		n1k1.Emit = func(format string, a ...interface{}) (n int, err error) {
			s := fmt.Sprintf(format, a...)

			s = strings.Replace(s, "base.LazyScope", "true", -1)

			out = append(out, s)

			return len(s), nil
		}

		n1k1.ExecOperator(&test.o, nil, nil)

		testOuts = append(testOuts, out)
	}

	c := []string{
		"package tmp",
		`import "bufio"`,
		`import "bytes"`,
		`import "strings"`,
		`import "testing"`,
		`import "github.com/couchbase/n1k1"`,
	}

	for testi, test := range tests {
		oj, _ := json.MarshalIndent(test.o, "// ", " ")

		c = append(c, "// ------------------------------------------")
		c = append(c, "// "+test.about)
		c = append(c, "// "+string(oj))
		c = append(c, "//")
		c = append(c, fmt.Sprintf("func TestGenerated%d(t *testing.T) {", testi))
		c = append(c, "  lazyYield := func(lazyVals n1k1.LazyVals) {}")
		c = append(c, "  _ = lazyYield\n")
		c = append(c, testOuts[testi]...)
		c = append(c, "}\n")
	}

	err := ioutil.WriteFile("./tmp/generated_by_n1k1_compiler_test.go",
		[]byte(strings.Join(c, "\n")), 0644)
	if err != nil {
		t.Fatal(err)
	}
}
