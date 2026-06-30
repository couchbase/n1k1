//go:build n1ql

package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/test/emit"
)

func TestCasesSimpleWithCompiler(t *testing.T) {
	tests := TestCasesSimple

	var testOuts [][]string

	for _, test := range tests {
		testOuts = append(testOuts, emit.OpToLines(&test.o))
	}

	c := []string{
		// The generated file imports glue (gated behind the n1ql tag), so it
		// must carry the same constraint or it breaks the default `go test ./...`.
		"//go:build n1ql",
		``,
		"package tmp",
		``,
		`import "bufio"`,
		`import "bytes"`,
		`import "container/heap"`,
		`import "encoding/binary"`,
		`import "math"`,
		`import "os"`,
		`import "strconv"`,
		`import "strings"`,
		`import "reflect"`,
		`import "testing"`,
		`import "github.com/couchbase/rhmap/store"`,
		`import "github.com/couchbase/n1k1/base"`,
		`import "github.com/couchbase/n1k1/glue"`,
		`import "github.com/couchbase/n1k1/test"`,
		``,
	}

	for testi, test := range tests {
		oj, _ := json.MarshalIndent(test.o, "// ", "  ")

		c = append(c, "// ------------------------------------------")
		c = append(c, "// "+test.about)
		c = append(c, "// "+string(oj))
		c = append(c, "//")
		c = append(c, fmt.Sprintf("func TestGenerated%d(t *testing.T) {", testi))

		c = append(c, `  lzTmpDir, lzVars, lzYieldVals, lzYieldErr, returnYields :=`)
		c = append(c, fmt.Sprintf(`    test.MakeYieldCaptureFuncs(nil, %d, %q)`,
			testi, test.expectErr))
		c = append(c, "  os.RemoveAll(lzTmpDir)")
		c = append(c, "  _ = lzVars")
		c = append(c, "  _ = lzYieldVals")
		c = append(c, "  _ = lzYieldErr")
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
