package test

import (
	"reflect"
	"testing"

	"github.com/couchbase/n1k1"
)

func TestCasesSimpleWithInterp(t *testing.T) {
	for testi, test := range TestCasesSimple {
		yieldVals, yieldErr, returnYields :=
			MakeYieldFuncs(t, testi, test.expectErr)

		n1k1.ExecOperator(&test.o, yieldVals, yieldErr)

		yields := returnYields()

		if len(yields) != len(test.expectYields) ||
			!reflect.DeepEqual(yields, test.expectYields) {
			t.Fatalf("testi: %d, test: %+v,\n"+
				" len(yields): %d,\n"+
				" len(test.expectYields): %d,\n"+
				" expectYields: %+v,\n"+
				" got yields: %+v",
				testi, test,
				len(yields), len(test.expectYields),
				test.expectYields, yields)
		}
	}
}
