package test

import (
	"reflect"
	"testing"

	"github.com/couchbase/n1k1"
	"github.com/couchbase/n1k1/base"
)

func TestCasesSimpleWithInterp(t *testing.T) {
	for testi, test := range TestCasesSimple {
		var yields []base.LazyVals

		lazyYield := func(lazyVals base.LazyVals) {
			var lazyValsCopy base.LazyVals
			for _, v := range lazyVals {
				lazyValsCopy = append(lazyValsCopy,
					append(base.LazyVal(nil), v...))
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

		n1k1.ExecOperator(&test.o, lazyYield, lazyYieldErr)

		if len(yields) != len(test.expectYields) ||
			!reflect.DeepEqual(yields, test.expectYields) {
			t.Fatalf("testi: %d, test: %+v,\n"+
				" len(yields): %d,\n"+
				" len(test.expectYields): %d,\n"+
				" expectYields: %+v,\n"+
				" got yields: %+v",
				testi, test,
				len(yields), len(test.expectYields), test.expectYields, yields)
		}
	}
}
