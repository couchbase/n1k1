package test

import (
	"testing"

	"github.com/couchbase/n1k1/base"
)

func BenchmarkValCompareObjSmall(b *testing.B) {
	benchmarkValCompare(b, `{"c":[],"a":1,"b":"2"}`, `{"b":"2","a":1,"c":[]}`)
}

func BenchmarkValCompareArraySmall(b *testing.B) {
	benchmarkValCompare(b ,`[1,2,"3"]`, `[1,2,"4"]`)
}

func BenchmarkValCompareNum(b *testing.B) {
	benchmarkValCompare(b ,`10000`, `10001`)
}

func benchmarkValCompare(b *testing.B, aStr, bStr string) {
	aBytes := []byte(aStr)
	bBytes := []byte(bStr)

	v := base.NewValComparer()

	v.Compare(aBytes, bBytes) // Seeds preallocated buffers.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.Compare(aBytes, bBytes)
	}
}
