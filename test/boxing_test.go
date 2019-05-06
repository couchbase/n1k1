package test

import (
	"testing"
)

// Test via: go test -v -bench=Boxing -benchmem ./test
func BenchmarkBoxing(b *testing.B) {
	m := []interface{}{123.3}

	for i := 0; i < b.N; i++ {
		var y MyVal
		y = m
		foo(b, y)
		y = 123
		foo(b, y)
	}
}

type MyVal interface{}

func foo(b *testing.B, y MyVal) {
	_, oka := y.([]interface{})
	_, oki := y.(int)
	if !oka && !oki {
		b.Fatalf("wrong type, %+v", y)
	}
}
