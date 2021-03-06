package base

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCanonicalJSON(t *testing.T) {
	testCanonicalJSON(t, nil, false)
}

func TestCanonicalJSONRecycleOut(t *testing.T) {
	testCanonicalJSON(t, nil, true)
}

func TestCanonicalJSONReuseValComparer(t *testing.T) {
	testCanonicalJSON(t, NewValComparer(), false)
}

func TestCanonicalJSONReuseValComparerRecycleOut(t *testing.T) {
	testCanonicalJSON(t, NewValComparer(), true)
}

func testCanonicalJSON(t *testing.T, vIn *ValComparer, reuseOut bool) {
	tests := []string{
		"",
		"not-JSON",
		"not-JSON}}}}",
		"0",
		"1",
		"0.0",
		"1.0",
		"-0",
		"-1",
		"-0.0",
		"-1.0",
		`"hello"`,
		`"he said, \"Hi, Sam\""`,
		`""`,
		`" "`,
		`"\""`,
		`null`,
		`true`,
		`false`,
		`[]`,
		`[1,2,3]`,
		`[1,2.0,true,false,null,[1,"yes\"Sir\""],{},"hi"]`,
		`{}`,
		`{"a":1}`,
		`{"b":1,"a":1}`,
		`{"b":{"y":10,"x":"hi"},"a":1}`,
		`{"b\"x":1,"a\"x":1}`,
		`{"b":1,"a":"aa\"AA"}`,
	}

	var out []byte

	for testi, test := range tests {
		v := vIn
		if v == nil {
			v = NewValComparer()
		}

		var u interface{}

		uerr := json.Unmarshal([]byte(test), &u)

		cj, cerr := v.CanonicalJSON([]byte(test), out)
		if (cerr != nil) != (uerr != nil) {
			t.Fatalf("testi: %d, test: %s, cerr: %v, uerr: %v",
				testi, test, cerr, uerr)
		}

		if reuseOut {
			out = cj[:0]
		}

		if cerr != nil {
			continue
		}

		mj, merr := json.Marshal(u)
		if merr != nil {
			t.Fatalf("testi: %d, test: %s, merr: %v",
				testi, test, merr)
		}

		if !bytes.Equal(cj, mj) {
			t.Fatalf("testi: %d, test: %s,\n cj: %s,\n mj: %s",
				testi, test, cj, mj)
		}
	}
}

func BenchmarkCanonicalJSON(b *testing.B) {
	v := NewValComparer()

	x := []byte(`[1, "2", []]`)

	o := make([]byte, 0, 1000)

	v.CanonicalJSON(x, o)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		v.CanonicalJSON(x, o)
	}
}
