package base

import (
	"bytes"
	"testing"

	"github.com/buger/jsonparser"
)

func TestJsonParser(t *testing.T) {
	tests := []struct {
		json string
		path []string

		err          string
		outValue     string
		outValueType jsonparser.ValueType
		outOffset    int
	}{
		{
			json:         `123`,
			path:         []string{},
			outValue:     `123`,
			outValueType: jsonparser.Number,
			outOffset:    3,
		},
		{
			json:         `   123`,
			path:         []string{},
			outValue:     `123`,
			outValueType: jsonparser.Number,
			outOffset:    6,
		},
		{
			json:         `   `,
			path:         []string{},
			err:          "expected some bad json error",
			outValue:     ``,
			outValueType: jsonparser.NotExist,
			outOffset:    -1,
		},
		{
			json:         `   " hello world "  `,
			path:         []string{},
			outValue:     ` hello world `,
			outValueType: jsonparser.String,
			outOffset:    18,
		},
		{
			json:         `   [ "hello", [] ]  `,
			path:         []string{},
			outValue:     `[ "hello", [] ]`,
			outValueType: jsonparser.Array,
			outOffset:    18,
		},
		{
			json:         `   [ "hello", []   `,
			path:         []string{},
			err:          "should detect a missing closing bracket",
			outValue:     ``,
			outValueType: jsonparser.Array,
			outOffset:    3,
		},
		{
			json:         `   [ "hello", [] ]  `,
			path:         []string{"[0]"},
			outValue:     `hello`,
			outValueType: jsonparser.String,
			outOffset:    12,
		},
		{
			json:         `   [ "hello", [] ]  `,
			path:         []string{"[1]"},
			outValue:     `[]`,
			outValueType: jsonparser.Array,
			outOffset:    16,
		},
		{
			json:         `   [ "hello", [] ]  `,
			path:         []string{"[2]"},
			err:          "should have error as array is too short",
			outValue:     ``,
			outValueType: jsonparser.NotExist,
			outOffset:    -1,
		},
		{
			json:         `   "hello  `,
			path:         []string{},
			err:          "should detect a missing closing string",
			outValue:     ``,
			outValueType: jsonparser.String,
			outOffset:    3,
		},
		{
			json:         `   " hello\"world "  `,
			path:         []string{},
			outValue:     " hello\\\"world ",
			outValueType: jsonparser.String,
			outOffset:    19,
		},
	}

	for testi, test := range tests {
		value, valueType, offset, err := jsonparser.Get([]byte(test.json), test.path...)
		if (err != nil) != (test.err != "") {
			t.Fatalf("testi: %d, test: %+v, err: %v",
				testi, test, err)
		}

		if string(value) != test.outValue {
			t.Fatalf("testi: %d, test: %+v, wrong value: %s",
				testi, test, value)
		}

		if valueType != test.outValueType {
			t.Fatalf("testi: %d, test: %+v, wrong valueType: %d",
				testi, test, valueType)
		}

		if offset != test.outOffset {
			t.Fatalf("testi: %d, test: %+v, wrong offset: %d",
				testi, test, offset)
		}
	}
}

func TestJsonParserUnescape(t *testing.T) {
	v, err := jsonparser.Unescape([]byte(` hello\"world `), nil)
	if err != nil {
		t.Errorf("not expecting err")
	}
	if string(v) != ` hello"world ` {
		t.Errorf("got: %s, %#v", v, v)
	}
}

func TestValComparer(t *testing.T) {
	testValComparer(t, nil)
}

func TestValComparerReuse(t *testing.T) {
	testValComparer(t, NewValComparer())
}

func testValComparer(t *testing.T, vIn *ValComparer) {
	tests := []struct {
		a string
		b string
		c int
	}{
		{
			a: ``,
			b: ``,
			c: 0,
		},
		{
			a: ``,
			b: `123`,
			c: -1,
		},
		{
			a: `123`,
			b: ``,
			c: 1,
		},
		{
			a: `99`,
			b: `123`,
			c: -1,
		},
		{
			a: `123`,
			b: `99`,
			c: 1,
		},
		{
			a: `123`,
			b: `123`,
			c: 0,
		},
		{
			a: `[123]`,
			b: `[123]`,
			c: 0,
		},
		{
			a: `[1,2,3]`,
			b: `[1,2,3]`,
			c: 0,
		},
		{
			a: `[1,2]`,
			b: `[1,2,3]`,
			c: -1,
		},
		{
			a: `[]`,
			b: `[1,2,3]`,
			c: -1,
		},
		{
			a: `[1,2,1]`,
			b: `[1,2,3]`,
			c: -1,
		},
		{
			a: `[1,2,3,0]`,
			b: `[1,2,3]`,
			c: 1,
		},
		{
			a: `[1,2,"b"]`,
			b: `[1,2,"a"]`,
			c: 1,
		},
		{
			a: `[1,2,3,0]`,
			b: `[]`,
			c: 1,
		},
		{
			a: ` [ 1 ,   2  , 3 , 0 ] `,
			b: `  [  ] `,
			c: 1,
		},
		{
			a: `{}`,
			b: `{}`,
			c: 0,
		},
		{
			a: `{"a":1}`,
			b: `{"a":1}`,
			c: 0,
		},
		{
			a: `{"a":"y"}`,
			b: `{"a":"x"}`,
			c: 1,
		},
		{
			a: `{"a\"X":"y"}`,
			b: `{"a\"X":"x"}`,
			c: 1,
		},
		{
			a: `{"a\"X":"y\"Y\"Y"}`,
			b: `{"a\"X":"x\"X\"X"}`,
			c: 1,
		},
		{
			a: `{"a":1,"b":2}`,
			b: `{"b":2,"a":1}`,
			c: 0,
		},
		{
			a: `{"a":1,"b":2,"c":3}`,
			b: `{"b":2,"a":1}`,
			c: 1,
		},
		{
			a: `{"a":1,"b":2}`,
			b: `{"b":2,"a":1,"c":3}`,
			c: -1,
		},
		{
			a: `{"c":1,"b":2}`,
			b: `{"b":2,"a":1}`,
			c: -1,
		},
		{
			a: `{"a":1,"b":2}`,
			b: `{"b":2,"c":1}`,
			c: 1,
		},
		{
			a: `{"a":1,"b\"B":2}`,
			b: `{"b\"B":2,"c":1}`,
			c: 1,
		},
	}

	for testi, test := range tests {
		v := vIn
		if v == nil {
			v = NewValComparer()
		}

		c := v.Compare([]byte(test.a), []byte(test.b))
		if c != test.c {
			t.Fatalf("testi: %d, test: %+v, c: %d",
				testi, test, c)
		}
	}
}

func BenchmarkValCompare(b *testing.B) {
	v := NewValComparer()

	x := []byte(`[1,"2",[]]`)
	y := []byte(`[1,"2",[]]`)

	v.Compare(x, y)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		v.Compare(x, y)
	}
}

func BenchmarkEncodeAsString(b *testing.B) {
	v := NewValComparer()
	o := make([]byte, 0, 1000)

	s := []byte("hello\"world")
	j := []byte(`"hello\"world"`)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		out, err := v.EncodeAsString(s, o)
		if err != nil {
			b.Fatalf("err: %v", err)
		}

		if !bytes.Equal(j, out) {
			b.Fatalf("not equal")
		}
	}
}

func BenchmarkParseString(b *testing.B) {
	x := []byte(`"hello\"world"`)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Parse(x)
	}
}

