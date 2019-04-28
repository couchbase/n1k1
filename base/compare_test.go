package base

import (
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
