package glue

import (
	"testing"
)

func TestParseStatement(t *testing.T) {
	tests := []struct {
		stmt string
		err  string
	}{
		{"SELECT 1", ""},
		{"SELECT 1+2 AS three", ""},
		{"BOGUS not going to parse well", "bogus n1ql"},
	}

	for _, test := range tests {
		_, err := ParseStatement(test.stmt, "", true)
		if (err != nil) != (test.err != "") {
			t.Errorf("err != test.err")
		}
	}
}
