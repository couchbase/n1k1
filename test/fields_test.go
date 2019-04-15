package test

import (
	"testing"

	"github.com/couchbase/n1k1/base"
)

func TestFieldsIndexOf(t *testing.T) {
	fs := base.Fields{"a", "b"}
	if fs.IndexOf("a") != 0 {
		t.Fatal("should have worked")
	}
	if fs.IndexOf("b") != 1 {
		t.Fatal("should have worked")
	}
	if fs.IndexOf("c") != -1 {
		t.Fatal("should have worked")
	}

	fs = base.Fields{}
	if fs.IndexOf("c") != -1 {
		t.Fatal("should have worked")
	}
}
