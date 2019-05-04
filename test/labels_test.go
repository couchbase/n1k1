package test

import (
	"testing"

	"github.com/couchbase/n1k1/base"
)

func TestLabelsIndexOf(t *testing.T) {
	fs := base.Labels{"a", "b"}
	if fs.IndexOf("a") != 0 {
		t.Fatal("should have worked")
	}
	if fs.IndexOf("b") != 1 {
		t.Fatal("should have worked")
	}
	if fs.IndexOf("c") != -1 {
		t.Fatal("should have worked")
	}

	fs = base.Labels{}
	if fs.IndexOf("c") != -1 {
		t.Fatal("should have worked")
	}
}
