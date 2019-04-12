package n1k1

import (
	"reflect"
)

var TypeCatalog = map[string]string{
	"bool":   "scalar",
	"null":   "scalar",
	"number": "scalar",
	"string": "scalar",
	"array":  "composite",
	"object": "composite",
	"":       "composite", // The unknown or any type.
}

// -----------------------------------------------------

type Types []string

func SetLastType(a Types, t string) {
	a[len(a)-1] = t
}

func TakeLastType(a Types) string {
	t := a[len(a)-1]
	a[len(a)-1] = ""
	return t
}

// -----------------------------------------------------

func DeepEqual(a, b interface{}) bool {
	return reflect.DeepEqual(a, b)
}
