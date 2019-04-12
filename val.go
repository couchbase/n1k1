package n1k1

import (
	"errors"
)

var ErrMissing = errors.New("missing")

// -----------------------------------------------------

const LazyValEmpty = LazyVal("")

type LazyVal string

type LazyVals []LazyVal

func LazyValsEqual(a, b LazyVals) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}
