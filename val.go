package n1k1

import (
	"errors"
)

var ErrMissing = errors.New("missing")

// -----------------------------------------------------

const LazyValMissing = LazyVal("")

const LazyValNull = LazyVal("null")

const LazyValTrue = LazyVal("true")

const LazyValFalse = LazyVal("false")

// -----------------------------------------------------

type LazyVal string

type LazyVals []LazyVal
