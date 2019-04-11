package n1ko

type Fields []string

func (a Fields) IndexOf(s string) int {
	for i, v := range a {
		if v == s {
			return i
		}
	}

	return -1
}

// -----------------------------------------------------

const LazyTrue = true

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

// -----------------------------------------------------

type LazyProjectFunc func(lazyVals, lazyValsPre LazyVals) LazyVals

func MakeProjectFunc(fields Fields, projectFields Fields) (
	lazyProjectFunc LazyProjectFunc) {
	lazyProjectFunc = func(lazyVals, lazyValsPre LazyVals) (
		lazyValsOut LazyVals) {
		lazyValsOut = lazyValsPre // Optional pre-alloc'ed slice.
		for _, projectField := range projectFields {
			idx := fields.IndexOf(projectField)
			if idx >= 0 {
				lazyValsOut = append(lazyValsOut, lazyVals[idx])
			} else {
				lazyValsOut = append(lazyValsOut, LazyValEmpty)
			}
		}
		return lazyValsOut
	}

	return lazyProjectFunc
}

// -----------------------------------------------------

type Predicate []string // Ex: ["eq", "fState", "sCalifornia"].

type LazyPredicateFunc func(LazyVals) bool

func MakePredicateFunc(fields Fields, predicate Predicate) (
	lazyPredicateFunc LazyPredicateFunc) {
	if predicate[0] == "eq" {
		var lazyRefFunc LazyRefFunc

		lazyRefFunc =
			MakeRefFunc(fields, Ref(predicate[1]))
		lazyA := lazyRefFunc

		lazyRefFunc =
			MakeRefFunc(fields, Ref(predicate[2]))
		lazyB := lazyRefFunc

		lazyPredicateFunc = func(lazyVals LazyVals) bool {
			return lazyA(lazyVals) == lazyB(lazyVals)
		}

		return lazyPredicateFunc
	}

	panic("unknown predicate")
}

// -----------------------------------------------------

type Ref string

type LazyRefFunc func(LazyVals) LazyVal

func MakeRefFunc(fields Fields, ref Ref) (lazyRefFunc LazyRefFunc) {
	if ref[0] == 'f' {
		idx := fields.IndexOf(string(ref[1:]))
		if idx < 0 {
			lazyRefFunc = func(lazyVals LazyVals) LazyVal {
				return LazyValEmpty
			}
		} else {
			lazyRefFunc = func(lazyVals LazyVals) LazyVal {
				if len(lazyVals) <= idx {
					return LazyValEmpty
				}

				return lazyVals[idx]
			}
		}

		return lazyRefFunc
	}

	if ref[0] == 's' {
		s := ref[1:] // Constant string.
		lazyRefFunc = func(lazyVals LazyVals) LazyVal {
			return LazyVal(s)
		}
		return lazyRefFunc
	}

	panic("unknown ref")
}
