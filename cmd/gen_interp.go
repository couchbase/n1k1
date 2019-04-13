package cmd

import (
	"io/ioutil"
	"log"
	"regexp"
	"strings"
)

var LazyPrefixRE = regexp.MustCompile(`lazy[A-Z]`)

func LazyPrefixREFunc(lazyX string) string {
	return strings.ToLower(lazyX[len(lazyX)-1:])
}

func GenInterp(sourceDir, outDir string) error {
	log.Printf(" GenInterp, outDir: %s\n", outDir)

	return GenInterpMain(sourceDir, outDir, nil, true, true)
}

func GenInterpMain(sourceDir, outDir string,
	cbOuter func(out []string, line string) ([]string, string),
	filterLazyText bool,
	allowTests bool) error {
	sourcePackage := "package n1k1"

	outDirParts := strings.Split(outDir, "/")
	outPackage := "package " + outDirParts[len(outDirParts)-1]

	var out []string // Collected output or resulting lines.

	cb := func(kind, data string) error {
		switch kind {
		case "fileStart":
			out = nil

			fileName := data

			log.Printf("  fileName: %s\n", fileName)

		case "fileLine":
			line := data

			line = strings.Replace(line, sourcePackage, outPackage, -1)

			// An optional callback can examine and modify the
			// previous output and examine the incoming line.
			if cbOuter != nil {
				out, line = cbOuter(out, line)
			}

			if filterLazyText {
				// Converts "LazyFooBar" into "FooBar".
				line = strings.Replace(line, "Lazy", "", -1)

				// Converts "lazyFooBar" into "fooBar".
				line = LazyPrefixRE.ReplaceAllStringFunc(line,
					LazyPrefixREFunc)
			}

			out = append(out, line)

		case "fileEnd":
			fileName := data

			err := ioutil.WriteFile(outDir + "/" + fileName,
				[]byte(strings.Join(out, "\n")), 0644)
			if err != nil {
				return err
			}
		}

		return nil
	}

	var skipSuffixes []string
	if !allowTests {
		skipSuffixes = append(skipSuffixes, "_test.go")
	}

	return VisitFiles(sourceDir, ".go", skipSuffixes, cb)
}
