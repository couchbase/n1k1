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
	log.Printf(" GenInterp\n")

	sourcePackage := "package n1k1"

	outDirParts := strings.Split(outDir, "/")
	outPackage := "package " + outDirParts[len(outDirParts)-1]

	var out []string

	cb := func(kind, data string) error {
		switch kind {
		case "fileLine":
			line := data

			line = strings.Replace(line, sourcePackage, outPackage, -1)

			// Converts "LazyFooBar" into "FooBar".
			line = strings.Replace(line, "Lazy", "", -1)

			// Converts "lazyFooBar" into "fooBar".
			line = LazyPrefixRE.ReplaceAllStringFunc(line,
				LazyPrefixREFunc)

			out = append(out, line)

		case "fileStart":
			out = nil

			fileName := data

			log.Printf("  fileName: %s\n", fileName)

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

	return VisitFiles(sourceDir, ".go", cb)
}
