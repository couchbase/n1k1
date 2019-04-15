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

	return GenInterpMain(sourceDir, outDir, nil, nil,
		func(fileName string, out []string) error {
			return ioutil.WriteFile(outDir+"/"+fileName,
				[]byte(strings.Join(out, "\n")), 0644)
		})
}

func GenInterpMain(sourceDir, outDir string,
	cbFileStart func(fileName string) error,
	cbFileLine func(out []string, line string) ([]string, string, error),
	cbFileEnd func(fileName string, out []string) error) error {
	sourcePackage := "package n1k1"

	outDirParts := strings.Split(outDir, "/")
	outPackage := "package " + outDirParts[len(outDirParts)-1]

	var out []string // Collected output or resulting lines.

	cb := func(kind, data string) (err error) {
		switch kind {
		case "fileStart":
			fileName := data

			log.Printf("  fileName: %s\n", fileName)

			out = nil

			if cbFileStart != nil {
				err = cbFileStart(fileName)
			}

		case "fileLine":
			line := data

			line = strings.Replace(line, sourcePackage, outPackage, -1)

			// Optional callback that can examine the incoming line,
			// and modify the line and/or the out state.
			if cbFileLine != nil {
				out, line, err = cbFileLine(out, line)
			}

			out = append(out, line)

		case "fileEnd":
			fileName := data

			err = cbFileEnd(fileName, out)
		}

		return err
	}

	return VisitFiles(sourceDir, ".go", cb)
}
