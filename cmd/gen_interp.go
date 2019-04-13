package cmd

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"log"
	"regexp"
	"strings"
)

var lazyPrefixRE = regexp.MustCompile(`lazy[A-Z]`)

func GenInterp(sourceDir, outDir string) error {
	fileNames, err := FileNames(sourceDir, ".go")
	if err != nil {
		return err
	}

	sourcePackage := "package n1k1"

	outDirParts := strings.Split(outDir, "/")
	outPackage := "package " + outDirParts[len(outDirParts)-1]

	log.Printf(" GenInterp\n")

	for _, fileName := range fileNames {
		log.Printf("  fileName: %s\n", fileName)

		fileBytes, err := ioutil.ReadFile(sourceDir + "/" + fileName)
		if err != nil {
			return err
		}

		var out []string

		s := bufio.NewScanner(bytes.NewBuffer(fileBytes))
		for s.Scan() {
			line := s.Text()

			line = strings.Replace(line, sourcePackage, outPackage, -1)

			// Converts "LazyFooBar" into "FooBar".
			line = strings.Replace(line, "Lazy", "", -1)

			// Converts "lazyFooBar" into "fooBar".
			line = lazyPrefixRE.ReplaceAllStringFunc(line,
				func(lazyX string) string {
					return strings.ToLower(lazyX[len(lazyX)-1:])
				})

			out = append(out, line)
		}

		err = ioutil.WriteFile(outDir + "/" + fileName,
			[]byte(strings.Join(out, "\n")), 0644)
		if err != nil {
			return err
		}
	}

	return nil
}
