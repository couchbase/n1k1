package cmd

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"log"
	"strings"
)

func GenInterp(sourceDir, outDir string) error {
	fileNames, err := FileNames(sourceDir, ".go")
	if err != nil {
		return err
	}

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
