package cmd

import (
	"io/ioutil"
	"log"
)

func GenInterp(sourceDir, outDir string) error {
	fileNames, err := FileNames(sourceDir, ".go")
	if err != nil {
		return err
	}

	log.Printf("fileNames: %v\n", fileNames)

	for _, fileName := range fileNames {
		fileBytes, err := ioutil.ReadFile(sourceDir + "/" + fileName)
		if err != nil {
			return err
		}

		log.Printf("  fileName: %s, len: %d\n", fileName, len(fileBytes))
	}

	return nil
}
