package cmd

import (
	"log"
)

func GenInterp(sourceDir, outDir string) {
	fileNames, err := FileNames(sourceDir, ".go")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("fileNames: %v\n", fileNames)
}
