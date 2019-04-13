package cmd

import (
	"log"
)

func GenCompiler(sourceDir, outDir string) error {
	log.Printf(" GenCompiler\n")

	return GenInterpMain(sourceDir, outDir, nil)
}
