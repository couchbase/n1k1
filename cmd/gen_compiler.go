package cmd

import (
	"log"
)

func GenCompiler(sourceDir, outDir string) error {
	log.Printf(" GenCompiler, outDir: %s\n", outDir)

	cbOuter := func(out []string, line string) ([]string, string) {
		return out, line
	}

	return GenInterpMain(sourceDir, outDir, cbOuter)
}
