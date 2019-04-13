package main

import (
	"flag"
	"log"
	"os"

	"github.com/couchbaselabs/n1k1/cmd"
)

var sourceDir = flag.String("sourceDir", ".",
	"top-level directory of n1k1 source files")

func main() {
	flag.Parse()

	log.Printf("%s\n", os.Args[0])
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("  -%s=%s\n", f.Name, f.Value)
	})

	cmd.GenInterp(*sourceDir)

	cmd.GenCompiler(*sourceDir)
}
