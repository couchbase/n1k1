package main

import (
	"flag"
	"log"
	"os"
)

var sourceDir = flag.String("sourceDir", "engine",
	"directory of the n1k1 engine source files")

var outDir = flag.String("outDir", "intermed",
	"output directory for the generated intermed package")

func main() {
	flag.Parse()

	log.Printf("%s\n", os.Args[0])

	flag.VisitAll(func(f *flag.Flag) {
		log.Printf(" -%s=%s\n", f.Name, f.Value)
	})

	err := IntermedBuild(*sourceDir, *outDir)
	if err != nil {
		log.Fatal(err)
	}
}
