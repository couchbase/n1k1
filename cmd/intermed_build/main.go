package main

import (
	"flag"
	"log"
	"os"
)

var sourceDir = flag.String("sourceDir", ".",
	"top-level directory of n1k1 source files")

func main() {
	flag.Parse()

	log.Printf("%s\n", os.Args[0])
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf(" -%s=%s\n", f.Name, f.Value)
	})

	err := IntermedBuild(*sourceDir, *sourceDir+"/intermed")
	if err != nil {
		log.Fatal(err)
	}
}
