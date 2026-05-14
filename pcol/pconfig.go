package main

import (
	"flag"
	"log"
	"strconv"
)

type Config struct {
	bridgePort int
	controlsPort int
}

const usage = `
./pcol <bridgeport> <clientport>
`

func parseArgs(cfg Config) Config {
	log.Println("usage:", usage)

	flag.Parse()
	positionalArgs := flag.Args()

	if len(positionalArgs) != 2 {
		log.Fatal("You must enter two ports")
	}

	var err error
	args := []*int{&cfg.bridgePort, &cfg.controlsPort}

	for n, arg := range args {
		*arg, err = strconv.Atoi(positionalArgs[n])
		if err != nil {
			log.Fatalf("Invalid port number: %v", err)
		}
	}

	return cfg
}
