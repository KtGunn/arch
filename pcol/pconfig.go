package main

import (
	"flag"
	"log"
	"strconv"
)

type Config struct {
	bridgePort int
	logicPort  int
	logicId    string
}

const usage = `
./pcol <bridgeport> <logicport> <logicid>
`

func parseArgs(cfg Config) Config {
	log.Println("usage:", usage)

	flag.Parse()
	positionalArgs := flag.Args()

	if len(positionalArgs) != 3 {
		log.Fatal("You must enter two ports")
	}

	var err error
	args := []*int{&cfg.bridgePort, &cfg.logicPort}

	for n, arg := range args {
		*arg, err = strconv.Atoi(positionalArgs[n])
		if err != nil {
			log.Fatalf("Invalid port number: %v", err)
		}
	}
	cfg.logicId = positionalArgs[2]
	
	return cfg
}
