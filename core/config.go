package main

import (
	"flag"
	"log"
	"strconv"
)

type Config struct {
	role string
	port int
}

const usage = `
./core -role <server,client> <port>
`

func parseArgs(cfg Config) Config {
	log.Println("usage:", usage)

	flag.StringVar(&cfg.role, "role", cfg.role, "server or client")
	flag.Parse()

	positionalArgs := flag.Args()

	if len(positionalArgs) != 1 {
		log.Fatal("You must enter a port number!")
	}

	var err error
	cfg.port, err = strconv.Atoi(positionalArgs[0])

	if err != nil {
		log.Fatal("You must enter a legit port number!", positionalArgs[0])
	}

	return cfg
}
