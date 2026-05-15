package main

import (
	"log"
	"flag"
	"strconv"
)


const usage = `
./controls <port>
`

func parseInput() int {

	flag.Parse()

	positionalArgs := flag.Args()
	if len(positionalArgs) != 1 {
		log.Fatal(usage)
	}
	
	port, err := strconv.Atoi(positionalArgs[0])
	if err != nil {
		log.Fatal("You must enter a legit port number!")
	}
	return port
}

func main () {
	log.Println("**** Welcome to Main of Controls *****")
	port := parseInput()

	LaunchServer(port)
}
