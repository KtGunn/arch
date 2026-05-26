package main

import (
	"log"
	"flag"
	"strconv"
)


const usage = `
./controls <port> <identifier(str)>
`

func parseInput() (int, string) {

	flag.Parse()

	positionalArgs := flag.Args()
	if len(positionalArgs) != 2 {
		log.Fatal(usage)
	}
	
	port, err := strconv.Atoi(positionalArgs[0])
	if err != nil {
		log.Fatal("You must enter a legit port number!")
	}

	id := positionalArgs[1]

	return port, id
}

func main () {
	log.Println("**** Welcome to Main of Controls *****")

	port, id := parseInput()

	LaunchServer(port, id)
}
