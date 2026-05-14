package main

import (
	"log"
	"flag"
	"os"
	"strconv"
)


const usage = `
./controls <port>
`

func parseInput() int {
	flag.Parse()
	if len(os.Args) != 2 {
		log.Fatal(usage)
	}

	
	port, err := strconv.Atoi(os.Args[1])
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
