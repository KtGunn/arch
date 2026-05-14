//
// Created May 13 2026 (260513)
// By Kris Gunnarsson
//
// Purpose to create a grpc server to act as a bridge
// between systems.

package main




import (
	"log"
	"flag"
	"strconv"
	"os"
)


// Command Line Interface
//
const usage = `
./bridge <port>
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


// Main
//
func main() {

	log.Println("**** Welcome to Bridge ****")
	port := parseInput()

	LaunchBridge(port)
}
