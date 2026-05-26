package main

import (
	"context"
	"log"
	"sync"
	"time"
)

var wg = sync.WaitGroup{}


// This module will
// 1. create a client to the Bridge Server
// 2. create a user who will send out Queries
//    periodically.
// 3. the user may also request streamed Data
//
func main() {
	log.Println("**** Welcome to Main of Core *****")

	cfg := Config{
		role: "server",
		port: 9067,
	}
	cfg = parseArgs(cfg)

	log.Println("role", cfg.role)
	log.Println("port", cfg.port)

	ctx, cancel := context.WithCancel(context.Background())
	
	LaunchClient(cfg.port, ctx)
	log.Println(" The client has been created.")

	howManyQueries := 1
	receiveStream := false
	LaunchUser(howManyQueries, receiveStream)

	wg.Wait()
	log.Println(" All done?")
}
