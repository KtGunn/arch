package main

import (
	"context"
	"log"
	"sync"
	"time"
)

var wg = sync.WaitGroup{}

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

	switch cfg.role {
	case "server":

	case "client":
		LaunchClient(cfg.port, ctx)

	default:
		log.Fatal("role is wrong! ", cfg.role)

	}
	log.Println(" -- select --")

	select {
	case <-time.After(20 * time.Second):
		log.Println(" time is up")
		cancel()
	}

	wg.Wait()
	log.Println(" All done?")
}
