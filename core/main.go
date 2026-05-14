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

	cfg := readCommandLine()

	ctx, cancel := context.WithCancel(context.Background())
	createAClient(cfg.bridgeport, ctx)

	defUser := createAUser("", 0)
	
	select {
	case <-time.After(20 * time.Second):
		log.Println(" time is up")
		cancel()
	}

	wg.Wait()
	log.Println(" All done?")
}
