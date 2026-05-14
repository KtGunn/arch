package main

import (
	"context"
	"log"
)


func main() {
	log.Println("**** Welcome to Pcol/Controls I/F *****")

	cfg := Config{
		controlsPort: 9067,
		bridgePort: 6907,
	}
	cfg = parseArgs(cfg)

	log.Println("bridge at", cfg.bridgePort, "controls at", cfg.controlsPort)
	ctx, _ := context.WithCancel(context.Background())

	LaunchClients(ctx, cfg.bridgePort, cfg.controlsPort)

	log.Println(" All done?")
}
