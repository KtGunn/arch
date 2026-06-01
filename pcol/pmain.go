package main

import (
	"context"
	"log"
	"mockup/pcol/logic"
)


func main() {
	log.Println("**** Welcome to Pcol/Controls I/F *****")

	cfg := parseArgs(Config{
		logicPort:  9067,
		bridgePort: 6907,
		logicId:    "dunno",
	})

	log.Println("bridge at", cfg.bridgePort, "controls at", cfg.logicPort)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	PChannels = NewPChannels()
	PChannels.init()

	go func() {
		logic.RunLogic(cfg.logicPort, cfg.logicId)
	}()

	LaunchClients(ctx, cfg.bridgePort, cfg.logicPort, cfg.logicId)

	log.Println(" All done?")
}
