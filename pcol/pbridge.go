package main

import (
	"log"
	"context"
	"time"
	"io"
	"fmt"	
	pb "mockup/proto"
)


var stateResponse = make(chan string)
var dataStream = make(chan string)


func EngageBridge(ctx context.Context) int {
	log.Println("Hello to Bridge")


	goRoutines := 1
	go func(ctx context.Context, client pb.BridgeClient){
		StateQueryStream(ctx, client)
		wg.Done()
	}(ctx, BridgeClient.Client)
	

	goRoutines++
	go func(ctx context.Context, client pb.BridgeClient){
		DataStream(ctx, client)
		wg.Done()
	}(ctx, BridgeClient.Client)

	go func() {
		count := 0
		for {
			time.Sleep(5 *time.Second)
			dataStream <- fmt.Sprintf("HCount %d", count)
			count++
		}
	}()
	return goRoutines

}


func DataStream(ctx context.Context, client pb.BridgeClient) {
	stream, err := client.Data(ctx)
	if err != nil {
		log.Println("No stream ")
	}

	for {
		select {
		case data := <-dataStream:
			out := &pb.HeadCount{
				Present: data,
			}
			stream.Send(out)
		default:
		}
	}

}


func StateQueryStream(ctx context.Context, client pb.BridgeClient) {
	
	stream, err := client.YourState(ctx)
	if err != nil {
		log.Println("No stream ")
	}

	go func() {
		for {
			query, err := stream.Recv()
			
			if err == io.EOF {
				log.Println("BS eof", err)
			}
			if err != nil {
				log.Println("state response stream error", err)
			}
			
			log.Println("pBb recv:" , query)

			stateResponse<-"Here's your reort!"
		}
	}()
	
	for {
		
		select
		{
		case out := <-stateResponse :
			outmsg := &pb.StateResponse{
				Reply: out,
			}
			stream.Send(outmsg)
			
		case <-time.After(12*time.Second):
			log.Println("..pbridge tick")
		}
		
	}

}
