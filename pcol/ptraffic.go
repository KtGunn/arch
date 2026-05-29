package main

import (
	"log"
	"context"
	"time"
	"io"

	pb "mockup/proto"
	"google.golang.org/grpc"
	empty "github.com/golang/protobuf/ptypes/empty"
)

var TrafficID string

// EngageTraffic
// The function launches go routines to capture
// and send data to and from the servers
//
func EngageTraffic(ctx context.Context) int {

	goRoutines := 1
	go func(ctx context.Context, client pb.TrafficClient){
		StateToAndFro(ctx, client)
		wg.Done()
	}(ctx, TrafficClient.Client)


	goRoutines++
	go func(ctx context.Context, client pb.TrafficClient){
		Data(ctx, client)
		wg.Done()
	}(ctx, TrafficClient.Client)

	return goRoutines
}



// StateResponse
//  This function Receives StateResponse from Controls Traffic Server
//  and will pass it on to the Bridge client.

func StateToAndFro(ctx context.Context, client pb.TrafficClient) error {

	for {
		select {
		case query := <-PChannels.stateQyToControls:
			stateResponse, err := client.YourState(ctx, query)
			if err != nil {
				log.Println("error querying Controls", err)
				return err
			}

			registerAResponse(stateResponse)
		}
	}
}


func Data(ctx context.Context, client pb.TrafficClient) {

	var stream grpc.ServerStreamingClient[pb.HeadCount]
	var err error
	nul := &empty.Empty{}

	log.Println(" top of Data(ctx,client)...")

	for {
		stream, err = client.Data(ctx, nul)
		if err == io.EOF {
			log.Println("Control stream done.")
			return
		}
		if err != nil {
			log.Println("No control stream yet...")
			time.Sleep(3*time.Second)
			continue
		}
		break
	}


	log.Println(" stream to Traffic is established...")
	for {

		headCount, err := stream.Recv()

		if err == io.EOF {
			log.Println(" eof")
			return
		}

		if err != nil {
			return
		}

		log.Println("..Traf", headCount)

		select {
		case <-ctx.Done():
			return
		default:
			freshData(headCount)
		}
	}
}
