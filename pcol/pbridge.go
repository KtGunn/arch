package main

import (
	"log"
	"context"
	"time"
	"io"

	"google.golang.org/grpc"

	pb "mockup/proto"
	 empty "github.com/golang/protobuf/ptypes/empty"
)



func EngageBridge(ctx context.Context) int {
	log.Println("Hello to Bridge")


	goRoutines := 1
	go func(ctx context.Context, client pb.BridgeClient){
		StateFroAndTo(ctx, client)
		wg.Done()
	}(ctx, BridgeClient.Client)


	goRoutines++
	go func(ctx context.Context, client pb.BridgeClient){
		DataStream(ctx, client)
		wg.Done()
	}(ctx, BridgeClient.Client)

	return goRoutines
}


func DataStream(ctx context.Context, client pb.BridgeClient) {
	for {
		log.Println("opening head count data stream")

		var stream grpc.ClientStreamingClient[pb.HeadCount, empty.Empty]
		var closed bool

		stream, closed = openDataStream(ctx,client)
		if closed {
			log.Println("Data Open Context is closed, Bye...")
			return
		}

		closed = receiveDataStream(ctx, stream)
		if closed {
			log.Println("Data Receive Context is closed, Bye...")
			return
		}
	}
}

func openDataStream(ctx context.Context,	client pb.BridgeClient) (grpc.ClientStreamingClient[pb.HeadCount, empty.Empty], bool) {

	for {

		stream, err := client.Data(ctx)
		if err == nil {
			return stream, false
		}

		select {
		case <-ctx.Done():
			return nil, true
		case <-time.After(5*time.Second):
			// we go on
		}
	}
}

func receiveDataStream(ctx context.Context,
	stream grpc.ClientStreamingClient[pb.HeadCount, empty.Empty]) bool {

	for {
		select {
		case data := <-PChannels.dataStream:
			out := &pb.HeadCount{
				Present: data,
			}
			stream.Send(out)
		default:
		}
	}

}


func StateFroAndTo(ctx context.Context, client pb.BridgeClient) error {

	var stream grpc.BidiStreamingClient[pb.StateResponse,pb.StateQuery]
	var err error
	
	for {
		stream, err = client.YourState(ctx)

		if err == io.EOF {
			log.Println(" state stream is done!")
			return err
		}

		if err != nil {
			log.Println("No bridge stream yet...")
			time.Sleep(3*time.Second)

		} else {
			break
		}
	}
	

	//
	// State Query
	//
	go func() {
		for {
			query, err := stream.Recv()
			if err == io.EOF {
				log.Println("...State query stream EOF")
				return
			}
			if err != nil {
				log.Println("...State query stream !nil")
				return
			}
			log.Println(" Received query", query.Ask)
			postAQuery(query.Ask)
		}
	}()

	//
	// State Response
	//
	for {
		select {
		case response := <- PChannels.stateRespToBridge:
			stream.Send(&pb.StateResponse{
				Reply: response,
			})

		case <-time.After(7*time.Second):
			log.Println(" bs tick")
		}
	}
}


