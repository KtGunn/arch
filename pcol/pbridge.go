package main

import (
	"log"
	"context"
	"time"
	"io"
	"fmt"

	"google.golang.org/grpc"

	pb "mockup/proto"
	 empty "github.com/golang/protobuf/ptypes/empty"
)


var (
	stateResponse = make(chan string)
	dataStream = make(chan string)
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
		case data := <-dataStream:
			out := &pb.HeadCount{
				Present: data,
			}
			stream.Send(out)
		default:
		}
	}

}


func StateFroAndTo(ctx context.Context, client pb.BridgeClient) error {

	stream, err := client.YourState(ctx)
	if err != nil {
		log.Println("failed to open state stream:", err)
		return err
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
		case response := <- PChannels.stateResponse:
			stream.Send(&pb.StateResponse{
				Reply: response,
			})

		case <-time.After(7*time.Second):
			log.Println(" bs tick")
		}
	}
}


