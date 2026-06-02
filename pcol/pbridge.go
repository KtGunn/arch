package main

import (
	"log"
	"context"
	"time"
	"io"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "mockup/proto"
	 empty "github.com/golang/protobuf/ptypes/empty"
)



/////////////////////////////////////////////////////////
//
// EngageBridge(...)
//
func EngageBridge(ctx context.Context, id string) int {


	goRoutines := 1
	go func(ctx context.Context, client pb.BridgeClient){
		log.Println("Establishing Bridge State")
		StateFroAndTo(ctx, client, id)
		wg.Done()
	}(ctx, BridgeClient.Client)


	goRoutines++
	go func(ctx context.Context, client pb.BridgeClient){
		log.Println("Establishing Bridge Data")
		DataStream(ctx, client, id)
		wg.Done()
	}(ctx, BridgeClient.Client)

	return goRoutines
}



/////////////////////////////////////////////////////////
//
// DataStream(...)
//  client streaming to bridge server
//
func DataStream(ctx context.Context, client pb.BridgeClient, id string) {

	md := metadata.Pairs("identity", id)
	ctx = metadata.NewOutgoingContext(ctx, md)
	
	for {
		log.Println("***opening head count data stream")

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

	var stream grpc.ClientStreamingClient[pb.HeadCount, empty.Empty]
	var err error

	for {

		stream, err = client.Data(ctx)
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
			stream.Send(data)
		default:
		}
	}

}


func StateFroAndTo(ctx context.Context, client pb.BridgeClient, id string) {
	
	md := metadata.Pairs("identity", id)
	ctx = metadata.NewOutgoingContext(ctx, md)
	
	for {
		log.Println("...opening stream for State data")
		
		var stream grpc.BidiStreamingClient[pb.StateResponse,pb.StateQuery]
		var closed bool

		stream, closed = openStateFroAndTo(ctx, client)
		if closed {
			log.Println("State stream context was closed. Bye...")
			return
		}

		closed = passStateFroAndTo(ctx, stream)
		if closed {
			log.Println("State stream has been closed. Bye...")
		}
	}
}

func openStateFroAndTo(ctx context.Context, client pb.BridgeClient) (grpc.BidiStreamingClient[pb.StateResponse,pb.StateQuery], bool) {

	var err error
	var stream grpc.BidiStreamingClient[pb.StateResponse,pb.StateQuery]

	for {
		log.Println(" trying to open bridge for query")
		stream, err = client.YourState(ctx)
		if err == nil {
			return stream, false
		}

		select {
		case <-ctx.Done():
			return nil, true
		case <-time.After(4*time.Second):
			// we go on
		}
	}
}

func passStateFroAndTo(ctx context.Context, stream grpc.BidiStreamingClient[pb.StateResponse,pb.StateQuery]) bool {

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
			log.Println(" Received query", query)
			postAQuery(query)
		}
	}()

	//
	// State Response
	//
	for {
		select {
		case response := <- PChannels.stateRespToBridge:
			stream.Send(response)

		case <-time.After(7*time.Second):
			log.Println(" bs tick")
		}
	}

	return true
}

/*
   func StateFroAndTo(ctx context.Context, client pb.BridgeClient, id string) error {


	
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
			continue
		}
		
		break
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
			log.Println(" Received query", query)
			postAQuery(query)
		}
	}()

	//
	// State Response
	//
	for {
		select {
		case response := <- PChannels.stateRespToBridge:
			stream.Send(response)

		case <-time.After(7*time.Second):
			log.Println(" bs tick")
		}
	}
}


*/
