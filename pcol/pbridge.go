package main

import (
	"log"
	"context"
	"time"
	"io"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

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
		log.Println("***opening Bridge Data stream")

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

		log.Println(" Bridge data stream not open...")
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
	log.Println("Bridge Data stream is open")

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
		log.Println("...opening Bridge State stream")

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
		log.Println(" Bridge query stream not open...")
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
	log.Println("Bridge Query stream is open")

	recvErrCh := make (chan error, 1)

	//
	// State Query
	//
	go func(recvErrCh chan error) {
		for {
			query, err := stream.Recv()
			if err == io.EOF {
				log.Println("...State query stream EOF. Gone!")
				recvErrCh <- err
				return
			}
			if err != nil {
				log.Println("...State query stream !nil. Gone!")
				recvErrCh <- err
				return
			}

			log.Println(" Received query", query)
			postAQuery(query)
		}
	}(recvErrCh)

	//
	// State Response
	//
getout:
	for {
		select {

		case err := <-recvErrCh:
			if err == io.EOF  {
				log.Println(" state query error ; io.EOF: server closed")
			} else {
				st, _ := status.FromError(err)
				log.Println(" state query error ;", err, "code", st.Code())
			}
			break getout

		case response := <- PChannels.stateRespToBridge:

			if err := stream.Send(response); err != nil {
				log.Println("...State query Send stream !nil")
				break getout
			}

		case <-time.After(7*time.Second):
			log.Println(" bs tick")
		}
	}

	log.Println(" returning from passStateFroAndTo")
	return true
}

