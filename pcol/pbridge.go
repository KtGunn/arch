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


func StateQueryStream(ctx context.Context, client pb.BridgeClient) {
	for {
		log.Println("Opening the data stream...")

		
		var stream grpc.BidiStreamingClient[pb.StateResponse,pb.StateQuery]
		var closed bool

		stream, closed = openStateStream(ctx, client)
		if closed {
			log.Println(" Data context closed ")
			return
		}

		closed = doStateStream(ctx, stream)
		if closed {
			log.Println(" Receive context closed ")
			return
		}
	}
}


// openStateStream
//
func openStateStream(ctx context.Context,
	client pb.BridgeClient) (grpc.BidiStreamingClient[pb.StateResponse,pb.StateQuery], bool) {
	
	for {
		
		stream, err := client.YourState(ctx)
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

func doStateStream(ctx context.Context,
	stream grpc.BidiStreamingClient[pb.StateResponse,pb.StateQuery]) bool {
	
	go func() {
		for {
			query, err := stream.Recv()
			
			if err == io.EOF {
				log.Println("BS eof", err)
				return
			}
			if err != nil {
				log.Println("state response stream error", err)
				return
			}
			
			log.Println("pBb recv:" , query)

			stateResponse<-"Here's your retort!"
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
			
		case <-ctx.Done():
			return true

		case <-time.After(12*time.Second):
			log.Println("..pbridge tick")
		}
		
	}

}
