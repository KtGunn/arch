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


// EngageTraffic
// The function launches go routines to capture
// and send data to and from the servers
//
func EngageTraffic(ctx context.Context) int {
	
	goRoutines := 1
	go func(ctx context.Context, client pb.TrafficClient){
		StateQuery(ctx, client)
		wg.Done()
	}(ctx, TrafficClient.Client)
	

	goRoutines++
	go func(ctx context.Context, client pb.TrafficClient){
		Data(ctx, client)
		wg.Done()
	}(ctx, TrafficClient.Client)
	
	return goRoutines

}

func StateQuery(ctx context.Context, client pb.TrafficClient) {
	
	for {
		
		answer, err := client.YourState(ctx,
			&pb.StateQuery{
				Ask: "State please!",
			})
		
		if err != nil {
			log.Println("Failed to query: ", err)
			<-time.After(10 * time.Second)
			continue
		}
		log.Println("Traf:", answer)
		
		select {
			
		case <-ctx.Done():
			log.Println(" Context :: state query")
			return
			
		case <-time.After(5 * time.Second):
			// queries issue periodically
		}
		
	}
}


func Data(ctx context.Context, client pb.TrafficClient) {

	var stream grpc.ServerStreamingClient[pb.HeadCount]
	var err error
	nul := &empty.Empty{}

	for {
		stream, err = client.Data(ctx, nul)

		if err == nil {
			log.Println("Br.client has a data stream")
			break
		}

		time.Sleep(3*time.Second)
	}


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
			// we go on
		}
	}
}
