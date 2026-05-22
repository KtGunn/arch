package main

import (
	"log"
	"fmt"
	"time"
	"context"
	"io"

	pb "mockup/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	empty "github.com/golang/protobuf/ptypes/empty"
)


// CLIENT data definition
//
type ClientData struct {
	client pb.TrafficClient
	conn *grpc.ClientConn
	address string
	ctx context.Context
}

func NewClientData() *ClientData {
	return &ClientData{}
}

var Client *ClientData




// LaunchClient
//
func LaunchClient(port int, ctx context.Context) {
	log.Println("Launching client at ", port)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	Client = NewClientData()
	Client.address = fmt.Sprintf("localhost:%d", port)

	var err error
	Client.conn, err = grpc.NewClient(Client.address, opts...)
	if err != nil {
		log.Fatalf("failed to launch a client: %v", err)
	}

	Client.client = pb.NewTrafficClient(Client.conn)
	Client.ctx = ctx

	wg.Add(1)
	go func(ctx context.Context){
		EngageClient(ctx)
		wg.Done()

	}(ctx)


	wg.Add(1)
	go func(client pb.TrafficClient, ctx context.Context){
		Data(ctx, client)
		wg.Done()

	}(Client.client, Client.ctx)

	return
}


func EngageClient(ctx context.Context) {

	for {

		answer, err := Client.client.YourState(ctx, &pb.StateQuery{
			Ask: "State please!",
		})

		if err != nil {
			log.Println("Failed to query: ", err)
			<-time.After(10 * time.Second)
			continue
		}
		log.Println(" answer is ...", answer)

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

	for {
		log.Println("Opening the data stream...")

		var stream grpc.ServerStreamingClient[pb.HeadCount]
		var closed bool

		stream, closed = openData(ctx, client)
		if closed {
			log.Println(" Data context closed ")
			return
		}

		closed = receiveData(ctx, stream)
		if closed {
			log.Println(" Receive context closed ")
			return
		}
	}
}


// openData
//
func openData(ctx context.Context, client pb.TrafficClient) (grpc.ServerStreamingClient[pb.HeadCount], bool) {

	for {

		nul := &empty.Empty{}
		stream, err := client.Data(Client.ctx, nul)
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

func receiveData(ctx context.Context, stream grpc.ServerStreamingClient[pb.HeadCount]) bool {

	for {

		headCount, err := stream.Recv()

		if err == io.EOF {
			log.Println(" eof")
			return false
		}

		if err != nil {
			return false
		}

		log.Println(headCount)

		select {
		case <-ctx.Done():
			return true
		default:
			// we go on
		}
	}
}
