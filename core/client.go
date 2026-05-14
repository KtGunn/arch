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
	client pb.BridgeClient
	conn *grpc.ClientConn
	address string
	ctx context.Context
}

func NewClientData() *ClientData {
	return &ClientData{}
}

var Client *ClientData
// --



// CreateAClient
//
func createAClient(port int, ctx context.Context) {
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

	Client.client = pb.NewBridgeClient(Client.conn)
	Client.ctx = ctx


	return
}

