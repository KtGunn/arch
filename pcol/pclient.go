package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	
	pb "mockup/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)


// Protocol needs two clients,
//  client for the 'Controls Server'
//  client for the 'Bridge Server'
//
var (
	TrafficClient *GRPCClient[pb.TrafficClient]
	BridgeClient  *GRPCClient[pb.BridgeClient]
)


// Generic GRPC client
//
type GRPCClient[T any] struct {
	Client  T
	conn    *grpc.ClientConn
	address string
	ctx     context.Context
}


var wg = sync.WaitGroup{}


// LaunchClient
//
func LaunchClients(ctx context.Context, portB int, portC int) {
	log.Println("Launching clients")

	BridgeClient = NewGRPCClient(ctx, portB, pb.NewBridgeClient)
	bridgeGoes := EngageBridge(ctx)
	wg.Add(bridgeGoes)

	TrafficClient = NewGRPCClient(ctx, portC, pb.NewTrafficClient)
	trafficGoes := EngageTraffic(ctx)
	wg.Add(trafficGoes)

	wg.Wait()
}


// Creation of a Generic GRPC client
//
func NewGRPCClient[T any](ctx context.Context, port int, newClient func(grpc.ClientConnInterface) T) *GRPCClient[T] {

	address := fmt.Sprintf("localhost:%d", port)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		log.Fatalf("failed to launch client at %s: %v", address, err)
	}

	log.Printf("Client connected at %s", address)
	return &GRPCClient[T]{
		Client:  newClient(conn),
		conn:    conn,
		address: address,
		ctx:     ctx,
	}
}

func (c *GRPCClient[T]) Close() error {
	return c.conn.Close()
}


