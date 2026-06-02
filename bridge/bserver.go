package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
	"context"
	pb "mockup/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)


////////////////////////////////////////////////////////////
// Bridge Server definition
//
var Bridge *BridgeServerData

type BridgeServerData struct {
	pb.UnimplementedBridgeServer
	Connections sync.Map
}

func NewBridge() *BridgeServerData {
	return &BridgeServerData{}
}
//...




////////////////////////////////////////////////////////////
// LaunchBridge
//
func LaunchBridge(port int) {

	lis, err := net.Listen("tcp",
		fmt.Sprintf("localhost:%d", port),
	)

	if err != nil {
		log.Fatal("Bridge failed to open the port", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	Bridge = NewBridge()
	pb.RegisterBridgeServer(grpcServer, Bridge)

	log.Println("Bridge Server waiting client connections on port", port)
	grpcServer.Serve(lis)
}
//...




////////////////////////////////////////////////////////////
// Extract the Identity of the connection
// from metadata
//
func idFromMetadata(tag string, ctx context.Context) (string, error) {

	var ok bool
	var md map[string][]string

	md, ok = metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Errorf(codes.DataLoss, "metadata missing from context")
	}

	var t []string

	if t, ok = md[tag]; !ok {
		return "", fmt.Errorf("no identity from context: %+v", md)
	}

	return t[0], nil
}


////////////////////////////////////////////////////////////
// Data -- CLIENT STREAMING
// --
func (s *BridgeServerData) Data(stream pb.Bridge_DataServer) error {
	log.Printf(":: Data stream: %+v", stream)

	identity, err := idFromMetadata("identity", stream.Context())
	if err != nil {
		log.Println(err)
	}

	defer func() {
		log.Println("Data ending; client", identity, "to be removed")
		removeClient(identity,"def-Data")
	}()
	
	userPresent.Do(pcolIsConnected)

	for {
		in, err := stream.Recv()
		
		if err == io.EOF {
			log.Println("Data eof")
			return err
		} else {
			log.Println("Data nil")
			return err
		}
		
		freshData(in, identity)
	}

}

// YourState
// This rpc passes,.
//
//	StateRequest server->client and
//	 StateResponse client<-server
//
// --
func (s *BridgeServerData) YourState(stream grpc.BidiStreamingServer[pb.StateResponse, pb.StateQuery]) error {
	log.Printf(":: State stream: %+v", stream)

	identity, err := idFromMetadata("identity", stream.Context())
	if err != nil {
		log.Println(err)
	}
	
	defer func() {
		removeClient(identity,"state-def-Data")
	}()

	queryPipe := addClient(identity, StateStreamer, stream)
	log.Println(" client has been added and pipe returned", queryPipe)

	userPresent.Do(pcolIsConnected)

	

	//
	// State Response
	//
	go func() {
		for {

			select {

			case <-stream.Context().Done():
				log.Println("State stream context closed. Bye!")
				return

			default:
				// go on
			}

			reply, err := stream.Recv()

			if err == io.EOF {
				log.Println("BS eof", err)
				removeClient(identity, "go-State-a")
				return
			}

			if err != nil {
				log.Println("BS !nil", err)
				removeClient(identity, "go-State-b")
				return
			}
			
			stateResponse(reply, identity)
		}
	}()

	//
	// State Query
	//
getout:
	for {

		log.Println(identity, " awaiting qy")
		select {

		case <-stream.Context().Done():
			log.Println("State stream context closed QY. Bye!")
			return fmt.Errorf("context close")
			
		case query := <-queryPipe:
			log.Println(" bs sending a query")

			if err = stream.Send(&query); err != nil {
				log.Println(" Bserver state query stream error:", err)
				break getout
			}

		case <-time.After(2 * time.Second):
			log.Println("bs tick")

		}

	}

	removeClient(identity, "state-return")
	return err
}
