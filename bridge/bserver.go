package main

import (
	"fmt"
	"io"
	"log"
	"net"

	"time"

	pb "mockup/proto"
	"google.golang.org/grpc"
)


// Bridge Server definition
//
type BridgeServerData struct {
	pb.UnimplementedBridgeServer
}
func NewBridge() *BridgeServerData {
	return &BridgeServerData{}
}
var Bridge *BridgeServerData





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
	

	log.Println("Bridge waiting for data traffic on ", port)
	grpcServer.Serve(lis)
}




// Data
// This rpc is CLIENT STREAMING
// --
func (s *BridgeServerData) Data(stream pb.Bridge_DataServer) error {

	for {
		in, err := stream.Recv()

		if err == io.EOF {
			log.Println("Data eof")
			return err
		}
		if err != nil {
			log.Println("Data nil")
			return err
		}

		freshData(in)
	}

	return nil
}


// YourState
// This rpc passes,.
//
//	StateRequest server->client and
//	 StateResponse client<-server
// --
func (s *BridgeServerData) YourState(
	stream grpc.BidiStreamingServer[pb.StateResponse, pb.StateQuery]) error {

	log.Println("BR in YourState")

	//
	// State Response
	//
	go func(){
		for {
			reply, err := stream.Recv()
			if err == io.EOF {
				log.Println("BS eof", err)
				return
			}
			if err != nil {
				log.Println("BS !nil", err)
				return
			}

			stateResponse(reply)
		}

	}()



	//
	// State Query
	//
	for {

		select  {

		case query := <-BChannels.stateQuery:
			log.Println(" bs sending a query")

			stream.Send(&pb.StateQuery{
				Ask: query,
			})
			
		case <-time.After(10*time.Second):
			log.Println("bs tick")

		}

	}
	

	return nil
}
