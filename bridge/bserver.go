package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	
	"time"

	pb "mockup/proto"
	"google.golang.org/grpc"
)


// Bridge Server definition
//
type BridgeServerData struct {
	pb.UnimplementedBridgeServer
	Connections sync.Map
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

	log.Printf("New stream: %+v", stream)

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

		log.Printf("data: ID %s stream %+v\n", in.Id, stream)
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

	log.Printf("New State stream: %+v\n", stream)
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

			log.Printf("state: ID %s stream %+v\n", reply.Id, stream)
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
			stream.Send(query)
			
		case <-time.After(10*time.Second):
			log.Println("bs tick")

		}

	}
	

	return nil
}
