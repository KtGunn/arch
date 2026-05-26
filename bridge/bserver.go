package main

import (
	"fmt"
	"io"
	"log"
	"net"

	//"time"

	pb "mockup/proto"
	"google.golang.org/grpc"
	empty "github.com/golang/protobuf/ptypes/empty"
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


// This rpc passes,.
//
//	StateQuery -> client
// --
func (s *BridgeServerData) In(stream pb.Bridge_InServer) error {

	for {
		resp, err := stream.Recv()
		if err != nil {
			log.Println("error on receiving response stream")
		}
		
		BChannels.stateResponse <- resp.Reply
	}
	return nil

}


// This rpc passes,.
//
//	StateResponse <- client
// --
func (s *BridgeServerData) Out(qy *empty.Empty,
	stream grpc.ServerStreamingServer[pb.StateQuery]) error {
	
	for {
		select {
		case state := <-BChannels.stateQuery:
			log.Println("Bridge sending state to client")
			qy := &pb.StateQuery{
				Ask: state,
			}
			stream.Send(qy)
		}
	}

	return nil
}
