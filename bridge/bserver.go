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


var stateQuery = make(chan string)

// LaunchBridge
//
func LaunchBridge(port int) {

	// 'Drive' function for testing purposes only.
	//
	go func() {
		for {
			time.Sleep(3*time.Second)
			stateQuery<-"This is a query"
		}
	}()
	// --
	
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
		_, err := stream.Recv()

		if err == io.EOF {
			log.Println("Data eof")
			return err
		}
		if err != nil {
			log.Println("Data nil")
			return err
		}

		log.Println(" st<-dt")
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

	go func(){
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				log.Println("BS eof", err)
				return
			}
			if err != nil {
				log.Println("BS !nil", err)
				return
			}
			log.Println(" st<-ys")
		}

	}()


	for {

		select
		{
		case out := <-stateQuery :
			outmsg := &pb.StateQuery{
				Ask: out,
			}
			log.Println(" st->ys")
			stream.Send(outmsg)
			
		case <-time.After(10*time.Second):
			log.Println("bs tick")
		}

	}
	

	return nil
}
