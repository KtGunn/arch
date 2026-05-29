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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

var once sync.Once

// CLIENTS LIST
//
var (
	Clients map[string]grpc.BidiStreamingServer[pb.StateResponse, pb.StateQuery]
)



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
	

	Clients = make(map[string]grpc.BidiStreamingServer[pb.StateResponse, pb.StateQuery])
	
	log.Println("Bridge waiting for data traffic on ", port)
	grpcServer.Serve(lis)
}




// Data
// This rpc is CLIENT STREAMING
// --
func (s *BridgeServerData) Data(stream pb.Bridge_DataServer) error {
	log.Printf(":: Data stream: %+v", stream)
	
	var ok bool
	var md map[string][]string
	md, ok = metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Errorf(codes.DataLoss, "BridgeServer: failed to get metadata")
	}

	var identity string
	var t []string
	if t, ok = md["identity"]; !ok {
		log.Fatalf("no identity from stream: %+v", md)
	}
	identity = t[0]


	once.Do(pcolIsConnected)
	
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

		freshData(in, identity)
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
	log.Printf(":: State stream: %+v", stream)


	var ok bool
	var md map[string][]string
	md, ok = metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Errorf(codes.DataLoss, "BridgeServer: failed to get metadata")
	}

	var identity string
	var t []string
	if t, ok = md["identity"]; !ok {
		log.Fatalf("no identity from stream: %+v", md)
	}
	identity = t[0]
	addStateQueryClient(identity, stream)	

	once.Do(pcolIsConnected)

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

			stateResponse(reply, identity)
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

func addStateQueryClient(
	id string,
	stream grpc.BidiStreamingServer[pb.StateResponse,pb.StateQuery]) {

	var strm grpc.BidiStreamingServer[pb.StateResponse,pb.StateQuery]
	var ok bool

	strm, ok = Clients[id]
	if !ok {
		log.Println(" we're adding ", id, "for state queries")
		Clients[id] = strm
	} else {
		log.Println( id, "is knwon to us")
	}
}

