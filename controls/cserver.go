package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"
	"google.golang.org/grpc"

	pb "mockup/proto"
	empty "github.com/golang/protobuf/ptypes/empty"
)


// Control Server definition
//
type ControlServerType struct {
	pb.UnimplementedTrafficServer
	ctx context.Context
}
func NewControlServer() *ControlServerType {
	return &ControlServerType{}
}
var ControlServer *ControlServerType
// --



// Channels
//
var (
	dataChan     chan string
	responseChan chan string
)

var ID string

func InitChannels() {
	dataChan = make(chan string)
	responseChan = make(chan string)
}
// --



// LaunchServer
func LaunchServer(port int, id string) {

	ID = id
	
	lis, err := net.Listen("tcp",
		fmt.Sprintf("localhost:%d", port),
	)

	if err != nil {
		log.Fatal("Port open failed ", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	ControlServer = NewControlServer()
	
	ControlServer.ctx, _ = context.WithCancel(context.Background())
	
	pb.RegisterTrafficServer(grpcServer, ControlServer)
	
	InitChannels()
	grpcServer.Serve(lis)
}

// Data
// the server streaming method
func (s *ControlServerType) Data(_ *empty.Empty, stream pb.Traffic_DataServer) error {
	
	log.Println(" ready to stream head count")
	
	bugOut := make(chan interface{})
	
	defer func(bug chan interface{}) {
	    bug <-struct{}{}
  }(bugOut)
	
	go generateHeadCount(bugOut)
	
	for {
	
	
	select {
	case message := <-dataChan:
		log.Println(" data got message")
		data := &pb.HeadCount{
			Id: ID,
			Present: message,
		}
		if err := stream.Send(data); err != nil {
			log.Println("Client might be gone")
			return err
		}
		
	case <-s.ctx.Done():
		return nil
		
	case <-time.After(5 * time.Second):
		log.Println(" cD tick")
		
		}
	}

	return nil
}

// YourState
// response to the query
var instance int

func (s *ControlServerType) YourState(ctx context.Context,
	ask *pb.StateQuery) (*pb.StateResponse, error) {

	instance++

	log.Println("rep->", instance)
	return &pb.StateResponse{
		Id: ID,
		Reply: fmt.Sprintf(" %d all systems are go", instance),
	}, nil
}
