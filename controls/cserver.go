package main

import (
	"net"
	"fmt"
	"log"
	"time"
	"context"
	"math/rand/v2"
	pb "mockup/proto"
	empty "github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)


// Control Server definition
//
type ControlServerType struct {
	pb.UnimplementedTrafficServer
}

func NewControlServer() *ControlServerType{
	return &ControlServerType{}
}

var ControlServer *ControlServerType



// LaunchServer
//
func LaunchServer(port int) {

	lis, err := net.Listen("tcp",
		fmt.Sprintf("localhost:%d", port),
	)

	if err != nil {
			log.Fatal("Port open failed ", err)
	}

	var opts[]grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	ControlServer = NewControlServer()
	pb.RegisterTrafficServer(grpcServer, ControlServer)

	grpcServer.Serve(lis)
}



// Data
// the server streaming method
//
func (s *ControlServerType) Data(_ *empty.Empty, stream pb.Traffic_DataServer) error {

	log.Println(" ready to stream head count")

	names := []string{"bob", "joe", "ken", "art", "cye", "skinny" }

	count := 0
	for {
		
		message := fmt.Sprintf("%d: ", count)
		
		for n := range names {
			if rand.IntN(100) >= 50 {
				name := names[n]
				message += name + " "
			}
		}

		log.Println(" st->", count)
		count++

		data := &pb.HeadCount{
			Present: message,
		}

		if err := stream.Send(data); err != nil {
			log.Println("Client might be gone")
			return nil
		}

		<-time.After(1 * time.Second)
	}
	
	return nil
}



// YourState
// response to the query
//
var instance int

func (s *ControlServerType) YourState(ctx context.Context,
	ask *pb.StateQuery) (*pb.StateResponse, error) {

	instance++
	
	log.Println("rep->", instance)
	return &pb.StateResponse{
		Reply: fmt.Sprintf(" %d all systems are go", instance),
	}, nil
}
