package main

import (
	"log"
	pb "mockup/proto"

	"google.golang.org/grpc"
)

// Clients
//
// Clients are those who connect to services of the bridge server.
// Clients conect through 'streams'
// We wish to know who the clients are so that messages can be directed
// to them specifically, and also route messages from them to the
// appropriate modules on the server's end.

// /////////////////////////////////////////////////////////////////////////
// CLIENTS LIST
var newClients map[string]streamHolder

type streamHolder interface {
	Type() string
}

type streamData[T any, K any] struct {
	streamType    string
	messageStream T
	pipeLine      chan K
}

func (s *streamData[T, K]) Type() string {
	return s.streamType
}

//...

func removeClient(id string, tag string) {
	log.Println("Removing", id, "tag", tag)

	stH, ok := newClients[id]
	if !ok {
		log.Println("this client", id, "is already gone", stH)
		return
	}

	/*if stH.Type() == StateStreamer {
		streamer := stH.(*streamData[grpc.BidiStreamingServer[pb.StateResponse, pb.StateQuery], pb.StateQuery])
		streamer.messageStream.Context().Cancel
		close(streamer.pipeLine)
	}*/
	delete(newClients, id)
}

func addClient(id string, kind string, stream any) chan pb.StateQuery {
	log.Println(" ADDING A CLIENT", kind)

	if newClients == nil {
		newClients = make(map[string]streamHolder)
	}

	stD, exists := newClients[id]
	log.Println("std", stD, "exists", exists)

	if !exists {

		s, ok := stream.(grpc.BidiStreamingServer[pb.StateResponse, pb.StateQuery])
		log.Println("s", s, "ok", ok)
		if !ok {
			return nil
		}

		log.Println(" new state stream client added")
		newClients[id] = &streamData[grpc.BidiStreamingServer[pb.StateResponse, pb.StateQuery], pb.StateQuery]{
			streamType:    kind,
			messageStream: s,
			pipeLine:      make(chan pb.StateQuery),
		}

		stD = newClients[id]
	}

	if stD.Type() != StateStreamer {
		log.Println("not type *streamData[Bridge_YourStateClient]")
	}

	return newClients[id].(*streamData[grpc.BidiStreamingServer[pb.StateResponse, pb.StateQuery], pb.StateQuery]).pipeLine
}
