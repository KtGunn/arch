package main

import (
	pb "mockup/proto"
)

type Streams struct {
	dataStream  *pb.Bridge_DataServer
	stateStream *pb.Bridge_YourStateServer
}

var IdConnections = make(map[string]*Streams)

func NewConnection(identifier string, stype string, stream any) {

	streams, ok := IdConnections[identifier]
	if !ok {
		streams = &Streams{}
		IdConnections[identifier] = streams
	}

	switch stype {
	case "data":
		streams.dataStream = stream.(*pb.Bridge_DataServer)
	case "state":
		streams.stateStream = stream.(*pb.Bridge_YourStateServer)
	}
}

func getStream(identifier string, stype string) (any, bool) {
	streams, ok := IdConnections[identifier]
	if !ok {
		return nil, false
	}

	switch stype {
	case "data":
		return streams.dataStream, streams.dataStream != nil
	case "state":
		return streams.stateStream, streams.stateStream != nil
	default:
		return nil, false
	}
}
