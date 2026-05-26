package main

import (
	pb "mockup/proto"
)


type Streams struct {
	dataStream pb.Bridge_DataServer
	stateStream pb.Bridge_YourStateServer
}

type Connection struct {
	identifier string
	streams Streams
}



