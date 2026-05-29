package main


import (
	"log"
	pb "mockup/proto"

)


// Do.Once

func pcolIsConnected() {
	close(BChannels.pcolConnected)
}


// Channels to send out going messages
// to the bridge server
//
type BridgeServerChannels struct {

	stateQuery chan *pb.StateQuery
	stateResponse chan *pb.StateResponse
	dataStream chan *pb.HeadCount

	pcolConnected chan interface{} // signal Pcol connection
}

var BChannels *BridgeServerChannels

func NewBChannels() *BridgeServerChannels {
	return &BridgeServerChannels{}
}

func (s *BridgeServerChannels) init() {
	s.stateQuery = make(chan *pb.StateQuery)
	s.stateResponse = make(chan *pb.StateResponse)
	s.dataStream = make(chan *pb.HeadCount)

	s.pcolConnected = make(chan interface{})
}

func freshData(data *pb.HeadCount, id string) {
	log.Println(id, "Data", data)
}


func stateResponse(reply *pb.StateResponse, id string) {
	log.Println(id, "REGISTERING a response:", reply)
	BChannels.stateResponse <- reply
}

func postAStateQuery(ask *pb.StateQuery) {
	log.Println("...POSTING a query:", ask)

	BChannels.stateQuery <- ask
}

