package main

import (
	"log"
	pb "mockup/proto"
)



var PChannels *PcolChannels

func NewPChannels() *PcolChannels {
	return &PcolChannels{}
}

type PcolChannels struct {
	stateQyToControls chan *pb.StateQuery
	stateRespToBridge chan *pb.StateResponse
	dataStream chan *pb.HeadCount
}


func (s *PcolChannels) init() {
  s.stateQyToControls = make(chan *pb.StateQuery)
  s.stateRespToBridge = make(chan *pb.StateResponse)
  s.dataStream = make(chan *pb.HeadCount)
}


// Traffic -> Bridge
func freshData(data *pb.HeadCount) {
	log.Println(".Data ")
	PChannels.dataStream <- data
}

// Bridge -> Traffic
func postAQuery(query *pb.StateQuery) {
	log.Println(" ..Query")
	PChannels.stateQyToControls <- query
}


// Traffic -> Bridge
func registerAResponse(response *pb.StateResponse) {
	log.Println(" Reply..")
	PChannels.stateRespToBridge <- response
}
