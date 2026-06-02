package main

import (
	"fmt"
	"log"
	"time"
	"sync"
	"math/rand/v2"
	
	pb "mockup/proto"
)

const (
	StateStreamer = "stateQy"
	DataStreamer = "dataStream"
)

var userPresent sync.Once

var Users []*User

type User struct {
	name string
	id   int
}

func NewUser(name string, id int) *User {
	return &User{
		name: name,
		id:   id,
	}
}

func addUser(name string, id int) {
	user := NewUser(name, id)
	Users = append(Users, user)
}

func getUser(id int) *User {
	for _, u := range Users {
		if u.id == id {
			return u
		}
	}
	return nil
}



func launchUser(id int, howManyQueries int) {

	user := getUser(id)
	ask := fmt.Sprintf("%s query", user.name)
	log.Println(ask)

	log.Println(" user is waiting for pcol to be connected...")
	<-BChannels.pcolConnected

	n := 0
	for {
		for key, streamer := range newClients {
			time.Sleep(time.Duration(2+rand.IntN(8))*time.Second)
			
			log.Println("type", streamer.Type())
			if streamer.Type() == StateStreamer {
				st := streamer.(*streamData[pb.Bridge_YourStateServer,pb.StateQuery])
				pipe := st.pipeLine
				
				doQuery(pipe, &pb.StateQuery{
					Ask: fmt.Sprintf("qy.%d->%s", n, key),
				})
				n++
			}
		}
	}
}

func doQuery(pipe chan pb.StateQuery, query *pb.StateQuery) {
	postAStateQuery(pipe, query)
	response := <-BChannels.stateResponse
	log.Println("QyResponse:", response)
}
