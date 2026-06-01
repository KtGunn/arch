package main

import (
	"fmt"
	"log"
	"time"
	"sync"
	
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
			
			log.Println("type", streamer.Type())
			if streamer.Type() == StateStreamer {
				st := streamer.(*streamData[pb.Bridge_YourStateServer,pb.StateQuery])
				pipe := st.pipeLine
				
				log.Println(" POSTING A QUERY")
				doQuery(pipe, &pb.StateQuery{
					Ask: fmt.Sprintf("qy.%d->%s", n, key),
				})
				n++
			}
		}
		log.Println(" ... zzzz sleeping")
		time.Sleep(5*time.Second)
	}
}

func doQuery(pipe chan pb.StateQuery, query *pb.StateQuery) {
	postAStateQuery(pipe, query)
	response := <-BChannels.stateResponse
	log.Println("QyResponse:", response)
}
