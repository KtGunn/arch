package main

import (
	"log"
	"fmt"
	"time"
	pb "mockup/proto"
)


var Users []*User

type User struct {
	name string
	id int
}

func NewUser(name string, id int) *User {
	return &User{
		name: name,
		id: id,
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
	log.Println(" ...pcol is connected")


	n := 0
	for {
		for key, _ := range newClients {
			// Later: for key, value := range Clients {}
			doQuery(&pb.StateQuery{
				Ask: fmt.Sprintf("qy.%d->%s", n, key),
			})
			n++
		}

		time.Sleep(10*time.Second)
	}
}

func doQuery(query *pb.StateQuery) {
	postAStateQuery(query)
	_ = <-BChannels.stateResponse
}
