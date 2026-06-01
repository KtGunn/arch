package logic

import (
	"fmt"

	"math/rand/v2"
	"time"
)

func generateHeadCount(bugOut chan interface{}) {

	names := []string{"bob", "joe", "ken", "art", "cye", "skinny"}
	count := -1

	for {

		count++
		message := fmt.Sprintf("%d: ", count)

		for n := range names {
			if rand.IntN(100) >= 50 {
				name := names[n]
				message += name + " "
			}
		}

		select {
			
		case <-time.After(time.Duration(2+rand.IntN(4)) * time.Second):
			Logger.Println(" <-", message)
			dataChan <- message

		case <-bugOut:
			Logger.Println(" head count generator bugging out!")
			return
		}
	}
}
