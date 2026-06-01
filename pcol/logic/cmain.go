package logic

import (
	"log"
	"os"
	"fmt"
)

var Logger *log.Logger

func RunLogic (port int, id string) {
	
	logFile := openALog(fmt.Sprintf("ll_%s.log", id))
	Logger.Println("RunLogic port", port, "id", id)

	defer func(){
		logFile.Close()
	}()

	LaunchServer(port, id)
}


func openALog(logFileName string) *os.File {

	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}

 	Logger = log.New(file, "", log.LstdFlags)

	//Note: setting output will mix console and file outputs together
	//      we want them separated!
	//log.SetOutput(file)

	return file
}
