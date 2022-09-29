package main

import (
	"fmt"
	"hdfs/message"
	"log"
	"net"
	"os"
)

func main() {
	// directory of the storage path and hostname:port of the controller
	if len(os.Args) < 2 {
		fmt.Println("Missing arguments")
		os.Exit(3)
	}

	//storagePath := os.Args[1]
	//hostnameAndPort := strings.Split(os.Args[2], ":")
	//hostname := hostnameAndPort[0]
	//port, err := strconv.Atoi(hostnameAndPort[1])

	//if err != nil {
	//	log.Fatalln(err.Error())
	//	return
	//}

	listener, err := net.Listen("tcp", ":9998")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := message.NewMessageHandler(conn)
			print(msgHandler)
			// go handleClient(msgHandler, &m)
		}
	}

	//msg := message.Request{Directory: "", Ch}
	//msg = messages.Chat{Username: user, MessageBody: messageBody}
	//dirMsg := messages.Direct{DestinationUsername: destinationUser, Msg: &msg}
	//wrapper = &messages.Wrapper{
	//	Msg: &messages.Wrapper_DirectMessage{DirectMessage: &dirMsg},
	//}
}
