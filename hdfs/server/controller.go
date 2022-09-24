package main

import (
	"fmt"
	"hdfs/message"
	"log"
	"net"
	"strings"
)

func getDirectories(directory string) {
	messageArr := strings.Split(directory, "/")

	for i, s := range messageArr {
		fmt.Println(i, s)
	}
}

func handleClient(msgHandler *message.MessageHandler) {

	for {
		wrapper, _ := msgHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *message.Wrapper_RequestMessage:
			directory := msg.RequestMessage.GetDirectory()
			getDirectories(directory)
			fmt.Println(msg.RequestMessage.GetType())

		case *message.Wrapper_HbMessage:
		}
	}
}

func main() {
	//home := data_structure.New("home", "directory")

	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for {
		if conn, err := listener.Accept(); err == nil {

			msgHandler := message.NewMessageHandler(conn)
			go handleClient(msgHandler)
		}
	}
}
