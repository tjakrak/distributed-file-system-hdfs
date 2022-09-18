package server

import (
	"fmt"
	"log"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for {
		if conn, err := listener.Accept(); err == nil {

			fmt.Print(conn)
			//msgHandler := messages.NewMessageHandler(conn)
			//go handleClient(msgHandler, &m)
		}
	}
}
