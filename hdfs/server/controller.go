package server

import (
	"fmt"
	"log"
	"net"
)

func main() {
	//home := data_structure.New("home", "directory")

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
