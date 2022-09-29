package main

import (
	"hdfs/message"
	"log"
	"net"
)

func main() {
	// directory of the storage path and hostname:port of the controller
	//if len(os.Args) < 2 {
	//	fmt.Println("Missing arguments")
	//	os.Exit(3)
	//}

	//storagePath := os.Args[1]
	//hostnameAndPort := strings.Split(os.Args[2], ":")
	//hostname := hostnameAndPort[0]
	//port, err := strconv.Atoi(hostnameAndPort[1])

	//if err != nil {
	//	log.Fatalln(err.Error())
	//	return
	//}

	// Establish storage node server
	//listener, err := net.Listen("tcp", ":9998")
	//if err != nil {
	//	log.Fatalln(err.Error())
	//	return
	//}

	// Establish connection to the controller
	conn, err := net.Dial("tcp", "localhost:9999")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	msgHandler := message.NewMessageHandler(conn)

	// Send request to the controller
	msg := message.Heartbeat{HostAndPort: "localhost:9998"}
	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_HbMessage{HbMessage: &msg},
	}

	msgHandler.Send(wrapper)

	//for {
	//	if conn, err := listener.Accept(); err == nil {
	//		msgHandler := message.NewMessageHandler(conn)
	//		print(msgHandler)
	//		// go handleClient(msgHandler, &m)
	//	}
	//}

	//msg := message.Request{Directory: "", Ch}
	//msg = messages.Chat{Username: user, MessageBody: messageBody}
	//dirMsg := messages.Direct{DestinationUsername: destinationUser, Msg: &msg}
	//wrapper = &messages.Wrapper{
	//	Msg: &messages.Wrapper_DirectMessage{DirectMessage: &dirMsg},
	//}
}
