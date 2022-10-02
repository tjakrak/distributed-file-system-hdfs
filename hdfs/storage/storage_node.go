package main

import (
	"golang.org/x/sys/unix"
	"hdfs/message"
	"log"
	"net"
	"os"
	"time"
)

var thisId int32 = 0
var thisStorage = 0
var thisRetrieval = 0

func handleIncomingConnection(msgHandler *message.MessageHandler) {

	defer msgHandler.Close()
	for {
		wrapper, _ := msgHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *message.Wrapper_ClientReqMessage:

		case *message.Wrapper_HbMessage:
			id := msg.HbMessage.GetId()

			if id != 0 {
				log.Printf("%d Register is successful\n", id)
				thisId = id
			}

		case nil:
			log.Println("Received an empty message, terminating server")
			os.Exit(3)
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

func listenToIncomingConnection(listener net.Listener) {
	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := message.NewMessageHandler(conn)
			go handleIncomingConnection(msgHandler)
		}
	}
}

func startHeartBeat(conn net.Conn, hostAndPort string) {
	msgHandler := message.NewMessageHandler(conn)
	defer msgHandler.Close()

	go handleIncomingConnection(msgHandler)

	ticker := time.NewTicker(3 * time.Second)

	for {
		select {
		case <-ticker.C:
			var stat unix.Statfs_t
			wd, _ := os.Getwd()
			unix.Statfs(wd, &stat)
			spaceAvail := stat.Bavail * uint64(stat.Bsize)

			// Send request to the controller
			msg := message.Heartbeat{Id: thisId, HostAndPort: hostAndPort, SpaceAvailable: spaceAvail}
			wrapper := &message.Wrapper{
				Msg: &message.Wrapper_HbMessage{HbMessage: &msg},
			}
			msgHandler.Send(wrapper)
			log.Println("Send heartbeat")
		}
	}
}

func storeChunk(fileName string, chunkByte []byte) {
	// write to disk
	f, err := os.Create(fileName)

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	defer f.Close()

	// write/save buffer to disk
	os.WriteFile(fileName, chunkByte, os.ModeAppend)

}

func main() {
	// directory of the storage path and hostname:port of the controller
	//if len(os.Args) < 2 {
	//	fmt.Println("Missing arguments")
	//	os.Exit(3)
	//}

	//storagePath := os.Args[1]
	//hostAndPort := strings.Split(os.Args[2], ":")
	//hostname := hostnameAndPort[0]
	//port, err := strconv.Atoi(hostnameAndPort[1])

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
	go startHeartBeat(conn, "localhost:9999")

	// Establish storage node server
	listener, err := net.Listen("tcp", ":9998")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	listenToIncomingConnection(listener)

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
