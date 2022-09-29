package main

import (
	"hdfs/data_structure"
	"hdfs/message"
	"log"
	"net"
)

// Store storage node information
type storageNode struct {
	hostname string
	port     int
	isAlive  bool
}

var fileSystemTree = data_structure.NewFileSystemTree()
var idToLocation = make(map[int]storageNode)
var storageLocation = make(map[string]bool)
var sizePerChunk = 128

func handleIncomingConnection(msgHandler *message.MessageHandler) {

	for {
		wrapper, _ := msgHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *message.Wrapper_ClientReqMessage:
			directory := msg.ClientReqMessage.GetDirectory()
			log.Println(directory)

			if msg.ClientReqMessage.Type == 0 { // GET

			} else if msg.ClientReqMessage.Type == 1 { // PUT
				fileSystemTree.PutFile(directory)
			} else if msg.ClientReqMessage.Type == 2 { // DELETE
				fileSystemTree.DeleteFile(directory)
			} else if msg.ClientReqMessage.Type == 3 { // LS
				fileList, _ := fileSystemTree.ShowFiles(directory)

				resMsg := message.ControllerResponse{FileList: fileList, Type: 3}
				wrapper = &message.Wrapper{
					Msg: &message.Wrapper_ControllerResMessage{ControllerResMessage: &resMsg},
				}

				msgHandler.Send(wrapper)
			}

		case *message.Wrapper_HbMessage:

		}
	}
}

func main() {

	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := message.NewMessageHandler(conn)
			go handleIncomingConnection(msgHandler)
		}
	}
}
