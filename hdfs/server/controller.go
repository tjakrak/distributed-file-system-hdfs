package main

import (
	"hdfs/data_structure"
	"hdfs/message"
	"log"
	"net"
)

func handleClient(msgHandler *message.MessageHandler, fileSystemTree *data_structure.FileSystemTree) {

	for {
		wrapper, _ := msgHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *message.Wrapper_ClientReqMessage:
			directory := msg.ClientReqMessage.GetDirectory()

			if msg.ClientReqMessage.Type == 0 { // GET

			} else if msg.ClientReqMessage.Type == 1 { // PUT
				fileSystemTree.PutFile(directory)
			} else if msg.ClientReqMessage.Type == 2 { // DELETE

			} else if msg.ClientReqMessage.Type == 1 { // LS
				fileSystemTree.ShowFiles(directory)
			}

		case *message.Wrapper_HbMessage:
		}
	}
}

func main() {
	fileSystemTree := data_structure.NewFileSystemTree()

	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := message.NewMessageHandler(conn)
			go handleClient(msgHandler, fileSystemTree)
		}
	}
}
