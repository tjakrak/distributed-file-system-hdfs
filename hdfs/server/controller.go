package main

import (
	"hdfs/data_structure"
	"hdfs/message"
	"log"
	"net"
	"strconv"
	"strings"
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
var latestId = 0

var f = func(c rune) bool {
	return c == ':'
}

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
			id := msg.HbMessage.GetId()

			// Id equals 0 means the storage node has not registered yet
			if id == 0 {
				hostAndPort := msg.HbMessage.GetHostAndPort()
				if storageLocation[hostAndPort] == true {
					// send error that host and port already registered
				}

				hostAndPortArr := strings.FieldsFunc(hostAndPort, f)
				host := hostAndPortArr[0]
				port, _ := strconv.Atoi(hostAndPortArr[1])
				assignedId := latestId + 1
				sn := storageNode{host, port, true}

				idToLocation[assignedId] = sn
				storageLocation[hostAndPort] = true
			}
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
