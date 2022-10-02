package main

/* Source: https://socketloop.com/tutorials/golang-how-to-split-or-chunking-a-file-to-smaller-pieces */

import (
	"fmt"
	"hdfs/message"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"time"
)

func fileToChunk(filename string) *[][]byte {
	var listOfChunks [][]byte

	// Open the file if it exists
	file, err := os.Open(filename)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()

	// Getting the file size
	fileInfo, _ := file.Stat()
	var fileSize = fileInfo.Size()

	// 1 << 20 = 1 mb
	const chunkSize = 128 * (1 << 20)

	// Calculate total number of parts the file will be chunked into
	totalChunk := uint64(math.Ceil(float64(fileSize) / float64(chunkSize)))
	fmt.Println(totalChunk)

	// Iterate until all bytes are read
	for i := uint64(0); i < totalChunk; i++ {

		currChunkSize := int(math.Min(chunkSize, float64(fileSize-int64(i*chunkSize))))
		currChunk := make([]byte, currChunkSize) // byte array

		file.Read(currChunk)

		listOfChunks = append(listOfChunks, currChunk)
	}

	return &listOfChunks
}

func handleIncomingConnection(msgHandler *message.MessageHandler, c chan bool) {

	defer msgHandler.Close()
	for {
		wrapper, _ := msgHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *message.Wrapper_ControllerResMessage:

			if msg.ControllerResMessage.Type == 0 { // GET

			} else if msg.ControllerResMessage.Type == 1 { // PUT

				storageInfoListPerChunk := msg.ControllerResMessage.GetStorageInfoPerChunk()

				for key, val := range storageInfoListPerChunk {
					fmt.Println(key)
					fmt.Println(val.GetStorageInfo())
					for _, v := range val.GetStorageInfo() {
						fmt.Println(v.Host)
					}
				}

			} else if msg.ControllerResMessage.Type == 2 { // DELETE

			} else if msg.ControllerResMessage.Type == 3 { // LS
				fileList := msg.ControllerResMessage.GetFileList()

				for _, file := range fileList {
					fmt.Println(file)
				}
			}

			c <- true
		case *message.Wrapper_StorageResMessage:

		case nil:
			log.Println("Received an empty message, terminating server")
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

func sendRequest(msgHandler *message.MessageHandler, command string, path string) {

	var msg = message.ClientRequest{}
	switch command {
	case "-get":

	case "-put":

	case "-rm":
		msg = message.ClientRequest{Directory: path, Type: 2}
	case "-ls":
		msg = message.ClientRequest{Directory: path, Type: 3}
	}

	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_ClientReqMessage{ClientReqMessage: &msg},
	}

	msgHandler.Send(wrapper)
}

func main() {

	var listOfChunks [][]byte = *fileToChunk("../../L2-tjakrak/log.txt")
	log.Println("Number of parts: " + strconv.FormatInt(int64(len(listOfChunks)), 10))

	//for i := 0; i < len(listOfChunks); i++ {
	//	// write to disk
	//	fileName := "filePart_" + strconv.FormatInt(int64(i), 10)
	//	_, err := os.Create(fileName)
	//
	//	if err != nil {
	//		fmt.Println(err)
	//		os.Exit(1)
	//	}
	//
	//	// write/save buffer to disk
	//	os.WriteFile(fileName, listOfChunks[i], os.ModeAppend)
	//}

	/*
		file.writeAt go
	*/

	// Establish connection to the controller
	conn, err := net.Dial("tcp", "localhost:9999")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	msgHandler := message.NewMessageHandler(conn)
	c := make(chan bool)

	// Listening to any messages in the connection
	go handleIncomingConnection(msgHandler, c)

	// Send a request message to the server
	msg := message.ClientRequest{Directory: "test/hello/world.txt", FileSize: 450, Type: 1}
	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_ClientReqMessage{ClientReqMessage: &msg},
	}

	msgHandler.Send(wrapper)

	//msg = message.ClientRequest{Directory: "test/hello/", Type: 3}
	//wrapper = &message.Wrapper{
	//	Msg: &message.Wrapper_ClientReqMessage{ClientReqMessage: &msg},
	//}
	//
	//msgHandler.Send(wrapper)

	select {
	case res := <-c:
		fmt.Println(res)
	case <-time.After(30 * time.Second):
		fmt.Println("timeout")
	}

}
