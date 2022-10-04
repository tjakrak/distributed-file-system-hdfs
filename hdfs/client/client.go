package main

/* Source: https://socketloop.com/tutorials/golang-how-to-split-or-chunking-a-file-to-smaller-pieces */
/* Source: https://www.socketloop.com/tutorials/golang-recombine-chunked-files-example */

import (
	"encoding/base64"
	"fmt"
	"hdfs/message"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type chunkStruct struct {
	chunkByte []byte
	c         chan bool
}

var hdfsFileDir = ""
var localFileDir = ""
var msgHandlerMap = make(map[string]*message.MessageHandler)
var idToChunk []chunkStruct
var chunkContainerLock = sync.RWMutex{}

func chunkToFile(chunks [][]byte) {
	// Create a file to store the file
	newFile := localFileDir
	_, err := os.Create(newFile)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Open the newly created file
	file, err := os.OpenFile(newFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// To keep track the pointer position when we are appending
	var writePosition int = 0
	for i, chunk := range chunks {
		chunkSize := len(chunk)
		writePosition = writePosition + chunkSize

		_, err := file.Write(chunk)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		file.Sync() //flush to disk
		log.Println("Recombining part [", i, "] into : ", newFile)

	}

	file.Close()
}

func fileToChunk(filename string, chunkSize uint64) [][]byte {
	var chunkList [][]byte

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

	// Calculate total number of parts the file will be chunked into
	totalChunk := uint64(math.Ceil(float64(fileSize) / float64(chunkSize)))

	// Iterate until all bytes are read
	for i := uint64(0); i < totalChunk; i++ {

		currChunkSize := int(math.Min(float64(chunkSize), float64(fileSize-int64(i*chunkSize))))
		currChunk := make([]byte, currChunkSize) // byte array

		file.Read(currChunk)

		chunkList = append(chunkList, currChunk)
	}

	return chunkList
}

func handleIncomingConnection(msgHandler *message.MessageHandler, c chan bool) {

	defer msgHandler.Close()

	for {
		wrapper, _ := msgHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *message.Wrapper_ControllerResMessage:

			if msg.ControllerResMessage.Type == 0 { // GET
				chunkIdToSNInfo := msg.ControllerResMessage.GetStorageInfoPerChunk()
				chunkSize := msg.ControllerResMessage.GetChunkSize()
				idToChunk = make([]chunkStruct, chunkSize)

				// Iterating through each chunkId and storage location to store the chunk
				for chunkId, snList := range chunkIdToSNInfo {
					sendGetRequestSN(snList, chunkId)
				}

			} else if msg.ControllerResMessage.Type == 1 { // PUT

				// If file already exist
				err := msg.ControllerResMessage.GetError()
				if err != "" {
					fmt.Println(err)
					c <- true
					break
				}

				chunkIdToSNInfo := msg.ControllerResMessage.GetStorageInfoPerChunk()
				chunkSize := msg.ControllerResMessage.GetChunkSize()

				chunkList := fileToChunk(localFileDir, chunkSize)

				// Iterating through each chunkId and storage location to store the chunk
				for chunkId, snList := range chunkIdToSNInfo {
					fmt.Println(snList)
					for _, sn := range snList.GetStorageInfo() {
						host := sn.Host
						port := sn.Port
						hostAndPort := host + ":" + strconv.FormatInt(int64(port), 10)
						encodedChunkName := base64.StdEncoding.EncodeToString([]byte(hdfsFileDir + "-" + strconv.FormatInt(int64(chunkId), 10)))

						sendPutRequestSN(hostAndPort, chunkId, encodedChunkName, chunkList[chunkId], snList)
						break
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
			if msg.StorageResMessage.Type == 0 { // GET
				chunkId := msg.StorageResMessage.GetChunkId()
				chunkBytes := msg.StorageResMessage.GetChunkBytes()

				idToChunk[chunkId].c <- true
				idToChunk[chunkId].chunkByte = chunkBytes

			} else if msg.StorageResMessage.Type == 1 { // PUT
				fmt.Println("success")
				c <- true
			}

		case nil:
			log.Println("Received an empty message, terminating server")
			os.Exit(3)
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

func sendRequestController(msgHandler *message.MessageHandler, command string, path string) {

	var msg = message.ClientRequest{}
	switch command {
	case "-get":

	case "-put":
		file, err := os.Open(localFileDir) // For read access.
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		fStat, err := file.Stat()
		size := fStat.Size()
		fmt.Println(size)

		// send request to controller
		msg = message.ClientRequest{Directory: hdfsFileDir, FileSize: uint64(size), Type: 1}
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

func sendPutRequestSN(hostAndPort string, chunkId int32, chunkName string, chunk []byte, storageInfoList *message.StorageInfoList) {
	var msgHandler *message.MessageHandler

	if mh, ok := msgHandlerMap[hostAndPort]; ok {
		msgHandler = mh
	} else {
		conn, err := net.Dial("tcp", hostAndPort)
		if err != nil {
			log.Fatalln(err.Error())
			return
		}

		msgHandler = message.NewMessageHandler(conn)
		msgHandlerMap[hostAndPort] = msgHandler
	}

	c := make(chan bool)
	// Listening response from storage
	go handleIncomingConnection(msgHandler, c)

	// Send request to storage
	chunkIdToSNInfo := make(map[int32]*message.StorageInfoList)
	chunkIdToSNInfo[chunkId] = storageInfoList

	msg := message.ClientRequest{
		HashedDirectory:     chunkName,
		ChunkId:             chunkId,
		ChunkSize:           uint64(len(chunk)),
		ChunkBytes:          chunk,
		Type:                1,
		StorageInfoPerChunk: chunkIdToSNInfo,
	}

	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_ClientReqMessage{ClientReqMessage: &msg},
	}

	msgHandler.Send(wrapper)

	select {
	case res := <-c:
		fmt.Printf("sub channel %t\n", res)
	case <-time.After(60 * time.Second):
		fmt.Println("sub timeout")
	}
}

func sendGetRequestSN(snList *message.StorageInfoList, chunkId int32) {
	var msgHandler *message.MessageHandler

	for _, sn := range snList.GetStorageInfo() {
		host := sn.Host
		port := sn.Port
		hostAndPort := host + ":" + strconv.FormatInt(int64(port), 10)
		encodedChunkName := base64.StdEncoding.EncodeToString([]byte(hdfsFileDir + "-" + strconv.FormatInt(int64(chunkId), 10)))

		if mh, ok := msgHandlerMap[hostAndPort]; ok {
			msgHandler = mh
		} else {
			conn, err := net.Dial("tcp", hostAndPort)
			if err != nil {
				log.Fatalln(err.Error())
				return
			}

			msgHandler = message.NewMessageHandler(conn)
			msgHandlerMap[hostAndPort] = msgHandler
			go handleIncomingConnection(msgHandler, nil)
		}

		c := make(chan bool)

		idToChunk[chunkId].c = c

		msg := message.ClientRequest{
			HashedDirectory: encodedChunkName,
			ChunkId:         chunkId,
			Type:            0,
		}

		wrapper := &message.Wrapper{
			Msg: &message.Wrapper_ClientReqMessage{ClientReqMessage: &msg},
		}

		msgHandler.Send(wrapper)

		select {
		case res := <-c:
			fmt.Printf("get %t\n", res)
		case <-time.After(60 * time.Second):
			fmt.Printf("get timeout for chunk id: %d\n", chunkId)
		}
	}
}

func main() {

	var listOfChunks [][]byte = fileToChunk("../../L2-tjakrak/log.txt", 128*(1<<20))
	log.Println("Number of parts: " + strconv.FormatInt(int64(len(listOfChunks)), 10))

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
	localFileDir = "../../L2-tjakrak/log2.txt"
	hdfsFileDir = "../../L2-tjakrak/log2.txt"
	// hdfsFileDir = "/Users/ryantjakrakartadinata/go/src/L2-tjakrak/large-log.txt"
	file, err := os.Open(localFileDir) // For read access.
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fStat, err := file.Stat()
	size := fStat.Size()
	fmt.Println(size)

	// send request to controller
	msg := message.ClientRequest{Directory: hdfsFileDir, FileSize: uint64(size), Type: 1}
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

	for {
		select {
		case res := <-c:
			fmt.Println(res)
			os.Exit(0)
		case <-time.After(60 * time.Second):
			fmt.Println("timeout")
		}
	}

}

func sendPutRequest() {

}
