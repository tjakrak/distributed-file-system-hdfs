package main

/* Source: https://socketloop.com/tutorials/golang-how-to-split-or-chunking-a-file-to-smaller-pieces */
/* Source: https://www.socketloop.com/tutorials/golang-recombine-chunked-files-example */
/* Cmd to run:
PUT: go run client/client.go -put ../../L2-tjakrak/log2.txt ../../L2-tjakrak/log2.txt localhost:9999
LS: go run client/client.go -ls ../../L2-tjakrak/ localhost:9999
GET: go run client/client.go -get s3/log.txt ../../L2-tjakrak/log2.txt localhost:9999
     go run client/client.go -get <hdfs_dir> <local_dir> <controller>
*/

import (
	"encoding/base64"
	"fmt"
	"hdfs/message"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type chunkStruct struct {
	chunkByte []byte
	c         chan bool
}

var hdfsFileDir = ""
var localFileDir = ""
var controllerHostPort = ""
var msgHandlerMap = make(map[string]*message.MessageHandler)
var idToChunk []chunkStruct
var chunkContainerLock = sync.RWMutex{}

func chunkToFile(chunks []chunkStruct) {
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
		chunkSize := len(chunk.chunkByte)
		writePosition = writePosition + chunkSize

		_, err := file.Write(chunk.chunkByte)

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

		// Check where is the msg coming from
		switch msg := wrapper.Msg.(type) {
		case *message.Wrapper_ControllerResMessage:
			// If file already exist or not exist
			err := msg.ControllerResMessage.GetError()
			if err != "" {
				fmt.Println(err)
			}

			if msg.ControllerResMessage.Type == 0 { // GET
				chunkIdToSNInfo := msg.ControllerResMessage.GetStorageInfoPerChunk()
				chunkSize := msg.ControllerResMessage.GetChunkSize()
				idToChunk = make([]chunkStruct, chunkSize)
				var wg sync.WaitGroup

				// Iterating through each chunkId to get and store chunk to an array
				for chunkId, snList := range chunkIdToSNInfo {
					go sendGetRequestSN(snList, chunkId, &wg)
					wg.Add(1)
				}

				wg.Wait()

				// Put all the chunks together into a file
				chunkToFile(idToChunk)

			} else if msg.ControllerResMessage.Type == 1 { // PUT
				chunkIdToSNInfo := msg.ControllerResMessage.GetStorageInfoPerChunk()
				chunkSize := msg.ControllerResMessage.GetChunkSize()
				chunkList := fileToChunk(localFileDir, chunkSize)

				// Iterating through each chunkId and storage location to store the chunk
				for chunkId, snList := range chunkIdToSNInfo {
					// Get the first storage location from the list
					for _, sn := range snList.GetStorageInfo() {
						host := sn.Host
						port := sn.Port

						hostAndPort := host + ":" + strconv.FormatInt(int64(port), 10)
						encodedChunkName := base64.StdEncoding.EncodeToString([]byte(hdfsFileDir + "-" + strconv.FormatInt(int64(chunkId), 10)))

						// Send put request to the first storage in the list
						sendPutRequestSN(hostAndPort, chunkId, encodedChunkName, chunkList[chunkId], snList)
						break
					}
				}

			} else if msg.ControllerResMessage.Type == 2 { // DELETE

				fmt.Println("File deleted successfully")

			} else if msg.ControllerResMessage.Type == 3 { // LS
				fileList := msg.ControllerResMessage.GetFileList()

				// Print the file list to the terminal
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

func sendPutRequestSN(hostAndPort string, chunkId int32, chunkName string, chunk []byte, storageInfoList *message.StorageInfoList) {
	var msgHandler *message.MessageHandler

	conn, err := net.Dial("tcp", hostAndPort)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	msgHandler = message.NewMessageHandler(conn, hostAndPort)

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

	fmt.Println("WAITING")

	select {
	case res := <-c:
		fmt.Printf("sub channel %t\n", res)
	case <-time.After(60 * time.Second):
		fmt.Println("sub timeout")
	}

	close(c)
}

func sendGetRequestSN(snList *message.StorageInfoList, chunkId int32, wg *sync.WaitGroup) {
	var msgHandler *message.MessageHandler
	snListLength := len(snList.GetStorageInfo())

	// Iterating through the list of storage ids that contains the chunk
	for i, sn := range snList.GetStorageInfo() {
		host := sn.Host
		port := sn.Port
		hostAndPort := host + ":" + strconv.FormatInt(int64(port), 10)
		encodedChunkName := base64.StdEncoding.EncodeToString([]byte(hdfsFileDir + "-" + strconv.FormatInt(int64(chunkId), 10)))

		// Check if msg handler already created previously
		if mh, ok := msgHandlerMap[hostAndPort]; ok {
			msgHandler = mh
		} else {
			conn, err := net.Dial("tcp", hostAndPort)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// Create new msg handler
			msgHandler = message.NewMessageHandler(conn, hostAndPort)
			msgHandlerMap[hostAndPort] = msgHandler
			// Listening to the incoming connection from this msg handler
			go handleIncomingConnection(msgHandler, nil)
		}

		// Create channel and store it to a map where the key is the chunk id
		c := make(chan bool)
		idToChunk[chunkId].c = c

		// Build and send protobuf msg request
		msg := message.ClientRequest{
			HashedDirectory: encodedChunkName,
			ChunkId:         chunkId,
			Type:            0,
		}
		wrapper := &message.Wrapper{
			Msg: &message.Wrapper_ClientReqMessage{ClientReqMessage: &msg},
		}
		msgHandler.Send(wrapper)

		// Wait until get a response from the server
		select {
		// Case when we get back a response from the storage node
		case res := <-c:
			fmt.Printf("get %t\n", res)
			wg.Done()
			break
		// Case when we don't get any response from the storage node
		case <-time.After(60 * time.Second):
			if i >= snListLength-1 {
				log.Printf("Fail to get chunk id: %d\n", chunkId)
			} else {
				log.Printf("Retrying to fetch chunk id: %d\n", chunkId)
			}
		}
	}
}

func sendRequestController(opType string) {

	// Create connection with the controller
	conn, err := net.Dial("tcp", controllerHostPort)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	// Create msg handler obj (client and controller)
	msgHandler := message.NewMessageHandler(conn, controllerHostPort)
	c := make(chan bool)

	// Listening to any messages in the connection from controller
	go handleIncomingConnection(msgHandler, c)

	var msg = message.ClientRequest{}
	switch opType {
	case "-get":
		msg = message.ClientRequest{Directory: hdfsFileDir, Type: 0}
	case "-put":
		file, err := os.Open(localFileDir) // For read access.
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		fStat, err := file.Stat()
		size := fStat.Size()
		log.Printf("Size of the file: %d", size)

		msg = message.ClientRequest{Directory: hdfsFileDir, FileSize: uint64(size), Type: 1}
	case "-rm":
		msg = message.ClientRequest{Directory: hdfsFileDir, Type: 2}
	case "-ls":
		msg = message.ClientRequest{Directory: hdfsFileDir, Type: 3}
	}

	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_ClientReqMessage{ClientReqMessage: &msg},
	}

	msgHandler.Send(wrapper)

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

func parseCLI() string {
	cli := os.Args

	// Check if the user at least put 3 cmd arg
	if len(cli) < 4 {
		fmt.Println("Missing arguments")
		os.Exit(3)
	}

	// Get op type (get, put, ls, rm) from the cmd arg
	opType := strings.ToLower(cli[1])

	if opType == "-get" || opType == "-put" {
		// Need at least 4 cmd arg because we need both local and hdfs directory
		if len(cli) < 5 {
			fmt.Println("Missing arguments")
			os.Exit(3)
		}

		// Parse CLI
		if opType == "-put" {
			localFileDir = cli[2]
			hdfsFileDir = cli[3]
		} else {
			localFileDir = cli[3]
			hdfsFileDir = cli[2]
		}

		controllerHostPort = cli[4]

	} else if opType == "-ls" || opType == "-rm" {
		// Parse CLI
		hdfsFileDir = cli[2]
		controllerHostPort = cli[3]
	}

	return opType
}

func main() {
	opType := parseCLI()
	sendRequestController(opType)
}
