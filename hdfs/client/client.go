package main

/* Source: https://socketloop.com/tutorials/golang-how-to-split-or-chunking-a-file-to-smaller-pieces */
/* Source: https://www.socketloop.com/tutorials/golang-recombine-chunked-files-example */
/* Cmd to run:
PUT: go run client/client.go -put ../../L2-tjakrak/log2.txt home/log2.txt localhost:9999
LS: go run client/client.go -ls ../../L2-tjakrak/ localhost:9999
GET: go run client/client.go -get home/log2.txt s3/log.txt localhost:9999
     go run client/client.go -get <hdfs_dir> <local_dir> <controller>
USAGE: go run client/client.go -usage localhost:9999
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
var fileWriteLock = sync.Mutex{}
var fd *os.File
var idToChunk []chunkStruct

// fileToChunk open a file then split it into n chunk based
// on the chunk size
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

// handleIncomingConnection is listening to any incoming response/ack message from
// Storage Node or Controller and handle it accordingly
func handleIncomingConnection(msgHandler *message.MessageHandler, c chan bool) {

	defer msgHandler.Close()

	for {
		wrapper, _ := msgHandler.Receive()

		// Check where is the msg coming from
		switch msg := wrapper.Msg.(type) {
		case *message.Wrapper_ControllerResMessage:

			// Handling error when file already exist/ file not found
			err := msg.ControllerResMessage.GetError()
			if err != "" {
				fmt.Println(err)
			}

			if msg.ControllerResMessage.Type == 0 { // GET
				// Parse message from protobuf
				chunkIdToSNInfo := msg.ControllerResMessage.GetStorageInfoPerChunk()
				chunkSize := msg.ControllerResMessage.GetChunkSize()
				idToChunk = make([]chunkStruct, chunkSize)

				var wg sync.WaitGroup

				// Iterating through each chunkId to get and store chunk to an array
				for chunkId, snList := range chunkIdToSNInfo {
					wg.Add(1)
					go sendGetRequestSN(snList, chunkId, int64(chunkSize), &wg)
				}

				wg.Wait() // Blocking until all the threads finish executing

			} else if msg.ControllerResMessage.Type == 1 { // PUT
				// Parse message from protobuf
				chunkIdToSNInfo := msg.ControllerResMessage.GetStorageInfoPerChunk()
				chunkSize := msg.ControllerResMessage.GetChunkSize()
				chunkList := fileToChunk(localFileDir, chunkSize)

				// Iterating through each chunkId and storage location to store the chunk
				for chunkId, snList := range chunkIdToSNInfo {
					// Get the first storage location from the list
					for _, sn := range snList.GetStorageInfo() {
						host := sn.Host
						port := sn.Port

						// Concatenate host and port into one string
						hostAndPort := host + ":" + strconv.FormatInt(int64(port), 10)
						// Put filepath and chunk id together and encoded them
						encodedChunkName := base64.StdEncoding.EncodeToString([]byte(hdfsFileDir + "-" +
							strconv.FormatInt(int64(chunkId), 10)))

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

			} else if msg.ControllerResMessage.Type == 4 { // USAGE

				snFileList := msg.ControllerResMessage.GetNodeList()
				spaceAvail := msg.ControllerResMessage.GetSpaceAvailable()

				fmt.Printf("Available space: %d\n", spaceAvail)
				fmt.Println("Storage node status:")
				for _, v := range snFileList.GetStorageInfo() {
					fmt.Printf("Host: %s | Port: %d | Num of requests: %d\n",
						v.GetHost(), v.GetPort(), v.GetRequests())
				}

			}

			c <- true

		case *message.Wrapper_StorageResMessage:
			if msg.StorageResMessage.Type == 0 { // GET
				// Parse message from protobuf
				chunkId := msg.StorageResMessage.GetChunkId()
				chunkBytes := msg.StorageResMessage.GetChunkBytes()

				// Keep track the number of chunk we obtained base on the chunk id
				idToChunk[chunkId].c <- true
				idToChunk[chunkId].chunkByte = chunkBytes

			} else if msg.StorageResMessage.Type == 1 { // PUT
				// When storage node successfully store a chunk from the client
				fmt.Println("Success")
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

// sendPutRequestSN send put request to storage nodes to upload chunk of our local file.
// Host and port will be given by the controller.
func sendPutRequestSN(hostAndPort string, chunkId int32, chunkName string, chunk []byte, storageInfoList *message.StorageInfoList) {
	var msgHandler *message.MessageHandler

	conn, err := net.Dial("tcp", hostAndPort)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	msgHandler = message.NewMessageHandler(conn, "")

	c := make(chan bool)
	// Listening response from storage
	go handleIncomingConnection(msgHandler, c)

	// Send request to storage
	chunkIdToSNInfo := make(map[int32]*message.StorageInfoList)
	chunkIdToSNInfo[chunkId] = storageInfoList

	// Serialize request message to protobuf and send it to the storage node
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

	// Blocking until the handle incoming connection receive response from storage node
	log.Println("waiting...")
	select {
	case res := <-c:
		fmt.Printf("Put chunk %d: %t\n", chunkId, res)
	case <-time.After(60 * time.Second):
		fmt.Println("Timeout... Put chunk: false")
	}

	close(c)
}

// sendGetRequestSN send GET request to fetch chunk from storage nodes
func sendGetRequestSN(snList *message.StorageInfoList, chunkId int32, chunkSize int64, wg *sync.WaitGroup) {
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
			msgHandler = message.NewMessageHandler(conn, "")
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
			fmt.Printf("get chunk %d: %t\n", chunkId, res)
			fileWriteLock.Lock()
			_, err := fd.WriteAt(idToChunk[chunkId].chunkByte, int64(chunkId)*chunkSize)

			if err != nil {
				log.Println(err)
			}

			fileWriteLock.Unlock()
			wg.Done()
			return
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

// sendRequestController handle operation specified by the user command line input
func sendRequestController(opType string) {

	// Create connection with the controller
	conn, err := net.Dial("tcp", controllerHostPort)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	// Create msg handler obj (client and controller)
	msgHandler := message.NewMessageHandler(conn, "")
	c := make(chan bool)

	// Listening to any messages in the connection from controller
	go handleIncomingConnection(msgHandler, c)

	var msg = message.ClientRequest{}
	switch opType {
	case "-get":

		// Create a file to store the file
		newFile := localFileDir
		_, err = os.Create(newFile)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Open the newly created file
		fd, err = os.OpenFile(newFile, os.O_WRONLY, os.ModeAppend)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		msg = message.ClientRequest{Directory: hdfsFileDir, Type: 0}

	case "-put":
		file, err := os.Open(localFileDir) // For read access.
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		fStat, _ := file.Stat()
		size := fStat.Size()
		log.Printf("Size of the file: %d", size)

		msg = message.ClientRequest{Directory: hdfsFileDir, FileSize: uint64(size), Type: 1}
	case "-rm":
		msg = message.ClientRequest{Directory: hdfsFileDir, Type: 2}
	case "-ls":
		msg = message.ClientRequest{Directory: hdfsFileDir, Type: 3}
	case "-usage":
		msg = message.ClientRequest{Type: 4}
	}

	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_ClientReqMessage{ClientReqMessage: &msg},
	}

	msgHandler.Send(wrapper)

	for {
		select {
		case <-c:
			fmt.Println("Operation is successful")
			os.Exit(0)
		case <-time.After(60 * time.Second):
			fmt.Println("timeout")
		}
	}
}

// parseCLI making sure the user put the correct number of input
// and parse them to be stored on the global variable.
func parseCLI() string {
	cli := os.Args

	// Check if the user at least put 3 cmd arg
	if len(cli) < 3 {
		fmt.Println("Missing arguments")
		os.Exit(3)
	}

	// Get op type (get, put, ls, rm) from the cmd arg
	opType := strings.ToLower(cli[1])

	if opType == "-get" || opType == "-put" {
		// Need at least 5 cmd arg because we need both local and hdfs directory
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
		// Need at least 4 cmd arg because we need hdfs directory
		if len(cli) < 4 {
			fmt.Println("Missing arguments")
			os.Exit(3)
		}

		// Parse CLI
		hdfsFileDir = cli[2]
		controllerHostPort = cli[3]
	} else if opType == "-usage" {
		controllerHostPort = cli[2]
	}

	return opType
}

func main() {
	opType := parseCLI()
	sendRequestController(opType)
}
