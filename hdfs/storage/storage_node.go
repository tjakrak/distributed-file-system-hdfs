package main

import (
	"bufio"
	"fmt"
	"golang.org/x/sys/unix"
	"hdfs/message"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var thisId int32 = 0
var thisDir = ""
var thisStorage = 0
var thisRetrieval = 0
var thisHostAndPort = "localhost:9998"
var msgHandlerMap = make(map[string]*message.MessageHandler)

func handleIncomingConnection(msgHandler *message.MessageHandler, c chan bool) {

	defer msgHandler.Close()
	for {
		wrapper, _ := msgHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *message.Wrapper_ClientReqMessage:
			if msg.ClientReqMessage.Type == 0 { // GET
				hashedDir := msg.ClientReqMessage.GetHashedDirectory()
				chunkId := msg.ClientReqMessage.GetChunkId()
				chunkBytes := getByteFromFile(hashedDir)
				sendChunkBytes(msgHandler, chunkId, chunkBytes)
				// send response using msgHandler

			} else if msg.ClientReqMessage.Type == 1 { // PUT
				hashedDir := msg.ClientReqMessage.GetHashedDirectory()
				chunkBytes := msg.ClientReqMessage.GetChunkBytes()
				chunkIdToSNInfo := msg.ClientReqMessage.GetStorageInfoPerChunk()

				isStored := storeChunk(hashedDir, chunkBytes)
				isReplicated := replicateChunk(hashedDir, chunkBytes, chunkIdToSNInfo)

				if isStored && isReplicated {
					sendAck(msgHandler, 1)
				}
				//storageInfoListPerChunk := msg.ClientReqMessage.GetStorageInfoPerChunk()

			} else if msg.ClientReqMessage.Type == 2 { // DELETE
			}

		case *message.Wrapper_StorageReqMessage:
			hashedDir := msg.StorageReqMessage.GetHashedDirectory()
			chunkBytes := msg.StorageReqMessage.GetChunkBytes()

			fmt.Println(hashedDir)
			isStored := storeChunk(hashedDir, chunkBytes)

			if isStored {
				sendAck(msgHandler, 1)
			}

		case *message.Wrapper_StorageResMessage:
			isAck := msg.StorageResMessage.GetAck()

			if isAck {
				c <- true
			}

		case *message.Wrapper_HbMessage:
			id := msg.HbMessage.GetId()

			if id != 0 {
				log.Printf("%d Register is successful\n", id)
				thisId = id
			}

		case nil:
			log.Println("Received an empty message, terminating server")
			time.Sleep(30 * time.Second)
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

func listenToIncomingConnection(listener net.Listener) {
	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := message.NewMessageHandler(conn)
			go handleIncomingConnection(msgHandler, nil)
		}
	}
}

func startHeartBeat(conn net.Conn) {
	msgHandler := message.NewMessageHandler(conn)
	defer msgHandler.Close()

	go handleIncomingConnection(msgHandler, nil)

	ticker := time.NewTicker(3 * time.Second)

	for {
		select {
		case <-ticker.C:
			var stat unix.Statfs_t
			wd, _ := os.Getwd()
			unix.Statfs(wd, &stat)
			spaceAvail := stat.Bavail * uint64(stat.Bsize)

			// Send request to the controller
			msg := message.Heartbeat{Id: thisId, HostAndPort: thisHostAndPort, SpaceAvailable: spaceAvail}
			wrapper := &message.Wrapper{
				Msg: &message.Wrapper_HbMessage{HbMessage: &msg},
			}
			msgHandler.Send(wrapper)
			log.Println("Send heartbeat")
		}
	}
}

func sendAck(msgHandler *message.MessageHandler, opType int) {
	msg := message.StorageResponse{Ack: true, Type: message.OperationType(opType)}
	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_StorageResMessage{StorageResMessage: &msg},
	}
	msgHandler.Send(wrapper)
}

func storeChunk(fileName string, chunkByte []byte) bool {
	// write to disk
	f, err := os.Create(thisDir + fileName)

	if err != nil {
		log.Println(err)
		return false
	}

	defer f.Close()

	// write/save buffer to disk
	os.WriteFile(fileName, chunkByte, os.ModeAppend)
	return true
}

func replicateChunk(hashedDir string, chunk []byte, chunkIdToSNInfo map[int32]*message.StorageInfoList) bool {
	var wg sync.WaitGroup

	// Iterating through each chunkId and storage location to store the chunk
	for key, val := range chunkIdToSNInfo {
		for i, v := range val.GetStorageInfo() {
			if i > 0 {
				host := v.Host
				port := v.Port
				hostAndPort := host + ":" + strconv.FormatInt(int64(port), 10)

				wg.Add(1)
				go sendRequestStorage(hostAndPort, key, hashedDir, chunk, &wg)
			}
		}
	}

	wg.Wait()
	return true
}

func sendRequestStorage(hostAndPort string, chunkId int32, chunkName string, chunk []byte, wg *sync.WaitGroup) {

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

	msg := message.StorageRequest{
		HashedDirectory: chunkName,
		ChunkSize:       uint64(len(chunk)),
		ChunkBytes:      chunk,
	}

	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_StorageReqMessage{StorageReqMessage: &msg},
	}

	msgHandler.Send(wrapper)

	select {
	case res := <-c:
		fmt.Printf("replicate successful %t\n", res)
	case <-time.After(60 * time.Second):
		fmt.Println("timeout")
	}

	wg.Done()
}

func getByteFromFile(filename string) []byte {

	chunk, err := os.Open(filename)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer chunk.Close()

	chunkInfo, err := chunk.Stat()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// calculate the bytes size of each chunk
	// we are not going to rely on previous data and constant

	var chunkSize int64 = chunkInfo.Size()
	chunkBufferBytes := make([]byte, chunkSize)

	// read into chunkBufferBytes
	reader := bufio.NewReader(chunk)
	_, err = reader.Read(chunkBufferBytes)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return chunkBufferBytes
}

func sendChunkBytes(msgHandler *message.MessageHandler, chunkId int32, chunkBytes []byte) {
	msg := message.StorageResponse{Ack: true, Type: 0, ChunkId: chunkId, ChunkBytes: chunkBytes}
	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_StorageResMessage{StorageResMessage: &msg},
	}
	msgHandler.Send(wrapper)
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
	go startHeartBeat(conn)

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
