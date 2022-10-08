package main

// source: https://medium.com/@arpith/resetting-a-ticker-in-go-63858a2c17ec

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

// to run: go run storage/storage_node.go s1/ -port 9998 localhost:9999

var thisId int32 = 0
var thisDir = ""
var thisStorage = 0
var thisRetrieval = 0
var thisPort = ""
var thisHostAndPort = ""
var controllerHostAndPort = ""
var msgHandlerMap = make(map[string]*message.MessageHandler)
var msgHandlerMapLock = sync.RWMutex{}
var hashedDirToChan = make(map[string]chan bool)
var hashedDirToChanLock = sync.RWMutex{}
var lastHB = time.Time{}
var lastHBLock = sync.Mutex{}
var waitAck chan bool

func handleIncomingConnection(msgHandler *message.MessageHandler) {

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
					sendAck(msgHandler, "", 1)
				}
			}
		case *message.Wrapper_StorageReqMessage:
			hashedDir := msg.StorageReqMessage.GetHashedDirectory()
			chunkBytes := msg.StorageReqMessage.GetChunkBytes()

			isStored := storeChunk(hashedDir, chunkBytes)

			if isStored {
				sendAck(msgHandler, hashedDir, 1)
			}

		case *message.Wrapper_StorageResMessage:
			isAck := msg.StorageResMessage.GetAck()
			hashedDir := msg.StorageResMessage.GetHashedDirectory() // hashed dir + host and port

			if isAck {
				hashedDirToChanLock.Lock()
				hashedDirToChan[hashedDir] <- true
				delete(hashedDirToChan, hashedDir)
				hashedDirToChanLock.Unlock()
			}

		case *message.Wrapper_HbMessage:
			id := msg.HbMessage.GetId()

			if id != 0 {
				log.Printf("%d Register is successful\n", id)
				thisId = id
			} else {
				log.Println("Get ack from controller")
			}

			lastHBLock.Lock()
			lastHB = time.Now()
			lastHBLock.Unlock()

		case nil:
			log.Println("Received an empty message, terminating server")
			return
			//time.Sleep(5 * time.Second)
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

func listenToIncomingConnection(listener net.Listener) {
	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := message.NewMessageHandler(conn, "")
			go handleIncomingConnection(msgHandler)
		}
	}
}

func startHeartBeat(conn net.Conn, controllerHostPort string) {
	msgHandler := message.NewMessageHandler(conn, controllerHostPort)

	lastHBLock.Lock()
	lastHB = time.Now()
	lastHBLock.Unlock()

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
			msg := message.Heartbeat{Id: thisId, HostAndPort: thisHostAndPort, SpaceAvailable: spaceAvail}
			wrapper := &message.Wrapper{
				Msg: &message.Wrapper_HbMessage{HbMessage: &msg},
			}

			lastHBLock.Lock()
			if (lastHB.Add(5 * time.Second)).Before(time.Now()) {
				fmt.Println("Controller is dead")
				fmt.Println("Reconnecting")

				ticker.Stop()

				// Reconnecting to controller
				n := 0
				for n < 5 {
					var err error
					msgHandler, err = msgHandler.Retry()

					if err == nil {
						log.Println("Successfully reconnect to the controller")
						break
					}

					log.Println("Controller is inactive. Retrying...")
					time.Sleep(5 * time.Second)
					n++
				}

				go handleIncomingConnection(msgHandler)

				lastHB = time.Now()
				fmt.Println(".....")
				time.Sleep(5 * time.Second)
				ticker = time.NewTicker(3 * time.Second)
			}
			lastHBLock.Unlock()

			msgHandler.Send(wrapper)
			log.Println("Send heartbeat")
		}
	}
}

func sendAck(msgHandler *message.MessageHandler, hashedDir string, opType int) {
	msg := message.StorageResponse{Ack: true, HashedDirectory: hashedDir + thisHostAndPort, Type: message.OperationType(opType)}
	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_StorageResMessage{StorageResMessage: &msg},
	}
	msgHandler.Send(wrapper)
}

func storeChunk(fileName string, chunkByte []byte) bool {
	fileDir := thisDir + fileName

	// write to disk
	f, err := os.Create(fileDir)

	if err != nil {
		log.Println(err)
		return false
	}

	defer f.Close()

	// write/save buffer to disk
	os.WriteFile(fileDir, chunkByte, os.ModeAppend)
	return true
}

func replicateChunk(hashedDir string, chunk []byte, chunkIdToSNInfo map[int32]*message.StorageInfoList) bool {
	var wg sync.WaitGroup

	// Iterating through each chunkId and storage location to store the chunk
	for _, snInfo := range chunkIdToSNInfo {
		for i, v := range snInfo.GetStorageInfo() {
			if i > 0 {
				host := v.Host
				port := v.Port
				hostAndPort := host + ":" + strconv.FormatInt(int64(port), 10)

				wg.Add(1)
				go sendRequestStorage(hostAndPort, hashedDir, chunk, &wg)
			}
		}
	}

	wg.Wait()
	return true
}

func sendRequestStorage(hostAndPort string, hashedDir string, chunk []byte, wg *sync.WaitGroup) {

	var msgHandler *message.MessageHandler

	msgHandlerMapLock.Lock()
	if mh, ok := msgHandlerMap[hostAndPort]; ok {
		msgHandler = mh
	} else {
		conn, err := net.Dial("tcp", hostAndPort)
		if err != nil {
			log.Println(err)
			log.Printf("Replicate to: %s has failed\n", hostAndPort)
			return
		}

		msgHandler = message.NewMessageHandler(conn, hostAndPort)
		msgHandlerMap[hostAndPort] = msgHandler
	}
	msgHandlerMapLock.Unlock()

	c := make(chan bool)

	hashedDirToChanLock.Lock()
	hashedDirToChan[hashedDir+hostAndPort] = c
	hashedDirToChanLock.Unlock()

	// Listening response from storage
	go handleIncomingConnection(msgHandler)

	msg := message.StorageRequest{
		HashedDirectory: hashedDir,
		HostPort:        hostAndPort,
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

	chunk, err := os.Open(thisDir + filename)

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

func parseCLI() {
	// /directory -port 5555 localhost:9999
	cli := os.Args

	if len(cli) < 4 {
		fmt.Println("Missing arguments")
		os.Exit(3)
	}

	host, _ := os.Hostname()
	thisDir = cli[1]
	thisPort = cli[3]
	thisHostAndPort = host + ":" + thisPort
	controllerHostAndPort = cli[4]
}

func main() {

	parseCLI()

	// Establish connection to the controller
	conn, err := net.Dial("tcp", controllerHostAndPort)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	go startHeartBeat(conn, controllerHostAndPort)

	// Establish storage node server
	listener, err := net.Listen("tcp", ":"+thisPort)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	listenToIncomingConnection(listener)
}
