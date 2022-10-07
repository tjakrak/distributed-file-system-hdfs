package main

import (
	"hdfs/data_structure"
	"hdfs/message"
	"log"
	"sync"
	"time"
)

// To Run: go run server/controller.go -port 9999

import (
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
)

// Store storage node information
type storageNode struct {
	hostname   string
	port       int32
	isAlive    bool
	lastHB     time.Time
	spaceAvail uint64
}

const sizePerChunk int = 128000000 // 128 mb
const rep = 3

var fileSystemTree = data_structure.NewFileSystemTree()
var snIdToMemberInfo = make(map[int]*storageNode)
var snLocation = make(map[string]bool)
var snLatestId = 0

var fileSystemTreeLock = sync.RWMutex{}
var snIdToMemberInfoLock = sync.RWMutex{}
var snRegisterNewLock = sync.RWMutex{}

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
				chunkIdToSNInfo := make(map[int32]*message.StorageInfoList)

				// Get file based on the directory
				chunkIdToSNIdList, err := fileSystemTree.GetFile(directory)

				var resMsg = message.ControllerResponse{}
				if err != nil { // If file does not exist
					resMsg = message.ControllerResponse{Error: err.Error()}
				} else { // If file does exist
					for chunkId, snIdList := range chunkIdToSNIdList {
						storageInfoList := new(message.StorageInfoList)

						for _, snId := range snIdList {
							host := snIdToMemberInfo[int(snId)].hostname
							port := snIdToMemberInfo[int(snId)].port
							isAlive := snIdToMemberInfo[int(snId)].isAlive

							storageInfo := message.StorageInfo{Host: host, Port: port, IsAlive: isAlive}
							storageInfoList.StorageInfo = append(storageInfoList.StorageInfo, &storageInfo)
						}

						chunkIdToSNInfo[int32(chunkId)] = storageInfoList
					}

					resMsg = message.ControllerResponse{
						StorageInfoPerChunk: chunkIdToSNInfo,
						ChunkSize:           uint64(len(chunkIdToSNIdList)),
						Type:                0,
					}
				}

				sendControllerResponseMsg(msgHandler, &resMsg)

			} else if msg.ClientReqMessage.Type == 1 { // PUT
				fileSize := msg.ClientReqMessage.GetFileSize()
				numOfChunks := int(math.Ceil(float64(fileSize) / float64(sizePerChunk)))
				chunkIdToSNIdList := make(map[int][]int32)
				chunkIdToSNInfo := make(map[int32]*message.StorageInfoList)

				// Iterate over the total number of chunks
				for i := 0; i < numOfChunks; i++ {
					snIdList := make([]int32, rep)
					snRandIdList := getRandNumber()
					storageInfoList := new(message.StorageInfoList)

					lastCheckedIndex := 0
					// Getting three node storage
					for j := 0; j < rep; j++ {
						for k := lastCheckedIndex; k < len(snRandIdList); k++ {
							id := snRandIdList[k]

							if snIdToMemberInfo[id].isAlive {
								host := snIdToMemberInfo[id].hostname
								port := snIdToMemberInfo[id].port

								if j == 0 {
									log.Printf("Send put request to: %d\n", id)
								}

								snIdList[j] = int32(id)
								storageInfo := message.StorageInfo{Host: host, Port: port, IsAlive: true}
								storageInfoList.StorageInfo = append(storageInfoList.StorageInfo, &storageInfo)

								lastCheckedIndex = k + 1
								break
							}
						}
					}

					chunkIdToSNIdList[i] = snIdList
					chunkIdToSNInfo[int32(i)] = storageInfoList
				}

				resMsg := message.ControllerResponse{
					ChunkSize:           uint64(sizePerChunk),
					FileList:            nil,
					StorageInfoPerChunk: chunkIdToSNInfo,
					Type:                1,
				}

				fileSystemTreeLock.Lock()
				err := fileSystemTree.PutFile(directory, chunkIdToSNIdList)
				fileSystemTreeLock.Unlock()

				if err != nil {
					log.Println(err)
					resMsg = message.ControllerResponse{Error: err.Error()}
				}

				sendControllerResponseMsg(msgHandler, &resMsg)

			} else if msg.ClientReqMessage.Type == 2 { // DELETE

				fileSystemTreeLock.Lock()
				_, err := fileSystemTree.DeleteFile(directory)
				fileSystemTreeLock.Unlock()

				resMsg := message.ControllerResponse{}
				if err != nil {
					// If the directory end with a file and not a directory
					// Or directory does not exist
					resMsg = message.ControllerResponse{Error: err.Error()}
				} else {
					// If the path provided by the client is a directory
					resMsg = message.ControllerResponse{Type: 2}
				}

				sendControllerResponseMsg(msgHandler, &resMsg)

			} else if msg.ClientReqMessage.Type == 3 { // LS

				fileSystemTreeLock.RLock()
				fileList, err := fileSystemTree.ShowFiles(directory)
				fileSystemTreeLock.RUnlock()

				resMsg := message.ControllerResponse{}
				if err != nil {
					// If the directory end with a file and not a directory
					// Or directory does not exist
					resMsg = message.ControllerResponse{Error: err.Error()}
				} else {
					// If the path provided by the client is a directory
					resMsg = message.ControllerResponse{FileList: fileList, Type: 3}
				}

				sendControllerResponseMsg(msgHandler, &resMsg)
			}

		case *message.Wrapper_HbMessage:
			id := msg.HbMessage.GetId()

			// Id equals 0 means the storage node has not registered yet
			if id == 0 {
				newId, isSuccess := registerSN(msg, true)
				if !isSuccess {
					log.Println("Fail to register storage node")
				} else {
					fmt.Println("register success")
					resMsg := message.Heartbeat{Id: int32(newId)}
					sendHeartbeatMsg(msgHandler, &resMsg)
				}
			} else {
				if _, ok := snIdToMemberInfo[int(id)]; !ok {
					registerSN(msg, false)
				}

				resMsg := message.Heartbeat{Id: 0}

				sendHeartbeatMsg(msgHandler, &resMsg)
				heartBeatHandler(int(id))
			}
		}
	}
}

func registerSN(msg *message.Wrapper_HbMessage, isNew bool) (int, bool) {
	hostAndPort := msg.HbMessage.GetHostAndPort()
	spaceAvail := msg.HbMessage.GetSpaceAvailable()

	snRegisterNewLock.Lock()

	if snLocation[hostAndPort] == true || spaceAvail == 0 {
		return -1, false
	}

	hostAndPortArr := strings.FieldsFunc(hostAndPort, f)
	host := hostAndPortArr[0]
	port, _ := strconv.Atoi(hostAndPortArr[1])

	var assignedId int
	if isNew {
		assignedId = snLatestId + 1
		snLatestId += 1
	} else {
		assignedId = int(msg.HbMessage.GetId())
	}

	sn := storageNode{host, int32(port), true, time.Now(), spaceAvail}

	// Register storage node
	snIdToMemberInfo[assignedId] = &sn
	snLocation[hostAndPort] = true

	snRegisterNewLock.Unlock()

	return assignedId, true
}

func heartBeatChecker(duration time.Duration) {
	for tick := range time.Tick(duration) {
		snIdToMemberInfoLock.RLock()
		for id, val := range snIdToMemberInfo {
			if (val.lastHB.Add(duration)).Before(tick) {
				log.Printf("%d Is dead", id)
				val.isAlive = false
			}
		}
		snIdToMemberInfoLock.RUnlock()
	}
}

func heartBeatHandler(id int) {
	log.Printf("%d Receive heartbeat", id)

	currTime := time.Now()
	snIdToMemberInfoLock.Lock()
	snIdToMemberInfo[id].lastHB = currTime
	snIdToMemberInfoLock.Unlock()
}

func getRandNumber() []int {
	snRegisterNewLock.RLock()

	snRandIdList := make([]int, snLatestId)
	rand.Seed(time.Now().UnixNano())
	p := rand.Perm(snLatestId)
	for i, r := range p[:snLatestId] {
		snRandIdList[i] = r + 1
	}

	snRegisterNewLock.RUnlock()

	return snRandIdList
}

func sendControllerResponseMsg(msgHandler *message.MessageHandler, msg *message.ControllerResponse) {
	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_ControllerResMessage{ControllerResMessage: msg},
	}

	msgHandler.Send(wrapper)
}

func sendHeartbeatMsg(msgHandler *message.MessageHandler, msg *message.Heartbeat) {
	wrapper := &message.Wrapper{
		Msg: &message.Wrapper_HbMessage{HbMessage: msg},
	}

	msgHandler.Send(wrapper)
}

func main() {

	cli := os.Args

	// Check if the user put a port
	if len(cli) != 3 || strings.ToLower(cli[1]) != "-port" {
		fmt.Println("Missing arguments/too much arguments")
		os.Exit(3)
	}

	go heartBeatChecker(5 * time.Second)

	port := cli[2]
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	// Loop to keep listening for new message
	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := message.NewMessageHandler(conn, "")
			go handleIncomingConnection(msgHandler)
		}
	}
}
