package main

import (
	"hdfs/data_structure"
	"hdfs/message"
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
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
var snIdToMemberInfoLock = sync.RWMutex{}
var snLocation = make(map[string]bool)

//var snLatestId = 0

var snLatestId = 0

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

				_, err := fileSystemTree.PutFile(directory, chunkIdToSNIdList)

				if err != nil {
					log.Println(err)
					resMsg = message.ControllerResponse{Error: err.Error(), Type: 1}
				}

				sendControllerResponseMsg(msgHandler, &resMsg)

			} else if msg.ClientReqMessage.Type == 2 { // DELETE
				fileSystemTree.DeleteFile(directory)
			} else if msg.ClientReqMessage.Type == 3 { // LS
				fileList, _ := fileSystemTree.ShowFiles(directory)

				resMsg := message.ControllerResponse{FileList: fileList, Type: 3}
				sendControllerResponseMsg(msgHandler, &resMsg)
			}

		case *message.Wrapper_HbMessage:
			id := msg.HbMessage.GetId()

			// Id equals 0 means the storage node has not registered yet
			if id == 0 {
				newId, isSuccess := registerNewSN(msg)
				if !isSuccess {
					log.Println("Fail to register storage node")
				} else {
					resMsg := message.Heartbeat{Id: int32(newId)}
					sendHeartbeatMsg(msgHandler, &resMsg)
				}
			} else {
				heartBeatHandler(int(id))
			}
		}
	}
}

func registerNewSN(msg *message.Wrapper_HbMessage) (int, bool) {
	hostAndPort := msg.HbMessage.GetHostAndPort()
	spaceAvail := msg.HbMessage.GetSpaceAvailable()

	if snLocation[hostAndPort] == true || spaceAvail == 0 {
		return -1, false
	}

	hostAndPortArr := strings.FieldsFunc(hostAndPort, f)
	host := hostAndPortArr[0]
	port, _ := strconv.Atoi(hostAndPortArr[1])
	assignedId := snLatestId + 1
	sn := storageNode{host, int32(port), true, time.Now(), spaceAvail}

	// Register storage node
	snIdToMemberInfo[assignedId] = &sn
	snLocation[hostAndPort] = true

	snLatestId += 1

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
	defer snIdToMemberInfoLock.Unlock()
}

func getRandNumber() []int {
	snRandIdList := make([]int, snLatestId)
	rand.Seed(time.Now().UnixNano())
	p := rand.Perm(snLatestId)
	for i, r := range p[:snLatestId] {
		snRandIdList[i] = r + 1
	}

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

	//sn1 := storageNode{
	//	hostname:   "A",
	//	port:       1111,
	//	isAlive:    true,
	//	lastHB:     time.Time{},
	//	spaceAvail: 0,
	//}
	//
	//sn2 := storageNode{
	//	hostname:   "B",
	//	port:       2222,
	//	isAlive:    true,
	//	lastHB:     time.Time{},
	//	spaceAvail: 0,
	//}
	//
	//sn3 := storageNode{
	//	hostname:   "C",
	//	port:       3333,
	//	isAlive:    true,
	//	lastHB:     time.Time{},
	//	spaceAvail: 0,
	//}
	//
	//snIdToMemberInfo[1] = &sn1
	//snIdToMemberInfo[2] = &sn2
	//snIdToMemberInfo[3] = &sn3

	go heartBeatChecker(5 * time.Second)

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
