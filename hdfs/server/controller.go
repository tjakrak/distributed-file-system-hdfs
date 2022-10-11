package main

import (
	"encoding/gob"
	"fmt"
	"hdfs/data_structure"
	"hdfs/message"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// To Run: go run server/controller.go -port 9999

// Store storage node information
type storageNode struct {
	Hostname   string
	Port       int32
	IsAlive    bool
	LastHB     time.Time
	SpaceAvail int64
	NumOfReq   int32
}

const sizePerChunk int = 128000000 // 128 mb
const rep = 3

var fileSystemTree = data_structure.NewFileSystemTree()
var snIdToMemberInfo = make(map[int]*storageNode)
var snLocation = make(map[string]bool)
var snLatestId = 0
var totalSpaceAvail int64 = 0

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
				fileSystemTreeLock.RLock()
				chunkIdToSNIdList, err := fileSystemTree.GetFile(directory)
				fileSystemTreeLock.RUnlock()

				var resMsg = message.ControllerResponse{}
				if err != nil { // If file does not exist
					resMsg = message.ControllerResponse{Error: err.Error()}
				} else { // If file does exist
					for chunkId, snIdList := range chunkIdToSNIdList {
						snInfoList := new(message.StorageInfoList)

						for _, snId := range snIdList {
							snIdToMemberInfoLock.RLock()
							host := snIdToMemberInfo[int(snId)].Hostname
							port := snIdToMemberInfo[int(snId)].Port
							isAlive := snIdToMemberInfo[int(snId)].IsAlive
							snIdToMemberInfoLock.RUnlock()

							snInfo := message.StorageInfo{Host: host, Port: port, IsAlive: isAlive}
							snInfoList.StorageInfo = append(snInfoList.StorageInfo, &snInfo)
						}

						chunkIdToSNInfo[int32(chunkId)] = snInfoList
					}

					resMsg = message.ControllerResponse{
						StorageInfoPerChunk: chunkIdToSNInfo,
						ChunkSize:           uint64(sizePerChunk),
						NumOfChunk:          int32(len(chunkIdToSNIdList)),
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

							if snIdToMemberInfo[id].IsAlive {
								snIdToMemberInfoLock.RLock()
								host := snIdToMemberInfo[id].Hostname
								port := snIdToMemberInfo[id].Port
								snIdToMemberInfoLock.RUnlock()

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
				storeToDisk("file_system_tree.gob", "file_system_tree")
				fileSystemTreeLock.Unlock()

				if err != nil {
					log.Println(err)
					resMsg = message.ControllerResponse{Error: err.Error()}
				}

				sendControllerResponseMsg(msgHandler, &resMsg)

			} else if msg.ClientReqMessage.Type == 2 { // DELETE

				fileSystemTreeLock.Lock()
				_, err := fileSystemTree.DeleteFile(directory)
				storeToDisk("file_system_tree.gob", "file_system_tree")
				fileSystemTreeLock.Unlock()

				resMsg := message.ControllerResponse{}
				if err != nil {
					// If the directory end with a file and not a directory
					// Or directory does not exist
					resMsg = message.ControllerResponse{Type: 2, Error: err.Error()}
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
			} else if msg.ClientReqMessage.Type == 4 { // USAGE
				snInfoList := new(message.StorageInfoList)

				snIdToMemberInfoLock.RLock()
				for _, val := range snIdToMemberInfo {
					snInfo := message.StorageInfo{
						Host:     val.Hostname,
						Port:     val.Port,
						IsAlive:  val.IsAlive,
						Requests: val.NumOfReq,
					}

					snInfoList.StorageInfo = append(snInfoList.StorageInfo, &snInfo)
				}
				snIdToMemberInfoLock.RUnlock()

				resMsg := message.ControllerResponse{NodeList: snInfoList, SpaceAvailable: totalSpaceAvail, Type: 4}
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

				snIdToMemberInfoLock.RLock()
				_, ok := snIdToMemberInfo[int(id)]
				snIdToMemberInfoLock.RUnlock()

				if !ok {
					registerSN(msg, false)
				}

				resMsg := message.Heartbeat{Id: 0}

				heartBeatHandler(int(id))
				sendHeartbeatMsg(msgHandler, &resMsg)
			}
		}
	}
}

func registerSN(msg *message.Wrapper_HbMessage, isNew bool) (int, bool) {
	hostAndPort := msg.HbMessage.GetHostAndPort()
	spaceAvail := msg.HbMessage.GetSpaceAvailable()
	numOfReq := msg.HbMessage.GetRequests()

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
		storeToDisk("sn_latest_id.gob", "sn_latest_id")
	} else {
		assignedId = int(msg.HbMessage.GetId())
	}

	sn := storageNode{host, int32(port), true, time.Now(),
		spaceAvail, numOfReq}

	// Register storage node
	snIdToMemberInfoLock.Lock()
	snIdToMemberInfo[assignedId] = &sn
	storeToDisk("sn_id_to_member_info.gob", "sn_id_to_member_info")
	snIdToMemberInfoLock.Unlock()

	snLocation[hostAndPort] = true

	snRegisterNewLock.Unlock()

	return assignedId, true
}

func heartBeatChecker(duration time.Duration) {
	for tick := range time.Tick(duration) {
		snIdToMemberInfoLock.RLock()
		var currTotalSpaceAvail int64 = 0
		for id, val := range snIdToMemberInfo {
			currTotalSpaceAvail += val.SpaceAvail
			if (val.LastHB.Add(duration)).Before(tick) {
				log.Printf("%d Is dead", id)
				val.IsAlive = false
			}
		}
		snIdToMemberInfoLock.RUnlock()

		totalSpaceAvail = currTotalSpaceAvail
	}
}

func heartBeatHandler(id int) {
	log.Printf("%d Receive heartbeat", id)

	currTime := time.Now()
	snIdToMemberInfoLock.Lock()
	snIdToMemberInfo[id].LastHB = currTime
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

func storeToDisk(outputFile string, dataName string) {
	// Create a file
	file, err := os.Create(outputFile)

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// Serialize the data
	dataEncoder := gob.NewEncoder(file)

	if dataName == "file_system_tree" {
		err = dataEncoder.Encode(fileSystemTree)
	} else if dataName == "sn_id_to_member_info" {
		err = dataEncoder.Encode(snIdToMemberInfo)
	} else if dataName == "sn_latest_id" {
		err = dataEncoder.Encode(snLatestId)
	}

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	err = file.Close()

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func getBackupFromDisk(fileName string) {
	// open data file
	file, err := os.Open(fileName)

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	dataDecoder := gob.NewDecoder(file)
	if fileName == "file_system_tree.gob" {
		err = dataDecoder.Decode(&fileSystemTree)
	} else if fileName == "sn_id_to_member_info.gob" {
		err = dataDecoder.Decode(&snIdToMemberInfo)
	} else if fileName == "sn_latest_id.gob" {
		err = dataDecoder.Decode(&snLatestId)
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	file.Close()
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

	// Get back up from disk
	if _, err := os.Stat("./file_system_tree.gob"); err == nil {
		getBackupFromDisk("file_system_tree.gob")
	}
	if _, err := os.Stat("./sn_id_to_member_info.gob"); err == nil {
		getBackupFromDisk("sn_id_to_member_info.gob")
	}
	if _, err := os.Stat("./sn_latest_id.gob"); err == nil {
		getBackupFromDisk("sn_latest_id.gob")
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
