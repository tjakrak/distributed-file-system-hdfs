package main

/* Source: https://socketloop.com/tutorials/golang-how-to-split-or-chunking-a-file-to-smaller-pieces */

import (
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
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
	var fileSize int64 = fileInfo.Size()

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

}
