package data_structure

import (
	"errors"
	"strings"
)

var f = func(c rune) bool {
	return c == '/'
}

// FileSystemTree a data structure to store the structure of file tree in hdfs
type FileSystemTree struct {
	Root *node
}

// NewFileSystemTree is a constructor for FileSystemTree
func NewFileSystemTree() *FileSystemTree {
	tree := FileSystemTree{NewNode("/", "directory")}

	return &tree
}

// GetFile getter for FileSystemTree
func (fst *FileSystemTree) GetFile(filePath string) (map[int][]int32, error) {
	filePathArr := strings.FieldsFunc(filePath, f)
	currFile := fst.Root
	totalFiles := len(filePathArr)
	var chunks map[int][]int32

	// Iterating file path
	for i, file := range filePathArr {
		if !currFile.IsChildExist(file) {
			return nil, errors.New("No such file or directories: " + file)
		} else {
			// Get if we reach the destination file
			if i >= totalFiles-1 {
				currFile = currFile.GetChild(file)
				chunks, _ = currFile.GetChunks()
			} else {
				temp := currFile.GetChild(file)

				if temp.GetFileType() == "file" {
					return nil, errors.New("Not a directory: " + file)
				} else {
					currFile = temp
				}
			}
		}
	}

	return chunks, nil
}

// PutFile navigate and create file and folders base on the filepath
func (fst *FileSystemTree) PutFile(filePath string, chunkIdToLocation map[int][]int32) error {
	filePathArr := strings.FieldsFunc(filePath, f)
	currFile := fst.Root
	totalFiles := len(filePathArr)

	// Iterating file path
	for i, file := range filePathArr {

		// If file or folder not exist yet, create one
		if !currFile.IsChildExist(file) {

			var n *node
			if i < (totalFiles - 1) {
				n = NewNode(file, "directory")
			} else {
				n = NewNode(file, "file")
				n.AddChunks(chunkIdToLocation)
			}

			currFile.AddChild(n)
			currFile = n
		} else {

			if i < (totalFiles - 1) {
				currFile = currFile.GetChild(file)
			} else {
				return errors.New("File already exist: " + currFile.GetChild(file).GetName())
			}
		}
	}

	return nil
}

// DeleteFile delete a file from the file system tree
func (fst *FileSystemTree) DeleteFile(filePath string) (bool, error) {
	filePathArr := strings.FieldsFunc(filePath, f)
	currFile := fst.Root
	totalFiles := len(filePathArr)

	// Iterating file path
	for i, file := range filePathArr {
		if !currFile.IsChildExist(file) {
			return false, errors.New("No such file or directories: " + file)
		} else {

			if i >= totalFiles-1 { // If we reach the destination file
				currFile.DeleteChild(file)
			} else { // If we are still encounter a directory instead of a file
				temp := currFile.GetChild(file)

				if temp.GetFileType() == "file" {
					return false, errors.New("Not a directory: " + file)
				} else {
					currFile = temp // Move to the child
				}
			}
		}
	}

	return true, nil
}

func (fst *FileSystemTree) ShowFiles(filePath string) ([]string, error) {
	filePathArr := strings.FieldsFunc(filePath, f)
	currFile := fst.Root

	// Iterating through files in file path
	for _, file := range filePathArr {
		// Checking if file path exist
		if !currFile.IsChildExist(file) {
			return nil, errors.New("No such file or directories: " + file)
		} else {
			temp := currFile.GetChild(file)

			if temp.GetFileType() == "file" {
				return nil, errors.New("Not a directory: " + file)
			} else {
				currFile = temp
			}
		}
	}

	// Getting all the files name from the file path and send response to the client
	children, _ := currFile.GetChildren()
	fileList := make([]string, 0)
	for fileName, _ := range children {
		fileList = append(fileList, fileName)
	}

	return fileList, nil
}

type node struct {
	Name     string
	FileType string          // filetype to indicate directory or a file
	Chunks   map[int][]int32 // key: chunkId | val: will be node id
	Children map[string]*node
}

// NewNode constructor for node
func NewNode(name string, fileType string) *node {
	n := node{name, fileType, nil, nil}

	if fileType == "directory" {
		n.Children = make(map[string]*node)
	}

	return &n
}

func (n *node) GetName() string {
	return n.Name
}

func (n *node) GetFileType() string {
	return n.FileType
}

func (n *node) GetChunks() (map[int][]int32, error) {
	if n.FileType == "directory" {
		return nil, errors.New("Not a file: " + n.GetName())
	}

	return n.Chunks, nil
}

func (n *node) GetChild(childName string) *node {
	return n.Children[childName]
}

func (n *node) GetChildren() (map[string]*node, error) {
	if n.FileType == "file" {
		return nil, errors.New("Not a directory: " + n.GetName())
	}

	return n.Children, nil
}

func (n *node) AddChunks(chunks map[int][]int32) {
	n.Chunks = chunks
}

func (n *node) AddChild(child *node) {
	n.Children[child.Name] = child
}

func (n *node) DeleteChild(childName string) {
	delete(n.Children, childName)
}

func (n *node) IsChildExist(childName string) bool {
	if _, ok := n.Children[childName]; ok {
		return true
	}

	return false
}
