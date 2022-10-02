package data_structure

import (
	"errors"
	"strings"
)

type FileSystemTree struct {
	root *node
}

var f = func(c rune) bool {
	return c == '/'
}

func NewFileSystemTree() *FileSystemTree {
	tree := FileSystemTree{NewNode("/", "directory")}

	return &tree
}

func (fst *FileSystemTree) GetFile(filePath string) (bool, error) {
	filePathArr := strings.FieldsFunc(filePath, f)
	currFile := fst.root
	totalFiles := len(filePathArr)

	// Iterating file path
	for i, file := range filePathArr {
		if !currFile.IsChildExist(file) {
			return false, errors.New("No such file or directories: " + file)
		} else {
			// Get if we reach the destination file
			if i >= totalFiles-1 {
				currFile.DeleteChild(file)
			} else {
				temp := currFile.GetChild(file)

				if temp.GetFileType() == "file" {
					return false, errors.New("Not a directory: " + file)
				} else {
					currFile = temp
				}
			}
		}
	}

	return true, nil
}

func (fst *FileSystemTree) PutFile(filePath string, chunkIdToLocation map[int][]int32) (*node, error) {
	filePathArr := strings.FieldsFunc(filePath, f)
	currFile := fst.root
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
				return nil, errors.New("File already exist: " + currFile.GetChild(file).GetName())
			}
		}
	}

	return currFile, nil
}

func (fst *FileSystemTree) DeleteFile(filePath string) (bool, error) {
	filePathArr := strings.FieldsFunc(filePath, f)
	currFile := fst.root
	totalFiles := len(filePathArr)

	// Iterating file path
	for i, file := range filePathArr {
		if !currFile.IsChildExist(file) {
			return false, errors.New("No such file or directories: " + file)
		} else {
			// Delete if we reach the destination file
			if i >= totalFiles-1 {
				currFile.DeleteChild(file)
			} else {
				temp := currFile.GetChild(file)

				if temp.GetFileType() == "file" {
					return false, errors.New("Not a directory: " + file)
				} else {
					currFile = temp
				}
			}
		}
	}

	return true, nil
}

func (fst *FileSystemTree) ShowFiles(filePath string) ([]string, error) {
	filePathArr := strings.FieldsFunc(filePath, f)
	currFile := fst.root

	for _, file := range filePathArr {
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

	children, _ := currFile.GetChildren()
	fileList := make([]string, 0)
	for fileName, _ := range children {
		fileList = append(fileList, fileName)
	}

	return fileList, nil
}

type node struct {
	name     string
	fileType string          // filetype to indicate directory or a file
	chunks   map[int][]int32 // key: chunkId | val: will be node id
	children map[string]*node
}

func NewNode(name string, fileType string) *node {
	n := node{name, fileType, nil, nil}

	if fileType == "directory" {
		n.children = make(map[string]*node)
	}

	return &n
}

func (n *node) GetName() string {
	return n.name
}

func (n *node) GetFileType() string {
	return n.fileType
}

func (n *node) GetChunk() (map[int][]int32, error) {
	if n.fileType == "directory" {
		return nil, errors.New("Not a file: " + n.GetName())
	}

	return n.chunks, nil
}

func (n *node) GetChild(childName string) *node {
	return n.children[childName]
}

func (n *node) GetChildren() (map[string]*node, error) {
	if n.fileType == "file" {
		return nil, errors.New("Not a directory: " + n.GetName())
	}

	return n.children, nil
}

func (n *node) AddChunks(chunks map[int][]int32) {
	n.chunks = chunks
}

func (n *node) AddChild(child *node) {
	n.children[child.name] = child
}

func (n *node) DeleteChild(childName string) {
	delete(n.children, childName)
}

func (n *node) IsChildExist(childName string) bool {
	if _, ok := n.children[childName]; ok {
		return true
	}

	return false
}
