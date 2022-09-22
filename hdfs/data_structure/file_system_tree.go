package data_structure

import "errors"

type node struct {
	name     string
	fileType string           // filetype to indicate directory or a file
	chunk    map[string][]int // int will be node id
	children map[string]*node
}

func New(name string, fileType string) node {
	n := node{name, fileType, nil, nil}

	if fileType == "file" {
		n.chunk = make(map[string][]int)
	} else if fileType == "directory" {
		n.children = make(map[string]*node)
	}

	return n
}

func (n node) Name() string {
	return n.name
}

func (n node) Chunk() (map[string][]int, error) {
	if n.fileType == "directory" {
		return nil, errors.New("Not a file: " + n.Name())
	}

	return n.chunk, nil
}

func (n node) AddChunk(chunkId string) {
	n.chunk[chunkId] = nil
}

func (n node) Children() (map[string]*node, error) {
	if n.fileType == "file" {
		return nil, errors.New("Not a directory: " + n.Name())
	}

	return n.children, nil
}

func (n node) AddChild(child *node) {
	n.children[child.name] = child
}

func (n node) DeleteChild(childName string) {
	delete(n.children, childName)
}

func (n node) ChildExist(childName string) bool {
	if _, ok := n.children[childName]; ok {
		return true
	}

	return false
}
