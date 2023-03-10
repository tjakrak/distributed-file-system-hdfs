package unit_tests

/* go test ./... -v */

import (
	"fmt"
	"hdfs/data_structure"
	"os"
	"reflect"
	"testing"
)

func TestNodeAddChild(t *testing.T) {
	n1 := data_structure.NewNode("hello", "directory")
	n2 := data_structure.NewNode("world", "directory")
	n3 := data_structure.NewNode("!!!", "directory")

	n1.AddChild(n2)
	n1.AddChild(n3)

	children, _ := n1.GetChildren()
	actualLength := len(children)
	expectedLength := 2

	if actualLength != expectedLength {
		t.Errorf("actual %q, expected %q", actualLength, expectedLength)
	}
}

func TestNodeDeleteChild(t *testing.T) {
	n1 := data_structure.NewNode("hello", "directory")
	n2 := data_structure.NewNode("world", "directory")
	n3 := data_structure.NewNode("!!!", "directory")

	n1.AddChild(n2)
	n1.AddChild(n3)
	n1.DeleteChild("world")

	children, _ := n1.GetChildren()
	actualLength := len(children)
	expectedLength := 1

	if actualLength != expectedLength {
		t.Errorf("actual %q, expected %q", actualLength, expectedLength)
	}
}

func TestNodeChildExistTrue(t *testing.T) {
	n1 := data_structure.NewNode("hello", "directory")
	n2 := data_structure.NewNode("world", "directory")

	n1.AddChild(n2)

	actual := n1.IsChildExist("world")
	expected := true

	if actual != expected {
		t.Errorf("actual %t, expected %t", actual, expected)
	}
}

func TestNodeChildExistFalse(t *testing.T) {
	n1 := data_structure.NewNode("hello", "file")

	actual := n1.IsChildExist("world")
	expected := false

	if actual != expected {
		t.Errorf("actual %t, expected %t", actual, expected)
	}
}

func TestNodeAddChunk(t *testing.T) {
	n := data_structure.NewNode("hello", "file")
	chunkId := 1
	storageId := []int32{1, 2, 3}
	chunkIdToLocation := make(map[int][]int32)
	chunkIdToLocation[chunkId] = storageId

	n.AddChunks(chunkIdToLocation)

	actual, _ := n.GetChunks()
	fmt.Println(actual)

	if !reflect.DeepEqual(actual[1], storageId) {
		t.Errorf("actual %v, expected %v", actual, storageId)
	}
}

func TestPutFileSuccess(t *testing.T) {
	fileSystemTree := data_structure.NewFileSystemTree()
	err := fileSystemTree.PutFile("/hello/world.txt", nil)

	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}

	actual, _ := fileSystemTree.ShowFiles("/hello")
	expected := "world.txt"

	if actual[0] != expected {
		t.Errorf("actual %s, expected %s", actual, expected)
	}
}

func TestPutFileFail(t *testing.T) {
	fileSystemTree := data_structure.NewFileSystemTree()
	err := fileSystemTree.PutFile("/hello/world.txt", nil)
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}

	err = fileSystemTree.PutFile("/hello/world.txt", nil)
	fmt.Println(err)

	if err == nil {
		t.Errorf("File already exist, not allowed to have duplicate")
	}
}

func TestDeleteFileSuccess(t *testing.T) {
	fileSystemTree := data_structure.NewFileSystemTree()
	err := fileSystemTree.PutFile("/hello/world.txt", nil)
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}

	actual, _ := fileSystemTree.DeleteFile("/hello/world.txt")
	expected := true

	if actual != expected {
		t.Errorf("actual %t, expected %t", actual, expected)
	}
}

func TestGetFileSuccess(t *testing.T) {
	fileSystemTree := data_structure.NewFileSystemTree()
	snIdList := make(map[int][]int32)
	snIdList[0] = []int32{1, 2, 3}

	err := fileSystemTree.PutFile("/hello/world.txt", snIdList)
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}

	actual, err := fileSystemTree.GetFile("/hello/world.txt")
	expected := []int32{1, 2, 3}

	if !reflect.DeepEqual(actual[0], expected) {
		t.Errorf("actual %d, expected %d", actual, expected)
	}
}

func TestLsFileFail(t *testing.T) {
	fileSystemTree := data_structure.NewFileSystemTree()
	err := fileSystemTree.PutFile("/hello/world.txt", nil)

	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}

	_, err = fileSystemTree.ShowFiles("/hello/world.txt")
	fmt.Println(err)

	if err == nil {
		t.Errorf("Not a directory, should not show anything")
	}
}
