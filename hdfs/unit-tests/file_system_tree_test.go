package unit_tests

/* go test ./... -v */

import (
	"hdfs/storage"
	"testing"
)

func TestAddChild(t *testing.T) {
	n1 := storage.New("hello")
	n2 := storage.New("world")
	n3 := storage.New("!!!")

	n1.AddChild(&n2)
	n1.AddChild(&n3)
	actualLength := len(n1.Children())
	expectedLength := 2

	if actualLength != expectedLength {
		t.Errorf("actual %q, expected %q", actualLength, expectedLength)
	}
}

func TestDeleteChild(t *testing.T) {
	n1 := storage.New("hello")
	n2 := storage.New("world")
	n3 := storage.New("!!!")

	n1.AddChild(&n2)
	n1.AddChild(&n3)
	n1.DeleteChild("world")
	actualLength := len(n1.Children())
	expectedLength := 1

	if actualLength != expectedLength {
		t.Errorf("actual %q, expected %q", actualLength, expectedLength)
	}
}

func TestChildExist(t *testing.T) {
	n1 := storage.New("hello")
	n2 := storage.New("world")

	n1.AddChild(&n2)

	actual := n1.ChildExist("world")
	expected := true

	if actual != expected {
		t.Errorf("actual %t, expected %t", actual, expected)
	}
}
