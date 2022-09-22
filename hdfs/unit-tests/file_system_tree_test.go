package unit_tests

/* go test ./... -v */

import (
	"hdfs/data_structure"
	"testing"
)

func TestAddChild(t *testing.T) {
	n1 := data_structure.New("hello", "directory")
	n2 := data_structure.New("world", "directory")
	n3 := data_structure.New("!!!", "directory")

	n1.AddChild(&n2)
	n1.AddChild(&n3)

	children, _ := n1.Children()
	actualLength := len(children)
	expectedLength := 2

	if actualLength != expectedLength {
		t.Errorf("actual %q, expected %q", actualLength, expectedLength)
	}
}

func TestDeleteChild(t *testing.T) {
	n1 := data_structure.New("hello", "directory")
	n2 := data_structure.New("world", "directory")
	n3 := data_structure.New("!!!", "directory")

	n1.AddChild(&n2)
	n1.AddChild(&n3)
	n1.DeleteChild("world")

	children, _ := n1.Children()
	actualLength := len(children)
	expectedLength := 1

	if actualLength != expectedLength {
		t.Errorf("actual %q, expected %q", actualLength, expectedLength)
	}
}

func TestChildExistTrue(t *testing.T) {
	n1 := data_structure.New("hello", "directory")
	n2 := data_structure.New("world", "directory")

	n1.AddChild(&n2)

	actual := n1.ChildExist("world")
	expected := true

	if actual != expected {
		t.Errorf("actual %t, expected %t", actual, expected)
	}
}

func TestChildExistFalse(t *testing.T) {
	n1 := data_structure.New("hello", "file")

	actual := n1.ChildExist("world")
	expected := false

	if actual != expected {
		t.Errorf("actual %t, expected %t", actual, expected)
	}
}
