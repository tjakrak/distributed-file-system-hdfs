package storage

type node struct {
	name     string
	children map[string]*node
}

func New(name string) node {
	n := node{name, make(map[string]*node)}
	return n
}

func (n node) Name() string {
	return n.name
}

func (n node) Children() map[string]*node {
	return n.children
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
