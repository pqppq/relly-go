package bptree

type Key interface {
	// other key is
	// -1: less than
	//  0: equal
	//  1: greater than
	// this key
	Compare(other Key) int
}

type Value interface{}

const NODE_TYPE_BRANCH = "BRANCH"
const NODE_TYPE_LEAF = "LEAF"

type Node interface {
	Type() string
	Insert(pair Pair) error
	Split() (Key, Node, Node)
}
