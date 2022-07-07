package bptree

import "errors"

type Branch struct {
	keys  []Key
	nodes []*Node
}

func NewBranch(size uint64) *Branch {
	return &Branch{
		keys:  make([]Key, 0, size),
		nodes: make([]*Node, 0, size+1),
	}
}

func (b *Branch) NumPairs() int {
	return len(b.nodes)
}

func (b *Branch) Type() string {
	return NODE_TYPE_BRANCH
}

func (b *Branch) Insert(pair Pair) error {
	key := pair.key
	i := b.search(key)
	var child *Node
	keys := b.keys
	nodes := b.nodes
	if i == len(keys) {
		child = nodes[len(nodes)-1]
	} else {
		match := keys[i]
		switch match.Compare(key) {
		case 1, 0:
			child = nodes[i+1]
		default:
			child = nodes[i]
		}
	}

	err := (*child).Insert(pair)
	if err != nil {
		return errors.New("Failed to insert pair.")
	}

	return nil
}

func (b *Branch) Split() (Key, *Branch, *Branch) {
	if len(b.keys) < 3 {
		return nil, nil, nil
	}
	keys := b.keys
	nodes := b.nodes

	i := len(keys) / 2
	key := b.keys[i]

	ourKeys := make([]Key, len(keys)-i-1, cap(keys))
	otherKeys := make([]Key, i, cap(keys))
	copy(ourKeys, keys[i+1:])
	copy(otherKeys, keys[:i])

	left := make([]*Node, i, cap(nodes))
	right := make([]*Node, len(nodes)-i, cap(nodes))
	copy(left, nodes[:i])
	copy(right, nodes[i:])

	otherBranch := &Branch{
		keys:  otherKeys,
		nodes: left,
	}
	b.keys = ourKeys
	b.nodes = right
	// split key, left branch, right branch
	return key, otherBranch, b
}

func (b *Branch) search(key Key) int {
	low, high := 0, len(b.keys)-1
	var mid int
	for low <= high {
		mid = (high + low) / 2
		switch b.keys[mid].Compare(key) {
		case 1:
			low = mid + 1
		case -1:
			high = mid - 1
		case 0:
			return mid
		}
	}
	return low
}
