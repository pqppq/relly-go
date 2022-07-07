package bptree

import (
	"bytes"
	"encoding/gob"
	"errors"
)

type Leaf struct {
	next *Leaf
	prev *Leaf
	slot *Slot
}

type Pair struct {
	key   Key
	value Value
}

func NewLeaf(body []byte) (*Leaf, error) {
	slot, err := newSlot(body)
	if err != nil {
		return nil, errors.New("failed to crete Leaf node.")
	}

	Leaf := &Leaf{}
	Leaf.slot = slot
	return Leaf, nil
}

func (l *Leaf) SetNext(next *Leaf) {
	l.next = next
	return
}

func (l *Leaf) SetPrev(prev *Leaf) {
	l.prev = prev
	return
}

func (l *Leaf) Type() string {
	return NODE_TYPE_LEAF
}

func (l *Leaf) Insert(pair Pair) error {
	buf := bytes.NewBuffer(nil)
	err := gob.NewEncoder(buf).Encode(pair)
	if err != nil {
		return err
	}

	b := buf.Bytes()
	if l.slot.freeSpase() < len(b) {
		return errors.New("No free space to insert.")
	}

	index := l.search(pair.key)
	if l.PairAt(index).key == pair.key {
		return errors.New("Key must be unique.")
	}

	l.slot.Insert(index, b)
	return nil
}

func (l *Leaf) Split() (Key, *Leaf, *Leaf) {
	n := int(l.slot.header.numRecords / 2)
	b := make([]byte, len(l.slot.body))

	p := l.PairAt(n)
	k := p.key

	newLeaf, err := NewLeaf(b)
	if err != nil {
		return nil, nil, nil
	}
	// transfer data
	for i := 0; i < n; i++ {
		pi := l.PairAt(0)
		_ = newLeaf.Insert(pi)
		l.slot.Remove(0)
	}

	return k, newLeaf, l
}

func (l *Leaf) numRecords() int {
	return l.slot.numRecords()
}

func (l *Leaf) isHalfFull() bool {
	return 2*l.slot.freeSpase() < l.slot.capacity()
}

func (l *Leaf) PairAt(index int) Pair {
	var p Pair
	b := l.slot.read(index)
	buf := bytes.NewBuffer(b)
	_ = gob.NewDecoder(buf).Decode(&p)
	return p
}

func (l *Leaf) search(key Key) int {
	low, high := 0, l.slot.numRecords()-1
	var mid int
	for low <= high {
		mid = (low + high) / 2
		km := l.PairAt(mid).key
		switch km.Compare(key) {
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
