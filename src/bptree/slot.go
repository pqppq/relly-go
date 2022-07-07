package bplustree

import (
	"errors"
	"unsafe"
)

type Slot struct {
	header *slotHeader
	body   []byte
	// body
	//                   <---> free space
	// |pointer1|pointer2|...|body2|body1|
	//                       ^ offset
}

type slotHeader struct {
	numRecords uint16
	offset     uint16
}

type pointer struct {
	offset uint16
	length uint16
}

const pointerSize = int(unsafe.Sizeof(pointer{}))

func newSlot(body []byte) (*Slot, error) {
	s := Slot{}
	headerSize := int(unsafe.Sizeof(*s.header))
	if headerSize+1 > len(body) {
		return nil, errors.New("header must be aligned.")
	}

	s.header = (*slotHeader)(unsafe.Pointer(&body[0]))
	s.body = body[headerSize:]
	return &s, nil
}

func (s *Slot) read(index int) []byte {
	return s.data(s.pointers()[index])
}

func (s *Slot) write(index int, buf []byte) {
	data := s.read(index)
	copy(data, buf)
}

func (s *Slot) capacity() int {
	return len(s.body) - pointerSize
}

func (s *Slot) numRecords() int {
	return int(s.header.numRecords)
}

func (s *Slot) freeSpase() int {
	n := s.numRecords()
	return int(s.header.offset) - pointerSize*n
}

func (s *Slot) pointers() []*pointer {
	n := s.numRecords()
	ps := make([]*pointer, n)

	for i := 0; i < n; i++ {
		offset := i * pointerSize
		ps[i] = (*pointer)(unsafe.Pointer(&s.body[offset]))
	}
	return ps
}

func (s *Slot) data(p *pointer) []byte {
	offset, length := p.offset, p.length
	return s.body[offset : offset+length]
}

func (s *Slot) Init() {
	s.header.numRecords = 0
	s.header.offset = uint16(len(s.body))
}

func (s *Slot) Insert(index int, buf []byte) error {
	length := len(buf)
	if s.freeSpase() < pointerSize+length {
		return errors.New("cannot inesert: no free space in slot body.")
	}

	n := s.numRecords()
	s.header.numRecords++
	s.header.offset -= uint16(length)

	pointers := s.pointers()
	for i := n - 1; i >= index; i-- {
		*pointers[i+1] = *pointers[i]
	}

	p := pointers[index]
	p.offset = s.header.offset
	p.length = uint16(length)

	s.write(index, buf)
	return nil
}

func (s *Slot) resize(index int, newLength int) error {
	pointers := s.pointers()
	length := int(pointers[index].length)
	diff := newLength - length

	if diff == 0 {
		return nil
	}

	if diff > s.freeSpase() {
		return errors.New("cannot resize: no free space in slot body.")
	}

	//                            v pointer offset
	// |...|pointer|...|___________|...|body|...|
	//                             ^ body offset
	//                             |
	//                        <----+ diff
	// |...|pointer|...|______|...|_____body|...|
	//                        ^ new body offset

	bodyOffset := s.header.offset
	pointerOffset := pointers[index].offset

	shiftStart := int(bodyOffset)
	shiftEnd := int(pointerOffset)

	newBodyOffset := int(bodyOffset) - diff
	s.header.offset = uint16(newBodyOffset)

	buf := make([]byte, shiftEnd-shiftStart)
	copy(buf, s.body[shiftStart:shiftEnd])
	copy(s.body[newBodyOffset:], buf)

	for _, pointer := range pointers {
		if pointer.offset <= pointerOffset {
			pointer.offset = uint16(int(pointer.offset) - diff)
		}
	}

	pointer := pointers[index]
	pointer.length = uint16(newLength)
	if newLength == 0 {
		pointer.offset = uint16(newBodyOffset)
	}
	return nil

}

func (s *Slot) Remove(index int) {
	s.resize(index, 0)
	pointers := s.pointers()
	for i := index + 1; i < s.numRecords(); i++ {
		*pointers[i-1] = *pointers[i]
	}
	s.header.numRecords--
}
