package bptree

import (
	"bytes"
	"testing"
)

func TestSlot(t *testing.T) {
	size := 1024
	body := make([]byte, size)

	slot, err := newSlot(body)
	if err != nil {
		t.Fatal("failed to create slot.")
	}
	slot.Init()

	slot.Insert(slot.numRecords(), []byte("A"))
	slot.Insert(slot.numRecords(), []byte("B"))
	slot.Insert(slot.numRecords(), []byte("C"))
	// slot |pA|pB|pC|...|A|B|C|

	A := slot.read(0)
	B := slot.read(1)
	C := slot.read(2)

	if slot.numRecords() != 3 {
		t.Fatal("wrong record number.")
	}

	if !bytes.Equal(A, []byte("A")) ||
		!bytes.Equal(B, []byte("B")) ||
		!bytes.Equal(C, []byte("C")) {
		t.Fatal("read data is not that expected.")
	}

	slot.Remove(1)
	// slot |pA|pC|...|A|C|

	if slot.numRecords() != 2 {
		t.Fatal("wrong record number.")
	}

	A = slot.read(0)
	C = slot.read(1)
	if !bytes.Equal(A, []byte("A")) ||
		!bytes.Equal(C, []byte("C")) {
		t.Fatal("read data is not that expected.")
	}

	slot.Insert(1, []byte("D"))
	// slot |pA|pD|pC|...|A|D|C|

	if slot.numRecords() != 3 {
		t.Fatal("wrong record number.")
	}

	A = slot.read(0)
	D := slot.read(1)
	C = slot.read(2)

	if !bytes.Equal(A, []byte("A")) ||
		!bytes.Equal(D, []byte("D")) ||
		!bytes.Equal(C, []byte("C")) {
		t.Fatal("read data is not that expected.")
	}
}
