package rellygo

import (
	"bytes"
	"os"
	"testing"
)

const HEAP_FILE_PATH = "../heap"

func TestDisk(t *testing.T) {

	f, err := os.OpenFile(HEAP_FILE_PATH, os.O_RDWR, 0666)
	defer f.Close()
	if err != nil {
		t.Fatal(err)
	}
	d, err := newDiskManager(f)
	if err != nil {
		t.Error(err)
	}
	pgid := PageId(0)
	pw := newPage()
	copy(pw.data, []byte("Hello, world!"))
	err = d.writePage(pw, pgid)
	if err != nil {
		t.Error(err)
	}

	pr := newPage()
	err = d.readPage(pr, pgid)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(pw.data, pr.data) {
		t.Fatal("Data not equal")
	}
}
