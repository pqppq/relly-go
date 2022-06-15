package rellygo

import (
	"bytes"
	"os"
	"testing"
)

func TestBufPool(t *testing.T) {
	f, err := os.OpenFile(HEAP_FILE_PATH, os.O_RDWR, 0666)
	if err != nil {
		t.Error(err)
	}
	defer f.Close()
	dm, err := newDiskManager(f)
	if err != nil {
		t.Fatal(err)
	}
	ps := 1
	bp := newBufferPool(ps)
	m := newBufferPoolManager(dm, bp)

	// create b1 -> pool [b1] -> b1
	b1, err := m.createPage()
	if err != nil {
		t.Fatal("failed to create page")
	}
	p1 := b1.pageId
	b1.isDirty = true
	c1 := make([]byte, PAGE_SIZE)
	copy(c1, []byte("Content1"))
	copy(b1.page.data, c1)
	err = m.diskManager.writePage(b1.page, p1)
	if err != nil {
		t.Fatal("failed to write page data")
	}

	// pool [b1] -- fetch 0 --> b
	b, err := m.fetchPage(b1.pageId)
	if !bytes.Equal(b.page.data, c1) {
		t.Fatal("page data is not equal")
	}

	// take back b1
	// buffer pool manager can evict b1
	err = m.takeBackPage(b1)
	if err != nil {
		t.Fatal("failed to take back page")
	}

	// evict b1 ->  write b1 -> pool []
	// create b2 -> pool [b2]
	b2, err := m.createPage()
	if err != nil {
		t.Fatal("failed to create page")
	}
	p2 := b2.pageId
	c2 := make([]byte, PAGE_SIZE)
	copy(c2, []byte("Content1"))
	copy(b2.page.data, c2)
	b2.isDirty = true
	err = m.diskManager.writePage(b2.page, p2)
	b, err = m.fetchPage(b2.pageId)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b.page.data, c2) {
		t.Fatal("should be failed to fetch page")
	}

	// b2 is still referenced
	// buffer pool manager fails to evict b2
	b, err = m.fetchPage(p1)
	if err == nil {
		t.Fatal(err)
	}

	// take back b2
	// buffer pool manager can evict b2
	m.takeBackPage(b2)
	b, err = m.fetchPage(p1)
	if err != nil {
		t.Fatal("failed to fetch page")
	}
}
