package rellygo

import (
	"os"
)

type DiskManager struct {
	file       *os.File
	nextPageId PageId
}

func newDiskManager(file *os.File) (*DiskManager, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	next := PageId(size / PAGE_SIZE)
	return &DiskManager{file, next}, nil
}

func (m *DiskManager) readPage(page *Page, pageId PageId) error {
	offset := int64(PAGE_SIZE * pageId)
	_, err := m.file.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = m.file.Read(page.data)
	if err != nil {
		return err
	}
	return nil
}

func (m *DiskManager) writePage(page *Page, pageId PageId) error {
	offset := int64(PAGE_SIZE * pageId)
	_, err := m.file.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = m.file.Write(page.data)
	if err != nil {
		return err
	}
	return nil
}

func (m *DiskManager) allocPage() PageId {
	pgid := m.nextPageId
	m.nextPageId++
	return pgid
}

func (m *DiskManager) sync() error {
	return m.file.Sync()
}
