package rellygo

import (
	"errors"
)

type BufferId int

type Buffer struct {
	isDirty bool
	pageId  PageId
	page    *Page
}

type Frame struct {
	buffer     Buffer
	refCount   int
	usageCount int
}

type BufferPool struct {
	buffers      []Frame
	nextVictimId BufferId
}

func newBufferPool(poolSize int) *BufferPool {
	buffers := make([]Frame, poolSize)
	for i := 0; i < poolSize; i++ {
		buffers[i].buffer = Buffer{
			page:    newPage(),
			pageId:  INVALID_PAGE_ID,
			isDirty: false}
	}
	return &BufferPool{buffers: buffers, nextVictimId: 0}
}

func (bp *BufferPool) incrementVictimId() {
	next := (int(bp.nextVictimId) + 1) % bp.size()
	bp.nextVictimId = BufferId(next)
}

func (bp *BufferPool) size() int {
	return len(bp.buffers)
}

func (bp *BufferPool) evict() (bid BufferId, err error) {
	poolSize := bp.size()
	count := 0
	next := bp.nextVictimId

	for {
		frame := &bp.buffers[next]
		if frame.usageCount == 0 {
			bid = next
			break
		}
		if frame.refCount == 0 {
			frame.usageCount--
			count = 0
		} else {
			count++
			if count >= poolSize {
				return -1, errors.New("Failed to evict Buffer: No Free Buffer to evice.")
			}
		}
		bp.incrementVictimId()
	}
	return bid, nil
}

type BufferPoolManager struct {
	diskManager *DiskManager
	pool        *BufferPool
	pageTable   map[PageId]BufferId
}

func newBufferPoolManager(d *DiskManager, bp *BufferPool) *BufferPoolManager {
	return &BufferPoolManager{
		d,
		bp,
		map[PageId]BufferId{},
	}
}

func (m *BufferPoolManager) fetchPage(pgid PageId) (b *Buffer, err error) {
	bid, ok := m.pageTable[pgid]
	if ok {
		f := &m.pool.buffers[bid]
		f.usageCount++
		f.refCount++
		return &f.buffer, nil
	}
	bid, err = m.pool.evict()
	if err != nil {
		return nil, err
	}
	f := &m.pool.buffers[bid]
	vb := &f.buffer
	vpgid := vb.pageId

	if vpgid != INVALID_PAGE_ID {
		delete(m.pageTable, vpgid)
		if vb.isDirty {
			// write page data before eviction
			err = m.diskManager.writePage(vb.page, vpgid)
			if err != nil {
				return nil, err
			}
		}
	}
	if vpgid != INVALID_PAGE_ID {
	}
	// set new page
	b = &Buffer{
		page:    newPage(),
		pageId:  pgid,
		isDirty: false}
	err = m.diskManager.readPage(b.page, pgid)
	if err != nil {
		return nil, err
	}
	f.usageCount = 1
	f.refCount = 1
	m.pageTable[pgid] = bid
	return b, nil
}

func (m *BufferPoolManager) createPage() (b *Buffer, err error) {
	bid, err := m.pool.evict()
	if err != nil {
		return nil, err
	}

	f := &m.pool.buffers[bid]
	vpgid := f.buffer.pageId
	vb := &f.buffer
	if vpgid != INVALID_PAGE_ID && vb.isDirty {
		// write page data before eviction
		err = m.diskManager.writePage(vb.page, vpgid)
		if err != nil {
			return nil, err
		}
		delete(m.pageTable, vpgid)
	}

	pid := m.diskManager.allocPage()

	f.buffer = Buffer{
		isDirty: false,
		pageId:  pid,
		page:    newPage()}
	m.pageTable[pid] = bid
	return &f.buffer, nil
}

func (m *BufferPoolManager) takeBackPage(b *Buffer) error {
	bid, ok := m.pageTable[b.pageId]
	if !ok {
		return errors.New("Page not exist in table")
	}
	f := &m.pool.buffers[bid]
	if f.refCount == 0 {
		// Nothing to do
	}
	f.refCount--
	return nil
}

func (m *BufferPoolManager) flush() error {
	for _, bid := range m.pageTable {
		f := &m.pool.buffers[bid]
		b := f.buffer
		err := m.diskManager.writePage(b.page, b.pageId)
		if err != nil {
			return err
		}
		f.buffer.isDirty = false
	}
	m.diskManager.sync()
	return nil
}
