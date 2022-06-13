package rellygo

import (
	"math"
)

type PageId uint64

const PAGE_SIZE = 4096
const INVALID_PAGE_ID = PageId(math.MaxUint64)

func (id *PageId) Valid() bool {
	return *id != INVALID_PAGE_ID
}

type Page struct {
	data []byte
}

func newPage() *Page {
	p := new(Page)
	p.data = make([]byte, PAGE_SIZE)
	return p
}
