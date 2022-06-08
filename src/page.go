package rellygo

import (
	"bytes"
	"math"
)

type PageId uint64
type Page struct {
	buffer bytes.Buffer
	pageId PageId
}

const PAGE_SIZE = 4096
const INVALID_PAGE_ID PageId = math.MaxUint64

func New(n uint64) *Page {
	p := new(Page)
	if PageId(n) != INVALID_PAGE_ID {
		p.pageId = PageId(n)
	} else {
		p.pageId = INVALID_PAGE_ID
	}
	buf := make([]byte, PAGE_SIZE)
	p.buffer = *bytes.NewBuffer(buf)

	return p
}
