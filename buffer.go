package gorpc

import (
	"io"
	"sync"
)

var bufferPool sync.Pool

func init() {
	bufferPool = sync.Pool{
		New: func() any {
			return &Buffer{
				slice: make([]byte, 0),
				pool:  &bufferPool,
			}
		},
	}
}

type Buffer struct {
	cursor int
	slice  []byte
	pool   *sync.Pool
}

func (b *Buffer) Size() int {
	return len(b.slice)
}

func (b *Buffer) Bytes() []byte {
	return b.slice
}

func (b *Buffer) Clear() {
	b.slice = b.slice[:0]
}

func (b *Buffer) Reserve(size int) {
	if cap(b.slice) < size {
		b.slice = make([]byte, size)
	}
	if len(b.slice) < size {
		for i := len(b.slice); i < size; i++ {
			b.slice = append(b.slice, 0)
		}
	}
	b.slice = b.slice[:size]
}

func (b *Buffer) Read(p []byte) (n int, err error) {
	n = copy(p, b.slice[b.cursor:])
	if n == 0 {
		return n, io.EOF
	}
	b.cursor += n
	return
}

func (b *Buffer) Close() error {
	b.pool.Put(b)
	return nil
}
