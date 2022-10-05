package gorpc

import (
	"bytes"
	"sync"
)

var bufferPool = sync.Pool{
	New: func() any {
		return &Buffer{
			bytes.NewBuffer(make([]byte, 0)),
		}
	},
}

type Buffer struct {
	*bytes.Buffer
}

func (b *Buffer) Close() error {
	b.Reset()
	bufferPool.Put(b)
	return nil
}
