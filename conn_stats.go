package gorpc

import (
	"io"
	"sync"
)

// ConnStats provides connection statistics. Applied to both gorpc.Client
// and gorpc.Server.
type ConnStats struct {
	// The number of rpc calls performed.
	RPCCalls uint64

	// The number of bytes written to the underlying connections.
	BytesWritten uint64

	// The number of bytes read from the underlying connections.
	BytesRead uint64

	// The number of Read() calls.
	ReadCalls uint64

	// The number of Read() errors.
	ReadErrors uint64

	// The number of Write() calls.
	WriteCalls uint64

	// The number of Write() errors.
	WriteErrors uint64

	// The number of Dial() calls.
	DialCalls uint64

	// The number of Dial() errors.
	DialErrors uint64

	// The number of Accept() calls.
	AcceptCalls uint64

	// The number of Accept() errors.
	AcceptErrors uint64

	// lock is for 386 builds. See https://github.com/valyala/gorpc/issues/5 .
	lock sync.Mutex
}

type writerCounter struct {
	w  io.Writer
	cs *ConnStats
}

type readerCounter struct {
	r  io.Reader
	cs *ConnStats
}

func newWriterCounter(w io.Writer, cs *ConnStats) io.Writer {
	return &writerCounter{
		w:  w,
		cs: cs,
	}
}

func newReaderCounter(r io.Reader, cs *ConnStats) io.Reader {
	return &readerCounter{
		r:  r,
		cs: cs,
	}
}

func (w *writerCounter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.cs.incWriteCalls()
	if err != nil {
		w.cs.incWriteErrors()
	}
	w.cs.addBytesWritten(uint64(n))
	return n, err
}

func (r *readerCounter) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.cs.incReadCalls()
	if err != nil {
		r.cs.incReadErrors()
	}
	r.cs.addBytesRead(uint64(n))
	return n, err
}
