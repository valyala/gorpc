package gorpc

import (
	"encoding/gob"
	"io"
	"log"
	"sync/atomic"
)

// LoggerFunc is an error logging function to pass to gorpc.SetErrorLogger().
type LoggerFunc func(format string, args ...interface{})

var errorLogger = LoggerFunc(log.Printf)

// SetErrorLogger sets the given error logger to use in gorpc.
//
// By default log.Printf is used for error logging.
func SetErrorLogger(f LoggerFunc) {
	errorLogger = f
}

// RegisterType registers the given type to send via rpc.
//
// The client must register all the response types the server may send.
// The server must register all the request types the client may send.
//
// There is no need in registering base Go types such as int, string, bool,
// float64, etc. or arrays, slices and maps containing base Go types.
func RegisterType(x interface{}) {
	gob.Register(x)
}

type wireMessage struct {
	ID   uint64
	Data interface{}
}

func logError(format string, args ...interface{}) {
	errorLogger(format, args...)
}

// ConnStats provides connection statistics. Applied to both gorpc.Client
// and gorpc.Server.
type ConnStats struct {
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
}

type writerCounter struct {
	w            io.Writer
	bytesWritten *uint64
	writeCalls   *uint64
	writeErrors  *uint64
}

type readerCounter struct {
	r          io.Reader
	bytesRead  *uint64
	readCalls  *uint64
	readErrors *uint64
}

func newWriterCounter(w io.Writer, s *ConnStats) io.Writer {
	return &writerCounter{
		w:            w,
		bytesWritten: &s.BytesWritten,
		writeCalls:   &s.WriteCalls,
		writeErrors:  &s.WriteErrors,
	}
}

func newReaderCounter(r io.Reader, s *ConnStats) io.Reader {
	return &readerCounter{
		r:          r,
		bytesRead:  &s.BytesRead,
		readCalls:  &s.ReadCalls,
		readErrors: &s.ReadErrors,
	}
}

func (w *writerCounter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	atomic.AddUint64(w.writeCalls, 1)
	if err != nil {
		atomic.AddUint64(w.writeErrors, 1)
	}
	atomic.AddUint64(w.bytesWritten, uint64(n))
	return n, err
}

func (r *readerCounter) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	atomic.AddUint64(r.readCalls, 1)
	if err != nil {
		atomic.AddUint64(r.readErrors, 1)
	}
	atomic.AddUint64(r.bytesRead, uint64(n))
	return n, err
}
