package gorpc

import (
	"encoding/gob"
	"log"
	"time"
)

const (
	// DefaultRequestTimeout is the default timeout for client request.
	DefaultRequestTimeout = 20 * time.Second

	// DefaultPendingMessages is the default number of pending messages
	// handled by Client and Server.
	DefaultPendingMessages = 32 * 1024

	// DefaultFlushDelay is the default delay between message flushes
	// on Client and Server.
	DefaultFlushDelay = 5 * time.Millisecond

	// DefaultBufferSize is the default size for Client and Server buffers.
	DefaultBufferSize = 64 * 1024
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
