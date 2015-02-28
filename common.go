package gorpc

import (
	"encoding/gob"
	"log"
)

type LoggerFunc func(format string, args ...interface{})

var errorLogger = LoggerFunc(log.Printf)

// Use the given error logger in gorpc.
func SetErrorLogger(f LoggerFunc) {
	errorLogger = f
}

// Registers the given type to send via rpc.
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
