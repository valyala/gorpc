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
