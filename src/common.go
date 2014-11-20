package gorpc

import (
	"encoding/gob"
	"log"
)

type LoggerFunc func(format string, args ...interface{})

var errorLogger = LoggerFunc(log.Printf)

func SetErrorLogger(f LoggerFunc) {
	errorLogger = f
}

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
