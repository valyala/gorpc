package gorpc

import (
	"encoding/gob"
	"log"
	"net"
	"time"
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

func setupKeepalive(conn net.Conn) error {
	tcpConn := conn.(*net.TCPConn)
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return err
	}
	if err := tcpConn.SetKeepAlivePeriod(10 * time.Second); err != nil {
		return err
	}
	return nil
}
