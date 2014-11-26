package gorpc

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"runtime"
	"time"
)

type HandlerFunc func(request interface{}) (response interface{})

type Server struct {
	Addr    string
	Handler HandlerFunc
}

func (s *Server) Serve() error {
	return serverHandler(s)
}

func serverHandler(s *Server) error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		logError("rpc.Server: [%s]. Cannot listen to: [%s]", s.Addr, err)
		return fmt.Errorf("rpc.Server: [%s]. Cannot listen to: [%s]", s.Addr, err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			logError("rpc.Server: [%s]. Cannot accept new connection: [%s]", s.Addr, err)
			time.Sleep(time.Second)
			continue
		}
		go serverHandleConnection(s, conn)
	}
}

func serverHandleConnection(s *Server, conn net.Conn) {
	responsesChan := make(chan *serverMessage, 1024)
	stopChan := make(chan struct{})

	readerDone := make(chan struct{}, 1)
	go serverReader(s, conn, responsesChan, stopChan, readerDone)

	writerDone := make(chan struct{}, 1)
	go serverWriter(s, conn, responsesChan, stopChan, writerDone)

	select {
	case <-readerDone:
		close(stopChan)
		conn.Close()
		<-writerDone
	case <-writerDone:
		close(stopChan)
		conn.Close()
		<-readerDone
	}
}

type serverMessage struct {
	ID       uint64
	Request  interface{}
	Response interface{}
}

func serverReader(s *Server, r io.Reader, responsesChan chan<- *serverMessage, stopChan <-chan struct{}, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	br := bufio.NewReader(r)
	d := gob.NewDecoder(br)
	for {
		var m wireMessage
		if err := d.Decode(&m); err != nil {
			logError("rpc.Server: [%s]. Cannot decode message: [%s]", s.Addr, err)
			return
		}
		rpcM := &serverMessage{
			ID:      m.ID,
			Request: m.Data,
		}
		go serveRequest(s, responsesChan, stopChan, rpcM)
	}
}

func serveRequest(s *Server, responsesChan chan<- *serverMessage, stopChan <-chan struct{}, m *serverMessage) {
	defer func() {
		select {
		case <-stopChan:
		case responsesChan <- m:
		}

		if x := recover(); x != nil {
			logError("rpc.Server: [%s]. Panic occured: %v", s.Addr, x)

			stackTrace := make([]byte, 1<<20)
			n := runtime.Stack(stackTrace, false)
			logError("rpc.Server: [%s]. Stack trace: %s", s.Addr, stackTrace[:n])
		}
	}()

	m.Response = s.Handler(m.Request)
}

func serverWriter(s *Server, w io.Writer, responsesChan <-chan *serverMessage, stopChan <-chan struct{}, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	bw := bufio.NewWriter(w)
	e := gob.NewEncoder(bw)
	for {
		var rpcM *serverMessage

		select {
		case <-stopChan:
			return
		case rpcM = <-responsesChan:
		default:
			if err := bw.Flush(); err != nil {
				logError("rpc.Server: [%s]. Cannot flush responses to wire: [%s]", s.Addr, err)
				return
			}
			time.Sleep(5 * time.Millisecond)
			select {
			case <-stopChan:
				return
			case rpcM = <-responsesChan:
			}
		}

		m := wireMessage{
			ID:   rpcM.ID,
			Data: rpcM.Response,
		}
		if err := e.Encode(&m); err != nil {
			logError("rpc.Server: [%s]. Cannot send response to wire: [%s]", s.Addr, err)
			return
		}
	}
}
