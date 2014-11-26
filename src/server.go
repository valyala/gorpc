package gorpc

import (
	"bufio"
	"compress/flate"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"runtime"
	"time"
)

type HandlerFunc func(remoteAddr string, request interface{}) (response interface{})

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
		if err = setupKeepalive(conn); err != nil {
			logError("rpc.Server: [%s]. Cannot setup keepalive: [%s]", s.Addr, err)
		}
		go serverHandleConnection(s, conn)
	}
}

func serverHandleConnection(s *Server, conn net.Conn) {
	responsesChan := make(chan *serverMessage, 1024)
	stopChan := make(chan struct{})

	readerDone := make(chan struct{}, 1)
	remoteAddr := conn.RemoteAddr().String()
	go serverReader(s, conn, remoteAddr, responsesChan, stopChan, readerDone)

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
	ID         uint64
	Request    interface{}
	Response   interface{}
	RemoteAddr string
}

func serverReader(s *Server, r io.Reader, remoteAddr string, responsesChan chan<- *serverMessage, stopChan <-chan struct{}, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	br := bufio.NewReader(r)
	zr := flate.NewReader(br)
	defer zr.Close()
	d := gob.NewDecoder(zr)
	for {
		var m wireMessage
		if err := d.Decode(&m); err != nil {
			logError("rpc.Server: [%s]. Cannot decode message: [%s]", s.Addr, err)
			return
		}
		rpcM := &serverMessage{
			ID:         m.ID,
			Request:    m.Data,
			RemoteAddr: remoteAddr,
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

	m.Response = s.Handler(m.RemoteAddr, m.Request)
}

func serverWriter(s *Server, w io.Writer, responsesChan <-chan *serverMessage, stopChan <-chan struct{}, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	bw := bufio.NewWriter(w)
	zw, _ := flate.NewWriter(bw, flate.DefaultCompression)
	defer zw.Close()
	e := gob.NewEncoder(zw)
	for {
		var rpcM *serverMessage

		select {
		case <-stopChan:
			return
		case rpcM = <-responsesChan:
		default:
			if err := zw.Flush(); err != nil {
				logError("rpc.Server: [%s]. Cannot flush compressed data to wire: [%s]", s.Addr, err)
				return
			}
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
