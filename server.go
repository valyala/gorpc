package gorpc

import (
	"bufio"
	"compress/flate"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"time"
)

// Server handler function.
type HandlerFunc func(remoteAddr string, request interface{}) (response interface{})

// Rpc server.
type Server struct {
	// TCP address to listen to for incoming connections.
	Addr string

	// Handler function for incoming messages.
	Handler HandlerFunc

	// The maximum delay between response flushes to clients.
	// Default value is 5ms.
	FlushDelay time.Duration

	serverStopChan chan struct{}
	stopWg         sync.WaitGroup
}

// Starts rpc server.
func (s *Server) Start() error {
	if s.serverStopChan != nil {
		panic("rpc.Server: server is already running. Stop it before starting it again")
	}
	s.serverStopChan = make(chan struct{})

	if s.FlushDelay <= 0 {
		s.FlushDelay = 5 * time.Millisecond
	}

	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		logError("rpc.Server: [%s]. Cannot listen to: [%s]", s.Addr, err)
		return fmt.Errorf("rpc.Server: [%s]. Cannot listen to: [%s]", s.Addr, err)
	}

	s.stopWg.Add(1)
	go serverHandler(s, ln)
	return nil
}

// Stops rpc server.
func (s *Server) Stop() {
	close(s.serverStopChan)
	s.stopWg.Wait()
	s.serverStopChan = nil
}

// Starts rpc server and blocks until it is stopped.
func (s *Server) Serve() error {
	err := s.Start()
	s.stopWg.Wait()
	return err
}

func serverHandler(s *Server, ln net.Listener) {
	defer s.stopWg.Done()

	var conn net.Conn
	var err error

	for {
		acceptChan := make(chan struct{}, 1)
		go func() {
			if conn, err = ln.Accept(); err != nil {
				logError("rpc.Server: [%s]. Cannot accept new connection: [%s]", s.Addr, err)
				time.Sleep(time.Second)
			}
			acceptChan <- struct{}{}
		}()

		select {
		case <-s.serverStopChan:
			ln.Close()
			return
		case <-acceptChan:
		}

		if err != nil {
			continue
		}
		if err = setupKeepalive(conn); err != nil {
			logError("rpc.Server: [%s]. Cannot setup keepalive: [%s]", s.Addr, err)
		}

		s.stopWg.Add(1)
		go serverHandleConnection(s, conn)
	}
}

func serverHandleConnection(s *Server, conn net.Conn) {
	defer s.stopWg.Done()

	var enabledCompression bool
	var err error
	zChan := make(chan bool, 1)
	go func() {
		var buf [1]byte
		if _, err = conn.Read(buf[:]); err != nil {
			logError("rpc.Server: [%s]. Error when reading handshake from client: [%s]", s.Addr, err)
		}
		zChan <- (buf[0] != 0)
	}()
	select {
	case enabledCompression = <-zChan:
		if err != nil {
			return
		}
	case <-s.serverStopChan:
		return
	case <-time.After(10 * time.Second):
		logError("rpc.Server: [%s]. Cannot obtain handshake from client %s during 10s", s.Addr, conn)
		return
	}

	responsesChan := make(chan *serverMessage, 1024)
	stopChan := make(chan struct{})

	readerDone := make(chan struct{}, 1)
	remoteAddr := conn.RemoteAddr().String()
	go serverReader(s, conn, remoteAddr, responsesChan, stopChan, readerDone, enabledCompression)

	writerDone := make(chan struct{}, 1)
	go serverWriter(s, conn, responsesChan, stopChan, writerDone, enabledCompression)

	select {
	case <-readerDone:
		close(stopChan)
		conn.Close()
		<-writerDone
	case <-writerDone:
		close(stopChan)
		conn.Close()
		<-readerDone
	case <-s.serverStopChan:
		close(stopChan)
		conn.Close()
		<-readerDone
		<-writerDone
	}
}

type serverMessage struct {
	ID         uint64
	Request    interface{}
	Response   interface{}
	RemoteAddr string
}

func serverReader(s *Server, r io.Reader, remoteAddr string, responsesChan chan<- *serverMessage, stopChan <-chan struct{}, done chan<- struct{}, enabledCompression bool) {
	defer func() { done <- struct{}{} }()

	br := bufio.NewReader(r)

	rr := br
	if enabledCompression {
		zr := flate.NewReader(br)
		defer zr.Close()
		rr = bufio.NewReader(zr)
	}
	d := gob.NewDecoder(rr)

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

func serverWriter(s *Server, w io.Writer, responsesChan <-chan *serverMessage, stopChan <-chan struct{}, done chan<- struct{}, enabledCompression bool) {
	defer func() { done <- struct{}{} }()

	bw := bufio.NewWriter(w)

	ww := bw
	var zw *flate.Writer
	if enabledCompression {
		zw, _ = flate.NewWriter(bw, flate.BestSpeed)
		defer zw.Close()
		ww = bufio.NewWriter(zw)
	}
	e := gob.NewEncoder(ww)

	var flushChan <-chan time.Time

	for {
		var rpcM *serverMessage

		select {
		case <-stopChan:
			return
		case rpcM = <-responsesChan:
			if flushChan == nil {
				flushChan = time.After(s.FlushDelay)
			}
		case <-flushChan:
			if enabledCompression {
				if err := ww.Flush(); err != nil {
					logError("rpc.Server: [%s]. Cannot flush data to compressed stream: [%s]", s.Addr, err)
					return
				}
				if err := zw.Flush(); err != nil {
					logError("rpc.Server: [%s]. Cannot flush compressed data to wire: [%s]", s.Addr, err)
					return
				}
			}
			if err := bw.Flush(); err != nil {
				logError("rpc.Server: [%s]. Cannot flush responses to wire: [%s]", s.Addr, err)
				return
			}
			flushChan = nil
			continue
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
