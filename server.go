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
//
// remoteAddr contains client address returned by net.TCPConn.RemoteAddr()
// Request and response types may be arbitrary.
// All the request types the client may send to the server must be registered
// before Server.Start() with gorpc.RegisterType().
type HandlerFunc func(remoteAddr string, request interface{}) (response interface{})

// Rpc server.
type Server struct {
	// TCP address to listen to for incoming connections.
	Addr string

	// Handler function for incoming messages.
	Handler HandlerFunc

	// The maximum number of pending responses in the queue.
	// Default is 32768.
	PendingResponsesCount int

	// The maximum delay between response flushes to clients.
	// Default is 5ms.
	FlushDelay time.Duration

	// Size of send buffer per each TCP connection in bytes.
	// Default is 1M.
	SendBufferSize int

	// Size of recv buffer per each TCP connection in bytes.
	// Default is 1M.
	RecvBufferSize int

	serverStopChan chan struct{}
	stopWg         sync.WaitGroup
}

// Starts rpc server.
//
// All the request types the client may send to the server must be registered
// before Server.Start() with gorpc.RegisterType().
func (s *Server) Start() error {
	if s.Handler == nil {
		panic("rpc.Server: Server.Handler cannot be nil")
	}

	if s.serverStopChan != nil {
		panic("rpc.Server: server is already running. Stop it before starting it again")
	}
	s.serverStopChan = make(chan struct{})

	if s.PendingResponsesCount <= 0 {
		s.PendingResponsesCount = 32768
	}
	if s.FlushDelay <= 0 {
		s.FlushDelay = 5 * time.Millisecond
	}
	if s.SendBufferSize <= 0 {
		s.SendBufferSize = 1024 * 1024
	}
	if s.RecvBufferSize <= 0 {
		s.RecvBufferSize = 1024 * 1024
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

// Stops rpc server. Stopped server can be started again.
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

func setupKeepalive(conn net.Conn) error {
	tcpConn := conn.(*net.TCPConn)
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return err
	}
	if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
		return err
	}
	return nil
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
			conn.Close()
			return
		}
	case <-s.serverStopChan:
		conn.Close()
		return
	case <-time.After(10 * time.Second):
		logError("rpc.Server: [%s]. Cannot obtain handshake from client during 10s", s.Addr)
		conn.Close()
		return
	}

	responsesChan := make(chan *serverMessage, s.PendingResponsesCount)
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

	br := bufio.NewReaderSize(r, s.RecvBufferSize)

	rr := br
	if enabledCompression {
		zr := flate.NewReader(br)
		defer zr.Close()
		rr = bufio.NewReaderSize(zr, s.RecvBufferSize)
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

	bw := bufio.NewWriterSize(w, s.SendBufferSize)

	ww := bw
	var zw *flate.Writer
	if enabledCompression {
		zw, _ = flate.NewWriter(bw, flate.BestSpeed)
		defer zw.Close()
		ww = bufio.NewWriterSize(zw, s.SendBufferSize)
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
