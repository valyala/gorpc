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
	"sync/atomic"
	"time"
)

// Server handler function.
//
// clientAddr contains client address returned by net.Conn.RemoteAddr().
// Request and response types may be arbitrary.
// All the request types the client may send to the server must be registered
// with gorpc.RegisterType() before starting the server.
// There is no need in registering base Go types such as int, string, bool,
// float64, etc. or arrays, slices and maps containing base Go types.
type HandlerFunc func(clientAddr string, request interface{}) (response interface{})

// Custom listener passed to Server.Listener must satisfy this interface.
type Listener interface {
	// Listener must return incoming connections from clients.
	// The server passes Server.Addr in addr parameter.
	// clientAddr must contain client's address in user-readable view.
	//
	// It is expected that the returned conn immediately
	// sends all the data passed via Write() to the client.
	// Otherwise gorpc may hang.
	Accept(addr string) (conn io.ReadWriteCloser, clientAddr string, err error)

	// Listener must immediately return errors from all pending Accept()
	// calls if Close() is called.
	//
	// All subsequent calls to Accept() must immediately return error.
	Close() error
}

// Rpc server.
//
// Default server settings are optimized for high load, so don't override
// them without valid reason.
type Server struct {
	// TCP address to listen to for incoming connections.
	Addr string

	// Handler function for incoming requests.
	//
	// Server calls this function for each incoming request.
	// The function must process the request and return the corresponding response.
	Handler HandlerFunc

	// The maximum number of pending responses in the queue.
	// Default is 32768.
	PendingResponses int

	// The maximum delay between response flushes to clients.
	// Default is 5ms.
	FlushDelay time.Duration

	// Size of send buffer per each TCP connection in bytes.
	// Default is 1M.
	SendBufferSize int

	// Size of recv buffer per each TCP connection in bytes.
	// Default is 1M.
	RecvBufferSize int

	// The server obtains new client connections via Listener.Accept().
	//
	// Override the listener if you want custom underlying transport
	// for gorpc. For example, UDP-based, encrypted or SOAP-based :)
	// Don't forget overriding Client.Dial() callback accordingly.
	//
	// By default it returns TCP connections accepted from Server.Addr.
	Listener Listener

	// Connection statistics.
	//
	// The stats doesn't reset automatically. Feel free resetting it
	// any time you wish.
	Stats ConnStats

	serverStopChan chan struct{}
	stopWg         sync.WaitGroup
}

// Starts rpc server.
//
// All the request types the client may send to the server must be registered
// with gorpc.RegisterType() before starting the server.
// There is no need in registering base Go types such as int, string, bool,
// float64, etc. or arrays, slices and maps containing base Go types.
func (s *Server) Start() error {
	if s.Handler == nil {
		panic("gorpc.Server: Server.Handler cannot be nil")
	}

	if s.serverStopChan != nil {
		panic("gorpc.Server: server is already running. Stop it before starting it again")
	}
	s.serverStopChan = make(chan struct{})

	if s.PendingResponses <= 0 {
		s.PendingResponses = 32768
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

	if s.Listener == nil {
		ln, err := net.Listen("tcp", s.Addr)
		if err != nil {
			err := fmt.Errorf("gorpc.Server: [%s]. Cannot listen to: [%s]", s.Addr, err)
			logError("%s", err)
			return err
		}
		s.Listener = &defaultListener{
			L: ln,
		}
	}

	s.stopWg.Add(1)
	go serverHandler(s)
	return nil
}

type defaultListener struct {
	L net.Listener
}

func (ln *defaultListener) Accept(addr string) (conn io.ReadWriteCloser, clientAddr string, err error) {
	c, err := ln.L.Accept()
	if err != nil {
		return nil, "", err
	}
	if err = setupKeepalive(c); err != nil {
		logError("gorpc.Server: [%s]->[%s]. Cannot setup keepalive: [%s]", c.RemoteAddr(), addr, err)
		c.Close()
		return nil, "", err
	}
	return c, c.RemoteAddr().String(), nil
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

func (ln *defaultListener) Close() error {
	return ln.L.Close()
}

// Stops rpc server. Stopped server can be started again.
func (s *Server) Stop() {
	close(s.serverStopChan)
	s.stopWg.Wait()
	s.serverStopChan = nil
}

// Starts rpc server and blocks until it is stopped.
func (s *Server) Serve() error {
	if err := s.Start(); err != nil {
		return err
	}
	s.stopWg.Wait()
	return nil
}

func serverHandler(s *Server) {
	defer s.stopWg.Done()

	var conn io.ReadWriteCloser
	var clientAddr string
	var err error

	for {
		acceptChan := make(chan struct{})
		go func() {
			if conn, clientAddr, err = s.Listener.Accept(s.Addr); err != nil {
				logError("gorpc.Server: [%s]. Cannot accept new connection: [%s]", s.Addr, err)
				time.Sleep(time.Second)
			}
			close(acceptChan)
		}()

		select {
		case <-s.serverStopChan:
			s.Listener.Close()
			return
		case <-acceptChan:
			atomic.AddUint64(&s.Stats.AcceptCalls, 1)
		}

		if err != nil {
			atomic.AddUint64(&s.Stats.AcceptErrors, 1)
			continue
		}

		s.stopWg.Add(1)
		go serverHandleConnection(s, conn, clientAddr)
	}
}

func serverHandleConnection(s *Server, conn io.ReadWriteCloser, clientAddr string) {
	defer s.stopWg.Done()

	var enabledCompression bool
	var err error
	zChan := make(chan bool, 1)
	go func() {
		var buf [1]byte
		if _, err = conn.Read(buf[:]); err != nil {
			logError("gorpc.Server: [%s]. Error when reading handshake from client: [%s]", s.Addr, err)
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
		logError("gorpc.Server: [%s]. Cannot obtain handshake from client during 10s", s.Addr)
		conn.Close()
		return
	}

	responsesChan := make(chan *serverMessage, s.PendingResponses)
	stopChan := make(chan struct{})

	readerDone := make(chan struct{})
	go serverReader(s, conn, clientAddr, responsesChan, stopChan, readerDone, enabledCompression)

	writerDone := make(chan struct{})
	go serverWriter(s, conn, clientAddr, responsesChan, stopChan, writerDone, enabledCompression)

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
	ClientAddr string
}

func serverReader(s *Server, r io.Reader, clientAddr string, responsesChan chan<- *serverMessage, stopChan <-chan struct{}, done chan<- struct{}, enabledCompression bool) {
	defer func() { close(done) }()

	r = newReaderCounter(r, &s.Stats)
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
			logError("gorpc.Server: [%s]->[%s]. Cannot decode request: [%s]", clientAddr, s.Addr, err)
			return
		}
		rpcM := &serverMessage{
			ID:         m.ID,
			Request:    m.Data,
			ClientAddr: clientAddr,
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
			logError("gorpc.Server: [%s]->[%s]. Panic occured: %v", m.ClientAddr, s.Addr, x)

			stackTrace := make([]byte, 1<<20)
			n := runtime.Stack(stackTrace, false)
			logError("gorpc.Server: [%s]->[%s]. Stack trace: %s", m.ClientAddr, s.Addr, stackTrace[:n])
		}
	}()

	m.Response = s.Handler(m.ClientAddr, m.Request)
}

func serverWriter(s *Server, w io.Writer, clientAddr string, responsesChan <-chan *serverMessage, stopChan <-chan struct{}, done chan<- struct{}, enabledCompression bool) {
	defer func() { close(done) }()

	w = newWriterCounter(w, &s.Stats)
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
					logError("gorpc.Server: [%s]->[%s]. Cannot flush data to compressed stream: [%s]", clientAddr, s.Addr, err)
					return
				}
				if err := zw.Flush(); err != nil {
					logError("gorpc.Server: [%s]->[%s]. Cannot flush compressed data to wire: [%s]", clientAddr, s.Addr, err)
					return
				}
			}
			if err := bw.Flush(); err != nil {
				logError("gorpc.Server: [%s]->[%s]. Cannot flush responses to wire: [%s]", clientAddr, s.Addr, err)
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
			logError("gorpc.Server: [%s]->[%s]. Cannot send response to wire: [%s]", clientAddr, s.Addr, err)
			return
		}
	}
}
