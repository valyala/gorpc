package gorpc

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// HandlerFunc is a server handler function.
//
// clientAddr contains client address returned by Listener.Accept().
// Request and response types may be arbitrary.
// All the request types the client may send to the server must be registered
// with gorpc.RegisterType() before starting the server.
// There is no need in registering base Go types such as int, string, bool,
// float64, etc. or arrays, slices and maps containing base Go types.
type HandlerFunc func(clientAddr string, request interface{}) (response interface{})

// Server implements RPC server.
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
	// Default is DefaultPendingMessages.
	PendingResponses int

	// The maximum delay between response flushes to clients.
	//
	// Negative values lead to immediate requests' sending to the client
	// without their buffering. This minimizes rpc latency at the cost
	// of higher CPU and network usage.
	//
	// Default is DefaultFlushDelay.
	FlushDelay time.Duration

	// Size of send buffer per each TCP connection in bytes.
	// Default is DefaultBufferSize.
	SendBufferSize int

	// Size of recv buffer per each TCP connection in bytes.
	// Default is DefaultBufferSize.
	RecvBufferSize int

	// The server obtains new client connections via Listener.Accept().
	//
	// Override the listener if you want custom underlying transport
	// and/or client authentication/authorization.
	// Don't forget overriding Client.Dial() callback accordingly.
	//
	// * NewTLSClient() and NewTLSServer() can be used for encrypted rpc.
	// * NewUnixClient() and NewUnixServer() can be used for fast local
	//   inter-process rpc.
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

// Start starts rpc server.
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
		s.PendingResponses = DefaultPendingMessages
	}
	if s.FlushDelay == 0 {
		s.FlushDelay = DefaultFlushDelay
	}
	if s.SendBufferSize <= 0 {
		s.SendBufferSize = DefaultBufferSize
	}
	if s.RecvBufferSize <= 0 {
		s.RecvBufferSize = DefaultBufferSize
	}

	if s.Listener == nil {
		s.Listener = &defaultListener{}
	}
	if err := s.Listener.Init(s.Addr); err != nil {
		err := fmt.Errorf("gorpc.Server: [%s]. Cannot listen to: [%s]", s.Addr, err)
		logError("%s", err)
		return err
	}

	s.stopWg.Add(1)
	go serverHandler(s)
	return nil
}

// Stop stops rpc server. Stopped server can be started again.
func (s *Server) Stop() {
	close(s.serverStopChan)
	s.stopWg.Wait()
	s.serverStopChan = nil
}

// Serve starts rpc server and blocks until it is stopped.
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
			if conn, clientAddr, err = s.Listener.Accept(); err != nil {
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

var serverMessagePool = &sync.Pool{
	New: func() interface{} {
		return &serverMessage{}
	},
}

func serverReader(s *Server, r io.Reader, clientAddr string, responsesChan chan<- *serverMessage, stopChan <-chan struct{}, done chan<- struct{}, enabledCompression bool) {
	defer func() { close(done) }()

	d := newMessageDecoder(r, s.RecvBufferSize, enabledCompression, &s.Stats)
	defer d.Close()

	var wm wireMessage
	for {
		if err := d.Decode(&wm); err != nil {
			logError("gorpc.Server: [%s]->[%s]. Cannot decode request: [%s]", clientAddr, s.Addr, err)
			return
		}

		m := serverMessagePool.Get().(*serverMessage)
		m.ID = wm.ID
		m.Request = wm.Data
		m.ClientAddr = clientAddr
		wm.Data = nil

		go serveRequest(s.Handler, s.Addr, responsesChan, stopChan, m)
	}
}

func serveRequest(handler HandlerFunc, serverAddr string, responsesChan chan<- *serverMessage, stopChan <-chan struct{}, m *serverMessage) {
	request := m.Request
	m.Request = nil
	m.Response = callHandlerWithRecover(handler, m.ClientAddr, serverAddr, request)

	// Select hack for better performance.
	// See https://github.com/valyala/gorpc/pull/1 for details.
	select {
	case responsesChan <- m:
	default:
		select {
		case responsesChan <- m:
		case <-stopChan:
		}
	}
}

func callHandlerWithRecover(handler HandlerFunc, clientAddr, serverAddr string, request interface{}) interface{} {
	defer func() {
		if x := recover(); x != nil {
			logError("gorpc.Server: [%s]->[%s]. Panic occured: %v", clientAddr, serverAddr, x)

			stackTrace := make([]byte, 1<<20)
			n := runtime.Stack(stackTrace, false)
			logError("gorpc.Server: [%s]->[%s]. Stack trace: %s", clientAddr, serverAddr, stackTrace[:n])
		}
	}()
	return handler(clientAddr, request)
}

func serverWriter(s *Server, w io.Writer, clientAddr string, responsesChan <-chan *serverMessage, stopChan <-chan struct{}, done chan<- struct{}, enabledCompression bool) {
	defer func() { close(done) }()

	e := newMessageEncoder(w, s.SendBufferSize, enabledCompression, &s.Stats)
	defer e.Close()

	var flushChan <-chan time.Time
	t := time.NewTimer(s.FlushDelay)
	var wm wireMessage
	for {
		var m *serverMessage

		select {
		case m = <-responsesChan:
		default:
			select {
			case <-stopChan:
				return
			case m = <-responsesChan:
			case <-flushChan:
				if err := e.Flush(); err != nil {
					logError("gorpc.Server: [%s]->[%s]: Cannot flush responses to underlying stream: [%s]", clientAddr, s.Addr, err)
					return
				}
				flushChan = nil
				continue
			}

		}

		if flushChan == nil {
			flushChan = getFlushChan(t, s.FlushDelay)
		}

		wm.ID = m.ID
		wm.Data = m.Response

		m.Response = nil
		serverMessagePool.Put(m)

		if err := e.Encode(wm); err != nil {
			logError("gorpc.Server: [%s]->[%s]. Cannot send response to wire: [%s]", clientAddr, s.Addr, err)
			return
		}
		wm.Data = nil
	}
}
