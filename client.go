package gorpc

import (
	"bufio"
	"compress/flate"
	"encoding/gob"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Client implements RPC client.
//
// The client must be started with Client.Start() before use.
//
// It is absolutely safe and encouraged using a single client across arbitrary
// number of concurrently running goroutines.
//
// Default client settings are optimized for high load, so don't override
// them without valid reason.
type Client struct {
	// Server TCP address to connect to.
	Addr string

	// The number of concurrent connections the client should establish
	// to the sever.
	// By default only one connection is established.
	Conns int

	// The maximum number of pending requests in the queue.
	// Default is 32768.
	PendingRequests int

	// Maximum request time.
	// Default value is 30s.
	RequestTimeout time.Duration

	// Disable data compression.
	// By default data compression is enabled.
	DisableCompression bool

	// Size of send buffer per each TCP connection in bytes.
	// Default value is 1M.
	SendBufferSize int

	// Size of recv buffer per each TCP connection in bytes.
	// Default value is 1M.
	RecvBufferSize int

	// The client calls this callback when it needs new connection
	// to the server.
	// The client passes Client.Addr into Dial().
	//
	// Override this callback if you want custom underlying transport
	// and/or client authentication/authorization.
	// Don't forget overriding Server.Listener accordingly.
	//
	// If you need encrypted transport, then feel free using NewTLSDial()
	// on the client and NewTLSListener() helpers on the server.
	//
	// It is expected that the returned conn immediately
	// sends all the data passed via Write() to the server.
	// Otherwise gorpc may hang.
	// The conn implementation must call Flush() on underlying buffered
	// streams before returning from Write().
	//
	// By default it returns TCP connections established
	// to the Client.Addr.
	Dial func(addr string) (conn io.ReadWriteCloser, err error)

	// Connection statistics.
	//
	// The stats doesn't reset automatically. Feel free resetting it
	// any time you wish.
	Stats ConnStats

	requestsChan chan *clientMessage

	clientStopChan chan struct{}
	stopWg         sync.WaitGroup
}

// Start starts rpc client. Establishes connection to the server on Client.Addr.
//
// All the response types the server may return must be registered
// via gorpc.RegisterType() before starting the client.
// There is no need in registering base Go types such as int, string, bool,
// float64, etc. or arrays, slices and maps containing base Go types.
func (c *Client) Start() {
	if c.clientStopChan != nil {
		panic("gorpc.Client: the given client is already started. Call Client.Stop() before calling Client.Start() again!")
	}

	if c.PendingRequests <= 0 {
		c.PendingRequests = 32768
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.SendBufferSize <= 0 {
		c.SendBufferSize = 1024 * 1024
	}
	if c.RecvBufferSize <= 0 {
		c.RecvBufferSize = 1024 * 1024
	}

	c.requestsChan = make(chan *clientMessage, c.PendingRequests)
	c.clientStopChan = make(chan struct{})

	if c.Conns <= 0 {
		c.Conns = 1
	}
	if c.Dial == nil {
		c.Dial = defaultDial
	}
	for i := 0; i < c.Conns; i++ {
		c.stopWg.Add(1)
		go clientHandler(c)
	}
}

// Stop stops rpc client. Stopped client can be started again.
func (c *Client) Stop() {
	close(c.clientStopChan)
	c.stopWg.Wait()
	c.clientStopChan = nil
}

// Call sends the given request to the server and obtains response
// from the server.
// Returns non-nil error if the response cannot be obtained during
// Client.RequestTimeout or server connection problems occur.
//
// Request and response types may be arbitrary. All the response types
// the server may return must be registered via gorpc.RegisterType() before
// starting the client.
// There is no need in registering base Go types such as int, string, bool,
// float64, etc. or arrays, slices and maps containing base Go types.
//
// Don't forget starting the client with Client.Start() before calling Client.Call().
func (c *Client) Call(request interface{}) (response interface{}, err error) {
	return c.CallTimeout(request, c.RequestTimeout)
}

// CallTimeout sends the given request to the server and obtains response
// from the server.
// Returns non-nil error if the response cannot be obtained during
// the given timeout or server connection problems occur.
//
// Request and response types may be arbitrary. All the response types
// the server may return must be registered via gorpc.RegisterType() before
// starting the client.
// There is no need in registering base Go types such as int, string, bool,
// float64, etc. or arrays, slices and maps containing base Go types.
//
// Don't forget starting the client with Client.Start() before calling Client.Call().
func (c *Client) CallTimeout(request interface{}, timeout time.Duration) (response interface{}, err error) {
	m := clientMessage{
		Request: request,
		Done:    make(chan struct{}),
	}
	tc := time.NewTimer(timeout)
	select {
	case c.requestsChan <- &m:
	default:
		select {
		case c.requestsChan <- &m:
		case <-tc.C:
			err := fmt.Errorf("gorpc.Client: [%s]. Requests' queue with size=%d is overflown", c.Addr, cap(c.requestsChan))
			logError("%s", err)
			return nil, err
		}
	}
	select {
	case <-m.Done:
		tc.Stop()
		return m.Response, m.Error
	case <-tc.C:
		err := fmt.Errorf("gorpc.Client: [%s]. Cannot obtain response during timeout=%s", c.Addr, timeout)
		logError("%s", err)
		return nil, err
	}
}

func clientHandler(c *Client) {
	defer c.stopWg.Done()

	var conn io.ReadWriteCloser
	var err error

	for {
		dialChan := make(chan struct{})
		go func() {
			if conn, err = c.Dial(c.Addr); err != nil {
				logError("gorpc.Client: [%s]. Cannot establish rpc connection: [%s]", c.Addr, err)
				time.Sleep(time.Second)
			}
			close(dialChan)
		}()

		select {
		case <-c.clientStopChan:
			return
		case <-dialChan:
			atomic.AddUint64(&c.Stats.DialCalls, 1)
		}

		if err != nil {
			atomic.AddUint64(&c.Stats.DialErrors, 1)
			continue
		}
		clientHandleConnection(c, conn)
	}
}

func clientHandleConnection(c *Client, conn io.ReadWriteCloser) {
	var buf [1]byte
	if !c.DisableCompression {
		buf[0] = 1
	}
	_, err := conn.Write(buf[:])
	if err != nil {
		logError("gorpc.Client: [%s]. Error when writing handshake to server: [%s]", c.Addr, err)
		conn.Close()
		return
	}

	stopChan := make(chan struct{})

	pendingRequests := make(map[uint64]*clientMessage)
	var pendingRequestsLock sync.Mutex

	writerDone := make(chan error, 1)
	go clientWriter(c, conn, pendingRequests, &pendingRequestsLock, stopChan, writerDone)

	readerDone := make(chan error, 1)
	go clientReader(c, conn, pendingRequests, &pendingRequestsLock, readerDone)

	select {
	case err = <-writerDone:
		close(stopChan)
		conn.Close()
		<-readerDone
	case err = <-readerDone:
		close(stopChan)
		conn.Close()
		<-writerDone
	case <-c.clientStopChan:
		close(stopChan)
		conn.Close()
		<-readerDone
		<-writerDone
	}

	if err != nil {
		logError("%s", err)
	}
	for _, m := range pendingRequests {
		m.Error = err
		close(m.Done)
	}
}

type clientMessage struct {
	Request  interface{}
	Response interface{}
	Done     chan struct{}
	Error    error
}

func clientWriter(c *Client, w io.Writer, pendingRequests map[uint64]*clientMessage, pendingRequestsLock *sync.Mutex, stopChan <-chan struct{}, done chan<- error) {
	var err error
	defer func() { done <- err }()

	w = newWriterCounter(w, &c.Stats)
	bw := bufio.NewWriterSize(w, c.SendBufferSize)

	ww := bw
	var zw *flate.Writer
	if !c.DisableCompression {
		zw, _ = flate.NewWriter(bw, flate.BestSpeed)
		defer zw.Close()
		ww = bufio.NewWriterSize(zw, c.SendBufferSize)
	}
	e := gob.NewEncoder(ww)

	var msgID uint64
	for {
		var rpcM *clientMessage

		select {
		case <-stopChan:
			return
		default:
		}

		select {
		case rpcM = <-c.requestsChan:
		default:
			if !c.DisableCompression {
				if err := ww.Flush(); err != nil {
					err = fmt.Errorf("gorpc.Client: [%s]. Cannot flush data to compressed stream: [%s]", c.Addr, err)
					return
				}
				if err := zw.Flush(); err != nil {
					err = fmt.Errorf("gorpc.Client: [%s]. Cannot flush compressed data to wire: [%s]", c.Addr, err)
					return
				}
			}
			if err := bw.Flush(); err != nil {
				err = fmt.Errorf("gorpc.Client: [%s]. Cannot flush requests to wire: [%s]", c.Addr, err)
				return
			}
			select {
			case <-stopChan:
				return
			case rpcM = <-c.requestsChan:
			}
		}

		msgID++
		pendingRequestsLock.Lock()
		n := len(pendingRequests)
		pendingRequests[msgID] = rpcM
		pendingRequestsLock.Unlock()

		if n > 10*c.PendingRequests {
			err = fmt.Errorf("gorpc.Client: [%s]. The server didn't return %d responses yet. Closing server connection in order to prevent client resource leaks", c.Addr, n)
			return
		}

		m := wireMessage{
			ID:   msgID,
			Data: rpcM.Request,
		}
		if err := e.Encode(&m); err != nil {
			err = fmt.Errorf("gorpc.Client: [%s]. Cannot send request to wire: [%s]", c.Addr, err)
			return
		}
	}
}

func clientReader(c *Client, r io.Reader, pendingRequests map[uint64]*clientMessage, pendingRequestsLock *sync.Mutex, done chan<- error) {
	var err error
	defer func() { done <- err }()

	r = newReaderCounter(r, &c.Stats)
	br := bufio.NewReaderSize(r, c.RecvBufferSize)

	rr := br
	if !c.DisableCompression {
		zr := flate.NewReader(br)
		defer zr.Close()
		rr = bufio.NewReaderSize(zr, c.RecvBufferSize)
	}
	d := gob.NewDecoder(rr)

	for {
		var m wireMessage
		if err := d.Decode(&m); err != nil {
			err = fmt.Errorf("gorpc.Client: [%s]. Cannot decode response: [%s]", c.Addr, err)
			return
		}

		pendingRequestsLock.Lock()
		rpcM, ok := pendingRequests[m.ID]
		delete(pendingRequests, m.ID)
		pendingRequestsLock.Unlock()
		if !ok {
			err = fmt.Errorf("gorpc.Client: [%s]. Unexpected msgID=[%d] obtained from server", c.Addr, m.ID)
			return
		}

		rpcM.Response = m.Data
		close(rpcM.Done)
	}
}
