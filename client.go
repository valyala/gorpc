package gorpc

import (
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
	// Server address to connect to.
	//
	// The address format depends on the underlying transport provided
	// by Client.Dial. The following transports are provided out of the box:
	//   * TCP - see NewTCPClient() and NewTCPServer().
	//   * TLS - see NewTLSClient() and NewTLSServer().
	//   * Unix sockets - see NewUnixClient() and NewUnixServer().
	//
	// By default TCP transport is used.
	Addr string

	// The number of concurrent connections the client should establish
	// to the sever.
	// By default only one connection is established.
	Conns int

	// The maximum number of pending requests in the queue.
	//
	// The number of pending requsts should exceed the expected number
	// of concurrent goroutines calling client's methods.
	// Otherwise a lot of ClientError.Overflow errors may appear.
	//
	// Default is DefaultPendingMessages.
	PendingRequests int

	// Delay between request flushes.
	//
	// Negative values lead to immediate requests' sending to the server
	// without their buffering. This minimizes rpc latency at the cost
	// of higher CPU and network usage.
	//
	// Default value is DefaultFlushDelay.
	FlushDelay time.Duration

	// Maximum request time.
	// Default value is DefaultRequestTimeout.
	RequestTimeout time.Duration

	// Disable data compression.
	// By default data compression is enabled.
	DisableCompression bool

	// Size of send buffer per each TCP connection in bytes.
	// Default value is DefaultBufferSize.
	SendBufferSize int

	// Size of recv buffer per each TCP connection in bytes.
	// Default value is DefaultBufferSize.
	RecvBufferSize int

	// The client calls this callback when it needs new connection
	// to the server.
	// The client passes Client.Addr into Dial().
	//
	// Override this callback if you want custom underlying transport
	// and/or client authentication/authorization.
	// Don't forget overriding Server.Listener accordingly.
	//
	// * NewTLSClient() and NewTLSServer() can be used for encrypted rpc.
	// * NewUnixClient() and NewUnixServer() can be used for fast local
	//   inter-process rpc.
	//
	// By default it returns TCP connections established
	// to the Client.Addr.
	Dial DialFunc

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
		c.PendingRequests = DefaultPendingMessages
	}
	if c.FlushDelay == 0 {
		c.FlushDelay = DefaultFlushDelay
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = DefaultRequestTimeout
	}
	if c.SendBufferSize <= 0 {
		c.SendBufferSize = DefaultBufferSize
	}
	if c.RecvBufferSize <= 0 {
		c.RecvBufferSize = DefaultBufferSize
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
// The returned error can be casted to ClientError.
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
// The returned error can be casted to ClientError.
//
// Request and response types may be arbitrary. All the response types
// the server may return must be registered via gorpc.RegisterType() before
// starting the client.
// There is no need in registering base Go types such as int, string, bool,
// float64, etc. or arrays, slices and maps containing base Go types.
//
// Don't forget starting the client with Client.Start() before calling Client.Call().
func (c *Client) CallTimeout(request interface{}, timeout time.Duration) (response interface{}, err error) {
	m := acquireClientMessage(request, false)
	t := acquireTimer(timeout)

	select {
	case c.requestsChan <- m:
		select {
		case <-m.Done:
			response, err = m.Response, m.Error

			m.Response = nil
			m.Error = nil
			clientMessagePool.Put(m)
		case <-t.C:
			err = fmt.Errorf("gorpc.Client: [%s]. Cannot obtain response during timeout=%s", c.Addr, timeout)
			logError("%s", err)
			err = &ClientError{
				Timeout: true,
				err:     err,
			}
		}
	default:
		m.Request = nil
		clientMessagePool.Put(m)

		err = fmt.Errorf("gorpc.Client: [%s]. Requests' queue with size=%d is overflown", c.Addr, cap(c.requestsChan))
		logError("%s", err)
		err = &ClientError{
			Overflow: true,
			err:      err,
		}
	}
	releaseTimer(t)
	return
}

// Send sends the given request to the server and doesn't wait for response.
//
// Since this is 'fire and forget' function, which never waits for response,
// it cannot guarantee that the server receives and successfully processes
// the given request. Though in most cases under normal conditions requests
// should reach the server and it should successfully process them.
// Send semantics is similar to UDP messages' semantics.
//
// The server may return arbitrary response on Send() request, but the response
// is totally ignored.
//
// Don't forget starting the client with Client.Start() before calling Client.Send().
func (c *Client) Send(request interface{}) {
	m := acquireClientMessage(request, true)

	select {
	case c.requestsChan <- m:
	default:
		m.Request = nil
		clientMessagePool.Put(m)

		logError("gorpc.Client: [%s]. Requests' queue with size=%d is overflown", c.Addr, cap(c.requestsChan))
	}
}

// AsyncResult is a result returned from Client.CallAsync*().
type AsyncResult struct {
	// The response can be read only after <-Done unblocks.
	Response interface{}

	// The error can be read only after <-Done unblocks.
	// The error can be casted to ClientError.
	Error error

	// Response and Error become available after <-Done unblocks.
	Done <-chan struct{}
}

// CallAsync starts async rpc call, which should be completed
// during Client.RequestTimeout.
//
// Rpc call is complete after <-AsyncResult.Done unblocks.
// If you want canceling the request, just throw away the returned AsyncResult.
//
// Don't forget starting the client with Client.Start() before
// calling Client.CallAsync().
func (c *Client) CallAsync(request interface{}) *AsyncResult {
	return c.CallAsyncTimeout(request, c.RequestTimeout)
}

// CallAsyncTimeout starts async rpc call, which should be completed
// during the given timeout.
//
// Rpc call is complete after <-AsyncResult.Done unblocks.
// If you want canceling the request, just throw away the returned AsyncResult.
//
// Don't forget starting the client with Client.Start() before
// calling Client.CallAsyncTimeout().
func (c *Client) CallAsyncTimeout(request interface{}, timeout time.Duration) *AsyncResult {
	ch := make(chan struct{})
	r := &AsyncResult{
		Done: ch,
	}
	go func() {
		r.Response, r.Error = c.CallTimeout(request, timeout)
		close(ch)
	}()
	return r
}

// ClientError is an error Client methods can return.
type ClientError struct {
	// Set if the error is timeout-related.
	Timeout bool

	// Set if the error is connection-related.
	Connection bool

	// Set if the error is server-related.
	Server bool

	// Set if the error is related to internal resources' overflow.
	// Increase PendingRequests if you see a lot of such errors.
	Overflow bool

	err error
}

func (e *ClientError) Error() string {
	return e.err.Error()
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
		err = &ClientError{
			Connection: true,
			err:        err,
		}
	}
	for _, m := range pendingRequests {
		m.Error = err
		m.Done <- struct{}{}
	}
}

type clientMessage struct {
	Request      interface{}
	Response     interface{}
	SkipResponse bool
	Error        error
	Done         chan struct{}
}

var clientMessagePool sync.Pool

func acquireClientMessage(request interface{}, skipResponse bool) *clientMessage {
	mv := clientMessagePool.Get()
	if mv == nil {
		return &clientMessage{
			Request: request,
			Done:    make(chan struct{}, 1),
		}
	}

	m := mv.(*clientMessage)
	m.Request = request
	m.SkipResponse = skipResponse
	return m
}

func clientWriter(c *Client, w io.Writer, pendingRequests map[uint64]*clientMessage, pendingRequestsLock *sync.Mutex, stopChan <-chan struct{}, done chan<- error) {
	var err error
	defer func() { done <- err }()

	e := newMessageEncoder(w, c.SendBufferSize, !c.DisableCompression, &c.Stats)
	defer e.Close()

	t := time.NewTimer(c.FlushDelay)
	var flushChan <-chan time.Time
	var wr wireRequest
	for {
		var m *clientMessage

		select {
		case m = <-c.requestsChan:
		default:
			select {
			case <-stopChan:
				return
			case m = <-c.requestsChan:
			case <-flushChan:
				if err := e.Flush(); err != nil {
					err = fmt.Errorf("gorpc.Client: [%s]. Cannot flush requests to underlying stream: [%s]", c.Addr, err)
					return
				}
				flushChan = nil
				continue
			}
		}

		if flushChan == nil {
			flushChan = getFlushChan(t, c.FlushDelay)
		}

		wr.ID++

		if !m.SkipResponse {
			pendingRequestsLock.Lock()
			n := len(pendingRequests)
			pendingRequests[wr.ID] = m
			pendingRequestsLock.Unlock()

			if n > 10*c.PendingRequests {
				err = fmt.Errorf("gorpc.Client: [%s]. The server didn't return %d responses yet. Closing server connection in order to prevent client resource leaks", c.Addr, n)
				return
			}
		}

		wr.SkipResponse = m.SkipResponse
		wr.Request = m.Request
		m.Request = nil
		if m.SkipResponse {
			clientMessagePool.Put(m)
		}
		if err := e.Encode(wr); err != nil {
			err = fmt.Errorf("gorpc.Client: [%s]. Cannot send request to wire: [%s]", c.Addr, err)
			return
		}
		wr.Request = nil
	}
}

func clientReader(c *Client, r io.Reader, pendingRequests map[uint64]*clientMessage, pendingRequestsLock *sync.Mutex, done chan<- error) {
	var err error
	defer func() { done <- err }()

	d := newMessageDecoder(r, c.RecvBufferSize, !c.DisableCompression, &c.Stats)
	defer d.Close()

	var wr wireResponse
	for {
		if err := d.Decode(&wr); err != nil {
			err = fmt.Errorf("gorpc.Client: [%s]. Cannot decode response: [%s]", c.Addr, err)
			return
		}

		pendingRequestsLock.Lock()
		m, ok := pendingRequests[wr.ID]
		delete(pendingRequests, wr.ID)
		pendingRequestsLock.Unlock()

		if !ok {
			err = fmt.Errorf("gorpc.Client: [%s]. Unexpected msgID=[%d] obtained from server", c.Addr, wr.ID)
			return
		}

		m.Response = wr.Response

		wr.ID = 0
		wr.Response = nil
		if wr.Error != "" {
			m.Error = &ClientError{
				Server: true,
				err:    fmt.Errorf("gorpc.Client: [%s]. Server error: [%s]", c.Addr, wr.Error),
			}
			wr.Error = ""
		}

		m.Done <- struct{}{}
		atomic.AddUint64(&c.Stats.RpcCalls, 1)
	}
}
