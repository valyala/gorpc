package gorpc

import (
	"bufio"
	"compress/flate"
	"encoding/gob"
	"io"
	"net"
	"sync"
	"time"
)

// Rpc client.
type Client struct {
	// Server TCP address to connect to.
	Addr string

	// The number of concurrent connections the client should establish
	// to the sever.
	// By default only one connection is established.
	Conns int

	// The maximum number of pending requests in the queue.
	// Default is 1024.
	PendingRequestsCount int

	// Delay between request flushes.
	// Default value is 5ms.
	FlushDelay time.Duration

	// Maximum request time.
	// Default value is 30s.
	MaxRequestTime time.Duration

	// Enable data compression.
	EnableCompression bool

	// Size of send buffer per each TCP connection.
	// Default value is 4096.
	SendBufferSize int

	// Size of recv buffer per each TCP connection.
	// Default value is 4096.
	RecvBufferSize int

	requestsChan chan *clientMessage

	clientStopChan chan struct{}
	stopWg         sync.WaitGroup
}

// Starts rpc client. Establishes connection to Client.Addr.
func (c *Client) Start() {
	if c.clientStopChan != nil {
		panic("rpc.Client: the given client is already started. Call Client.Stop() before calling Client.Start() again!")
	}

	if c.FlushDelay <= 0 {
		c.FlushDelay = 5 * time.Millisecond
	}

	if c.MaxRequestTime <= 0 {
		c.MaxRequestTime = 30 * time.Second
	}
	if c.SendBufferSize <= 0 {
		c.SendBufferSize = 4096
	}
	if c.RecvBufferSize <= 0 {
		c.RecvBufferSize = 4096
	}

	if c.PendingRequestsCount <= 0 {
		c.PendingRequestsCount = 1024
	}
	c.requestsChan = make(chan *clientMessage, c.PendingRequestsCount)
	c.clientStopChan = make(chan struct{})

	if c.Conns <= 0 {
		c.Conns = 1
	}
	for i := 0; i < c.Conns; i++ {
		c.stopWg.Add(1)
		go clientHandler(c)
	}
}

// Stops rpc client. Stopped client can be started again.
func (c *Client) Stop() {
	close(c.clientStopChan)
	c.stopWg.Wait()
	c.clientStopChan = nil
}

// Sends the given request to the server and obtains response from the server.
// Requests must be sent only via clients started via Client.Start().
func (c *Client) Send(request interface{}) interface{} {
	return c.SendWithTimeout(request, c.MaxRequestTime)
}

// Sends the given request to the server and obtains response from the server.
// Waits for the response during the given timeout. Returns nil if the response
// cannot be obtained during the given timeout.
// Requests must be sent only via clients started via Client.Start().
func (c *Client) SendWithTimeout(request interface{}, timeout time.Duration) interface{} {
	m := clientMessage{
		Request: request,
		Done:    make(chan struct{}, 1),
	}
	select {
	case c.requestsChan <- &m:
		select {
		case <-m.Done:
			return m.Response
		case <-time.After(timeout):
			logError("rpc.Client: [%s]. Cannot obtain request during timeout=%s", c.Addr, timeout)
			return nil
		}
	default:
		logError("rpc.Client: [%s]. Requests' queue with size=%d is overflown", c.Addr, cap(c.requestsChan))
		return nil
	}
}

var dialer = &net.Dialer{
	Timeout:   10 * time.Second,
	KeepAlive: 30 * time.Second,
}

func clientHandler(c *Client) {
	defer c.stopWg.Done()

	var conn net.Conn
	var err error

	for {
		dialChan := make(chan struct{}, 1)
		go func() {
			if conn, err = dialer.Dial("tcp", c.Addr); err != nil {
				logError("rpc.Client: [%s]. Cannot establish rpc connection: [%s]", c.Addr, err)
				time.Sleep(time.Second)
			}
			dialChan <- struct{}{}
		}()

		select {
		case <-c.clientStopChan:
			return
		case <-dialChan:
		}

		if err != nil {
			continue
		}
		clientHandleConnection(c, conn)
	}
}

func clientHandleConnection(c *Client, conn net.Conn) {
	var buf [1]byte
	if c.EnableCompression {
		buf[0] = 1
	}
	if _, err := conn.Write(buf[:]); err != nil {
		logError("rpc.Client: [%s]. Error when writing handshake to server: [%s]", c.Addr, err)
		conn.Close()
		return
	}

	stopChan := make(chan struct{})

	pendingRequests := make(map[uint64]*clientMessage)
	var pendingRequestsLock sync.Mutex

	writerDone := make(chan struct{}, 1)
	go clientWriter(c, conn, pendingRequests, &pendingRequestsLock, stopChan, writerDone)

	readerDone := make(chan struct{}, 1)
	go clientReader(c, conn, pendingRequests, &pendingRequestsLock, readerDone)

	select {
	case <-writerDone:
		close(stopChan)
		conn.Close()
		<-readerDone
	case <-readerDone:
		close(stopChan)
		conn.Close()
		<-writerDone
	case <-c.clientStopChan:
		close(stopChan)
		conn.Close()
		<-readerDone
		<-writerDone
	}

	for _, m := range pendingRequests {
		m.Done <- struct{}{}
	}
}

type clientMessage struct {
	Request  interface{}
	Response interface{}
	Done     chan struct{}
}

func clientWriter(c *Client, w io.Writer, pendingRequests map[uint64]*clientMessage, pendingRequestsLock *sync.Mutex, stopChan <-chan struct{}, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	var msgID uint64
	bw := bufio.NewWriterSize(w, c.SendBufferSize)

	ww := bw
	var zw *flate.Writer
	if c.EnableCompression {
		zw, _ = flate.NewWriter(bw, flate.BestSpeed)
		defer zw.Close()
		ww = bufio.NewWriterSize(zw, c.SendBufferSize)
	}
	e := gob.NewEncoder(ww)

	var flushChan <-chan time.Time

	for {
		var rpcM *clientMessage

		select {
		case <-stopChan:
			return
		case rpcM = <-c.requestsChan:
			if flushChan == nil {
				flushChan = time.After(c.FlushDelay)
			}
		case <-flushChan:
			if c.EnableCompression {
				if err := ww.Flush(); err != nil {
					logError("rpc.Client: [%s]. Cannot flush data to compressed stream: [%s]", c.Addr, err)
					return
				}
				if err := zw.Flush(); err != nil {
					logError("rpc.Client: [%s]. Cannot flush compressed data to wire: [%s]", c.Addr, err)
					return
				}
			}
			if err := bw.Flush(); err != nil {
				logError("rpc.Client: [%s]. Cannot flush requests to wire: [%s]", c.Addr, err)
				return
			}
			flushChan = nil
			continue
		}

		msgID++
		pendingRequestsLock.Lock()
		pendingRequests[msgID] = rpcM
		pendingRequestsLock.Unlock()

		m := wireMessage{
			ID:   msgID,
			Data: rpcM.Request,
		}
		if err := e.Encode(&m); err != nil {
			logError("rpc.Client: [%s]. Cannot send request to wire: [%s]", c.Addr, err)
			rpcM.Done <- struct{}{}
			pendingRequestsLock.Lock()
			delete(pendingRequests, msgID)
			pendingRequestsLock.Unlock()
			return
		}
	}
}

func clientReader(c *Client, r io.Reader, pendingRequests map[uint64]*clientMessage, pendingRequestsLock *sync.Mutex, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	br := bufio.NewReaderSize(r, c.RecvBufferSize)

	rr := br
	if c.EnableCompression {
		zr := flate.NewReader(br)
		defer zr.Close()
		rr = bufio.NewReaderSize(zr, c.RecvBufferSize)
	}
	d := gob.NewDecoder(rr)

	for {
		var m wireMessage
		if err := d.Decode(&m); err != nil {
			logError("rpc.Client: [%s]. Cannot read response from wire: [%s]", c.Addr, err)
			return
		}

		pendingRequestsLock.Lock()
		rpcM, ok := pendingRequests[m.ID]
		delete(pendingRequests, m.ID)
		pendingRequestsLock.Unlock()
		if !ok {
			logError("rpc.Client: [%s]. Unexpected msgID=[%d] obtained from server", c.Addr, m.ID)
			return
		}

		rpcM.Response = m.Data
		rpcM.Done <- struct{}{}
	}
}
