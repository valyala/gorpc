package gorpc

import (
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	SetErrorLogger(NilErrorLogger)
}

func echoHandler(clientAddr string, request interface{}) interface{} {
	return request
}

func getRandomAddr() string {
	return fmt.Sprintf(":%d", rand.Intn(20000)+10000)
}

func TestBadClient(t *testing.T) {
	addr := getRandomAddr()
	s := NewTCPServer(addr, echoHandler)
	s.Start()
	defer s.Stop()

	for i := 0; i < 10; i++ {
		sendBadInput(t, addr, 0)
		sendBadInput(t, addr, 1)
	}

	time.Sleep(10 * time.Millisecond)
}

func sendBadInput(t *testing.T, addr string, isCompressed byte) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot establish connection to server on addr=[%s]: [%s]", addr, err)
	}

	data := randomData(65536)
	data[0] = isCompressed
	conn.Write(data)
	conn.Close()
}

func randomData(n int) []byte {
	data := make([]byte, n)
	for i := 0; i < n; i++ {
		data[i] = byte(rand.Int())
	}
	return data
}

func TestBadServer(t *testing.T) {
	addr := getRandomAddr()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("cannot listen on [%s]: [%s]", addr, err)
	}

	doneCh := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				close(doneCh)
				return
			}

			go func() {
				buf := make([]byte, 4096)
				conn.Read(buf)
				conn.Write(randomData(65536))
				conn.Close()
			}()
		}
	}()

	// enabled compression
	c := NewTCPClient(addr)
	c.DisableCompression = false
	c.Start()
	for i := 0; i < 10; i++ {
		c.Call("foobarbaz")
	}
	c.Stop()

	// disabled compression
	c = NewTCPClient(addr)
	c.DisableCompression = true
	c.Start()
	for i := 0; i < 10; i++ {
		c.Call("foobarbaz")
	}
	c.Stop()

	ln.Close()
	<-doneCh
}

func TestClientDoubleStart(t *testing.T) {
	c := NewTCPClient(getRandomAddr())
	c.Start()
	defer c.Stop()

	testPanic(t, func() {
		c.Start()
	})
}

func TestServerDoubleStart(t *testing.T) {
	s := NewTCPServer(getRandomAddr(), echoHandler)
	if err := s.Start(); err != nil {
		t.Fatalf("unexpected error when starting server: [%s]", err)
	}
	defer s.Stop()

	testPanic(t, func() {
		if err := s.Start(); err != nil {
			t.Fatalf("unexpected error when starting server: [%s]", err)
		}
	})
}

func TestStopStoppedClient(t *testing.T) {
	c := NewTCPClient(getRandomAddr())
	testPanic(t, func() {
		c.Stop()
	})
}

func TestStopStoppedServer(t *testing.T) {
	s := NewTCPServer(getRandomAddr(), echoHandler)
	testPanic(t, func() {
		s.Stop()
	})
}

func TestServerServe(t *testing.T) {
	s := &Server{
		Addr:    getRandomAddr(),
		Handler: echoHandler,
	}
	go func() {
		time.Sleep(time.Millisecond * 100)
		s.Stop()
	}()
	if err := s.Serve(); err != nil {
		t.Fatalf("Server.Serve() shouldn't return error. Returned [%s]", err)
	}
}

func TestServerStartStop(t *testing.T) {
	s := &Server{
		Addr:    getRandomAddr(),
		Handler: echoHandler,
	}
	for i := 0; i < 5; i++ {
		if err := s.Start(); err != nil {
			t.Fatalf("Server.Start() shouldn't return error. Returned [%s]", err)
		}
		s.Stop()
	}
}

func TestClientStartStop(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr:    addr,
		Handler: echoHandler,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr:  addr,
		Conns: 3,
	}
	for i := 0; i < 5; i++ {
		c.Start()
		time.Sleep(time.Millisecond * 10)
		c.Stop()
	}
}

func TestRequestTimeout(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr: addr,
		Handler: func(clientAddr string, request interface{}) interface{} {
			time.Sleep(10 * time.Second)
			return request
		},
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr:           addr,
		RequestTimeout: time.Millisecond,
	}
	c.Start()
	defer c.Stop()

	for i := 0; i < 10; i++ {
		resp, err := c.Call(123)
		if err == nil {
			t.Fatalf("Timeout error must be returned")
		}
		if !err.(*ClientError).Timeout {
			t.Fatalf("Unexpected error returned: [%s]", err)
		}
		if resp != nil {
			t.Fatalf("Unexpected response %+v: expected nil", resp)
		}
	}
}

func TestCallTimeout(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr: addr,
		Handler: func(clientAddr string, request interface{}) interface{} {
			time.Sleep(10 * time.Second)
			return request
		},
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: addr,
	}
	c.Start()
	defer c.Stop()

	for i := 0; i < 10; i++ {
		resp, err := c.CallTimeout(123, time.Millisecond)
		if err == nil {
			t.Fatalf("Timeout error must be returned")
		}
		if !err.(*ClientError).Timeout {
			t.Fatalf("Unexpected error returned: [%s]", err)
		}
		if resp != nil {
			t.Fatalf("Unexpected response %+v: expected nil", resp)
		}
	}
}

func TestNoServer(t *testing.T) {
	c := &Client{
		Addr:           getRandomAddr(),
		RequestTimeout: 100 * time.Millisecond,
	}
	c.Start()
	defer c.Stop()

	resp, err := c.Call("foobar")
	if err == nil {
		t.Fatalf("Timeout error must be returned")
	}
	if !err.(*ClientError).Timeout {
		t.Fatalf("Unexpected error: [%s]", err)
	}
	if resp != nil {
		t.Fatalf("Unepxected response: %+v. Expected nil", resp)
	}
}

func TestServerPanic(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr: addr,
		Handler: func(clientAddr string, request interface{}) interface{} {
			if request.(string) == "foobar" {
				panic("server panic")
			}
			return request
		},
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: addr,
	}
	c.Start()
	defer c.Stop()

	for i := 0; i < 3; i++ {
		resp, err := c.Call("foobar")
		if err == nil {
			t.Fatalf("Unexpected nil error")
		}
		if resp != nil {
			t.Fatalf("Unepxected response for panicing server: %+v. Expected nil", resp)
		}
		if !err.(*ClientError).Server {
			t.Fatalf("Unexpected error type: %v. Expected server error", err)
		}
	}

	for i := 0; i < 3; i++ {
		resp, err := c.Call("aaaaa")
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		if resp == nil {
			t.Fatalf("Unepxected nil response")
		}
		if resp.(string) != "aaaaa" {
			t.Fatalf("Unexpected response: %v. Expected 'aaaaa'", resp)
		}
	}
}

func TestServerStuck(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr: addr,
		Handler: func(clientAddr string, request interface{}) interface{} {
			time.Sleep(time.Second)
			return "aaa"
		},
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr:            addr,
		PendingRequests: 100,
	}
	c.Start()
	defer c.Stop()

	var res [1500]*AsyncResult
	var err error
	for j := 0; j < 15; j++ {
		for i := 0; i < 100; i++ {
			res[i+100*j], err = c.CallAsync("abc")
			if err != nil {
				t.Fatalf("%d. Unexpected error in CallAsync: [%s]", j*100+i, err)
			}
		}
		// This should prevent from overflow errors.
		time.Sleep(20 * time.Millisecond)
	}

	stuckErrors := 0

	timer := acquireTimer(300 * time.Millisecond)
	defer releaseTimer(timer)

	for i := 0; i < 1500; i++ {
		r := res[i]
		select {
		case <-r.Done:
		case <-timer.C:
			goto exit
		}

		if r.Error == nil {
			t.Fatalf("Stuck server returned response? %+v", r.Response)
		}
		ce := r.Error.(*ClientError)
		if ce.Connection {
			stuckErrors++
		} else if !ce.Overflow {
			t.Fatalf("Unexpected error returned: [%s]", ce)
		}
		if r.Response != nil {
			t.Fatalf("Unexpected response from stuck server: %+v", r.Response)
		}
	}
exit:

	if stuckErrors == 0 {
		t.Fatalf("Stuck server detector doesn't work?")
	}
}

type customConn struct {
	r io.ReadCloser
	w io.WriteCloser
}

func (c *customConn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *customConn) Write(p []byte) (int, error) {
	return c.w.Write(p)
}

func (c *customConn) Close() error {
	c.r.Close()
	return c.w.Close()
}

type customListener struct {
	remoteAddr string
	r          io.ReadCloser
	w          io.WriteCloser
	ch         chan struct{}
}

func newCustomListener(remoteAddr string, r io.ReadCloser, w io.WriteCloser) *customListener {
	return &customListener{
		remoteAddr: remoteAddr,
		r:          r,
		w:          w,
	}
}

func (ln *customListener) Init(addr string) error {
	ln.ch = make(chan struct{}, 1)
	ln.ch <- struct{}{}
	return nil
}

func (ln *customListener) Accept() (conn io.ReadWriteCloser, clientAddr string, err error) {
	_, ok := <-ln.ch
	if !ok {
		return nil, "", fmt.Errorf("listener is closed")
	}
	return &customConn{
		r: ln.r,
		w: ln.w,
	}, ln.remoteAddr, nil
}

func (ln *customListener) Close() error {
	close(ln.ch)
	return nil
}

func TestCustomTransport(t *testing.T) {
	rc, ws := io.Pipe()
	rs, wc := io.Pipe()

	s := &Server{
		Listener: newCustomListener("foobar", rs, ws),
		Handler: func(clientAddr string, request interface{}) interface{} {
			if clientAddr != "foobar" {
				t.Fatalf("Unexpected client address: [%s]. Expected [foobar]", clientAddr)
			}
			return request
		},
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Conns: 1,
		Dial: func(addr string) (conn io.ReadWriteCloser, err error) {
			return &customConn{
				r: rc,
				w: wc,
			}, nil
		},
	}
	c.Start()
	defer c.Stop()

	testIntClient(t, c)
}

func testIntClient(t *testing.T, c *Client) {
	for i := 0; i < 10; i++ {
		resp, err := c.Call(i)
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		x, ok := resp.(int)
		if !ok {
			t.Fatalf("Unexpected response type: %T. Expected int", resp)
		}
		if x != i {
			t.Fatalf("Unexpected value returned: %d. Expected %d", x, i)
		}
	}
}

type onConnectRwcWrapper struct {
	rwc io.ReadWriteCloser
	t   *testing.T
}

func (w *onConnectRwcWrapper) Read(p []byte) (int, error) {
	n, err := w.rwc.Read(p)
	sillyDecrypt(p)
	return n, err
}

func (w *onConnectRwcWrapper) Write(p []byte) (int, error) {
	sillyEncrypt(p)
	return w.rwc.Write(p)
}

func (w *onConnectRwcWrapper) Close() error {
	return w.rwc.Close()
}

func sillyEncrypt(p []byte) {
	for i := 0; i < len(p); i++ {
		p[i] ^= 42
	}
}

func sillyDecrypt(p []byte) {
	sillyEncrypt(p)
}

func newOnConnectFunc(t *testing.T) OnConnectFunc {
	return func(remoteAddr string, rwc io.ReadWriteCloser) (io.ReadWriteCloser, error) {
		return &onConnectRwcWrapper{
			rwc: rwc,
			t:   t,
		}, nil
	}
}

func TestOnConnect(t *testing.T) {
	addr := "./test-onconnect.sock"
	s := NewUnixServer(addr, echoHandler)
	s.OnConnect = newOnConnectFunc(t)
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewUnixClient(addr)
	c.OnConnect = newOnConnectFunc(t)
	c.Start()
	defer c.Stop()

	testIntClient(t, c)
}

func TestConcurrency(t *testing.T) {
	addr := getRandomAddr()
	s := NewTCPServer(addr, func(clientAddr string, request interface{}) interface{} {
		time.Sleep(time.Duration(request.(int)) * time.Millisecond)
		return request
	})
	s.Concurrency = 2
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewTCPClient(addr)
	c.Start()
	defer c.Stop()

	if err := c.Send(100); err != nil {
		t.Fatalf("Unepxected error in Send(): [%s]", err)
	}
	if err := c.Send(100); err != nil {
		t.Fatalf("Unepxected error in Send(): [%s]", err)
	}

	resp, err := c.CallTimeout(5, 50*time.Millisecond)
	if err == nil {
		t.Fatalf("Unexpected nil error")
	}
	if !err.(*ClientError).Timeout {
		t.Fatalf("Unexepcted error type: %v. Expected timeout error", err)
	}

	resp, err = c.CallTimeout(34, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Unexpected error: [%s]", err)
	}
	if resp.(int) != 34 {
		t.Fatalf("Unexpected response: [%d]. Expected [34]", resp)
	}
}

func TestTCPTransport(t *testing.T) {
	addr := getRandomAddr()
	s := NewTCPServer(addr, echoHandler)
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewTCPClient(addr)
	c.Start()
	defer c.Stop()

	testIntClient(t, c)
}

func TestUnixTransport(t *testing.T) {
	addr := "./gorpc-test-sock.unix"
	s := NewUnixServer(addr, echoHandler)
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewUnixClient(addr)
	c.Start()
	defer c.Stop()

	testIntClient(t, c)
}

func TestTLSTransport(t *testing.T) {
	certFile := "./ssl-cert-snakeoil.pem"
	keyFile := "./ssl-cert-snakeoil.key"
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("Cannot load TLS certificates: [%s]", err)
	}
	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	clientCfg := &tls.Config{
		InsecureSkipVerify: true,
	}

	addr := getRandomAddr()
	s := NewTLSServer(addr, echoHandler, serverCfg)
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewTLSClient(addr, clientCfg)
	c.Start()
	defer c.Stop()

	testIntClient(t, c)
}

func TestNoRequestBufferring(t *testing.T) {
	testNoBufferring(t, -1, DefaultFlushDelay)
}

func TestNoResponseBufferring(t *testing.T) {
	testNoBufferring(t, DefaultFlushDelay, -1)
}

func TestNoBufferring(t *testing.T) {
	testNoBufferring(t, DefaultFlushDelay, DefaultFlushDelay)
}

func testNoBufferring(t *testing.T, requestFlushDelay, responseFlushDelay time.Duration) {
	addr := getRandomAddr()
	s := &Server{
		Addr:       addr,
		Handler:    echoHandler,
		FlushDelay: responseFlushDelay,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr:           addr,
		RequestTimeout: 100 * time.Millisecond,
		FlushDelay:     requestFlushDelay,
	}
	c.Start()
	defer c.Stop()

	var wg sync.WaitGroup
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			testIntClient(t, c)
		}()
	}
	wg.Wait()
}

func TestSendNil(t *testing.T) {
	testSend(t, nil)
}

func TestSendInt(t *testing.T) {
	testSend(t, 12345)
}

func TestSendString(t *testing.T) {
	testSend(t, "foobar")
}

func testSend(t *testing.T, value interface{}) {
	var wg sync.WaitGroup

	addr := getRandomAddr()
	s := &Server{
		Addr: addr,
		Handler: func(clientAddr string, request interface{}) interface{} {
			if !reflect.DeepEqual(value, request) {
				t.Fatalf("Unexpected request: %#v. Expected %#v", request, value)
			}
			wg.Done()
			return "foobar_ignored"
		},
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: addr,
	}
	c.Start()
	defer c.Stop()

	wg.Add(100)
	for i := 0; i < 100; i++ {
		if err := c.Send(value); err != nil {
			t.Fatalf("Unexpected error in Send(): [%s]", err)
		}
	}
	wg.Wait()
}

func TestMixedCallSend(t *testing.T) {
	addr := getRandomAddr()
	s := NewTCPServer(addr, echoHandler)
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewTCPClient(addr)
	c.Start()
	defer c.Stop()

	for i := 0; i < 2; i++ {
		for i := 0; i < 1000; i++ {
			if err := c.Send("123211"); err != nil {
				t.Fatalf("Unexpected error in Send(): [%s]", err)
			}
		}
		testIntClient(t, c)
	}
}

func TestCallAsync(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr:    addr,
		Handler: echoHandler,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: addr,
	}
	c.Start()
	defer c.Stop()

	var res [10]*AsyncResult
	var err error
	for i := 0; i < 10; i++ {
		res[i], err = c.CallAsync(i)
		if err != nil {
			t.Fatalf("Unexpected error in CallAsync: [%s]", err)
		}
	}
	for i := 0; i < 10; i++ {
		r := res[i]
		<-r.Done
		if r.Error != nil {
			t.Fatalf("Unexpected error: [%s]", r.Error)
		}
		x, ok := r.Response.(int)
		if !ok {
			t.Fatalf("Unexpected response type: %T. Expected int", r.Response)
		}
		if x != i {
			t.Fatalf("Unexpected value returned: %d. Expected %d", x, i)
		}
	}
}

func TestClientPendingRequestsCount(t *testing.T) {
	addr := "./client-pending-requests-count.sock"
	respCh := make(chan struct{})
	reqCh := make(chan struct{}, 1)
	s := NewUnixServer(addr, func(clientAddr string, request interface{}) interface{} {
		reqCh <- struct{}{}
		<-respCh
		return request
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewUnixClient(addr)
	c.Start()
	defer c.Stop()

	if c.PendingRequestsCount() != 0 {
		t.Fatalf("unexpected number of pending requests: %d. Expected 0", c.PendingRequestsCount())
	}
	var resps []*AsyncResult
	for i := 0; i < 10; i++ {
		resp, err := c.CallAsync(nil)
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		select {
		case <-reqCh:
		case <-time.After(time.Second):
			t.Fatalf("it looks like server is stuck")
		}
		if c.PendingRequestsCount() != i+1 {
			t.Fatalf("unexpected number of pending request: %d. Expected %d", c.PendingRequestsCount(), i+1)
		}
		resps = append(resps, resp)
	}

	close(respCh)
	for _, resp := range resps {
		select {
		case <-resp.Done:
		case <-time.After(time.Second):
			t.Fatalf("it looks like rpc is broken")
		}
	}
	if c.PendingRequestsCount() != 0 {
		t.Fatalf("unexpected number of pending requests: %d. Expected 0", c.PendingRequestsCount())
	}
}

func TestNilHandler(t *testing.T) {
	addr := "./test-nil-handler.sock"
	s := NewUnixServer(addr, func(clientAddr string, request interface{}) interface{} {
		if request != nil {
			t.Fatalf("Unexpected request: %#v. Expected nil", request)
		}
		return nil
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewUnixClient(addr)
	c.Start()
	defer c.Stop()

	for i := 0; i < 10; i++ {
		resp, err := c.Call(nil)
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		if resp != nil {
			t.Fatalf("Unexpected response: %#v. Expected nil", resp)
		}
	}
}

func TestAsyncResultCancel(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr: addr,
		Handler: func(clientAddr string, request interface{}) interface{} {
			time.Sleep(time.Millisecond * 100)
			return request
		},
		Concurrency: 1,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr:           addr,
		SendBufferSize: 2,
	}
	c.Start()
	defer c.Stop()

	slowRes, err := c.CallAsync(123)
	if err != nil {
		t.Fatalf("unexpected error: [%s]", err)
	}

	var canceledResults []*AsyncResult
	for i := 0; i < 10; i++ {
		res, err := c.CallAsync(456)
		if err != nil {
			t.Fatalf("unexpected error when sending request #%d: [%s]", i, err)
		}
		res.Cancel()
		canceledResults = append(canceledResults, res)
	}

	select {
	case <-slowRes.Done:
	case <-time.After(time.Second):
		t.Fatalf("timeout")
	}

	canceledCalls := 0
	for _, res := range canceledResults {
		select {
		case <-res.Done:
		case <-time.After(time.Second):
			t.Fatalf("timeout")
		}
		if res.Error != nil {
			ce := res.Error.(*ClientError)
			if ce.Canceled {
				canceledCalls++
			}
		}
	}

	if canceledCalls == 0 {
		t.Fatalf("expecting at least one canceled call")
	}
}

func TestIntHandler(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr:    addr,
		Handler: func(clientAddr string, request interface{}) interface{} { return request.(int) + 234 },
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: addr,
	}
	c.Start()
	defer c.Stop()

	for i := 0; i < 10; i++ {
		resp, err := c.Call(i)
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		x, ok := resp.(int)
		if !ok {
			t.Fatalf("Unexpected response type: %T. Expected int", resp)
		}
		if x != i+234 {
			t.Fatalf("Unexpected value returned: %d. Expected %d", x, i+234)
		}
	}
}

func TestStringHandler(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr:    addr,
		Handler: func(clientAddr string, request interface{}) interface{} { return request.(string) + " world" },
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: addr,
	}
	c.Start()
	defer c.Stop()

	for i := 0; i < 10; i++ {
		resp, err := c.Call(fmt.Sprintf("hello %d,", i))
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		x, ok := resp.(string)
		if !ok {
			t.Fatalf("Unexpected response type: %T. Expected string", resp)
		}
		y := fmt.Sprintf("hello %d, world", i)
		if x != y {
			t.Fatalf("Unexpected value returned: [%s]. Expected [%s]", x, y)
		}
	}
}

func TestStructHandler(t *testing.T) {
	type S struct {
		A int
		B string
	}
	RegisterType(&S{})

	addr := getRandomAddr()
	s := &Server{
		Addr:    addr,
		Handler: func(clientAddr string, request interface{}) interface{} { return request.(*S) },
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: addr,
	}
	c.Start()
	defer c.Stop()

	for i := 0; i < 10; i++ {
		resp, err := c.Call(&S{
			A: i,
			B: fmt.Sprintf("aaa %d", i),
		})
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		x, ok := resp.(*S)
		if !ok {
			t.Fatalf("Unexpected response type: %T. Expected S", resp)
		}
		y := fmt.Sprintf("aaa %d", i)
		if x.A != i || x.B != y {
			t.Fatalf("Unexpected value returned: [%+v]. Expected S{A:%d,B:%s}", x, i, y)
		}
	}
}

func TestEchoHandler(t *testing.T) {
	type SS struct {
		A int
		B string
		T time.Time
	}
	RegisterType(&SS{})
	RegisterType(&time.Time{})

	addr := getRandomAddr()
	s := &Server{
		Addr:    addr,
		Handler: echoHandler,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: addr,
	}
	c.Start()
	defer c.Stop()

	resp, err := c.Call(1234)
	if err != nil {
		t.Fatalf("Unexpected error: [%s]", err)
	}
	expInt, ok := resp.(int)
	if !ok {
		t.Fatalf("Unexpected response type: %T. Expected int", resp)
	}
	if expInt != 1234 {
		t.Fatalf("Unexpected value returned: %d. Expected 1234", expInt)
	}

	resp, err = c.Call("abc")
	if err != nil {
		t.Fatalf("Unexpected error: [%s]", err)
	}
	expStr, ok := resp.(string)
	if !ok {
		t.Fatalf("Unexpected response type: %T. Expected string", resp)
	}
	if expStr != "abc" {
		t.Fatalf("Unexpected value returned: %s. Expected 'abc'", expStr)
	}

	tt := time.Now()
	resp, err = c.Call(tt)
	if err != nil {
		t.Fatalf("Unexpected error: [%s]", err)
	}
	expT, ok := resp.(*time.Time)
	if !ok {
		t.Fatalf("Unexpected response type: %T. Expected time.Time", resp)
	}
	if *expT != tt {
		t.Fatalf("Unexpected value returned: %s. Expected %s", *expT, tt)
	}

	sS := &SS{A: 432, B: "ssd", T: tt}
	resp, err = c.Call(sS)
	if err != nil {
		t.Fatalf("Unexpected error: [%s]", err)
	}
	expSs, ok := resp.(*SS)
	if !ok {
		t.Fatalf("Unexpected response type: %T. Expected SS", resp)
	}
	if expSs.A != 432 || expSs.B != "ssd" || expSs.T != tt {
		t.Fatalf("Unexpected value returned: %#v. Expected %#v", expSs, sS)
	}
}

func TestConcurrentCall(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr:       addr,
		Handler:    echoHandler,
		FlushDelay: time.Millisecond,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr:       addr,
		Conns:      2,
		FlushDelay: time.Millisecond,
	}
	c.Start()
	defer c.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				resp, err := c.Call(j)
				if err != nil {
					t.Fatalf("Unexpected error: [%s]", err)
				}
				if resp.(int) != j {
					t.Fatalf("Unexpected value: %d. Expected %d", resp, j)
				}
			}
		}()
	}
	wg.Wait()
}

func TestCompress(t *testing.T) {
	addr := getRandomAddr()
	s := &Server{
		Addr:       addr,
		Handler:    echoHandler,
		FlushDelay: time.Millisecond,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c1 := &Client{
		Addr:               addr,
		Conns:              2,
		FlushDelay:         time.Millisecond,
		DisableCompression: true,
	}
	c1.Start()
	defer c1.Stop()

	c2 := &Client{
		Addr:               addr,
		FlushDelay:         time.Millisecond,
		DisableCompression: false,
	}
	c2.Start()
	defer c2.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				s := fmt.Sprintf("foo bar baz %d aaabbb", j)
				resp, err := c1.Call(s)
				if err != nil {
					t.Fatalf("Unexpected error: [%s]", err)
				}
				if resp.(string) != s {
					t.Fatalf("Unexpected value: %s. Expected %s", resp, s)
				}
				resp, err = c2.Call(i + j)
				if err != nil {
					t.Fatalf("Unexpected error: [%s]", err)
				}
				if resp.(int) != i+j {
					t.Fatalf("Unexpected value: %d. Expected %d", resp, i+j)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestBatchCall(t *testing.T) {
	addr := "./test-batch-call.sock"
	s := NewUnixServer(addr, echoHandler)
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewUnixClient(addr)
	c.Start()
	defer c.Stop()

	b := c.NewBatch()

	N := 100
	results := make([]*BatchResult, N)
	for i := 0; i < N; i++ {
		r := b.Add(i)

		select {
		case <-r.Done:
			t.Fatalf("%d. <-Done must be locked before Batch.Call()", i)
		default:
		}

		results[i] = r
	}
	if err := b.Call(); err != nil {
		t.Fatalf("Unexpected error when calling batch rpcs: [%s]", err)
	}

	for i := 0; i < N; i++ {
		r := results[i]
		if r.Error != nil {
			t.Fatalf("Unexpected error in batch result %d: [%s]", i, r.Error)
		}
		if r.Response.(int) != i {
			t.Fatalf("Unexpected response in batch result %d: %+v", i, r.Response)
		}

		select {
		case <-r.Done:
		case <-time.After(10 * time.Millisecond):
			t.Fatalf("%d BatchResult.Done must be unblocked after Batch.Call()", i)
		}
	}
}

func TestBatchCallTimeout(t *testing.T) {
	addr := "./test-batch-call-timeout.sock"
	s := NewUnixServer(addr, func(remoteAddr string, request interface{}) interface{} {
		time.Sleep(200 * time.Millisecond)
		return 123
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewUnixClient(addr)
	c.Start()
	defer c.Stop()

	b := c.NewBatch()

	N := 100
	results := make([]*BatchResult, N)
	for i := 0; i < N; i++ {
		r := b.Add(i)

		select {
		case <-r.Done:
			t.Fatalf("%d. <-Done must be locked before Batch.Call()", i)
		default:
		}

		results[i] = r
	}
	err := b.CallTimeout(10 * time.Millisecond)
	if err == nil {
		t.Fatalf("Unexpected nil error when calling Batch.CallTimeout()")
	}
	if !err.(*ClientError).Timeout {
		t.Fatalf("Unexpected error in Batch.CallTimeout(): [%s]", err)
	}

	for i := 0; i < N; i++ {
		r := results[i]
		if r.Error == nil {
			t.Fatalf("Unexpected nil error in batch result %d", i)
		}
		if !r.Error.(*ClientError).Timeout {
			t.Fatalf("Unexpected error in batch result %d: [%s]", i, r.Error)
		}
		if r.Response != nil {
			t.Fatalf("Unexpected response in batch result %d: %+v", i, r.Response)
		}

		select {
		case <-r.Done:
		default:
			t.Fatalf("%d BatchResult.Done must be unblocked after Batch.Call()", i)
		}
	}
}

func TestBatchCallSkipResponse(t *testing.T) {
	N := 100
	serverCallsCounter := uint32(0)
	doneCh := make(chan struct{})

	addr := "./test-batch-call-skip-response.sock"
	s := NewUnixServer(addr, func(remoteAdrr string, request interface{}) interface{} {
		n := atomic.AddUint32(&serverCallsCounter, 1)
		if n == uint32(N) {
			close(doneCh)
		}
		return nil
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewUnixClient(addr)
	c.Start()
	defer c.Stop()

	b := c.NewBatch()
	for i := 0; i < N; i++ {
		b.AddSkipResponse(i)
	}
	if err := b.Call(); err != nil {
		t.Fatalf("Unexpected error when calling Batch.CallTimeout(): [%s]", err)
	}

	select {
	case <-doneCh:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("It looks like server didn't receive batched requests")
	}
}

func TestBatchCallMixed(t *testing.T) {
	N := 100
	serverCallsCounter := uint32(0)
	doneCh := make(chan struct{})

	addr := "./test-batch-call-mixed.sock"
	s := NewUnixServer(addr, func(remoteAdrr string, request interface{}) interface{} {
		n := atomic.AddUint32(&serverCallsCounter, 1)
		if n == uint32(N*2) {
			close(doneCh)
		}
		return request
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewUnixClient(addr)
	c.Start()
	defer c.Stop()

	b := c.NewBatch()

	results := make([]*BatchResult, N)
	for i := 0; i < N; i++ {
		r := b.Add(i)

		select {
		case <-r.Done:
			t.Fatalf("%d. <-Done must be locked before Batch.Call()", i)
		default:
		}

		results[i] = r

		b.AddSkipResponse(i + N)
	}
	if err := b.Call(); err != nil {
		t.Fatalf("Unexpected error when calling batch rpcs: [%s]", err)
	}

	for i := 0; i < N; i++ {
		r := results[i]
		if r.Error != nil {
			t.Fatalf("Unexpected error in batch result %d: [%s]", i, r.Error)
		}
		if r.Response.(int) != i {
			t.Fatalf("Unexpected response in batch result %d: %+v", i, r.Response)
		}

		select {
		case <-r.Done:
		default:
			t.Fatalf("%d BatchResult.Done must be unblocked after Batch.Call()", i)
		}
	}

	select {
	case <-doneCh:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("It looks like server didn't receive batched requests")
	}
}
