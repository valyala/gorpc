package gorpc

import (
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func init() {
	SetErrorLogger(func(format string, args ...interface{}) {})
}

func echoHandler(clientAddr string, request interface{}) interface{} {
	return request
}

func getRandomAddr() string {
	return fmt.Sprintf(":%d", rand.Intn(20000)+10000)
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
			panic("server panic")
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

	resp, err := c.Call("foobar")
	if err != nil {
		t.Fatalf("Unexpected error: [%s]", err)
	}
	if resp != nil {
		t.Fatalf("Unepxected response for panicing server: %+v. Expected nil", resp)
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
		RequestTimeout:  300 * time.Millisecond,
	}
	c.Start()
	defer c.Stop()

	var res [1500]*AsyncResult
	for j := 0; j < 15; j++ {
		for i := 0; i < 100; i++ {
			res[i+100*j] = c.CallAsync("abc")
		}
		// This should prevent from overflow errors.
		time.Sleep(10 * time.Millisecond)
	}

	timeoutErrors := 0
	stuckErrors := 0

	for i := 0; i < 1500; i++ {
		r := res[i]
		<-r.Done
		if r.Error == nil {
			t.Fatalf("Stuck server returned response? %+v", r.Response)
		}
		ce := r.Error.(*ClientError)
		if ce.Timeout {
			timeoutErrors++
		} else if ce.Connection {
			stuckErrors++
		} else if !ce.Overflow {
			t.Fatalf("Unexpected error returned: [%s]", ce)
		}
		if r.Response != nil {
			t.Fatalf("Unexpected response from stuck server: %+v", r.Response)
		}
	}

	if timeoutErrors > stuckErrors {
		t.Fatalf("Stuck server detector doesn't work? timeoutErrors=%d > stuckErrors=%d", timeoutErrors, stuckErrors)
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
	cfg := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}

	addr := getRandomAddr()
	s := NewTLSServer(addr, echoHandler, cfg)
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := NewTLSClient(addr, cfg)
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

func TestSend(t *testing.T) {
	var wg sync.WaitGroup

	addr := getRandomAddr()
	s := &Server{
		Addr: addr,
		Handler: func(clientAddr string, request interface{}) interface{} {
			x, ok := request.(int)
			if !ok {
				t.Fatalf("Unexpected request type: %T. Expected int", request)
			}
			if x != 12345 {
				t.Fatalf("Unexpected request: %d. Expected 12345", x)
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
		c.Send(12345)
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
		for i := 0; i < 100; i++ {
			c.Send("123211")
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
	for i := 0; i < 10; i++ {
		res[i] = c.CallAsync(i)
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
	}
	RegisterType(&SS{})

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

	resp, err = c.Call(&SS{A: 432, B: "ssd"})
	if err != nil {
		t.Fatalf("Unexpected error: [%s]", err)
	}
	expSs, ok := resp.(*SS)
	if !ok {
		t.Fatalf("Unexpected response type: %T. Expected SS", resp)
	}
	if expSs.A != 432 || expSs.B != "ssd" {
		t.Fatalf("Unexpected value returned: %+v. Expected SS{A:432,B:'ssd'}", expSs)
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
		go func() {
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
		}()
	}
	wg.Wait()
}
