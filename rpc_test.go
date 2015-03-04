package gorpc

import (
	"fmt"
	"io"
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

func TestServerServe(t *testing.T) {
	s := &Server{
		Addr:    ":15344",
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
		Addr:    ":15345",
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
	s := &Server{
		Addr:    ":15346",
		Handler: echoHandler,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr:  ":15346",
		Conns: 3,
	}
	for i := 0; i < 5; i++ {
		c.Start()
		time.Sleep(time.Millisecond * 10)
		c.Stop()
	}
}

func TestRequestTimeout(t *testing.T) {
	s := &Server{
		Addr: ":15357",
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
		Addr:           ":15357",
		RequestTimeout: time.Millisecond,
	}
	c.Start()
	defer c.Stop()

	for i := 0; i < 10; i++ {
		resp, err := c.Call(123)
		if err == nil {
			t.Fatalf("Timeout error must be returned")
		}
		if resp != nil {
			t.Fatalf("Unexpected response %+v: expected nil", resp)
		}
	}
}

func TestCallTimeout(t *testing.T) {
	s := &Server{
		Addr: ":15358",
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
		Addr: ":15358",
	}
	c.Start()
	defer c.Stop()

	for i := 0; i < 10; i++ {
		resp, err := c.CallTimeout(123, time.Millisecond)
		if err == nil {
			t.Fatalf("Timeout error must be returned")
		}
		if resp != nil {
			t.Fatalf("Unexpected response %+v: expected nil", resp)
		}
	}
}

func TestNoServer(t *testing.T) {
	c := &Client{
		Addr:           ":16368",
		RequestTimeout: 100 * time.Millisecond,
	}
	c.Start()
	defer c.Stop()

	resp, err := c.Call("foobar")
	if err == nil {
		t.Fatalf("Unexpected nil error")
	}
	if resp != nil {
		t.Fatalf("Unepxected response: %+v. Expected nil", resp)
	}
}

func TestServerPanic(t *testing.T) {
	s := &Server{
		Addr: ":16358",
		Handler: func(clientAddr string, request interface{}) interface{} {
			panic("server panic")
		},
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: ":16358",
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
	s := &Server{
		Addr: ":16359",
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
		Addr:            ":16359",
		PendingRequests: 100,
		RequestTimeout:  300 * time.Millisecond,
	}
	c.Start()
	defer c.Stop()

	startT := time.Now()
	timeoutErrors := 0
	stuckErrors := 0

	var wg sync.WaitGroup
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := c.Call("abc")
			if err == nil {
				t.Fatalf("Stuck server returned response? %+v", resp)
			}
			if resp != nil {
				t.Fatalf("Unexpected response from stuck server: %+v", resp)
			}
			if time.Since(startT) > c.RequestTimeout {
				timeoutErrors++
			} else {
				stuckErrors++
			}
		}()
	}
	wg.Wait()

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
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return &customListener{
		remoteAddr: remoteAddr,
		r:          r,
		w:          w,
		ch:         ch,
	}
}

func (ln *customListener) Accept(addr string) (conn io.ReadWriteCloser, clientAddr string, err error) {
	_, ok := <-ln.ch
	if !ok {
		return nil, "", fmt.Errorf("Listener is closed")
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

func TestIntHandler(t *testing.T) {
	s := &Server{
		Addr:    ":15347",
		Handler: func(clientAddr string, request interface{}) interface{} { return request.(int) + 234 },
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: ":15347",
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
	s := &Server{
		Addr:    ":15348",
		Handler: func(clientAddr string, request interface{}) interface{} { return request.(string) + " world" },
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: ":15348",
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

	s := &Server{
		Addr:    ":15349",
		Handler: func(clientAddr string, request interface{}) interface{} { return request.(*S) },
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: ":15349",
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

	s := &Server{
		Addr:    ":15350",
		Handler: func(clientAddr string, request interface{}) interface{} { return request },
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr: ":15350",
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
	s := &Server{
		Addr:       ":15351",
		Handler:    func(clientAddr string, request interface{}) interface{} { return request },
		FlushDelay: time.Millisecond,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c := &Client{
		Addr:       ":15351",
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
	s := &Server{
		Addr:       ":15352",
		Handler:    func(clientAddr string, request interface{}) interface{} { return request },
		FlushDelay: time.Millisecond,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start() failed: [%s]", err)
	}
	defer s.Stop()

	c1 := &Client{
		Addr:               ":15352",
		Conns:              2,
		FlushDelay:         time.Millisecond,
		DisableCompression: true,
	}
	c1.Start()
	defer c1.Stop()

	c2 := &Client{
		Addr:               ":15352",
		FlushDelay:         2 * time.Millisecond,
		DisableCompression: false,
	}
	c2.Start()
	defer c2.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
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
