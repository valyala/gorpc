package gorpc

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type BenchStruct struct {
	StringSlice []string
	IntSlice    []int
	StringMap   map[string]string
}

type NetrpcService struct{}

func (s *NetrpcService) Int(req int, resp *int) error {
	*resp = req
	return nil
}

func (s *NetrpcService) ByteSlice(req []byte, resp *[]byte) error {
	*resp = req
	return nil
}

func (s *NetrpcService) Struct(req *BenchStruct, resp *BenchStruct) error {
	*resp = *req
	return nil
}

func getTCPPipe(b *testing.B) (net.Conn, net.Conn) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("cannot listen to socket: %s", err)
	}

	ch := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			b.Fatalf("cannot accept incoming tcp conn: %s", err)
		}
		ch <- conn
	}()

	addr := ln.Addr().String()
	connC, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("cannot dial %s: %s", addr, err)
	}
	connS := <-ch
	return connC, connS
}

func BenchmarkNetrpcInt(b *testing.B) {
	connC, connS := getTCPPipe(b)
	defer connC.Close()
	defer connS.Close()

	s := rpc.NewServer()
	if err := s.Register(&NetrpcService{}); err != nil {
		b.Fatalf("Error when registering rpc service: %s", err)
	}
	go s.ServeConn(connS)

	c := rpc.NewClient(connC)
	defer c.Close()

	b.SetParallelism(250)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x int
		for i := 0; pb.Next(); i++ {
			if err := c.Call("NetrpcService.Int", i, &x); err != nil {
				b.Fatalf("Unexpected error when calling NetrpcService.Int(%d): %s", i, err)
			}
			if i != x {
				b.Fatalf("Unexpected response: %d. Expected %d", x, i)
			}
		}
	})
}

func BenchmarkNetrpcByteSlice(b *testing.B) {
	connC, connS := getTCPPipe(b)
	defer connC.Close()
	defer connS.Close()

	s := rpc.NewServer()
	if err := s.Register(&NetrpcService{}); err != nil {
		b.Fatalf("Error when registering rpc service: %s", err)
	}
	go s.ServeConn(connS)

	c := rpc.NewClient(connC)
	defer c.Close()

	req := []byte("byte slice byte slice aaa bbb ccc foobar")
	b.SetParallelism(250)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var resp []byte
		for i := 0; pb.Next(); i++ {
			if err := c.Call("NetrpcService.ByteSlice", req, &resp); err != nil {
				b.Fatalf("Unexpected error when calling NetrpcService.ByteSlice(%q): %s", req, err)
			}
			if !bytes.Equal(resp, req) {
				b.Fatalf("Unexpected response: %q. Expected %q", resp, req)
			}
		}
	})
}

func BenchmarkNetrpcStruct(b *testing.B) {
	connC, connS := getTCPPipe(b)
	defer connC.Close()
	defer connS.Close()

	s := rpc.NewServer()
	if err := s.Register(&NetrpcService{}); err != nil {
		b.Fatalf("Error when registering rpc service: %s", err)
	}
	go s.ServeConn(connS)

	c := rpc.NewClient(connC)
	defer c.Close()

	req := &BenchStruct{
		StringSlice: []string{"foo", "bar", "aaa asdfdsfs", "nothwidthstanding"},
		IntSlice:    []int{1, 2, 4, 5, 6, 6, 2, 324, 234324, 23432, 243, 432432},
		StringMap: map[string]string{
			"foo":              "bar",
			"aadsafjdslk":      "afdsasfdsafdsafkjkjkjlkjlkj",
			"kqjlqkwq":         "adsf kajdsf lkajlqkj lkewqrw",
			"112321a dsf fds3": "af",
		},
	}
	b.SetParallelism(250)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var resp BenchStruct
		for i := 0; pb.Next(); i++ {
			if err := c.Call("NetrpcService.Struct", req, &resp); err != nil {
				b.Fatalf("Unexpected error when calling NetrpcService.Struct(%#v): %s", req, err)
			}
			if !reflect.DeepEqual(&resp, req) {
				b.Fatalf("Unexpected response\n%#v\nExpected\n%#v\n", &resp, req)
			}
		}
	})
}

type GorpcService struct{}

func (s *GorpcService) Int(req int) int                      { return req }
func (s *GorpcService) ByteSlice(req []byte) []byte          { return req }
func (s *GorpcService) Struct(req *BenchStruct) *BenchStruct { return req }

func BenchmarkGorpcInt(b *testing.B) {
	addr := getRandomAddr()

	d := NewDispatcher()
	d.AddService("GorpcService", &GorpcService{})

	s := NewTCPServer(addr, d.NewHandlerFunc())
	if err := s.Start(); err != nil {
		b.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop()

	c := NewTCPClient(addr)
	c.Start()
	defer c.Stop()

	dc := d.NewServiceClient("GorpcService", c)

	b.SetParallelism(250)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			v, err := dc.Call("Int", i)
			if err != nil {
				b.Fatalf("Unexpected error when calling GorpcService.Int(%d): %s", i, err)
			}
			x, ok := v.(int)
			if !ok {
				b.Fatalf("Unexpected response type: %T. Expected int", v)
			}
			if i != x {
				b.Fatalf("Unexpected response: %d. Expected %d", x, i)
			}
		}
	})
}

func BenchmarkGorpcByteSlice(b *testing.B) {
	addr := getRandomAddr()

	d := NewDispatcher()
	d.AddService("GorpcService", &GorpcService{})

	s := NewTCPServer(addr, d.NewHandlerFunc())
	if err := s.Start(); err != nil {
		b.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop()

	c := NewTCPClient(addr)
	c.Start()
	defer c.Stop()

	dc := d.NewServiceClient("GorpcService", c)

	req := []byte("byte slice byte slice aaa bbb ccc foobar")
	b.SetParallelism(250)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			v, err := dc.Call("ByteSlice", req)
			if err != nil {
				b.Fatalf("Unexpected error when calling GorpcService.Int(%q): %s", req, err)
			}
			resp, ok := v.([]byte)
			if !ok {
				b.Fatalf("Unexpected response type: %T. Expected []byte", v)
			}
			if !bytes.Equal(resp, req) {
				b.Fatalf("Unexpected response: %q. Expected %q", resp, req)
			}
		}
	})
}

func BenchmarkGorpcStruct(b *testing.B) {
	addr := getRandomAddr()

	d := NewDispatcher()
	d.AddService("GorpcService", &GorpcService{})

	s := NewTCPServer(addr, d.NewHandlerFunc())
	if err := s.Start(); err != nil {
		b.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop()

	c := NewTCPClient(addr)
	c.Start()
	defer c.Stop()

	dc := d.NewServiceClient("GorpcService", c)

	req := &BenchStruct{
		StringSlice: []string{"foo", "bar", "aaa asdfdsfs", "nothwidthstanding"},
		IntSlice:    []int{1, 2, 4, 5, 6, 6, 2, 324, 234324, 23432, 243, 432432},
		StringMap: map[string]string{
			"foo":              "bar",
			"aadsafjdslk":      "afdsasfdsafdsafkjkjkjlkjlkj",
			"kqjlqkwq":         "adsf kajdsf lkajlqkj lkewqrw",
			"112321a dsf fds3": "af",
		},
	}
	b.SetParallelism(250)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			v, err := dc.Call("Struct", req)
			if err != nil {
				b.Fatalf("Unexpected error when calling GorpcService.Int(%#v): %s", req, err)
			}
			resp, ok := v.(*BenchStruct)
			if !ok {
				b.Fatalf("Unexpected response type: %T. Expected *BenchStruct", v)
			}
			if !reflect.DeepEqual(resp, req) {
				b.Fatalf("Unexpected response\n%#v\nExpected\n%#v\n", resp, req)
			}
		}
	})
}

func BenchmarkSendNil1Worker(b *testing.B) {
	benchSendNil(b, 1)
}

func BenchmarkSendNil10Workers(b *testing.B) {
	benchSendNil(b, 10)
}

func BenchmarkSendNil100Workers(b *testing.B) {
	benchSendNil(b, 100)
}

func BenchmarkSendNil1000Workers(b *testing.B) {
	benchSendNil(b, 1000)
}

func BenchSendNil10000Workers(b *testing.B) {
	benchSendNil(b, 10000)
}

func benchSendNil(b *testing.B, workersCount int) {
	var serverCalls uint64
	N := uint64(b.N)
	stopCh := make(chan struct{})

	addr := "./gorpc-bench.sock"
	s := NewUnixServer(addr, func(clientAddr string, request interface{}) interface{} {
		n := atomic.AddUint64(&serverCalls, 1)
		if n == N {
			close(stopCh)
		}
		return nil
	})

	c := NewUnixClient(addr)
	c.Conns = runtime.GOMAXPROCS(-1)
	c.PendingRequests = 100000
	c.DisableCompression = false

	benchClientServerExt(b, workersCount, c, s, func(n int) {
	start:
		if err := c.Send(nil); err != nil {
			if err.(*ClientError).Overflow {
				time.Sleep(time.Millisecond)
				goto start
			}
			b.Fatalf("Unexpected error in Send() on iteration %d: [%s]", n, err)
		}
	}, func() { <-stopCh })
}

func BenchmarkDispatcher1Worker(b *testing.B) {
	benchmarkDispatcher(b, 1)
}

func BenchmarkDispatcher10Workers(b *testing.B) {
	benchmarkDispatcher(b, 10)
}

func BenchmarkDispatcher100Workers(b *testing.B) {
	benchmarkDispatcher(b, 100)
}

func BenchmarkDispatcher1000Workers(b *testing.B) {
	benchmarkDispatcher(b, 1000)
}

func BenchmarkDispatcher10000Workers(b *testing.B) {
	benchmarkDispatcher(b, 10000)
}

type BenchmarkDispatcherService struct {
	n    uint64
	lock sync.Mutex
}

func (s *BenchmarkDispatcherService) AddAtomic(x int) uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.n += uint64(x)
	return s.n
}

func (s *BenchmarkDispatcherService) SubAtomic(y int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.n -= uint64(y)
}

func benchmarkDispatcher(b *testing.B, workersCount int) {
	service := &BenchmarkDispatcherService{}

	d := NewDispatcher()
	d.AddService("RealService", service)

	addr := "./dispatcher-bench.sock"
	s := NewUnixServer(addr, d.NewHandlerFunc())
	c := NewUnixClient(addr)
	dc := d.NewServiceClient("RealService", c)

	var nLock sync.Mutex
	var nLocal uint64

	benchClientServer(b, workersCount, c, s, func(n int) {
		if n%10 < 7 {
			nLock.Lock()
			nLocal += uint64(n)
			nLock.Unlock()

			if _, err := dc.Call("AddAtomic", n); err != nil {
				b.Fatalf("Error in AddAtomic: [%s]", err)
			}
		} else {
			nLock.Lock()
			nLocal -= uint64(n)
			nLock.Unlock()

			if _, err := dc.Call("SubAtomic", n); err != nil {
				b.Fatalf("Error in SubAtomic: [%s]", err)
			}
		}
	})

	if nLocal != service.n {
		b.Fatalf("Unexpected service state=%d. Expected %d", service.n, nLocal)
	}
}

func BenchmarkRealApp1Worker(b *testing.B) {
	simulateRealApp(b, 1)
}

func BenchmarkRealApp10Workers(b *testing.B) {
	simulateRealApp(b, 10)
}

func BenchmarkRealApp100Workers(b *testing.B) {
	simulateRealApp(b, 100)
}

func BenchmarkRealApp1000Workers(b *testing.B) {
	simulateRealApp(b, 1000)
}

func BenchmarkRealApp10000Workers(b *testing.B) {
	simulateRealApp(b, 10000)
}

func BenchmarkRealApp100000Workers(b *testing.B) {
	simulateRealApp(b, 100000)
}

func doRealWork() {
	time.Sleep(time.Duration(5+rand.Intn(10)) * time.Millisecond)
}

func simulateRealApp(b *testing.B, workersCount int) {
	addr := getRandomAddr()
	s := NewTCPServer(addr, func(clientAddr string, request interface{}) interface{} {
		doRealWork()
		return request
	})
	s.PendingResponses = workersCount

	c := NewTCPClient(addr)
	c.Conns = runtime.GOMAXPROCS(-1)
	c.PendingRequests = workersCount

	type realMessage struct {
		FooString string
		FooInt    int
		Map       map[string]string
		Bytes     []byte
	}
	RegisterType(&realMessage{})

	benchClientServer(b, workersCount, c, s, func(n int) {
		doRealWork()
		req := &realMessage{
			FooString: fmt.Sprintf("aaa bbb ccc %d", n),
			FooInt:    n,
			Map: map[string]string{
				"aaa": "bbb",
				"xxx": fmt.Sprintf("cvv%d", n+4),
			},
			Bytes: []byte(",.zssdsdkl23 23"),
		}
		resp, err := c.Call(req)
		if err != nil {
			b.Fatalf("Unexpected response error: [%s]", err)
		}
		x, ok := resp.(*realMessage)
		if !ok {
			b.Fatalf("Unexpected response type %T", resp)
		}
		if x.FooString != req.FooString || x.FooInt != req.FooInt || x.Map["aaa"] != req.Map["aaa"] || x.Map["xxx"] != req.Map["xxx"] || !bytes.Equal(x.Bytes, req.Bytes) {
			b.Fatalf("Unexpected response value %v. Expected %v", x, req)
		}
	})
}

func BenchmarkEchoNil1Worker(b *testing.B) {
	benchEchoNil(b, 1, false, false)
}

func BenchmarkEchoNil10Workers(b *testing.B) {
	benchEchoNil(b, 10, false, false)
}

func BenchmarkEchoNil100Workers(b *testing.B) {
	benchEchoNil(b, 100, false, false)
}

func BenchmarkEchoNil1000Workers(b *testing.B) {
	benchEchoNil(b, 1000, false, false)
}

func BenchmarkEchoNil10000Workers(b *testing.B) {
	benchEchoNil(b, 10000, false, false)
}

func BenchmarkEchoNilUnix1Worker(b *testing.B) {
	benchEchoNil(b, 1, false, true)
}

func BenchmarkEchoNilUnix10Workers(b *testing.B) {
	benchEchoNil(b, 10, false, true)
}

func BenchmarkEchoNilUnix100Workers(b *testing.B) {
	benchEchoNil(b, 100, false, true)
}

func BenchmarkEchoNilUnix1000Workers(b *testing.B) {
	benchEchoNil(b, 1000, false, true)
}

func BenchmarkEchoNilUnix10000Workers(b *testing.B) {
	benchEchoNil(b, 10000, false, true)
}

func BenchmarkEchoNilNocompress1Worker(b *testing.B) {
	benchEchoNil(b, 1, true, false)
}

func BenchmarkEchoNilNocompress10Workers(b *testing.B) {
	benchEchoNil(b, 10, true, false)
}

func BenchmarkEchoNilNocompress100Workers(b *testing.B) {
	benchEchoNil(b, 100, true, false)
}

func BenchmarkEchoNilNocompress1000Workers(b *testing.B) {
	benchEchoNil(b, 1000, true, false)
}

func BenchmarkEchoNilNocompress10000Workers(b *testing.B) {
	benchEchoNil(b, 10000, true, false)
}

func BenchmarkEchoInt1Worker(b *testing.B) {
	benchEchoInt(b, 1, false, false)
}

func BenchmarkEchoInt10Workers(b *testing.B) {
	benchEchoInt(b, 10, false, false)
}

func BenchmarkEchoInt100Workers(b *testing.B) {
	benchEchoInt(b, 100, false, false)
}

func BenchmarkEchoInt1000Workers(b *testing.B) {
	benchEchoInt(b, 1000, false, false)
}

func BenchmarkEchoInt10000Workers(b *testing.B) {
	benchEchoInt(b, 10000, false, false)
}

func BenchmarkEchoIntUnix1Worker(b *testing.B) {
	benchEchoInt(b, 1, false, true)
}

func BenchmarkEchoIntUnix10Workers(b *testing.B) {
	benchEchoInt(b, 10, false, true)
}

func BenchmarkEchoIntUnix100Workers(b *testing.B) {
	benchEchoInt(b, 100, false, true)
}

func BenchmarkEchoIntUnix1000Workers(b *testing.B) {
	benchEchoInt(b, 1000, false, true)
}

func BenchmarkEchoIntUnix10000Workers(b *testing.B) {
	benchEchoInt(b, 10000, false, true)
}

func BenchmarkEchoIntNocompress1Worker(b *testing.B) {
	benchEchoInt(b, 1, true, false)
}

func BenchmarkEchoIntNocompress10Workers(b *testing.B) {
	benchEchoInt(b, 10, true, false)
}

func BenchmarkEchoIntNocompress100Workers(b *testing.B) {
	benchEchoInt(b, 100, true, false)
}

func BenchmarkEchoIntNocompress1000Workers(b *testing.B) {
	benchEchoInt(b, 1000, true, false)
}

func BenchmarkEchoIntNocompress10000Workers(b *testing.B) {
	benchEchoInt(b, 10000, true, false)
}

func BenchmarkEchoString1Worker(b *testing.B) {
	benchEchoString(b, 1, false, false)
}

func BenchmarkEchoString10Workers(b *testing.B) {
	benchEchoString(b, 10, false, false)
}

func BenchmarkEchoString100Workers(b *testing.B) {
	benchEchoString(b, 100, false, false)
}

func BenchmarkEchoString1000Workers(b *testing.B) {
	benchEchoString(b, 1000, false, false)
}

func BenchmarkEchoString10000Workers(b *testing.B) {
	benchEchoString(b, 10000, false, false)
}

func BenchmarkEchoStringUnix1Worker(b *testing.B) {
	benchEchoString(b, 1, false, true)
}

func BenchmarkEchoStringUnix10Workers(b *testing.B) {
	benchEchoString(b, 10, false, true)
}

func BenchmarkEchoStringUnix100Workers(b *testing.B) {
	benchEchoString(b, 100, false, true)
}

func BenchmarkEchoStringUnix1000Workers(b *testing.B) {
	benchEchoString(b, 1000, false, true)
}

func BenchmarkEchoStringUnix10000Workers(b *testing.B) {
	benchEchoString(b, 10000, false, true)
}

func BenchmarkEchoStringNocompress1Worker(b *testing.B) {
	benchEchoString(b, 1, true, false)
}

func BenchmarkEchoStringNocompress10Workers(b *testing.B) {
	benchEchoString(b, 10, true, false)
}

func BenchmarkEchoStringNocompress100Workers(b *testing.B) {
	benchEchoString(b, 100, true, false)
}

func BenchmarkEchoStringNocompress1000Workers(b *testing.B) {
	benchEchoString(b, 1000, true, false)
}

func BenchmarkEchoStringNocompress10000Workers(b *testing.B) {
	benchEchoString(b, 10000, true, false)
}

func BenchmarkEchoStruct1Worker(b *testing.B) {
	benchEchoStruct(b, 1, false, false)
}

func BenchmarkEchoStruct10Workers(b *testing.B) {
	benchEchoStruct(b, 10, false, false)
}

func BenchmarkEchoStruct100Workers(b *testing.B) {
	benchEchoStruct(b, 100, false, false)
}

func BenchmarkEchoStruct1000Workers(b *testing.B) {
	benchEchoStruct(b, 1000, false, false)
}

func BenchmarkEchoStruct10000Workers(b *testing.B) {
	benchEchoStruct(b, 10000, false, false)
}

func BenchmarkEchoStructUnix1Worker(b *testing.B) {
	benchEchoStruct(b, 1, false, true)
}

func BenchmarkEchoStructUnix10Workers(b *testing.B) {
	benchEchoStruct(b, 10, false, true)
}

func BenchmarkEchoStructUnix100Workers(b *testing.B) {
	benchEchoStruct(b, 100, false, true)
}

func BenchmarkEchoStructUnix1000Workers(b *testing.B) {
	benchEchoStruct(b, 1000, false, true)
}

func BenchmarkEchoStructUnix10000Workers(b *testing.B) {
	benchEchoStruct(b, 10000, false, true)
}

func BenchmarkEchoStructNocompress1Worker(b *testing.B) {
	benchEchoStruct(b, 1, true, false)
}

func BenchmarkEchoStructNocompress10Workers(b *testing.B) {
	benchEchoStruct(b, 10, true, false)
}

func BenchmarkEchoStructNocompress100Workers(b *testing.B) {
	benchEchoStruct(b, 100, true, false)
}

func BenchmarkEchoStructNocompress1000Workers(b *testing.B) {
	benchEchoStruct(b, 1000, true, false)
}

func BenchmarkEchoStructNocompress10000Workers(b *testing.B) {
	benchEchoStruct(b, 10000, true, false)
}

func benchEchoNil(b *testing.B, workers int, disableCompression, isUnixTransport bool) {
	benchEchoFunc(b, workers, disableCompression, isUnixTransport, func(c *Client, n int) {
		resp, err := c.Call(nil)
		if err != nil {
			b.Fatalf("Unexpected error: [%s]", err)
		}
		if resp != nil {
			b.Fatalf("Unexpected response: %v", resp)
		}
	})
}

func benchEchoInt(b *testing.B, workers int, disableCompression, isUnixTransport bool) {
	benchEchoFunc(b, workers, disableCompression, isUnixTransport, func(c *Client, n int) {
		resp, err := c.Call(n)
		if err != nil {
			b.Fatalf("Unexpected error: [%s]", err)
		}
		if resp == nil {
			b.Fatalf("Unexpected nil response")
		}
		x, ok := resp.(int)
		if !ok {
			b.Fatalf("Unexpected response type: %T. Expected int", resp)
		}
		if x != n {
			b.Fatalf("Unexpected value returned: %d. Expected %d", x, n)
		}
	})
}

func benchEchoString(b *testing.B, workers int, disableCompression, isUnixTransport bool) {
	benchEchoFunc(b, workers, disableCompression, isUnixTransport, func(c *Client, n int) {
		s := fmt.Sprintf("test string %d", n)
		resp, err := c.Call(s)
		if err != nil {
			b.Fatalf("Unexpected error: [%s]", err)
		}
		if resp == nil {
			b.Fatalf("Unexpected nil response")
		}
		x, ok := resp.(string)
		if !ok {
			b.Fatalf("Unexpected response type: %T. Expected string", resp)
		}
		if x != s {
			b.Fatalf("Unexpected value returned: %s. Expected %s", x, s)
		}
	})
}

func benchEchoStruct(b *testing.B, workers int, disableCompression, isUnixTransport bool) {
	type BenchEchoStruct struct {
		A int
		B string
		C []byte
	}

	RegisterType(&BenchEchoStruct{})

	benchEchoFunc(b, workers, disableCompression, isUnixTransport, func(c *Client, n int) {
		s := &BenchEchoStruct{
			A: n,
			B: fmt.Sprintf("test string %d", n),
			C: []byte(fmt.Sprintf("test bytes %d", n)),
		}
		resp, err := c.Call(s)
		if err != nil {
			b.Fatalf("Unexpected error: [%s]", err)
		}
		if resp == nil {
			b.Fatalf("Unexpected nil response")
		}
		x, ok := resp.(*BenchEchoStruct)
		if !ok {
			b.Fatalf("Unexpected response type: %T. Expected BenchEchoStruct", resp)
		}
		if x.A != s.A || x.B != s.B || !bytes.Equal(x.C, s.C) {
			b.Fatalf("Unexpected value returned: %+v. Expected %+v", x, s)
		}
	})
}

func benchEchoFunc(b *testing.B, workers int, disableCompression, isUnixTransport bool, f func(*Client, int)) {
	s, c := createEchoServerAndClient(b, disableCompression, workers, isUnixTransport)
	benchClientServer(b, workers, c, s, func(n int) { f(c, n) })
}

func benchClientServer(b *testing.B, workers int, c *Client, s *Server, f func(int)) {
	benchClientServerExt(b, workers, c, s, f, func() {})
}

func benchClientServerExt(b *testing.B, workers int, c *Client, s *Server, f func(int), waitF func()) {
	if err := s.Start(); err != nil {
		b.Fatalf("Cannot start gorpc server: [%s]", err)
	}
	c.Start()

	defer s.Stop()
	defer c.Stop()

	var wg sync.WaitGroup

	var x uint64
	N := uint64(b.N)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for {
				n := atomic.AddUint64(&x, 1)
				if n > N {
					break
				}
				f(int(n))
			}
		}()
	}

	waitF()
	wg.Wait()

	//b.Logf("client: %+v\nserver: %+v\n", c.Stats, s.Stats)
}

func createEchoServerAndClient(b *testing.B, disableCompression bool, workers int, isUnixTransport bool) (s *Server, c *Client) {
	if isUnixTransport {
		addr := "./gorpc-bench.sock"
		s = NewUnixServer(addr, echoHandler)
		c = NewUnixClient(addr)
	} else {
		addr := getRandomAddr()
		s = &Server{
			Addr:    addr,
			Handler: echoHandler,
		}
		c = &Client{
			Addr: addr,
		}
		c.DisableCompression = disableCompression
	}

	s.Concurrency = workers
	s.PendingResponses = workers

	c.Conns = runtime.GOMAXPROCS(-1)
	c.PendingRequests = workers

	return s, c
}
