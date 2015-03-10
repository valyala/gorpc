package gorpc

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

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

	benchClientServer(b, workersCount, c, s, func(c *Client, n int) {
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
	benchClientServer(b, workers, c, s, f)
}

func benchClientServer(b *testing.B, workers int, c *Client, s *Server, f func(*Client, int)) {
	if err := s.Start(); err != nil {
		b.Fatalf("Cannot start gorpc server: [%s]", err)
	}
	c.Start()

	defer s.Stop()
	defer c.Stop()

	var wg sync.WaitGroup

	ch := make(chan int, 100000)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for n := range ch {
				f(c, n)
			}
		}()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- i
	}

	close(ch)

	wg.Wait()
}

func createEchoServerAndClient(b *testing.B, disableCompression bool, pendingMessages int, isUnixTransport bool) (s *Server, c *Client) {
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
	}

	c.Conns = runtime.GOMAXPROCS(-1)
	c.DisableCompression = disableCompression
	c.PendingRequests = pendingMessages

	return s, c
}
