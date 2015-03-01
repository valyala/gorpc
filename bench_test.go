package gorpc

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"testing"
)

func Benchmark_EchoInt_1_Worker(b *testing.B) {
	benchEchoInt(b, 1, false)
}

func Benchmark_EchoInt_10_Workers(b *testing.B) {
	benchEchoInt(b, 10, false)
}

func Benchmark_EchoInt_100_Workers(b *testing.B) {
	benchEchoInt(b, 100, false)
}

func Benchmark_EchoInt_1000_Workers(b *testing.B) {
	benchEchoInt(b, 1000, false)
}

func Benchmark_EchoInt_10000_Workers(b *testing.B) {
	benchEchoInt(b, 10000, false)
}

func Benchmark_EchoInt_Nocompress_1_Worker(b *testing.B) {
	benchEchoInt(b, 1, true)
}

func Benchmark_EchoInt_Nocompress_10_Workers(b *testing.B) {
	benchEchoInt(b, 10, true)
}

func Benchmark_EchoInt_Nocompress_100_Workers(b *testing.B) {
	benchEchoInt(b, 100, true)
}

func Benchmark_EchoInt_Nocompress_1000_Workers(b *testing.B) {
	benchEchoInt(b, 1000, true)
}

func Benchmark_EchoInt_Nocompress_10000_Workers(b *testing.B) {
	benchEchoInt(b, 10000, true)
}

func Benchmark_EchoString_1_Worker(b *testing.B) {
	benchEchoString(b, 1, false)
}

func Benchmark_EchoString_10_Workers(b *testing.B) {
	benchEchoString(b, 10, false)
}

func Benchmark_EchoString_100_Workers(b *testing.B) {
	benchEchoString(b, 100, false)
}

func Benchmark_EchoString_1000_Workers(b *testing.B) {
	benchEchoString(b, 1000, false)
}

func Benchmark_EchoString_10000_Workers(b *testing.B) {
	benchEchoString(b, 10000, false)
}

func Benchmark_EchoString_Nocompress_1_Worker(b *testing.B) {
	benchEchoString(b, 1, true)
}

func Benchmark_EchoString_Nocompress_10_Workers(b *testing.B) {
	benchEchoString(b, 10, true)
}

func Benchmark_EchoString_Nocompress_100_Workers(b *testing.B) {
	benchEchoString(b, 100, true)
}

func Benchmark_EchoString_Nocompress_1000_Workers(b *testing.B) {
	benchEchoString(b, 1000, true)
}

func Benchmark_EchoString_Nocompress_10000_Workers(b *testing.B) {
	benchEchoString(b, 10000, true)
}

func Benchmark_EchoStruct_1_Worker(b *testing.B) {
	benchEchoStruct(b, 1, false)
}

func Benchmark_EchoStruct_10_Workers(b *testing.B) {
	benchEchoStruct(b, 10, false)
}

func Benchmark_EchoStruct_100_Workers(b *testing.B) {
	benchEchoStruct(b, 100, false)
}

func Benchmark_EchoStruct_1000_Workers(b *testing.B) {
	benchEchoStruct(b, 1000, false)
}

func Benchmark_EchoStruct_10000_Workers(b *testing.B) {
	benchEchoStruct(b, 10000, false)
}

func Benchmark_EchoStruct_Nocompress_1_Worker(b *testing.B) {
	benchEchoStruct(b, 1, true)
}

func Benchmark_EchoStruct_Nocompress_10_Workers(b *testing.B) {
	benchEchoStruct(b, 10, true)
}

func Benchmark_EchoStruct_Nocompress_100_Workers(b *testing.B) {
	benchEchoStruct(b, 100, true)
}

func Benchmark_EchoStruct_Nocompress_1000_Workers(b *testing.B) {
	benchEchoStruct(b, 1000, true)
}

func Benchmark_EchoStruct_Nocompress_10000_Workers(b *testing.B) {
	benchEchoStruct(b, 10000, true)
}

func benchEchoInt(b *testing.B, workers int, disableCompression bool) {
	benchEchoFunc(b, workers, disableCompression, func(c *Client, n int) {
		resp, err := c.Send(n)
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

func benchEchoString(b *testing.B, workers int, disableCompression bool) {
	benchEchoFunc(b, workers, disableCompression, func(c *Client, n int) {
		s := fmt.Sprintf("test string %d", n)
		resp, err := c.Send(s)
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

func benchEchoStruct(b *testing.B, workers int, disableCompression bool) {
	type BenchEchoStruct struct {
		A int
		B string
		C []byte
	}

	RegisterType(&BenchEchoStruct{})

	benchEchoFunc(b, workers, disableCompression, func(c *Client, n int) {
		s := &BenchEchoStruct{
			A: n,
			B: fmt.Sprintf("test string %d", n),
			C: []byte(fmt.Sprintf("test bytes %d", n)),
		}
		resp, err := c.Send(s)
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

func benchEchoFunc(b *testing.B, workers int, disableCompression bool, f func(*Client, int)) {
	s, c := createEchoServerAndClient(disableCompression, b.N)
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

	log.Printf("\n")
	log.Printf("Client stats=%+v\n", c.Stats)
	log.Printf("Server stats=%+v\n", s.Stats)
}

func createEchoServerAndClient(disableCompression bool, pendingMessages int) (s *Server, c *Client) {
	s = &Server{
		Addr:             ":15347",
		Handler:          func(clientAddr string, request interface{}) interface{} { return request },
		PendingResponses: pendingMessages,
	}
	s.Start()

	c = &Client{
		Addr:               ":15347",
		DisableCompression: disableCompression,
		PendingRequests:    pendingMessages,
	}
	c.Start()

	return s, c
}
