package gorpc

import (
	"fmt"
	"testing"
)

func TestDispatcherRegisterFunc(t *testing.T) {
	d := NewDispatcher()
	d.RegisterFunc("foo", func(request string) string {
		return fmt.Sprintf("foo: %s", request)
	})
	d.RegisterFunc("bar", func(clientAddr string, request int) (int, error) {
		return 123, fmt.Errorf("bar_error: %d", request)
	})
	d.RegisterFunc("aaa", func(request int) error {
		return fmt.Errorf("aaa_error: %d", request)
	})

	bazCalls := 0
	d.RegisterFunc("baz", func(request int) {
		bazCalls++
	})

	type ReqStruct struct {
		a int
		B *ReqStruct
		C string
	}
	type respStruct struct {
		x int
	}
	d.RegisterFunc("bbb", func(request *ReqStruct) (*respStruct, error) {
		return &respStruct{
			x: request.B.a,
		}, nil
	})

	addr := getRandomAddr()
	s := NewTCPServer(addr, d.HandlerFunc())
	if err := s.Start(); err != nil {
		t.Fatalf("Error when starting server: [%s]", err)
	}
	defer s.Stop()

	c := NewTCPClient(addr)
	c.Start()
	defer c.Stop()

	fc := d.NewFuncClient(c)

	for i := 0; i < 10; i++ {
		req := fmt.Sprintf("abc %d", i)

		resp, err := fc.Call("foo", req)
		if err != nil {
			t.Fatalf("Error in rpc request: [%s]", err)
		}
		if resp.(string) != fmt.Sprintf("foo: %s", req) {
			t.Fatalf("Unexpected rpc response: [%s]. Expected [foo: %s]", resp, req)
		}

		resp, err = fc.Call("bar", i)
		if err == nil {
			t.Fatalf("bar call must return error")
		}
		if err.Error() != fmt.Sprintf("bar_error: %d", i) {
			t.Fatalf("Unexpected error: [%s]. Expected [bar_error: %d]", err, i)
		}
		if resp != nil {
			t.Fatalf("Unepxected response: [%+v]", resp)
		}

		resp, err = fc.Call("aaa", i)
		if err == nil {
			t.Fatalf("aaa call must return error")
		}
		if err.Error() != fmt.Sprintf("aaa_error: %d", i) {
			t.Fatalf("Unexpected error: [%s]. Expected [aaa_error: %d]", err, i)
		}
		if resp != nil {
			t.Fatalf("Unepxected response: [%+v]", resp)
		}

		resp, err = fc.Call("baz", i)
		if err != nil {
			t.Fatalf("Error in rpc request: [%s]", err)
		}
		if resp != nil {
			t.Fatalf("Unepxected response: [%+v]", resp)
		}

		resp, err = fc.Call("bbb", &ReqStruct{
			a: i+34,
			B: &ReqStruct{
				a: i,
				C: "foobar",
			},
		})
		if err != nil {
			t.Fatalf("Error in rpc request: [%s]", err)
		}
		resps, ok := resp.(*respStruct)
		if !ok {
			t.Fatalf("Unexpected response type: %T. Expected *respStruct", resp)
		}
		if resps.x != i {
			t.Fatalf("unexpected response.x=%d. Expected %d", resps.x, i)
		}
	}

	if bazCalls != 10 {
		t.Fatalf("Unexpected number of baz calls: %d. Expected 10", bazCalls)
	}
}
