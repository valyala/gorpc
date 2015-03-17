package gorpc

import (
	"fmt"
	"io"
	"testing"
	"unsafe"
)

func TestDispatcherEmptyFuncName(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("", func() {})
	})
}

func TestDispatcherInvalidFirstArgType(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func(clientAddr bool, request string) {})
	})
}

func TestDispatcherInvalidSecondResType(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func() (response int, err float64) { return })
	})
}

func TestDispatcherTooManyArgs(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func(clientAddr string, foo, bar int) {})
	})
}

func TestDispatcherTooManyRes(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func() (response int, err error, foobar string) { return })
	})
}

func TestDispatcherFuncArg(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func(req func()) {})
	})
}

func TestDispatcherChanArg(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func(req chan int) {})
	})
}

func TestDispatcherInterfaceArg(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func(req io.Reader) {})
	})
	testPanic(t, func() {
		d.RegisterFunc("foo", func(req interface{}) {})
	})
}

func TestDispatcherUnsafePointerArg(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func(req unsafe.Pointer) {})
	})
}

func TestDispatcherFuncRes(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func() (res func()) { return })
	})
}

func TestDispatcherChanRes(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func() (res chan int) { return })
	})
}

func TestDispatcherInterfaceRes(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func() (res io.Reader) { return })
	})
	testPanic(t, func() {
		d.RegisterFunc("foo", func() (res interface{}) { return })
	})
}

func TestDispatcherUnsafePointerRes(t *testing.T) {
	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func() (res unsafe.Pointer) { return })
	})
}

func TestDispatcherPassStructArgByValue(t *testing.T) {
	type RequestType struct {
		a int
		B string
	}

	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func(request RequestType) {})
	})
}

func TestDispatcherReturnStructResByValue(t *testing.T) {
	type ResponseType struct {
		A int
		b string
	}

	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func() (response ResponseType) { return })
	})
}

func TestDispatcherPassStructArgNoExportedFields(t *testing.T) {
	type RequestTypeNoExport struct {
		a int
		b string
	}

	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func(req *RequestTypeNoExport) {})
	})
}

func TestDispatcherReturnStructResNoExportedFields(t *testing.T) {
	type ResponseTypeNoExport struct {
		a int
		b string
	}

	d := NewDispatcher()
	testPanic(t, func() {
		d.RegisterFunc("foo", func() (resp *ResponseTypeNoExport) { return })
	})
}

func TestDispatcherStructsWithIdenticalFields(t *testing.T) {
	type Struct1 struct {
		A int
	}
	type Struct2 struct {
		A int
	}

	d := NewDispatcher()

	d.RegisterFunc("foo", func(request *Struct1) *Struct2 {
		return &Struct2{
			A: request.A,
		}
	})

	testDispatcher(t, d, func(fc *DispatcherClient) {
		reqs := &Struct1{
			A: 12356,
		}
		res, err := fc.Call("foo", reqs)
		if err != nil {
			t.Fatalf("Unepxected error: [%s]", err)
		}
		ress, ok := res.(*Struct2)
		if !ok {
			t.Fatalf("Unexpected response type: %T. Expected *Struct2", res)
		}
		if ress.A != reqs.A {
			t.Fatalf("Unexpected response: [%+v]. Expected [%+v]", ress, reqs)
		}
	})
}

func TestDispatcherInvalidArgType(t *testing.T) {
	d := NewDispatcher()
	d.RegisterFunc("foo", func(request string) {})
	testDispatcher(t, d, func(fc *DispatcherClient) {
		res, err := fc.Call("foo", 1234)
		if err == nil {
			t.Fatalf("Expected non-nil error")
		}
		if res != nil {
			t.Fatalf("Expected nil response")
		}
	})
}

func TestDispatcherUnknownFuncCall(t *testing.T) {
	d := NewDispatcher()
	d.RegisterFunc("foo", func(request string) {})
	testDispatcher(t, d, func(fc *DispatcherClient) {
		res, err := fc.Call("UnknownFunc", 1234)
		if err == nil {
			t.Fatalf("Expected non-nil error")
		}
		if res != nil {
			t.Fatalf("Expected nil response")
		}
	})
}

func TestDispatcherEchoFuncCall(t *testing.T) {
	d := NewDispatcher()
	d.RegisterFunc("Echo", func(request string) string { return request })
	testDispatcher(t, d, func(fc *DispatcherClient) {
		res, err := fc.Call("Echo", "foobar")
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		ress, ok := res.(string)
		if !ok {
			t.Fatalf("Unexpected response type: %T. Expected string", ress)
		}
		if ress != "foobar" {
			t.Fatalf("Unexpected response: [%s]. Expected [foobar]", ress)
		}
	})
}

func TestDispatcherStructArgCall(t *testing.T) {
	type RequestArg struct {
		A int
		B string
	}

	type ResponseArg struct {
		C string
		D int
	}

	d := NewDispatcher()
	d.RegisterFunc("fooBar", func(request *RequestArg) *ResponseArg {
		return &ResponseArg{
			C: request.B,
			D: request.A,
		}
	})

	testDispatcher(t, d, func(fc *DispatcherClient) {
		// verify call by reference
		reqs := &RequestArg{
			A: 123,
			B: "7822",
		}
		res, err := fc.Call("fooBar", reqs)
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		ress, ok := res.(*ResponseArg)
		if !ok {
			t.Fatalf("Unexpected response type: %T. Expected *ResponseArg", ress)
		}
		if ress.C != reqs.B || ress.D != reqs.A {
			t.Fatalf("Unexpected response: [%+v]. Expected &ResponseArg{C:%s, D:%d}", ress, reqs.B, reqs.A)
		}

		// verify call by value
		reqs.A = 7889
		reqs.B = "alkjjal"
		if res, err = fc.Call("fooBar", *reqs); err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		if ress, ok = res.(*ResponseArg); !ok {
			t.Fatalf("Unexpected response type: %T. Expected *ResponseArg", ress)
		}
		if ress.C != reqs.B || ress.D != reqs.A {
			t.Fatalf("Unexpected response: [%+v]. Expected &ResponseArg{C:%s, D:%d}", ress, reqs.B, reqs.A)
		}
	})
}

func TestDispatcherRecursiveStructArg(t *testing.T) {
	type RecMsg struct {
		A   int
		Rec *RecMsg
	}

	d := NewDispatcher()
	d.RegisterFunc("foo", func(req *RecMsg) *RecMsg { return req })

	testDispatcher(t, d, func(fc *DispatcherClient) {
		reqs := &RecMsg{
			A: 1,
			Rec: &RecMsg{
				A: 2,
			},
		}
		res, err := fc.Call("foo", reqs)
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		ress, ok := res.(*RecMsg)
		if !ok {
			t.Fatalf("Unepxected response type: %T. Expected *RecMsg", ress)
		}
		if ress.A != reqs.A || ress.Rec.A != reqs.Rec.A {
			t.Fatalf("Unepxected respons: [%+v]. Expected [%+v]", ress, reqs)
		}
	})
}

func TestDispatcherNoArgNoResCall(t *testing.T) {
	d := NewDispatcher()

	noArgNoResCalls := 0
	d.RegisterFunc("NoArgNoRes", func() { noArgNoResCalls++ })

	testDispatcher(t, d, func(fc *DispatcherClient) {
		N := 10
		for i := 0; i < N; i++ {
			res, err := fc.Call("NoArgNoRes", "ignoreThis")
			if err != nil {
				t.Fatalf("Unexpected error: [%s]", err)
			}
			if res != nil {
				t.Fatalf("Unexpected response: [%+v]", res)
			}
		}

		if noArgNoResCalls != N {
			t.Fatalf("Unepxected number of NoArgNoRes calls: %d. Expected %d", noArgNoResCalls, N)
		}
	})
}

func TestDispatcherOneArgNoResCall(t *testing.T) {
	d := NewDispatcher()

	serverS := 0
	clientS := 0
	d.RegisterFunc("OneArgNoRes", func(n int) { serverS += n })

	testDispatcher(t, d, func(fc *DispatcherClient) {
		for i := 0; i < 10; i++ {
			res, err := fc.Call("OneArgNoRes", i)
			if err != nil {
				t.Fatalf("Unexpected error: [%s]", err)
			}
			if res != nil {
				t.Fatalf("Unexpected response: [%+v]", res)
			}
			clientS += i
		}

		if clientS != serverS {
			t.Fatalf("Unepxected serverS=%d. Expected %d", serverS, clientS)
		}
	})
}

func TestDispatcherTwoArgNoResCall(t *testing.T) {
	d := NewDispatcher()

	serverS := 0
	clientS := 0
	d.RegisterFunc("TwoArgNoRes", func(clientAddr string, n int) { serverS += n })

	testDispatcher(t, d, func(fc *DispatcherClient) {
		for i := 0; i < 10; i++ {
			res, err := fc.Call("TwoArgNoRes", i)
			if err != nil {
				t.Fatalf("Unexpected error: [%s]", err)
			}
			if res != nil {
				t.Fatalf("Unexpected response: [%+v]", res)
			}
			clientS += i
		}

		if clientS != serverS {
			t.Fatalf("Unepxected serverS=%d. Expected %d", serverS, clientS)
		}
	})
}

func TestDispatcherNoArgErrorResCall(t *testing.T) {
	d := NewDispatcher()

	var returnErr error
	d.RegisterFunc("NoArgErrorRes", func() error { return returnErr })

	testDispatcher(t, d, func(fc *DispatcherClient) {
		returnErr = nil
		res, err := fc.Call("NoArgErrorRes", nil)
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		if res != nil {
			t.Fatalf("Unexpected response: [%+v]", res)
		}

		returnErr = fmt.Errorf("foobar")
		if res, err = fc.Call("NoArgErrorRes", nil); err == nil {
			t.Fatalf("Unexpected nil error")
		}
		if err.Error() != returnErr.Error() {
			t.Fatalf("Unexpected error: [%s]. Expected [%s]", err, returnErr)
		}
		if res != nil {
			t.Fatalf("Unexpected response: [%+v]", res)
		}
	})
}

func TestDispatcherNoArgOneResCall(t *testing.T) {
	d := NewDispatcher()

	d.RegisterFunc("NoArgOneResCall", func() string { return "foobar" })

	testDispatcher(t, d, func(fc *DispatcherClient) {
		res, err := fc.Call("NoArgOneResCall", nil)
		if err != nil {
			t.Fatalf("Unexpected error: [%s]", err)
		}
		ress, ok := res.(string)
		if !ok {
			t.Fatalf("Unexpected response type: %T. Expected string", res)
		}
		if ress != "foobar" {
			t.Fatalf("Unexpected response [%s]. Expected [foobar]", ress)
		}
	})
}

func testDispatcher(t *testing.T, d *Dispatcher, f func(fc *DispatcherClient)) {
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

	f(fc)
}

func testPanic(t *testing.T, f func()) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Fatalf("Panic expected")
	}()
	f()
}
