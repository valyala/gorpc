package gorpc

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

func ExampleServer() {
	// Register the given struct for passing as rpc request and/or response.
	// All structs intended for passing between client and server
	// must be registered via RegisterType().
	//
	// The struct may contain arbitrary fields, but only public (exported)
	// fields are passed between client and server.
	type ExampleStruct struct {
		Foo int

		// This feild won't be passed over the wire,
		// since it is private (unexported)
		bar string

		Baz string
	}
	RegisterType(&ExampleStruct{})

	// Start echo server
	handlerFunc := func(clientAddr string, request interface{}) interface{} {
		return request
	}
	s := NewTCPServer(":43216", handlerFunc)
	if err := s.Start(); err != nil {
		log.Fatalf("Cannot start server: [%s]", err)
	}
	defer s.Stop()

	// Connect client to the echo server
	c := NewTCPClient(":43216")
	c.Start()
	defer c.Stop()

	// Echo string
	res, err := c.Call("foobar")
	fmt.Printf("%+v, %+v\n", res, err)

	// Echo int
	res, err = c.Call(1234)
	fmt.Printf("%+v, %+v\n", res, err)

	// Echo string slice
	res, err = c.Call([]string{"foo", "bar"})
	fmt.Printf("%+v, %+v\n", res, err)

	// Echo struct
	res, err = c.Call(&ExampleStruct{
		Foo: 123,
		bar: "324",
		Baz: "mmm",
	})
	fmt.Printf("%+v, %+v\n", res, err)

	// Output:
	// foobar, <nil>
	// 1234, <nil>
	// [foo bar], <nil>
	// &{Foo:123 bar: Baz:mmm}, <nil>
}

type ExampleDispatcherService struct {
	state int
}

func (s *ExampleDispatcherService) Get() int { return s.state }

func (s *ExampleDispatcherService) Set(x int) { s.state = x }

func (s *ExampleDispatcherService) GetError42() (int, error) {
	if s.state == 42 {
		return 0, errors.New("error42")
	}
	return s.state, nil
}

func (s *ExampleDispatcherService) privateFunc(string) { s.state = 0 }

func ExampleDispatcher_serviceCalls() {
	d := NewDispatcher()

	// ExampleDispatcherService has the following functions:
	//
	//   * Get() int { return state }
	//   * Set(x int) { state = x }
	//   * GetError42() (int, error) {
	//         if state == 42 { return 0, errors.New("error42") }
	//         return state, nil
	//     }
	//   * privateFunc(string) { state = 0 }
	service := &ExampleDispatcherService{
		state: 123,
	}

	// Register exported service functions
	d.AddService("MyService", service)

	// Start rpc server serving registered service.
	addr := ":7892"
	s := NewTCPServer(addr, d.NewHandlerFunc())
	if err := s.Start(); err != nil {
		log.Fatalf("Cannot start rpc server: [%s]", err)
	}
	defer s.Stop()

	// Start rpc client connected to the server.
	c := NewTCPClient(addr)
	c.Start()
	defer c.Stop()

	// Create client wrapper for calling service functions.
	dc := d.NewServiceClient("MyService", c)

	res, err := dc.Call("Get", nil)
	fmt.Printf("Get=%+v, %+v\n", res, err)

	service.state = 456
	res, err = dc.Call("Get", nil)
	fmt.Printf("Get=%+v, %+v\n", res, err)

	res, err = dc.Call("Set", 78)
	fmt.Printf("Set=%+v, %+v, %+v\n", res, err, service.state)

	res, err = dc.Call("GetError42", nil)
	fmt.Printf("GetError42=%+v, %+v\n", res, err)

	service.state = 42
	res, err = dc.Call("GetError42", nil)
	fmt.Printf("GetError42=%+v, %+v\n", res, err)

	res, err = dc.Call("privateFunc", "123")
	fmt.Printf("privateFunc=%+v, %+v, %+v\n", res, err, service.state)

	// Output:
	// Get=123, <nil>
	// Get=456, <nil>
	// Set=<nil>, <nil>, 78
	// GetError42=78, <nil>
	// GetError42=<nil>, error42
	// privateFunc=<nil>, gorpc.Dispatcher: unknown method [MyService.privateFunc], 42
}

func ExampleDispatcher_AddFunc() {
	d := NewDispatcher()

	// Function without arguments and return values
	d.AddFunc("NoArgsNoRets", func() {})

	// Function with one argument and no return values
	d.AddFunc("OneArgNoRets", func(request string) {})

	// Function without arguments and one return value
	d.AddFunc("NoArgsOneRet", func() int { return 42 })

	// Function with two arguments and no return values.
	// The first argument must have string type - the server passes
	// client address in it.
	d.AddFunc("TwoArgsNoRets", func(clientAddr string, requests []byte) {})

	// Function with one argument and two return values.
	// The second return value must have error type.
	d.AddFunc("OneArgTwoRets", func(request []string) ([]string, error) {
		if len(request) == 42 {
			return nil, errors.New("need 42 strings")
		}
		return request, nil
	})
}

func ExampleDispatcher_funcCalls() {
	d := NewDispatcher()

	// Function without args and return values
	incCalls := 0
	d.AddFunc("Inc", func() { incCalls++ })

	// Function without args
	d.AddFunc("Func42", func() int { return 42 })

	// Echo function for string
	d.AddFunc("Echo", func(s string) string { return s })

	// Function with struct arg and return value
	type ExampleRequestStruct struct {
		Foo int
		Bar string
	}
	type ExampleResponseStruct struct {
		Baz    string
		BarLen int
	}
	d.AddFunc("Struct", func(s *ExampleRequestStruct) *ExampleResponseStruct {
		return &ExampleResponseStruct{
			Baz:    fmt.Sprintf("foo=%d, bar=%s", s.Foo, s.Bar),
			BarLen: len(s.Bar),
		}
	})

	// Echo function for map
	d.AddFunc("Map", func(m map[string]int) map[string]int { return m })

	// Echo function for slice
	d.AddFunc("Slice", func(s []string) []string { return s })

	// Function returning errors
	d.AddFunc("Error", func() error { return errors.New("error") })

	// Echo function, which may return error if arg is 42
	d.AddFunc("Error42", func(x int) (int, error) {
		if x == 42 {
			return 0, errors.New("error42")
		}
		return x, nil
	})

	// Echo function with client address' validation
	d.AddFunc("ClientAddr", func(clientAddr string, x int) (int, error) {
		clientHost := strings.SplitN(clientAddr, ":", 2)[0]
		if clientHost != "allowed.client.host" {
			return 0, fmt.Errorf("invalid rpc client host: [%s]", clientHost)
		}
		return x, nil
	})

	// Start the server serving all the registered functions above
	s := NewTCPServer(":12345", d.NewHandlerFunc())
	if err := s.Start(); err != nil {
		log.Fatalf("Cannot start rpc server: [%s]", err)
	}
	defer s.Stop()

	// Start the client and connect it to the server
	c := NewTCPClient(":12345")
	c.Start()
	defer c.Stop()

	// Create a client wrapper for calling server functions.
	dc := d.NewFuncClient(c)

	// Call functions defined above
	res, err := dc.Call("Inc", nil)
	fmt.Printf("Inc=%+v, %+v, %d\n", res, err, incCalls)

	res, err = dc.Call("Func42", nil)
	fmt.Printf("Func42=%+v, %+v\n", res, err)

	res, err = dc.Call("Echo", "foobar")
	fmt.Printf("Echo=%+v, %+v\n", res, err)

	reqst := &ExampleRequestStruct{
		Foo: 42,
		Bar: "bar",
	}
	res, err = dc.Call("Struct", reqst)
	fmt.Printf("Struct=%+v, %+v\n", res, err)

	res, err = dc.Call("Map", map[string]int{"foo": 1, "bar": 2})
	resm := res.(map[string]int)
	fmt.Printf("Map=foo:%d, bar:%d, %+v\n", resm["foo"], resm["bar"], err)

	res, err = dc.Call("Slice", []string{"foo", "bar"})
	fmt.Printf("Slice=%+v, %+v\n", res, err)

	res, err = dc.Call("Error", nil)
	fmt.Printf("Error=%+v, %+v\n", res, err)

	res, err = dc.Call("Error42", 123)
	fmt.Printf("Error42(123)=%+v, %+v\n", res, err)

	res, err = dc.Call("Error42", 42)
	fmt.Printf("Error42(42)=%+v, %+v\n", res, err)

	// Output:
	// Inc=<nil>, <nil>, 1
	// Func42=42, <nil>
	// Echo=foobar, <nil>
	// Struct=&{Baz:foo=42, bar=bar BarLen:3}, <nil>
	// Map=foo:1, bar:2, <nil>
	// Slice=[foo bar], <nil>
	// Error=<nil>, error
	// Error42(123)=123, <nil>
	// Error42(42)=<nil>, error42
}
