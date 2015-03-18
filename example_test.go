package gorpc

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

func ExampleDispatcher_func() {
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
			return 0, fmt.Errorf("Invalid rpc client host: [%s]", clientHost)
		}
		return x, nil
	})

	// Start the server serving all the registered functions above
	s := NewTCPServer(":12345", d.HandlerFunc())
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

	// Call functions above
	var err error
	var res interface{}

	res, err = dc.Call("Inc", nil)
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
