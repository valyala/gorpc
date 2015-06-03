package gorpc

import (
	"errors"
	"fmt"
	"log"
)

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
