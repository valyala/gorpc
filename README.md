gorpc
=====

Simple, fast and scalable golang RPC library for high load.

Unlike standard library at http://golang.org/pkg/net/rpc/ it multiplexes
requests over a small number of TCP connections. This provides the following
features useful for highly loaded RPC projects:

* It minimizes the number of connect() syscalls by pipelining request
  and response messages over a single TCP connection.

* It minimizes the number of send() syscalls by packing as much
  as possible pending requests and responses into a single compressed buffer
  before passing it into send() syscall.

* It minimizes the number of recv() syscalls by reading and buffering as much
  as possible data from the network.

These features help the OS minimizing overhead (CPU load, the number of
TCP connections in TIME_WAIT and CLOSE_WAIT states, the number of network
packets and the amount of network bandwidth) required for RPC processing under
high load.

By default TCP connections are used as underlying gorpc transport. But you can
use anything you want as an underlying transport - just provide custom
implementations for Client.Dial and Server.Listener.
RPC client authentication and authorization can be easily implemented via custom
underlying transport.

Currently gorpc with default settings is successfully used in highly loaded
production environment serving up to 40K qps. Switching from http-based rpc
to gorpc reduced required network bandwidth from 300 Mbit/s to 24 Mbit/s.

Usage
=====

Server:
```go
s := &Server{
	// Accept clients on this TCP address.
	Addr: ":12345",

	// Echo handler - just return back the message we received from the client
	Handler: func(clientAddr string, request interface{}) interface{} {
		log.Printf("Obtained request %+v from the client %s\n", request, clientAddr)
		return request
	},
}
if err := s.Serve(); err != nil {
	log.Fatalf("Cannot start rpc server: %s", err)
}
```

Client:
```go
c := &Client{
	// TCP address of the server.
	Addr: "rpc.server.addr:12345",
}
c.Start()

// RPC call
resp, err := c.Call("foobar")
if err != nil {
	log.Fatalf("Error when sending request to server: %s", err)
}
if resp.(string) != "foobar" {
	log.Fatalf("Unexpected response from the server: %+v", resp)
}

// Non-blocking request send
for i := 0; i < 10; i++ {
	c.Send(i)
}
```

Both client and server collect connection stats - the number of bytes
read / written and the number of calls / errors to send(), recv(), connect()
and accept(). This stats is available at Client.Stats and Server.Stats.

See tests for more usage examples.

Docs
====

See http://godoc.org/github.com/valyala/gorpc .
