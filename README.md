iorpc
=====

Simple and fast golang RPC library for zero-copy IO between filesystem and network.

## Example

Server:
```go
s := &iorpc.Server{
	// Accept clients on this TCP address.
	Addr: ":12345",

	// File server - send file to client
	Handler: func(clientAddr string, request iorpc.Request) (*iorpc.Response, error) {
		file, err := os.OpenFile("data", os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
    stat, err := file.Stat()
    if err != nil {
      return nil, err
    }
    return &iorpc.Response{Size: stat.Size(), Body: file}, nil
	},
}
if err := s.Serve(); err != nil {
	log.Fatalf("Cannot start rpc server: %s", err)
}
```

Client:
```go
c := &iorpc.Client{
	// TCP address of the server.
	Addr: "rpc.server.addr:12345",
}
c.Start()

// All client methods issuing RPCs are thread-safe and goroutine-safe,
// i.e. it is safe to call them from multiple concurrently running goroutines.
resp, err := c.Call(iorpc.Request{})
if err != nil {
	log.Fatalf("Error when sending request to server: %s", err)
}
defer resp.Body.Close()

file, err := os.Open("localfile")
if err != nil {
  log.Fatalf("Error when open ", err)
}
defer file.Close()

io.Copy(file, resp.Body)
```

Both client and server collect connection stats - the number of bytes
read / written and the number of calls / errors to send(), recv(), connect()
and accept(). This stats is available at Client.Stats and Server.Stats.

See tests for more usage examples.
