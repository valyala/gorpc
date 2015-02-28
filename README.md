gorpc
=====

Simple, fast and scalable rpc library for high load.

Unlike standard library at http://golang.org/pkg/net/rpc/ it multiplexes
requests over a small number of TCP connections. This provides the following
features useful for highly loaded rpc applications:

* It minimizes the number of connect() syscalls by pipelining request
  and response messages over a single TCP connection.

* It minimizes the number of send() syscalls by packing as much
  as possible pending requests and responses into a single compressed buffer
  before passing it into send() syscall.

* It minimizes the number of recv() syscalls by reading and buffering as much
  as possible data from the network.

These features help the OS minimizing overhead (CPU load, the number of network
packets and the amount of network bandwidth) required for rpc processing under
high load.

Currently gorpc with default settings is successfully used in highly loaded
production environment serving up to 40K qps. Switching from http-based rpc
to gorpc reduced required network bandwidth from 100 Mbit/s to 8 Mbit/s.


Docs
====

See http://godoc.org/github.com/valyala/gorpc .


Usage
=====

See tests.
