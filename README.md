gorpc
=====

Scalable rpc library for go.

Unlike the original library at http://golang.org/pkg/net/rpc/ it multiplexes
requests over a small number of connections. This results into much higher
requests' throughput over the network interface, since multiple requests
and responses may be sent in a single network packet.
