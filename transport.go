package gorpc

import (
	"crypto/tls"
	"io"
	"net"
	"time"
)

var (
	dialer = &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
)

// DialFunc is a function intended for setting to Client.Dial.
type DialFunc func(addr string) (conn io.ReadWriteCloser, err error)

func defaultDial(addr string) (conn io.ReadWriteCloser, err error) {
	return dialer.Dial("tcp", addr)
}

type defaultListener struct {
	L net.Listener
}

func newDefaultListener(addr string) (Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &defaultListener{
		L: ln,
	}, nil
}

func (ln *defaultListener) Accept(addr string) (conn io.ReadWriteCloser, clientAddr string, err error) {
	c, err := ln.L.Accept()
	if err != nil {
		return nil, "", err
	}
	if err = setupKeepalive(c); err != nil {
		c.Close()
		return nil, "", err
	}
	return c, c.RemoteAddr().String(), nil
}

func (ln *defaultListener) Close() error {
	return ln.L.Close()
}

func setupKeepalive(conn net.Conn) error {
	tcpConn := conn.(*net.TCPConn)
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return err
	}
	if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
		return err
	}
	return nil
}

// NewTLSDial creates Dial() function for TLS connections' establishing.
// The returned function is intended for Client.Dial assignment.
func NewTLSDial(cfg *tls.Config) DialFunc {
	return func(addr string) (conn io.ReadWriteCloser, err error) {
		c, err := tls.DialWithDialer(dialer, "tcp", addr, cfg)
		if err != nil {
			return nil, err
		}
		return c, err
	}
}

type tlsListener struct {
	L net.Listener
}

// NewTLSListener creates a TLS listener accepting connections on
// the given TCP addr.
// The returned listener is intended for Server.Listener assignment.
func NewTLSListener(addr string, cfg *tls.Config) (Listener, error) {
	ln, err := tls.Listen("tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
	return &tlsListener{
		L: ln,
	}, nil
}

func (ln *tlsListener) Accept(addr string) (conn io.ReadWriteCloser, clientAddr string, err error) {
	c, err := ln.L.Accept()
	if err != nil {
		return nil, "", err
	}
	return c, c.RemoteAddr().String(), nil
}

func (ln *tlsListener) Close() error {
	return ln.L.Close()
}
