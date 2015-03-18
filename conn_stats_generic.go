// +build !386

package gorpc

import (
	"sync/atomic"
)

func (cs *ConnStats) incRPCCalls() {
	atomic.AddUint64(&cs.RPCCalls, 1)
}

func (cs *ConnStats) addBytesWritten(n uint64) {
	atomic.AddUint64(&cs.BytesWritten, n)
}

func (cs *ConnStats) addBytesRead(n uint64) {
	atomic.AddUint64(&cs.BytesRead, n)
}

func (cs *ConnStats) incReadCalls() {
	atomic.AddUint64(&cs.ReadCalls, 1)
}

func (cs *ConnStats) incReadErrors() {
	atomic.AddUint64(&cs.ReadErrors, 1)
}

func (cs *ConnStats) incWriteCalls() {
	atomic.AddUint64(&cs.WriteCalls, 1)
}

func (cs *ConnStats) incWriteErrors() {
	atomic.AddUint64(&cs.WriteErrors, 1)
}

func (cs *ConnStats) incDialCalls() {
	atomic.AddUint64(&cs.DialCalls, 1)
}

func (cs *ConnStats) incDialErrors() {
	atomic.AddUint64(&cs.DialErrors, 1)
}

func (cs *ConnStats) incAcceptCalls() {
	atomic.AddUint64(&cs.AcceptCalls, 1)
}

func (cs *ConnStats) incAcceptErrors() {
	atomic.AddUint64(&cs.AcceptErrors, 1)
}
