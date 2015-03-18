// Separate implementation for 386, since it has broken support for atomics.
// See https://github.com/valyala/gorpc/issues/5 for details.

// +build 386

package gorpc

import (
	"sync/atomic"
)

func (cs *ConnStats) incRPCCalls() {
	cs.lock.Lock()
	cs.RPCCalls++
	cs.lock.Unlock()
}

func (cs *ConnStats) addBytesWritten(n uint64) {
	cs.lock.Lock()
	cs.BytesWritten++
	cs.lock.Unlock()
}

func (cs *ConnStats) addBytesRead(n uint64) {
	cs.lock.Lock()
	cs.BytesRead += n
	cs.lock.Unlock()
}

func (cs *ConnStats) incReadCalls() {
	cs.lock.Lock()
	cs.ReadCalls++
	cs.lock.Unlock()
}

func (cs *ConnStats) incReadErrors() {
	cs.lock.Lock()
	cs.ReadErrors++
	cs.lock.Unlock()
}

func (cs *ConnStats) incWriteCalls() {
	cs.lock.Lock()
	cs.WriteCalls++
	cs.lock.Unlock()
}

func (cs *ConnStats) incWriteErrors() {
	cs.lock.Lock()
	cs.WriteErrors++
	cs.lock.Unlock()
}

func (cs *ConnStats) incDialCalls() {
	cs.lock.Lock()
	cs.DialCalls++
	cs.lock.Unlock()
}

func (cs *ConnStats) incDialErrors() {
	cs.lock.Lock()
	cs.DialErrors++
	cs.lock.Unlock()
}

func (cs *ConnStats) incAcceptCalls() {
	cs.lock.Lock()
	cs.AcceptCalls++
	cs.lock.Unlock()
}

func (cs *ConnStats) incAcceptErrors() {
	cs.lock.Lock()
	cs.AcceptErrors++
	cs.lock.Unlock()
}
