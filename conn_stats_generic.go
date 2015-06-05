// +build !386

package gorpc

import (
	"sync/atomic"
)

// Snapshot returns connection statistics' snapshot.
//
// Use stats returned from ConnStats.Snapshot() on live Client and / or Server,
// since the original stats can be updated by concurrently running goroutines.
func (cs *ConnStats) Snapshot() *ConnStats {
	return &ConnStats{
		RPCCalls:     atomic.LoadUint64(&cs.RPCCalls),
		RPCTime:      atomic.LoadUint64(&cs.RPCTime),
		BytesWritten: atomic.LoadUint64(&cs.BytesWritten),
		BytesRead:    atomic.LoadUint64(&cs.BytesRead),
		ReadCalls:    atomic.LoadUint64(&cs.ReadCalls),
		ReadErrors:   atomic.LoadUint64(&cs.ReadErrors),
		WriteCalls:   atomic.LoadUint64(&cs.WriteCalls),
		WriteErrors:  atomic.LoadUint64(&cs.WriteErrors),
		DialCalls:    atomic.LoadUint64(&cs.DialCalls),
		DialErrors:   atomic.LoadUint64(&cs.DialErrors),
		AcceptCalls:  atomic.LoadUint64(&cs.AcceptCalls),
		AcceptErrors: atomic.LoadUint64(&cs.AcceptErrors),
	}
}

func (cs *ConnStats) incRPCCalls() {
	atomic.AddUint64(&cs.RPCCalls, 1)
}

func (cs *ConnStats) incRPCTime(dt uint64) {
	atomic.AddUint64(&cs.RPCTime, dt)
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
