package gorpc

import (
	"bufio"
	"compress/flate"
	"encoding/gob"
	"encoding/json"
	"io"
)

type EncodingFormat uint8

const (
	ENCODING_GOB = 0
	ENCODING_JSON = 1
)

// RegisterType registers the given type to send via rpc.
//
// The client must register all the response types the server may send.
// The server must register all the request types the client may send.
//
// There is no need in registering base Go types such as int, string, bool,
// float64, etc. or arrays, slices and maps containing base Go types.
//
// There is no need in registering argument and return value types
// for functions and methods registered via Dispatcher.
func RegisterType(x interface{}) {
	gob.Register(x)
}

type wireRequest struct {
	ID           uint64
	Request      interface{}
	SkipResponse bool
}

type wireResponse struct {
	ID       uint64
	Response interface{}
	Error    string
}

type Encoder interface {
	Encode(v interface{}) error
}

type Decoder interface {
	Decode(v interface{}) error
}

type messageEncoder struct {
	e  Encoder
	bw *bufio.Writer
	zw *flate.Writer
	ww *bufio.Writer
}

func (e *messageEncoder) Close() error {
	if e.zw != nil {
		return e.zw.Close()
	}
	return nil
}

func (e *messageEncoder) Flush() error {
	if e.zw != nil {
		if err := e.ww.Flush(); err != nil {
			return err
		}
		if err := e.zw.Flush(); err != nil {
			return err
		}
	}
	if err := e.bw.Flush(); err != nil {
		return err
	}
	return nil
}

func (e *messageEncoder) Encode(msg interface{}) error {
	return e.e.Encode(msg)
}

func newMessageEncoder(w io.Writer, bufferSize int, enableCompression bool, s *ConnStats, f EncodingFormat) *messageEncoder {
	w = newWriterCounter(w, s)
	bw := bufio.NewWriterSize(w, bufferSize)

	ww := bw
	var zw *flate.Writer
	if enableCompression {
		zw, _ = flate.NewWriter(bw, flate.BestSpeed)
		ww = bufio.NewWriterSize(zw, bufferSize)
	}

	var e Encoder
	switch (f) {
	case ENCODING_JSON:
		e = json.NewEncoder(ww)
	case ENCODING_GOB:
		e = gob.NewEncoder(ww)
	}

	return &messageEncoder{
		e:  e,
		bw: bw,
		zw: zw,
		ww: ww,
	}
}

type messageDecoder struct {
	d  Decoder
	zr io.ReadCloser
}

func (d *messageDecoder) Close() error {
	if d.zr != nil {
		return d.zr.Close()
	}
	return nil
}

func (d *messageDecoder) Decode(msg interface{}) error {
	return d.d.Decode(msg)
}

func newMessageDecoder(r io.Reader, bufferSize int, enableCompression bool, s *ConnStats, f EncodingFormat) *messageDecoder {
	r = newReaderCounter(r, s)
	br := bufio.NewReaderSize(r, bufferSize)

	rr := br
	var zr io.ReadCloser
	if enableCompression {
		zr = flate.NewReader(br)
		rr = bufio.NewReaderSize(zr, bufferSize)
	}

	var d Decoder
	switch (f) {
	case ENCODING_JSON:
		d = json.NewDecoder(rr)
	case ENCODING_GOB:
		d = gob.NewDecoder(rr)
	}

	return &messageDecoder{
		d:  d,
		zr: zr,
	}
}
