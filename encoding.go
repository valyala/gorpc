package iorpc

import (
	"encoding/binary"
	"encoding/gob"
	"io"
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

var (
	requestStartLineSize  = binary.Size(requestStartLine{})
	responseStartLineSize = binary.Size(responseStartLine{})
)

type requestStartLine struct {
	HeaderSize   uint32
	ID, BodySize uint64
}

type responseStartLine struct {
	ErrorSize    uint16
	HeaderSize   uint32
	ID, BodySize uint64
}

type wireRequest struct {
	ID      uint64
	Size    uint64
	Headers map[string]any
	Body    io.ReadCloser
}

type wireResponse struct {
	ID      uint64
	Size    uint64
	Headers map[string]any
	Body    io.ReadCloser
	Error   string
}

type messageEncoder struct {
	w             io.Writer
	headerBuffer  *Buffer
	headerEncoder *gob.Encoder
	stat          *ConnStats
}

func (e *messageEncoder) Close() error {
	// if e.zw != nil {
	// 	return e.zw.Close()
	// }
	return e.headerBuffer.Close()
}

func (e *messageEncoder) Flush() error {
	// if e.zw != nil {
	// 	if err := e.ww.Flush(); err != nil {
	// 		return err
	// 	}
	// 	if err := e.zw.Flush(); err != nil {
	// 		return err
	// 	}
	// }
	// if err := e.bw.Flush(); err != nil {
	// 	return err
	// }
	return nil
}

func (e *messageEncoder) encode(body io.ReadCloser) error {
	if e.headerBuffer.Len() > 0 {
		n, err := e.w.Write(e.headerBuffer.Bytes())
		if err != nil {
			e.stat.incWriteErrors()
			return err
		}
		e.stat.addBytesWritten(uint64(n))
	}

	if body != nil {
		defer body.Close()
		nc, err := io.Copy(e.w, body)
		if err != nil {
			e.stat.incWriteErrors()
			return err
		}
		e.stat.addBytesWritten(uint64(nc))
	}

	e.stat.incWriteCalls()
	return nil
}

func (e *messageEncoder) EncodeRequest(req wireRequest) error {
	if len(req.Headers) != 0 {
		e.headerBuffer.Reset()
		if err := e.headerEncoder.Encode(req.Headers); err != nil {
			e.stat.incWriteErrors()
			return err
		}
	}

	if err := binary.Write(e.w, binary.BigEndian, requestStartLine{
		ID:         req.ID,
		HeaderSize: uint32(e.headerBuffer.Len()),
		BodySize:   req.Size,
	}); err != nil {
		e.stat.incWriteErrors()
		return err
	}

	return e.encode(req.Body)
}

func (e *messageEncoder) EncodeResponse(resp wireResponse) error {
	if len(resp.Headers) != 0 {
		e.headerBuffer.Reset()
		if err := e.headerEncoder.Encode(resp.Headers); err != nil {
			e.stat.incWriteErrors()
			return err
		}
	}

	respErr := []byte(resp.Error)

	if err := binary.Write(e.w, binary.BigEndian, responseStartLine{
		ID:         resp.ID,
		ErrorSize:  uint16(len(respErr)),
		HeaderSize: uint32(e.headerBuffer.Len()),
		BodySize:   resp.Size,
	}); err != nil {
		e.stat.incWriteErrors()
		return err
	}

	if len(respErr) > 0 {
		n, err := e.w.Write(respErr)
		if err != nil {
			e.stat.incWriteErrors()
			return err
		}
		e.stat.addBytesWritten(uint64(n))
	}

	return e.encode(resp.Body)
}

func newMessageEncoder(w io.Writer, s *ConnStats) *messageEncoder {
	// w = newWriterCounter(w, s)
	// bw := bufio.NewWriterSize(w, bufferSize)

	// ww := bw
	// var zw *flate.Writer
	// if enableCompression {
	// 	zw, _ = flate.NewWriter(bw, flate.BestSpeed)
	// 	ww = bufio.NewWriterSize(zw, bufferSize)
	// }

	headerBuffer := bufferPool.Get().(*Buffer)

	return &messageEncoder{
		w:             w,
		headerBuffer:  headerBuffer,
		headerEncoder: gob.NewEncoder(headerBuffer),
		stat:          s,
	}
}

type messageDecoder struct {
	closeBody     bool
	r             io.Reader
	headerBuffer  *Buffer
	headerDecoder *gob.Decoder
	stat          *ConnStats
}

func (d *messageDecoder) Close() error {
	// if d.zr != nil {
	// 	return d.zr.Close()
	// }
	return d.headerBuffer.Close()
}

func (d *messageDecoder) DecodeRequest(req *wireRequest) error {
	var startLine requestStartLine
	if err := binary.Read(d.r, binary.BigEndian, &startLine); err != nil {
		d.stat.incReadErrors()
		return err
	}
	d.stat.addBytesRead(uint64(requestStartLineSize))

	req.ID = startLine.ID
	req.Size = startLine.BodySize

	if startLine.HeaderSize > 0 {
		d.headerBuffer.Reset()
		if _, err := io.CopyN(d.headerBuffer, d.r, int64(startLine.HeaderSize)); err != nil {
			d.stat.incReadErrors()
			return err
		}
		d.stat.addBytesRead(uint64(startLine.HeaderSize))
		if err := d.headerDecoder.Decode(&req.Headers); err != nil {
			d.stat.incReadErrors()
			return err
		}
	}

	if req.Size > 0 {
		buf := bufferPool.Get().(*Buffer)
		bytes, err := buf.ReadFrom(io.LimitReader(d.r, int64(req.Size)))
		if err != nil {
			return err
		}
		d.stat.addBytesRead(uint64(bytes))
		if d.closeBody {
			buf.Close()
		} else {
			req.Body = buf
		}
	}
	d.stat.incReadCalls()
	return nil
}

func (d *messageDecoder) DecodeResponse(resp *wireResponse) error {
	var startLine responseStartLine
	if err := binary.Read(d.r, binary.BigEndian, &startLine); err != nil {
		d.stat.incReadErrors()
		return err
	}
	d.stat.addBytesRead(uint64(responseStartLineSize))

	resp.ID = startLine.ID
	resp.Size = startLine.BodySize

	if startLine.ErrorSize > 0 {
		respErr := make([]byte, startLine.ErrorSize)
		if _, err := io.ReadFull(d.r, respErr); err != nil {
			d.stat.incReadErrors()
			return err
		}
		d.stat.addBytesRead(uint64(startLine.ErrorSize))
		resp.Error = string(respErr)
	}

	if startLine.HeaderSize > 0 {
		d.headerBuffer.Reset()
		if _, err := io.CopyN(d.headerBuffer, d.r, int64(startLine.HeaderSize)); err != nil {
			d.stat.incReadErrors()
			return err
		}
		d.stat.addBytesRead(uint64(startLine.HeaderSize))
		if err := d.headerDecoder.Decode(&resp.Headers); err != nil {
			d.stat.incReadErrors()
			return err
		}
	}

	if resp.Size > 0 {
		buf := bufferPool.Get().(*Buffer)
		bytes, err := buf.ReadFrom(io.LimitReader(d.r, int64(resp.Size)))
		if err != nil {
			return err
		}
		d.stat.addBytesRead(uint64(bytes))
		if d.closeBody {
			buf.Close()
		} else {
			resp.Body = buf
		}
	}
	d.stat.incReadCalls()
	return nil
}

func newMessageDecoder(r io.Reader, s *ConnStats, closeBody bool) *messageDecoder {
	headerBuffer := bufferPool.Get().(*Buffer)
	return &messageDecoder{
		r:             r,
		headerBuffer:  headerBuffer,
		headerDecoder: gob.NewDecoder(headerBuffer),
		stat:          s,
		closeBody:     closeBody,
	}
}
