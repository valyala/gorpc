package gorpc

import (
	"encoding/binary"
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
// func RegisterType(x interface{}) {
// 	gob.Register(x)
// }

type wireRequest struct {
	ID      uint64
	Size    uint64
	Request io.ReadCloser
}

type wireResponse struct {
	ID       uint64
	Size     uint64
	Response io.ReadCloser
	Error    string
}

type messageEncoder struct {
	w    io.Writer
	stat *ConnStats
}

func (e *messageEncoder) Close() error {
	// if e.zw != nil {
	// 	return e.zw.Close()
	// }
	return nil
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

func (e *messageEncoder) encode(header []byte, body io.ReadCloser) error {
	n, err := e.w.Write(header)
	if err != nil {
		e.stat.incWriteErrors()
		return err
	}
	e.stat.addBytesWritten(uint64(n))

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
	header := make([]byte, 0, 16)
	header = binary.BigEndian.AppendUint64(header, req.ID)
	header = binary.BigEndian.AppendUint64(header, req.Size)
	return e.encode(header, req.Request)
}

func (e *messageEncoder) EncodeResponse(resp wireResponse) error {
	respErr := []byte(resp.Error)
	header := make([]byte, 0, 20+len(respErr))
	header = binary.BigEndian.AppendUint64(header, resp.ID)
	header = binary.BigEndian.AppendUint64(header, resp.Size)
	header = binary.BigEndian.AppendUint32(header, uint32(len(respErr)))
	header = append(header, respErr...)
	return e.encode(header, resp.Response)
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

	return &messageEncoder{
		w:    w,
		stat: s,
	}
}

type messageDecoder struct {
	r    io.Reader
	stat *ConnStats
}

func (d *messageDecoder) Close() error {
	// if d.zr != nil {
	// 	return d.zr.Close()
	// }
	return nil
}

func (d *messageDecoder) DecodeRequest(req *wireRequest) error {
	header := make([]byte, 16)
	n, err := io.ReadFull(d.r, header)
	if err != nil {
		d.stat.incReadErrors()
		return err
	}
	d.stat.addBytesRead(uint64(n))

	req.ID = binary.BigEndian.Uint64(header[:8])
	req.Size = binary.BigEndian.Uint64(header[8:])
	if req.Size > 0 {
		buf := bufferPool.Get().(*Buffer)
		buf.Reserve(int(req.Size))
		body := io.LimitReader(d.r, int64(req.Size))
		index := 0
		for {
			n, err = body.Read(buf.Bytes()[index:])
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			index += n
		}
		req.Request = buf
		d.stat.addBytesRead(uint64(index))
	}
	d.stat.incReadCalls()
	return nil
}

func (d *messageDecoder) DecodeResponse(resp *wireResponse) error {
	header := make([]byte, 20)
	n, err := io.ReadFull(d.r, header)
	if err != nil {
		d.stat.incReadErrors()
		return err
	}
	d.stat.addBytesRead(uint64(n))

	resp.ID = binary.BigEndian.Uint64(header[:8])
	resp.Size = binary.BigEndian.Uint64(header[8:16])
	errSize := binary.BigEndian.Uint32(header[16:20])
	respErr := make([]byte, errSize)
	n, err = io.ReadFull(d.r, respErr)
	if err != nil {
		d.stat.incReadErrors()
		return err
	}
	d.stat.addBytesRead(uint64(n))
	resp.Error = string(respErr)

	// resp.Response = io.LimitReader(d.r, int64(resp.Size))
	if resp.Size > 0 {
		buf := bufferPool.Get().(*Buffer)
		buf.Reserve(int(resp.Size))
		body := io.LimitReader(d.r, int64(resp.Size))
		index := 0
		for {
			n, err = body.Read(buf.Bytes()[index:])
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			index += n
		}
		resp.Response = buf
		d.stat.addBytesRead(uint64(index))
	}
	d.stat.incReadCalls()
	return nil
}

func newMessageDecoder(r io.Reader, s *ConnStats) *messageDecoder {
	return &messageDecoder{
		r:    r,
		stat: s,
	}
}
