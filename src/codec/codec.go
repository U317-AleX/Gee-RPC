package codec

import "io"

type Header struct {
	ServiceMethod string // service name and method name, format "Service.Method"
	Seq           uint64 // sequence number chosen by cilent to identify rpcs
	Error         string // error info set by server
}

// interface of codec, which is used to encode and decode info body
type Codec interface {
	io.Closer                         // close connection
	ReadHeader(*Header) error         // read data header
	ReadBody(interface{}) error       // read data body
	Write(*Header, interface{}) error // write data header and body to data flow
}

type NewCodeFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodeFuncMap map[Type]NewCodeFunc

func init() {
	NewCodeFuncMap = make(map[Type]NewCodeFunc)
	NewCodeFuncMap[GobType] = NewGobCodec
}
