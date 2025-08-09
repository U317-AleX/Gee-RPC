package geerpc

import (
	"encoding/json"
	"fmt"
	"gee-rpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

const MagicNumber = 0x3bef5c

// Option for encode and decode way
// this struct is used to negotiate encode and decode way between client and server
// this struct will be encode as json
// server uses json decodes this struct and decodes the follows using
// codec this struct identifies
type Option struct {
	MagicNumber int // MagicNumber marks this rpc request
	CodecType codec.Type // client may choose different Codec to encode body
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType: codec.GobType,
}

// Server represents an RPC Server
type Server struct{}

// NewServer returns a new Server
func NewServer() *Server {
	return &Server{}
}

// DefaultServer is the default instance of *Server
var DefaultServer = NewServer()

// Accept accepts connetions using the listener and serves requests
// for each incoming connetions
func (server *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {_ = conn.Close()}()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	if opt.MagicNumber != opt.MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodeFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	server.serveCodec(f(conn))
}


// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

// serveCodec uses codec to read request and call handleRequest to handle it
func (server *Server) serveCodec(cc codec.Codec) {
	sending := new(sync.Mutex) // make sure to send a complete response
	wg := new(sync.WaitGroup)
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break // it's impossible to recover, so close the connetion
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg)
	}
	wg.Wait()
	_ = cc.Close()
}

// request stores all info of a call
type request struct {
	h *codec.Header // header of request
	argv, replyv reflect.Value // argv and reply of request, they're the request body
}

// read request header using codec and write it to h
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	// codec has binded with the connetion, it will decode data from this connetion and write it to h
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// read whole request using codec and write it to request struct
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	// TODO: now we dont know the type of request argv
	// day 1, just suppose it's string
	req.argv = reflect.New(reflect.TypeOf(""))
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	return req, nil
}

// send response to client
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// handle request
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	// TODO: should call registered rpc methods to get the right replyv
	// day 1, just print argv and send a hello message
	defer wg.Done()
	log.Println(req.h, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("geerpc resp %d", req.h.Seq))
	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}