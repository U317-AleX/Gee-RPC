package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"gee-rpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 0x3bef5c

// Option for encode and decode way
// this struct is used to negotiate encode and decode way between client and server
// this struct will be encode as json
// server uses json decodes this struct and decodes the follows using
// codec this struct identifies
type Option struct {
	MagicNumber    int           // MagicNumber marks this rpc request
	CodecType      codec.Type    // client may choose different Codec to encode body
	ConnectTimeout time.Duration // 0 means no limit
	HandleTimeout  time.Duration
}

// if dosen't order any option, use this
var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// Server represents an RPC Server
type Server struct {
	serviceMap sync.Map
}

func (server *Server) Registry(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

func Registry(rcvr interface{}) error {
	return DefaultServer.Registry(rcvr)
}

// use string serviceMethod who has format "<service>.<method>"
// to find service and method
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

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
	defer func() { _ = conn.Close() }()
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
	server.serveCodec(f(conn), &opt)
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

// serveCodec uses codec to read request and call handleRequest to handle it
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
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
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// request stores all info of a call
type request struct {
	h            *codec.Header // header of request
	argv, replyv reflect.Value // argv and reply of request, they're the request body
	mtype        *methodType   // required method of this RPC
	svc          *service      // required service of this RPC
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
// it reads header, argv, method type, service from client
// and create reply instance according to the client
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// make sure that argvi is a pointer, ReadBody need a pointer as parameter
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read boby err", err)
		return req, err
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
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{} // method has been called
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{} // have sent error info to client
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{} // have sent success info to client
	}()

	// if timeout is 0, meaning there's no timeout
	// simply wait called and sent signal to be sent and return
	if timeout == 0 {
		<-called
		<-sent
		return
	}

	// at timeout mement, if called, just wait for sent signal
	// otherwise, send error info to the client
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

const (
	Connected        = "200 Connected to Gee RPC"
	DefaultRPCPath   = "/_geerpc_"
	DefaultDebugPath = "/debug/geerpc"
)

// ServeHTTP is an HTTP request handler.
// It implements the http.Handler interface, allowing this Server object to be used by Go's HTTP server.
// This function is specifically designed to handle HTTP CONNECT requests to create a proxy tunnel.
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	
	// Type assertion to check if http.ResponseWriter (w) implements the http.Hijacker interface.
	// The Hijacker interface allows us to "hijack" the underlying TCP connection, taking control away from the HTTP server.
	// This is the crucial step for switching from the HTTP protocol to the raw TCP protocol.
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		log.Print("hijacking not supported by the underlying connection handler")
		return
	}

	// Hijack the connection. This returns the underlying net.Conn object.
	conn, _, err := hijacker.Hijack()
	if err != nil {
		// If hijacking fails, log the error and return.
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}

	// After a successful hijack, we must immediately send an HTTP response to the client to confirm the tunnel is established.
	// This response tells the client, "I've opened the tunnel for you; you can now start sending your encrypted data."
	// "200 Connection established" is a standard response for this.
	// Note that we are writing directly to the hijacked TCP connection (conn), not the http.ResponseWriter (w).
	// The 'connected' variable is a string constant, likely "200 Connection established".
	_, _ = io.WriteString(conn, "HTTP/1.0 "+Connected+"\n\n")

	// Pass the hijacked connection (conn) to the Server's ServeConn method.
	// ServeConn will typically handle the subsequent communication, such as establishing a proxy tunnel and forwarding data from conn to the destination server.
	// From this point on, all communication between the client and the server is a raw TCP data stream, independent of the HTTP protocol.
	server.ServeConn(conn)
}

// HandleHTTP registers an HTTP handler for building a RPC connection on rpcPath
// It is still necessary to invoke http.Serve(), typically in a go statement
func (server *Server) HandleHTTP() {
	http.Handle(DefaultRPCPath, server)
	http.Handle(DefaultDebugPath, debugHTTP{server})
	log.Println("rpc server debug path: ", DefaultDebugPath)
}

// HandleHTTP is a convenient approach for default server to registry HTTP handlers
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
