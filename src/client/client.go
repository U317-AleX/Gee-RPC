package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gee-rpc/codec"
	"gee-rpc/service"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Call represents an active RPC
type Call struct {
	Seq uint64 // sequence number to identify rpc
	ServiceMethod string // format "<service>.<method>"
	Args interface{} // arguments to the function
	Reply interface{} // reply from the function
	Error error // if error occurs, it will be set
	Done chan *Call // Strobes when call is complete
}

// when rpc finished, call done to inform caller of client
func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc codec.Codec // connnection, encoder and decoder
	opt *service.Option // codec type
	sending sync.Mutex // make sure sending in order
	header codec.Header
	mu sync.Mutex // protect following
	seq uint64 // Call ID, client use this to register Call
	pending map[uint64]*Call // calls waitting to be handled
	closing bool // user has called Close
	shutdown bool // server has told us to stop
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connetion is shut down")

// Close the connection
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// Give a Call ID and add it to pending
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq ++
	return call.Seq, nil
}

// Remove a Call from pending and get its 
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// called when there's err in server or client
// set shutdown to true and inform err to all the pending Calls
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// receive info from server
// there's three states:
// Call doesn't exist: The request may not have been sent completely, or it was canceled for some other reason, 
// but the server still processed it.
// Call exists, but the server processing failed: 
// The h.Error is not empty.
// Call exists, and the server processed it successfully: 
// The Reply value needs to be read from the body.
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
}

// When creating a Client instance, 
// the initial protocol exchange must be completed first 
// by sending Option information to the server. 
// After negotiating the message encoding/decoding method, 
// a sub-goroutine is created to call receive() to handle responses.
func NewClient(conn net.Conn, opt *service.Option) (*Client, error) {
	f := codec.NewCodeFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	if err := json.NewEncoder(conn).Encode(opt);err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *service.Option) *Client {
	client := &Client{
		seq: 1, // seq starts with 1, 0 means invalid call
		cc: cc,
		opt: opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

func parseOption(opts ...*service.Option) (*service.Option, error) {
	// if opts is nil or pass nil as parameter
	// use default Option
	if len(opts) == 0 || opts[0] == nil {
		return service.DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = service.DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = service.DefaultOption.CodecType
	}
	return opt, nil
}

type clientResult struct {
	client *Client
	err error
}

type newClientFunc func(conn net.Conn, opt *service.Option) (client *Client, err error)

func dialTimeout(f newClientFunc, network, address string, opts ... *service.Option) (client *Client, err error) {
	opt, err := parseOption(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// close the connetion if client is nil
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	// use a coroutine to create a client
	// when client is created, it sends signal to the channel
	// but there's a timer, when called DialTimeout
	// at the moment of timeout, it sends a signal to this channel also
	// so if the channel of time.After accepts a signal first
	// it's timeout 
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <- ch
		return result.client, result.err
	}
	select {
	case <- time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <- ch:
		return result.client, result.err
	}
}

// Dial is an intermediary layer between NewClient and the user
// It allows the user to avoid creating their own connection 
// and provides a default Option
func Dial(network, address string, opts ...*service.Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}

func (client *Client) send(call *Call) {
	// make sure that the client will send a complete request
	client.sending.Lock()
	defer client.sending.Unlock()

	// register this call
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// prepare request header
	// the header is re-used so it stored in Client
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// encode and send the request
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		// call may be nil, it usually means that Write partially failed
		// client has received the response and handled
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go is an asynchronous interface that returns a call instance. 
// When the Done channel is not empty, 
// the user can retrieve information from the Reply field.
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// Call is a wrapper around Go. It blocks on call.Done, 
// waiting for the response to return, 
// making it a synchronous interface.
// Call invokes the named function, waits for it to complete,
// and returns its error status
// context is set by user, it holds a timeout, at that moment it send a signal to it's channel
// if ctx.Done before call.Done, time is out
// else it's fine
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed" + ctx.Err().Error())
	case call := <- call.Done:
		return call.Error
	}
}

// NewHTTPClient new a Client instance via HTTP as transport protocol
// it just allows using http to build a tcp connection between client and server
// it is similar to Dial() above
func NewHTTPClient(conn net.Conn, opt *service.Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", service.DefaultRPCPath))

	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == service.Connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

// DialHTTP connects to an HTTP RPC server at the specified network address
// listening on the default HTTP RPC path
func DialHTTP(network, address string, opts ...*service.Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// XDial calls different functions to connect to a RPC server
// according the first parameter rpcAddr.
// rpcAddr is a general format (protocol@addr) to represent a rpc server
// eg, http@10.0.0.1:7001, tcp@10.0.0.1:9999, unix@/tmp/geerpc.sock
func XDial(rpcAddr string, opt ...*service.Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opt...)
	default:
		// tcp, unix or other transport protocol
		return Dial(protocol, addr, opt...)
	}
}