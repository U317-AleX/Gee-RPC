package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"gee-rpc"
	"gee-rpc/codec"
	"io"
	"log"
	"net"
	"sync"
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
	opt *geerpc.Option // codec type
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
func NewClient(conn net.Conn, opt *geerpc.Option) (*Client, error) {
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

func newClientCodec(cc codec.Codec, opt *geerpc.Option) *Client {
	client := &Client{
		seq: 1, // seq starts with 1, 0 means invalid call
		cc: cc,
		opt: opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

func parseOption(opts ...*geerpc.Option) (*geerpc.Option, error) {
	// if opts is nil or pass nil as parameter
	// use default Option
	if len(opts) == 0 || opts[0] == nil {
		return geerpc.DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = geerpc.DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = geerpc.DefaultOption.CodecType
	}
	return opt, nil
}

// Dial is an intermediary layer between NewClient and the user. 
// It allows the user to avoid creating their own connection 
// and provides a default Option
func Dial(network, address string, opts ...*geerpc.Option) (client *Client, err error) {
	opt, err := parseOption(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, opt)
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
// and returns its error status.
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}