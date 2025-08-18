package xclient

import (
	"context"
	"gee-rpc/client"
	"gee-rpc/service"
	"reflect"
	"sync"
)

// TODO: add with name function
type XClient struct {
	d Discovery
	mode SelectMode
	opt *service.Option
	mu sync.Mutex
	clients map[string]*client.Client
}

// The XClient constructor requires three parameters: 
// the service discovery instance Discovery, 
// the load balancing mode SelectMode, and the protocol option Option. 
// To maximize the reuse of existing socket connections, 
// clients is used to store successfully created Client instances, 
// and a Close method is provided to close the established connection upon completion.
func NewXClient(d Discovery, mode SelectMode, opt *service.Option) *XClient {
	return &XClient{
		d: d,
		mode: mode,
		opt: opt,
		clients: make(map[string]*client.Client),
	}
}

func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, client := range xc.clients {
		_ = client.Close()
		delete(xc.clients, key)
	}
	return nil
}

// fetch a client which connected with rpcAddr
func (xc *XClient) dial(rpcAddr string) (*client.Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	_client, ok := xc.clients[rpcAddr]
	// check if there's any client and whether it's avilable or not
	if ok && !_client.IsAvailable() {
		_ = _client.Close()
		delete(xc.clients, rpcAddr)
		_client = nil
	}
	// if there's no client on this address, create one
	if _client == nil {
		var err error
		_client, err = client.XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = _client
	}
	return _client, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

// Call invokes the named function, waits for it to complete
// and returns its error status
// xc will choose a proper server according to load-balance mode
func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	// get the current rpcAddr to use for load-balance
	rpcAddr, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// Broadcast invokes the named function for every server registered in discovery
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var e error
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string)  {
			defer wg.Done()
			clonedReply := reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			err := xc.call(rpcAddr, ctx, serviceMethod, args, clonedReply)
			if err != nil {
				e = err
				cancel() // if any call failed, cancel unfinished calls
			}
			mu.Lock()
			if err == nil && reply == nil {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
