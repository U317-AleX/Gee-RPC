package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// GeeRegistry is a simple registry center, provide following functions.
// add a server and receive heartbeat to keep it alive.
// returns all alive servers and delete dead servers sync simultaneously.

type servers map[string]*ServerItem

type GeeRegister struct {
	timeout time.Duration
	mu sync.Mutex
	servers servers // address to ServerItem
	services map[string]servers // name to servers
}

type ServerItem struct {
	Addr string
	start time.Time
}

const (
	defaultPath = "/_geerpc_/registry"
	defaultTimeout = time.Minute * 5
)

func New(timeout time.Duration) *GeeRegister {
	return &GeeRegister{
		timeout: timeout,
		servers: make(map[string]*ServerItem),
		services: make(map[string]servers),
	}
}

var DefaultGeeRegister = New(defaultTimeout)

func (r *GeeRegister) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{
			Addr: addr,
			start: time.Now(),
		}
	} else {
		r.servers[addr].start = time.Now() // if exists, update start time to keep alive
	}
}

func (r *GeeRegister) putServerWithName(name, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	svi := r.services[name]
	if svi == nil {
		r.services[name] = make(servers)
		r.services[name][addr] = &ServerItem{
			Addr: addr,
			start: time.Now(),
		}
		return
	}
	if svi[addr] == nil{
		r.services[name][addr] = &ServerItem{
			Addr: addr,
			start: time.Now(),
		}
		return
	}
	r.services[name][addr].start = time.Now()
}

func (r *GeeRegister) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	aliveServers := make([]string, 0)
	for addr := range r.servers {
		if r.timeout == 0 || time.Since(r.servers[addr].start) > r.timeout {
			delete(r.servers, addr)
		} else {
			aliveServers = append(aliveServers, addr)
		}
	}
	sort.Strings(aliveServers)
	return aliveServers
}

func (r *GeeRegister) aliveServersWithName(name string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	aliveServers := make([]string, 0)
	for addr := range r.services[name] {
		if r.timeout == 0 || time.Since(r.services[name][addr].start) > r.timeout {
			delete(r.services[name], addr)
		} else {
			aliveServers = append(aliveServers, addr)
		}
	}
	sort.Strings(aliveServers)
	return aliveServers
}

// TODO: Registry withName functions here
func (r *GeeRegister) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		query := req.URL.Query()
		serviceName := query.Get("name")
		if serviceName != "" {
			w.Header().Set("X-Geerpc-Servers", strings.Join(r.aliveServersWithName(serviceName), ","))
			return
		}
		w.Header().Set("X-Geerpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("X-Geerpc-Server")
		name := req.Header.Get("X-Geerpc-Name")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if name != "" {
			r.putServerWithName(name, addr)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *GeeRegister) HandleHTTP(registerPath string) {
	http.Handle(registerPath, r)
	log.Println("rpc registry path: ", registerPath)
}

func HandleHTTP() {
	DefaultGeeRegister.HandleHTTP(defaultPath)
}