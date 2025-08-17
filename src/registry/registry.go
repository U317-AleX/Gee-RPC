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
type GeeRegister struct {
	timeout time.Duration
	mu sync.Mutex
	servers map[string]*ServerItem
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

func (r *GeeRegister) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("X-Geerpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("X-Geerpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
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