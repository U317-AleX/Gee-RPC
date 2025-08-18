package xclient

import (
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// different load-balance mode
type SelectMode int

const (
	RandomSelect SelectMode = iota // select randomly
	RoundRobinSelect // select using Robbin algorithm
)

type Discovery interface {
	Refresh() error // refresh from remote registry
	Update(servers []string) error
	Get(mode SelectMode) (string, error)
	GetAll() ([]string, error)

	RefreshWithName(name string) error
	UpdateWithName(name string, servers []string) error
	GetWithName(name string, mode SelectMode) (string, error)
	GetAllWithName(name string)  ([]string, error)
}

type MultiServersDiscovery struct {
	r *rand.Rand // generate a random number
	mu sync.RWMutex // protect following
	servers []string // address of a server
	index int // record the selected position for robin algorithm
	services map[string][]string // service name to server addresses
}

func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		servers: servers,
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
		services: make(map[string][]string),
	}
	d.index = d.r.Intn(math.MaxInt32 - 1) // generate a random index
	return d
}

var _Discovery = (*MultiServersDiscovery) (nil)

// Refresh doesn't make sense for MultiServerDiscovery, so ignore it
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

func (d *MultiServersDiscovery) RefreshWithName(name string) error {
	return nil
}

// Update the servers of discovery if needed
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

func (d *MultiServersDiscovery) UpdateWithName(name string, servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.services[name] = servers
	return nil
}

// Get a server according to SelectMode
func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	case RandomSelect:
		return d.servers[d.r.Intn(n)], nil
	case RoundRobinSelect:
		s := d.servers[d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

func (d *MultiServersDiscovery) GetWithName(name string, mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.services[name])
	if n == 0 {
		return "", errors.New("rpc discovery with name: no available servers")
	}
	switch mode {
	case RandomSelect:
		return d.services[name][d.r.Intn(n)], nil
	case RoundRobinSelect:
		s := d.services[name][d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery with name: not supported select mode")
	}
}

// returns all servers in discovery
func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers)
	// var servers []string
	// for _, rpcAddr := range d.servers {
	// 	servers = append(servers, rpcAddr)
	// }
	return servers, nil
}

func (d *MultiServersDiscovery) GetAllWithName(name string) ([]string, error) {
	d.mu.RLock()
	defer d.mu.Unlock()
	servers := make([]string, len(d.services[name]), len(d.services[name]))
	copy(servers, d.services[name])
	return servers, nil
}

// GeeRegisterDiscovery is similar to ultiServersDiscovery
// but it use registry center
type GeeRegistryDiscovery struct {
	*MultiServersDiscovery
	registry string // the url for registry center
	timeout time.Duration // to avoid using useless server 
	lastUpdate time.Time //to avoid using useless server
	lastUpdateWithName map[string]time.Time // service name to time.Time
}

const defaultTimeout = time.Second * 10

func NewGeeRegistryDiscovery(registerAddr string, timeout time.Duration) *GeeRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultTimeout
	}
	d := &GeeRegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry: registerAddr,
		timeout: timeout,
	}
	return d
}


func (d *GeeRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

func (d *GeeRegistryDiscovery) UpdateWithName(name string, servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.services[name] = servers
	d.lastUpdateWithName[name] = time.Now()
	return nil
}

func (d *GeeRegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-Geerpc-Servers"), ",")
	d.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			d.servers = append(d.servers, strings.TrimSpace(server))
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *GeeRegistryDiscovery) RefreshWithName(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastUpdateWithName[name].Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry with name: refresh servers from registry", d.registry)
	params := url.Values{}
	params.Add("name", name)
	fullURL := fmt.Sprintf("%s?%s",d.registry, params.Encode())
	log.Println("fullURL: ", fullURL)
	resp, err := http.Get(fullURL)
	if err != nil {
		log.Println("rpc registry refresh err", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-Geerpc-Servers"), ",")
	d.services[name] = make([]string, 0)
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			d.services[name] = append(d.services[name], strings.TrimSpace(server))
		}
	}
	d.lastUpdateWithName[name] = time.Now()
	return nil
}

func (d *GeeRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode)
}

func (d *GeeRegistryDiscovery) GetWithName(name string, mode SelectMode) (string, error) {
	if err := d.RefreshWithName(name); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.GetWithName(name, mode)
}

func (d *GeeRegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}

func (d *GeeRegistryDiscovery) GetAllWithName(name string) ([]string, error) {
	if err := d.RefreshWithName(name); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAllWithName(name)
}