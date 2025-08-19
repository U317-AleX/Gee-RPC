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
	Refresh(name ...string) error // refresh from remote registry
	Update(servers []string, name ...string) error
	Get(mode SelectMode, name ...string) (string, error)
	GetAll(name ...string) ([]string, error)
	GetNames(_useLocalCache ...bool) ([]string, error) // get service names
}

type MultiServersDiscovery struct {
	r *rand.Rand // generate a random number
	mu sync.RWMutex // protect following
	servers []string // address of a server
	index int // record the selected position for robin algorithm
	services map[string][]string // service name to server
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


func (d *MultiServersDiscovery) GetNames(_useLocalCache ...string) ([]string, error) {
	// return local cache names
	names := make([]string, 0)
	for name := range d.services {
		names = append(names, name)
	}
	return names, nil
}

// Refresh doesn't make sense for MultiServerDiscovery, so ignore it
func (d *MultiServersDiscovery) Refresh(name ...string) error {
	return nil
}

// Update the servers of discovery if needed
func (d *MultiServersDiscovery) Update(servers []string, name ...string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(name) > 0 {
		if len(name) != 1 {
			return errors.ErrUnsupported
		}
		d.services[name[0]] = servers
		return nil
	}
	d.servers = servers
	return nil
}

// Get a server according to SelectMode
func (d *MultiServersDiscovery) Get(mode SelectMode, name ...string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(name) > 0 {
		if len(name) != 1 {
			return "", errors.ErrUnsupported
		}
		n := len(d.services[name[0]])
		if n == 0 {
			return "", errors.New("rpc discovery with name: no available servers")
		}
		switch mode {
		case RandomSelect:
			return d.services[name[0]][d.r.Intn(n)], nil
		case RoundRobinSelect:
			s := d.services[name[0]][d.index%n]
			d.index = (d.index + 1) % n
			return s, nil
		default:
			return "", errors.New("rpc discovery with name: not supported select mode")
		}
	}

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

// returns all servers in discovery
func (d *MultiServersDiscovery) GetAll(name ...string) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(name) > 0 {
		if len(name) != 1 {
			return nil, errors.ErrUnsupported
		}
		servers := make([]string, len(d.services[name[0]]), len(d.services[name[0]]))
		copy(servers, d.services[name[0]])
		return servers, nil
	}

	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}

const (
	local int = iota
	remote 
)

// GeeRegisterDiscovery is similar to ultiServersDiscovery
// but it use registry center
type GeeRegistryDiscovery struct {
	*MultiServersDiscovery
	serviceNames map[string]interface{} // serviceNames
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
		lastUpdateWithName: map[string]time.Time{},
		serviceNames: map[string]interface{}{},
	}
	return d
}

// Refresh remote service name and return all service name
func (d *GeeRegistryDiscovery) GetNames(_useLocalCache ...bool) ([]string, error) {
	if len(_useLocalCache) > 1 {
		return nil, errors.ErrUnsupported
	}

	var useLocalCache bool
	if len(_useLocalCache) == 0 {
		useLocalCache = true
	} else {
		useLocalCache = _useLocalCache[0]
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if useLocalCache && d.lastUpdate.Add(d.timeout).After(time.Now()) {
		names := make([]string, 0)
		for name := range d.serviceNames {
			names = append(names, name)
		}
		return names, nil
	}

	params := url.Values{}
	params.Add("method", "Get-Names")
	fullURL := fmt.Sprintf("%s?%s",d.registry, params.Encode())
	resp, err := http.Get(fullURL)
	if err != nil {
		log.Println("rpc registry getnames err", err)
		return nil, err
	}
	names := strings.Split(resp.Header.Get("X-Geerpc-Names"), ",")
	// delete local cache of remote names
	for name := range d.serviceNames {
		if d.serviceNames[name] == remote {
			delete(d.serviceNames, name)
		}
	}
	// update local cache of remote names
	for _, name := range names {
		if d.serviceNames[name] == nil {
			d.serviceNames[name] = remote
		}
	}
	_names := make([]string, 0)
	for name := range d.serviceNames {
		_names = append(_names, name)
	}
	return _names, nil
}

// Update the servers of discovery if needed
func (d *GeeRegistryDiscovery) Update(servers []string, name ...string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(name) > 0 {
		if len(name) != 1 {
			return errors.ErrUnsupported
		}
		_name := "local:" + name[0]
		d.services[_name] = servers
		d.lastUpdateWithName[_name] = time.Now()
		d.serviceNames[_name] = local
		return nil
	}
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

// Refresh remote service with remote name, 
// it would not affect service with local name
func (d *GeeRegistryDiscovery) Refresh(name ...string) error {
	d.GetNames(false)
	
	d.mu.Lock()
	defer d.mu.Unlock()
	
	if len(name) > 0 {
		if len(name) != 1 {
			return errors.ErrUnsupported
		}
		if d.serviceNames[name[0]] == local {
			return nil
		}
		if d.lastUpdateWithName[name[0]].Add(d.timeout).After(time.Now()) {
			return nil
		}
		log.Println("rpc registry with name: refresh servers from registry", d.registry)
		params := url.Values{}
		params.Add("method", "Get-Servers")
		params.Add("name", name[0])
		fullURL := fmt.Sprintf("%s?%s",d.registry, params.Encode())
		resp, err := http.Get(fullURL)
		if err != nil {
			log.Println("rpc registry refresh err", err)
			return err
		}
		servers := strings.Split(resp.Header.Get("X-Geerpc-Servers"), ",")
		d.services[name[0]] = make([]string, 0)
		for _, server := range servers {
			if strings.TrimSpace(server) != "" {
				d.services[name[0]] = append(d.services[name[0]], strings.TrimSpace(server))
			}
		}
		d.lastUpdateWithName[name[0]] = time.Now()
		return nil
	}

	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	params := url.Values{}
	params.Add("method", "Get-Servers")
	fullURL := fmt.Sprintf("%s?%s",d.registry, params.Encode())
	resp, err := http.Get(fullURL)
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

func (d *GeeRegistryDiscovery) Get(mode SelectMode, name ...string) (string, error) {
	if err := d.Refresh(name...); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode, name...)
}

func (d *GeeRegistryDiscovery) GetAll(name ...string) ([]string, error) {
	if err := d.Refresh(name...); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll(name...)
}