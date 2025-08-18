package service

import (
	"log"
	"net/http"
	"time"
)

const (
	defaultTimeout = time.Minute * 5
)

// Heartbeat send a heartbeat message every once in a while
// it's a helper function for a server to registry or send heartbeat
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		// make sure there is enough time to send heart beat
		// before it's removed from registry
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<- t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry center", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Geerpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err", err)
		return err
	}
	return nil
}

func HeartbeatWithName(name, registry, addr string, duration time.Duration) {
	if duration == 0 {
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeatWithName(name, registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<- t.C
			err = sendHeartbeatWithName(name, registry, addr)
		}
	}()
}

func sendHeartbeatWithName(name, registry, addr string) error {
	log.Println(addr, "send heart beat to registry center with name", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Geerpc-Name", name)
	req.Header.Set("X-Geerpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat with name err", err)
		return err
	}
	return nil
}