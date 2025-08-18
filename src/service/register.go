package service

import (
	"errors"
	"log"
	"net/http"
	"time"
)

const (
	defaultTimeout = time.Minute * 5
)

// Heartbeat send a heartbeat message every once in a while
// it's a helper function for a server to registry or send heartbeat
func Heartbeat(registry, addr string, duration time.Duration, name ...string) {
	if duration == 0 {
		// make sure there is enough time to send heart beat
		// before it's removed from registry
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr, name...)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<- t.C
			err = sendHeartbeat(registry, addr, name...)
		}
	}()
}

func sendHeartbeat(registry, addr string, name ...string) error {
	if len(name) > 0 {
		if len(name) != 1 {
			return errors.ErrUnsupported
		}
		log.Println(addr, "send heart beat to registry center with name", registry)
		httpClient := &http.Client{}
		req, _ := http.NewRequest("POST", registry, nil)
		req.Header.Set("X-Geerpc-Name", name[0])
		req.Header.Set("X-Geerpc-Server", addr)
		if _, err := httpClient.Do(req); err != nil {
			log.Println("rpc server: heart beat with name err", err)
			return err
		}
		return nil
	}

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