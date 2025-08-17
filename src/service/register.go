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
// it's a helper function for a server to register or send heartbeat
func Heartbeat(register, addr string, duration time.Duration) {
	if duration == 0 {
		// make sure there is enough time to send heart beat
		// before it's removed from registry
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(register, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<- t.C
			err = sendHeartbeat(register, addr)
		}
	}()
}

func sendHeartbeat(register, addr string) error {
	log.Println(addr, "send heart beat to register center", register)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", register, nil)
	req.Header.Set("X-Geerpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err", err)
		return err
	}
	return nil
}