package main

import (
	"fmt"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
	"log"
	"time"
)

type Etcd struct {
	Scope     string
	Timeout   int
	Ttl       int
	Endpoints []string
	client    client.KeysAPI
}

func (e *Etcd) Available() bool {
	cfg := client.Config{
		Endpoints:               e.Endpoints,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Duration(e.Timeout) * time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	e.client = client.NewKeysAPIWithPrefix(c, fmt.Sprintf("/v2/keys/service/%s/", e.Scope))
	_, err = e.client.Get(context.Background(), "/", nil)
	if err != nil {
		log.Fatal(err)
		return false
	}
	return true
}

func (e *Etcd) WinInitializationRace(nodeName string) bool {
	_, err := e.client.Set(context.Background(), "/initialize", nodeName, &client.SetOptions{PrevExist: client.PrevNoExist})
	if err != nil {
		switch err := err.(type) {
		default:
			log.Fatalf("Error contacting etcd: %v", err)
		case client.Error:
			if err.Code == client.ErrorCodeNodeExist {
				return false
			}
			log.Fatalf("Error racing to initialize returned client.Error: %v", err)
		}
	}
	return true
}
