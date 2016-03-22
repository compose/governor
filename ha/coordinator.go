package ha

import (
	"fmt"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
	"log"
	"time"
)

type Coordinator interface {
	Ping() error
	TryInitializationRace(nodeName string) (bool, error)
	Leader() (Leader, error)
	TakeLeader(value string, force bool) (bool, error)
}

type CoordinatorConfig struct {
	Scope     string
	Timeout   int
	Endpoints []string
	Ttl       int
}

type Leader struct {
	Name             string
	ConnectionString string
}

func NewCoordinator(args CoordinatorConfig) (Coordinator, error) {
	return newEtcd()
}

func newEtcd(scope string, timeout, ttl string, endpoints []string) (*etcd, error) {
	cfg := client.Config{
		Endpoints:               Endpoints,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Duration(timeout) * time.Second,
	}

	c, err := client.New(cfg)
	if err != nil {
		return nil, err
	}

	keyAPI = client.NewKeysAPIWithPrefix(c, fmt.Sprintf("/v2/keys/service/%s/", scope))

	e := Etcd{
		scope:   args.Scope,
		timeout: args.Timeout,
		ttl:     args.Ttl,
		client:  keyAPI,
	}

	return e, nil
}

type etcd struct {
	scope   string
	timeout int
	ttl     int
	client  client.KeysAPI
}

func (e *etcd) Ping() error {
	_, err = e.client.Get(context.Background(), "/", nil)
	return err
}

func (e *etcd) TryInitializationRace(nodeName string) (bool, error) {
	_, err := e.client.Set(context.Background(), "/initialize", nodeName, &client.SetOptions{PrevExist: client.PrevNoExist})
	if err != nil {
		switch err := err.(type) {
		default:
			return false, err
		case client.Error:
			if err.Code == client.ErrorCodeNodeExist {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

func (e *etcd) TakeLeader(value string, force bool) (bool, error) {
	var prevOption string
	if force {
		prevOption = client.PrevIgnore
	} else {
		prevOption = client.PrevNoExists
	}
	_, err := e.client.Set(
		context.Background(),
		"/leader",
		value,
		&client.SetOptions{PrevExist: prevOption})

	if err != nil {
		switch err := err.(type) {
		default:
			return false, err
		case client.Error:
			if err.Code == client.ErrorCodeNodeExist {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

func (e *Etcd) Leader() (Leader, error) {
	var leader Leader
	leader_name, err := e.client.Get(context.Background(), "/leader", nil)
	if err != nil {
		return leader, err
	}
	leader_connection_string, err := e.client.Get(context.Background(), fmt.Sprintf("/members/%s", leader_name.Node.Value), nil)
	if err != nil {
		return leader, err
	}
	log.Fatal(leader_connection_string)
	return Leader{Name: leader_name.Node.Value, ConnectionString: leader_connection_string.Node.Value}, err
}
