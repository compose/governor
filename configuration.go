package main

import (
	"errors"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
)

type Configuration struct {
	LoopWait int        `yaml:"loop_wait"`
	FSM      fsm.Config `yaml:"fsm"`
	//Haproxy    HaproxyConfig `yaml:"haproxy_status"`
	Postgresql ha.PostgresqlConfig `yaml:"postgresql"`
}

type HaproxyConfig struct {
	Listen string
}

func LoadConfiguration(path string) (Configuration, error) {
	var configuration Configuration

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return configuration, err
	}

	err = yaml.Unmarshal(data, &configuration)
	if err != nil {
		return configuration, err
	}

	err = configuration.validate()
	if err != nil {
		return configuration, err
	}

	return configuration, err
}

func (c *Configuration) validate() error {
	if c.LoopWait*2 > c.Etcd.Ttl {
		return errors.New("etcd TTL should be at least 2x the loop wait.")
	}
	return nil
}
