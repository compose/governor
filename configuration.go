package main

import (
	"errors"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
)

type Configuration struct {
	LoopWait   int              `yaml:"loop_wait"`
	Etcd       EtcdConfig       `yaml:"etcd"`
	Haproxy    HaproxyConfig    `yaml:"haproxy_status"`
	Postgresql PostgresqlConfig `yaml:"postgresql"`
}

type EtcdConfig struct {
	Scope string
	Ttl   int
}

type HaproxyConfig struct {
	Listen string
}

type PostgresqlConfig struct {
	Name                 string                    `yaml:"name"`
	Listen               string                    `yaml:"listen"`
	DataDirectory        string                    `yaml:"data_dir"`
	MaximumLagOnFailover int                       `yaml:"maximum_lag_on_failover"`
	Replication          PostgresqlReplicationInfo `yaml:"replication"`
	Parameters           map[string]interface{}    `yaml:"parameters"`
}

type PostgresqlReplicationInfo struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Network  string `yaml:"network"`
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
