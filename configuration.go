package main

import (
	"errors"
	"github.com/compose/governor/fsm"
	"github.com/compose/governor/service"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
)

type Configuration struct {
	LoopWait   int                       `yaml:"loop_wait"`
	DataDir    string                    `yaml:"data_dir"`
	FSM        *fsm.Config               `yaml:"fsm"`
	Postgresql *service.PostgresqlConfig `yaml:"postgresql"`
	HAHealth   string                    `yaml:"haproxy_health_endpoint"`
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
	if c.LoopWait*2 > c.FSM.MemberTTL {
		return errors.New("MemberTTL should be at least 2x the loop wait.")
	}
	if c.LoopWait*2 > c.FSM.LeaderTTL {
		return errors.New("LeaderTTL should be at least 2x the loop wait.")
	}
	return nil
}
