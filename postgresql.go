package main

import (
	"database/sql"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
)

type Postgresql struct {
	Name                 string                    `yaml:"name"`
	Listen               string                    `yaml:"listen"`
	DataDirectory        string                    `yaml:"data_dir"`
	MaximumLagOnFailover int                       `yaml:"maximum_lag_on_failover"`
	Replication          PostgresqlReplicationInfo `yaml:"replication"`
	Parameters           map[string]interface{}    `yaml:"parameters"`
	connection           *sql.DB
}

type PostgresqlReplicationInfo struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Network  string `yaml:"network"`
}

func (p *Postgresql) Initialize() error {
	cmd := exec.Command("initdb", "-D", p.DataDirectory)
	err := cmd.Start()
	if err != nil {
		return err
	}
	log.Printf("Initializing Postgres database.")
	err = cmd.Wait()
	return err
}

func (p *Postgresql) Start() error {
	return nil
}

func (p *Postgresql) Stop() error {
	return nil
}

func (p *Postgresql) Promote() error {
	return nil
}

func (p *Postgresql) Demote() error {
	return nil
}

func (p *Postgresql) NeedsInitialization() bool {
	files, err := ioutil.ReadDir(p.DataDirectory)
	if err != nil {
		if os.IsNotExist(err) {
			return true
		}
		log.Fatal(err)
	}
	for _, file := range files {
		log.Printf(file.Name())
	}
	return false
}
