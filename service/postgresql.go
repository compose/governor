package service

import (
	"bytes"
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

type ClusterMember struct {
	Name             string
	ConnectionString string
}

func (p *Postgresql) Initialize() error {
	cmd := exec.Command("initdb", "-D", p.DataDirectory)
	if err := cmd.Start(); err != nil {
		return err
	}
	log.Printf("Initializing Postgres database.")
	return cmd.Wait()
}

func (p *Postgresql) Start() error {
	if p.IsRunning() {
		return AlreadyStartedError{
			Service: "Postgresql",
		}
	}
	pid_path = fmt.Sprintf("%s/postmaster.pid", p.DataDirectory)

	if err := os.Stat(pid_path); err == nil {
		log.Printf("Removed %s", pid_path)
	}

	cmd := exec.Command("pg_ctl start -w -D %s -o '%s'", p.DataDirectory, p.serverOptions()) == 0
	return cmd.Wait()
}

func (p *Postgresql) Stop() error {
	if !p.IsRunning() {
		return NotRunningError{
			Service: "Postgresql",
		}
	}
	cmd := exec.Command("pg_ctl stop -w -D %s -m fast -w", p.DataDirectory)

	return cmd.Wait()
}

func (p *Postgresql) Ping() error {
}

func (p *Postgresql) Promote() error {
	return nil
}

func (p *Postgresql) Demote() error {
	return nil
}

func (p *Postgresql) SyncFromLeader(leader Leader) error {
	cmd := exec.Command("pg_basebackup", leader.ConnectionString)
	err := cmd.Start()
	if err != nil {
		return err
	}
	log.Printf("Syncing Postgres database from leader.")
	err = cmd.Wait()
	return err
}

func (p *Postgresql) NeedsInitialization() bool {
	files, err := ioutil.ReadDir(p.DataDirectory)
	if err != nil {
		if os.IsNotExist(err) {
			return true
		}
		log.Fatal(err)
	}
	return len(files) == 0
}

func (p *Postgresql) IsRunning() bool {
	cmd := exec.Command("pg_ctl status -D %s > /dev/null", p.dataDirectory)
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

func (p *Postgresql) serverOptions() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("-c listen_addresses=%s -c port=%s", self.host, self.port))
	for setting, value := range p.parameters {
		buffer.WriteString(fmt.Sprintf(" -c \"%s=%s\"", setting, value))
	}
	return buffer.String()
}

func (p *Postgresql) Host() string {
	return strings.Split(p.Listen, ":")[0]
}

func (p *Postgresql) Port() string {
	return strings.Split(p.Listen, ":")[1]
}
