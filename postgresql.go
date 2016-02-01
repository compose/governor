package main

import (
	"database/sql"
	//"log"
)

type Postgresql struct {
	name     string
	host     string
	port     int
	data_dir string
	//replication ReplicationInfo
	config     map[string]string
	connection *sql.DB
}

func createPostgresql(config Configuration) (Postgresql, error) {
	return Postgresql{}, nil
}

func (*Postgresql) initialize() error {
	return nil
}

func (*Postgresql) start() error {
	return nil
}

func (*Postgresql) stop() error {
	return nil
}

func (*Postgresql) promote() error {
	return nil
}

func (*Postgresql) demote() error {
	return nil
}
