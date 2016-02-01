package main

import (
	//"fmt"
	"io"
	"log"
	"os"
	//"os/exec"
	//"time"
)

func IsEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}

func main() {
	configuration, err := LoadConfiguration("postgres0.yml")
	if err != nil {
		log.Fatalf("Error loading governor configuration: %v", err)
	}

	log.Printf("Configuration is: %v", configuration.Postgresql.MaximumLagOnFailover)

	//postgresql, err := createPostgresql(configuration)
	//if err != nil {
	//log.Fatal("Error create Postgresql object: %v", err)
	//}

	//// if data is empty and etcd is empty
	//err = postgresql.initialize()
	//if err != nil {
	//log.Fatal("Error initializing Postgresql database: %v", err)
	//}

	//ha, err := CreateHA(configuration, etcd, postgresql)

	//cmd := startPostgres()
	//current_pid := os.Getpid()

	//defer stopPostgres()

	//runTime := 1
	//for cmd.ProcessState == nil && runTime < 30 {
	//current_process, err := os.FindProcess(current_pid)
	//if err != nil {
	//log.Printf("Error finding current process status: %v", err)
	//}
	//log.Printf("Current process status: %v", current_process)
	//log.Printf("Postgres is running: %v", runTime)
	//time.Sleep(1 * time.Second)
	//runTime += 1
	//}
}
