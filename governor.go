package main

import (
	"flag"
	//"fmt"
	"log"
	//"os/exec"
	"time"
)

var configuration_file = flag.String("config", "./postgresql0.yml", "the yaml based configuration file.")

func main() {
	configuration, err := LoadConfiguration("postgres0.yml")
	if err != nil {
		log.Fatalf("Error loading governor configuration: %v", err)
	}

	for !configuration.Etcd.Available() {
		log.Printf("Etcd is unreachable.  Waiting 10 seconds and trying again at %v", configuration.Etcd.Endpoints)
		time.Sleep(10 * time.Second)
	}

	if configuration.Postgresql.NeedsInitialization() {
		log.Printf("Postgres needs to initialize; racing to etcd initialization key.")
		if configuration.Etcd.WinInitializationRace(configuration.Postgresql.Name) {
			err = configuration.Postgresql.Initialize()
			if err != nil {
				log.Fatal("Error initializing Postgresql database: %v", err)
			}
		} else {
			for configuration.Postgresql.NeedsInitialization() {
				log.Printf("Getting leader from Etcd")
				leader, err := configuration.Etcd.Leader()
				if err != nil {
					log.Printf("Error getting leader from Etcd: %v", err)
					time.Sleep(10 * time.Second)
				}
				err = configuration.Postgresql.SyncFromLeader(leader)
				if err != nil {
					time.Sleep(10 * time.Second)
				}
			}
		}
	}

	//ha, err := CreateHA(configuration)

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
