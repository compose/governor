package main

import (
	"fmt"
	"log"
	//"os/exec"
	"flag"
	"github.com/compose/governor/fsm"
	"github.com/compose/governor/ha"
	"github.com/compose/governor/service"
	"path/filepath"
	"time"
)

var configuration_file = flag.String("config", "./postgresql0.yml", "the yaml based configuration file.")

func main() {
	configuration, err := LoadConfiguration("postgres0.yml")
	if err != nil {
		log.Fatalf("Error loading governor configuration: %v", err)
	}

	dataDir, err := filepath.Abs(configuration.DataDir)
	if err != nil {
		log.Fatalf("Error with data dir path: %s", err.Error())
	}
	configuration.DataDir = dataDir

	configuration.Postgresql.DataDirectory = fmt.Sprintf("%s%s", configuration.DataDir, "/pg/")
	configuration.FSM.DataDir = fmt.Sprintf("%s%s", configuration.DataDir, "/fsm/")

	pg, err := service.NewPostgresql(configuration.Postgresql)
	if err != nil {
		log.Fatalf("Error creating new postgresql")
	}

	fmt.Println("Creating new FSM")
	singleLeaderState, err := fsm.NewGovernorFSM(configuration.FSM)
	if err != nil {
		log.Fatalf("Error creating new FSM")
	}

	haConf := &ha.SingleLeaderHAConfig{
		Service:    pg,
		FSM:        singleLeaderState,
		UpdateWait: time.Duration(configuration.LoopWait) * time.Millisecond,
	}

	fmt.Println("Creating new HA")
	ha := ha.NewSingleLeaderHA(haConf)
	fmt.Println("Running new HA")
	if err := ha.Run(); err != nil {
		log.Fatalf("Error Running HA, %+v", err)
	}
}
