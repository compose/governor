package main

import (
	"fmt"
	"log"
	//"os/exec"
	"flag"
	"github.com/compose/governor/fsm"
	"github.com/compose/governor/ha"
	"github.com/compose/governor/service"
	"time"
)

var configuration_file = flag.String("config", "./postgresql0.yml", "the yaml based configuration file.")

func main() {
	configuration, err := LoadConfiguration("postgres0.yml")
	if err != nil {
		log.Fatalf("Error loading governor configuration: %v", err)
	}

	configuration.Postgresql.DataDirectory = fmt.Sprintf("%s/%s", configuration.DataDir, "/pg")
	configuration.FSM.DataDir = fmt.Sprintf("%s/%s", configuration.DataDir, "/fsm")

	fmt.Println(configuration)

	pg, err := service.NewPostgresql(configuration.Postgresql)
	if err != nil {
		log.Fatalf("Error creating new postgresql")
	}

	singleLeaderState, err := fsm.NewGovernorFSM(configuration.FSM)
	if err != nil {
		log.Fatalf("Error creating new FSM")
	}

	haConf := &ha.SingleLeaderHAConfig{
		Service:    pg,
		FSM:        singleLeaderState,
		UpdateWait: time.Duration(configuration.LoopWait) * time.Millisecond,
	}

	ha := ha.NewSingleLeaderHA(haConf)
	if err := ha.Run(); err != nil {
		log.Fatalf("Error Running HA")
	}
}
