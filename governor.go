package main

import (
	"fmt"
	//"os/exec"
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/compose/governor/api"
	"github.com/compose/governor/fsm"
	"github.com/compose/governor/ha"
	"github.com/compose/governor/service"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"time"
)

var configurationFile = flag.String("config", "./postgresql0.yml", "the yaml based configuration file.")
var version string

var outVersion = flag.Bool("version", false, "Output version info")

func main() {
	flag.Parse()

	if *outVersion {
		fmt.Printf("Governor: version - %s", version)
		os.Exit(0)
	}

	log.WithFields(log.Fields{
		"package": "governor",
	}).Infof("Loading configuration")
	configuration, err := LoadConfiguration(*configurationFile)

	if err != nil {
		log.Fatalf("Error loading governor configuration: %+v", err)
	}

	dataDir, err := filepath.Abs(configuration.DataDir)
	if err != nil {
		log.Fatalf("Error with data dir path: %+v", err)
	}
	configuration.DataDir = dataDir

	configuration.Postgresql.DataDirectory = fmt.Sprintf("%s%s", configuration.DataDir, "/pg/")
	configuration.FSM.DataDir = fmt.Sprintf("%s%s", configuration.DataDir, "/fsm/")

	log.WithFields(log.Fields{
		"package": "governor",
	}).Infof("Configuration Loaded: %v", configuration)

	pg, err := service.NewPostgresql(configuration.Postgresql)
	if err != nil {
		log.Fatalf("Error creating new postgresql: %+v", err)
	}

	log.WithFields(log.Fields{
		"package": "governor",
	}).Infof("Creating new FSM")
	singleLeaderState, err := fsm.NewGovernorFSM(configuration.FSM)
	if err != nil {
		log.Fatalf("Error creating new FSM, %+v", err)
	}

	log.WithFields(log.Fields{
		"package": "governor",
	}).Infof("Successfully created new FSM")

	haConf := &ha.SingleLeaderHAConfig{
		Service:    pg,
		FSM:        singleLeaderState,
		UpdateWait: time.Duration(configuration.LoopWait) * time.Millisecond,
	}

	log.WithFields(log.Fields{
		"package": "governor",
	}).Infof("Creating new HA")

	singleHA := ha.NewSingleLeaderHA(haConf)

	log.WithFields(log.Fields{
		"package": "governor",
	}).Infof("Running new HA")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(singleHA *ha.SingleLeaderHA, singleLeaderState fsm.SingleLeaderFSM, pg service.SingleLeaderService) {
		for _ = range c {
			log.WithFields(log.Fields{
				"package": "governor",
			}).Info("Shutting down")

			if err := singleHA.Stop(); err != nil {
				log.WithFields(log.Fields{
					"package": "governor",
				}).Errorf("Did not successfully teardown %+v", err)
				os.Exit(1)
			}

			log.WithFields(log.Fields{
				"package": "governor",
			}).Info("Clean Shutdown Finished")
		}
	}(singleHA, singleLeaderState, pg)

	go func() {
		router, err := api.Router(singleLeaderState, singleHA, pg)
		if err != nil {
			log.Error("Could not start API")
		}
		http.ListenAndServe(fmt.Sprintf(":%d", configuration.APIPort), router)
	}()

	if err := singleHA.Run(); err != nil {
		log.Fatalf("Error Running HA, %+v", err)
	}
}
