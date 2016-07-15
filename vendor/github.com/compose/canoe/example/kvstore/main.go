package main

import (
	"flag"
	"fmt"
	"github.com/compose/canoe"
	"math/rand"
	_ "os"
	_ "os/signal"
	"strconv"
	"strings"
	_ "syscall"
	"time"
)

func main() {
	apiPort := flag.Int("api-port", 8080, "Port to serve API and discovery")
	raftPort := flag.Int("raft-port", 1234, "Port to serve raft")
	bootstrap := flag.Bool("bootstrap", false, "Is this the bootstrap node")
	peers := flag.String("peers", "", "List of peers")
	dataDir := flag.String("data-dir", "", "Directory to store persistent data")
	flag.Parse()
	rand.Seed(int64(time.Now().Nanosecond()))

	kv := NewKV()
	config := &canoe.NodeConfig{
		FSM:            kv,
		RaftPort:       *raftPort,
		APIPort:        *apiPort,
		BootstrapPeers: strings.Split(*peers, ","),
		BootstrapNode:  *bootstrap,
		DataDir:        *dataDir,
		SnapshotConfig: &canoe.SnapshotConfig{
			Interval: 5 * time.Second,
		},
	}

	if *peers == "" {
		config.BootstrapPeers = []string{}
	}

	raft, err := canoe.NewNode(config)
	if err != nil {
		fmt.Println("ERRERERERE ", err.Error())
	}
	kv.raft = raft

	/*sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)
	go func() {
		_ = <-sigc
		raft.Stop()
		os.Exit(0)
	}()*/

	if err := raft.Start(); err != nil {
		panic(err)
	}

	randomOps(kv, "kv1")
}

func randomOps(kv *KV, id string) {
	for {
		time.Sleep(time.Duration(rand.Int()%3) * time.Second)
		switch rand.Int() % 2 {
		case 0:
			key := rand.Int() % 30
			val := rand.Int() % 30
			fmt.Printf("Trying to Commit from kvstore: %d, %d\n", key, val)
			if err := kv.Set(strconv.Itoa(key), val); err != nil {
				fmt.Printf("ERROR setting %s to %s\n", key, val)
			} else {
				fmt.Printf("%s: Set %d to %d\n", id, key, val)
			}
			continue
		case 1:
			snap, err := kv.Snapshot()
			if err != nil {
				fmt.Printf("Error getting current state\n")
			} else {
				fmt.Printf("%s: Current State = %s\n", id, string(snap))
			}
			/*case 2:
			if kv.raft.IsRunning() {
				fmt.Printf("Pausing Raft\n")
				kv.raft.Pause()
				time.Sleep(time.Duration(rand.Int()%5) * time.Second)
				fmt.Printf("Resuming Raft\n")
				kv.raft.Resume()
				time.Sleep(time.Duration(rand.Int()%5) * time.Second)
			}*/
		}
	}
}
