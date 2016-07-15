package main

import (
	"encoding/json"
	"fmt"
	"github.com/compose/canoe"
	"github.com/gorilla/mux"
	"sync"
)

func NewKV() *KV {
	return &KV{
		stateMap: make(map[string]interface{}),
	}
}

type KV struct {
	sync.Mutex
	stateMap map[string]interface{}

	raft *canoe.Node
}

type KVOp string

var SetOp = "set"
var DeleteOp = "delete"

type Command struct {
	Op   string
	Data interface{}
}

func (c *Command) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Op   string      `json:"op"`
		Data interface{} `json:"data"`
	}{
		Op:   c.Op,
		Data: c.Data,
	})
}

func (c *Command) UnmarshalJSON(data []byte) error {
	type Alias struct {
		Op   string      `json:"op"`
		Data interface{} `json:"data"`
	}
	var cmd Alias
	if err := json.Unmarshal(data, &cmd); err != nil {
		return err
	}

	switch cmd.Op {
	case SetOp:
		var setReq KVSetRequest
		setReq.Key = cmd.Data.(map[string]interface{})["key"].(string)
		setReq.Val = cmd.Data.(map[string]interface{})["val"].(interface{})
		cmd.Data = setReq
		c.Op = cmd.Op
		c.Data = setReq
		return nil
	case DeleteOp:
		var delReq KVDeleteRequest
		delReq.Key = cmd.Data.(map[string]interface{})["key"].(string)
		cmd.Data = delReq
		c.Op = cmd.Op
		c.Data = delReq
		return nil
	default:
		c.Op = cmd.Op
		c.Data = cmd.Data
		return nil
	}
}

type KVDeleteRequest struct {
	Key string `json:"key"`
}

type KVSetRequest struct {
	Key string      `json:"key"`
	Val interface{} `json:"val"`
}

func (kv *KV) Set(key string, val interface{}) error {
	command := &Command{
		Op: SetOp,
		Data: &KVSetRequest{
			Key: key,
			Val: val,
		},
	}

	data, err := json.Marshal(command)
	if err != nil {
		return err
	}

	return kv.raft.Propose(data)
}

// set is ONLY to be called by the Apply
func (kv *KV) set(setReq KVSetRequest) {
	kv.Lock()
	defer kv.Unlock()

	kv.stateMap[setReq.Key] = setReq.Val
}

func (kv *KV) Get(key string) interface{} {
	kv.Lock()
	defer kv.Unlock()

	return kv.stateMap[key]
}

func (kv *KV) Delete(key string) error {
	command := &Command{
		Op: DeleteOp,
		Data: &KVDeleteRequest{
			Key: key,
		},
	}

	data, err := json.Marshal(command)
	if err != nil {
		return err
	}

	return kv.raft.Propose(data)
}

func (kv *KV) delete(delReq KVDeleteRequest) {
	kv.Lock()
	defer kv.Unlock()
	delete(kv.stateMap, delReq.Key)
}

// Apply completes the FSM requirement
func (kv *KV) Apply(log canoe.LogData) error {
	var cmd Command
	if err := json.Unmarshal(log, &cmd); err != nil {
		return err
	}

	switch cmd.Op {
	case SetOp:
		kv.set(cmd.Data.(KVSetRequest))
	case DeleteOp:
		kv.delete(cmd.Data.(KVDeleteRequest))
	default:
		return fmt.Errorf("Unknown OP")
	}
	return nil
}

func (kv *KV) Snapshot() (canoe.SnapshotData, error) {
	kv.Lock()
	defer kv.Unlock()
	return json.Marshal(kv.stateMap)
}

func (kv *KV) Restore(data canoe.SnapshotData) error {
	var newStateMap map[string]interface{}

	if err := json.Unmarshal(data, &newStateMap); err != nil {
		return err
	}

	kv.Lock()
	defer kv.Unlock()

	kv.stateMap = newStateMap
	return nil
}

func (kv *KV) RegisterAPI(router *mux.Router) {
	return
}
