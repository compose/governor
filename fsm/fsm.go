package fsm

import (
	"encoding/json"
	"errors"
	"github.com/compose/canoe"
	"github.com/gorilla/mux"
	"reflect"
	"sync"
	"time"
)

type fsm struct {
	sync.Mutex

	syncTicker <-chan time.Time

	raft *canoe.Node

	initID  *uint64
	gotInit chan bool

	current bool

	leader    *leaderBackend
	leaderc   chan *LeaderUpdate
	leaderTTL int64

	members   map[string]*memberBackend
	memberc   chan *MemberUpdate
	memberTTL int64

	stopc    chan struct{}
	stoppedc chan struct{}
}

type SingleLeaderFSM interface {
	UniqueID() uint64

	// this is blocking until we have an init since
	// init is set ONLY once in the life of the FSM
	RaceForInit(timeout time.Duration) (bool, error)

	// TODO: Treat as a proper chan? With observers?
	LeaderCh() <-chan *LeaderUpdate
	RaceForLeader(leader Leader) error
	RefreshLeader() error
	ForceLeader(leader Leader) error
	DeleteLeader() error
	Leader(leader Leader) (bool, error)

	MemberCh() <-chan *MemberUpdate
	SetMember(member Member) error
	RefreshMember(id string) error
	DeleteMember(id string) error
	Member(id string, member Member) (bool, error)
	Members(members interface{}) error

	CompletedRestore() bool

	Cleanup() error
	Destroy() error
}

var ErrorRaceTimedOut = errors.New("Waiting for init race timed out")

// Because this is blocking give timeout to wait before erring and coming back
// This also allows for an exponential backoff
func (f *fsm) RaceForInit(timeout time.Duration) (bool, error) {
	if f.initID != nil {
		return false, nil
	}

	if err := f.proposeRaceForInit(); err != nil {
		return false, err
	}

	select {
	case <-time.After(timeout):
		return false, ErrorRaceTimedOut
	case val := <-f.gotInit:
		return val, nil
	}

}

type Config struct {
	RaftPort       int
	APIPort        int
	BootstrapPeers []string
	BootstrapNode  bool
	DataDir        string
	ClusterID      uint64
	LeaderTTL      int64
}

// TODO: Implement TTL for members
func NewGovernorFSM(config *Config) (SingleLeaderFSM, error) {
	newFSM := &fsm{
		leaderTTL:  config.LeaderTTL,
		members:    make(map[string]*memberBackend),
		syncTicker: time.Tick(500 * time.Millisecond),
		stopc:      make(chan struct{}),
		stoppedc:   make(chan struct{}),
	}

	raftConfig := &canoe.NodeConfig{
		FSM:            newFSM,
		ClusterID:      config.ClusterID,
		RaftPort:       config.RaftPort,
		APIPort:        config.APIPort,
		BootstrapPeers: config.BootstrapPeers,
		DataDir:        config.DataDir,
		SnapshotConfig: &canoe.SnapshotConfig{
			Interval: 20 * time.Second,
		},
	}

	node, err := canoe.NewNode(raftConfig)
	if err != nil {
		return nil, err
	}

	newFSM.raft = node

	if err := newFSM.start(); err != nil {
		return nil, err
	}

	// TODO: do this a couple times since a single could get lost
	// TODO: Perhaps have this expire on occasion and force a touching update
	if err := newFSM.proposeNewNodeUpToDate(); err != nil {
		return nil, err
	}

	return newFSM, nil
}

func (f *fsm) start() error {
	if err := f.raft.Start(); err != nil {
		return err
	}

	go func(f *fsm) {
		if err := f.run(); err != nil {
			panic(err)
		}
	}(f)

	return nil
}

func (f *fsm) run() error {
	defer func(f *fsm) {
		close(f.stoppedc)
	}(f)

	ttlTicker := time.NewTicker(500 * time.Millisecond)

	for {
		select {
		case <-ttlTicker.C:
			if err := f.proposeDeleteStaleLeader(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *fsm) UniqueID() uint64 {
	return f.raft.UniqueID()
}

// LeaderCh returns a channel with LeaderUpdates
// LeaderCh does not block. Note: this means if the user is not monitoring
// LeaderCh then the LeaderUpdate will be lost it is the user's
// responsibility to ensure the channel is consumed as aggressively as is needed
// based on expected update to the leader
func (f *fsm) LeaderCh() <-chan *LeaderUpdate {
	return f.leaderc
}

func (f *fsm) RaceForLeader(leader Leader) error {
	return f.proposeRaceLeader(leader)
}

func (f *fsm) RefreshLeader() error {
	return f.proposeRefreshLeader()
}

func (f *fsm) ForceLeader(leader Leader) error {
	return f.proposeForceLeader(leader)
}

func (f *fsm) DeleteLeader() error {
	return f.proposeDeleteLeader()
}

func (f *fsm) Leader(leader Leader) (bool, error) {
	f.Lock()
	defer f.Unlock()
	if f.leader == nil {
		f.leader = nil
		return false, nil
	}

	if err := leader.UnmarshalFSM(f.leader.Data); err != nil {
		return false, err
	}

	return true, nil
}

func (f *fsm) MemberCh() <-chan *MemberUpdate {
	return f.memberc
}

func (f *fsm) SetMember(member Member) error {
	return f.proposeSetMember(member)
}

func (f *fsm) RefreshMember(id string) error {
	return f.proposeRefreshMember(id)
}

func (f *fsm) DeleteMember(id string) error {
	return f.proposeDeleteMember(id)
}

func (f *fsm) Member(id string, member Member) (bool, error) {
	f.Lock()
	defer f.Unlock()
	if data, ok := f.members[id]; ok {
		err := member.UnmarshalFSM(data.Data)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// Members gives all the members of the cluster
// you must pass a pointer to a slice of
func (f *fsm) Members(members interface{}) error {
	// Documented here http://stackoverflow.com/questions/25384640/why-golang-reflect-makeslice-returns-un-addressable-value
	// And the example from mgo http://bazaar.launchpad.net/+branch/mgo/v2/view/head:/session.go#L2769
	// This explains the odd reason for specifying the pointer to slice
	resultv := reflect.ValueOf(members)
	memberType := reflect.TypeOf((*Member)(nil)).Elem()

	if resultv.Kind() != reflect.Ptr ||
		resultv.Elem().Kind() != reflect.Slice ||
		!reflect.PtrTo(resultv.Elem().Type().Elem()).Implements(memberType) {

		return errors.New("Must provide a pointer to slice of Member - &[]Member")
	}

	sliceType := resultv.Elem().Type().Elem()
	retMembers := reflect.Indirect(reflect.New(resultv.Elem().Type()))

	f.Lock()
	defer f.Unlock()
	for _, memberBackend := range f.members {
		member := reflect.New(sliceType).Interface().(Member)
		if err := member.UnmarshalFSM(memberBackend.Data); err != nil {
			return err
		}

		retMembers.Set(
			reflect.Append(
				reflect.Indirect(retMembers),
				reflect.Indirect(reflect.ValueOf(member)),
			),
		)
	}

	resultv.Elem().Set(retMembers)

	return nil
}

func (f *fsm) CompletedRestore() bool {
	f.Lock()
	defer f.Unlock()

	return f.current
}

func (f *fsm) Cleanup() error {
	if err := f.raft.Stop(); err != nil {
		return err
	}
	close(f.stopc)

	select {
	case <-f.stoppedc:
	case <-time.Tick(10 * time.Second):
		return ErrorTimedOutCleanup
	}

	return nil
}

func (f *fsm) Destroy() error {
	if err := f.raft.Destroy(); err != nil {
		return err
	}

	close(f.stopc)

	select {
	case <-f.stoppedc:
	case <-time.Tick(10 * time.Second):
		return ErrorTimedOutCleanup
	}

	return nil
	close(f.stopc)

	select {
	case <-f.stoppedc:
	case <-time.Tick(10 * time.Second):
		return ErrorTimedOutDestroy
	}

	return nil
}

type fsmSnapshot struct {
	Members map[string]*memberBackend `json:"members"`
	Leader  *leaderBackend            `json:"leader"`
}

func (f *fsm) Restore(data canoe.SnapshotData) error {
	var fsmSnap fsmSnapshot

	if err := json.Unmarshal(data, &fsmSnap); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()
	// Don't worry with chan notifications here
	// As snapshots are only applied at startup
	f.members = fsmSnap.Members
	f.leader = fsmSnap.Leader

	return nil
}

func (f *fsm) Snapshot() (canoe.SnapshotData, error) {
	f.Lock()
	defer f.Unlock()
	return json.Marshal(&fsmSnapshot{
		Members: f.members,
		Leader:  f.leader,
	})
}

func (f *fsm) RegisterAPI(router *mux.Router) {
	return
}
