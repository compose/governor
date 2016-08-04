package fsm

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/compose/canoe"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type fsm struct {
	sync.Mutex
	observationLock sync.Mutex

	syncTicker <-chan time.Time

	raft *canoe.Node

	initID  *uint64
	gotInit chan bool

	current bool

	leader          *leaderBackend
	leaderChans     map[uint64]chan LeaderUpdate
	leaderObserveID uint64
	leaderTTL       int64

	members         map[string]*memberBackend
	memberChans     map[uint64]chan MemberUpdate
	memberObserveID uint64
	memberTTL       int64

	stopc    chan struct{}
	stoppedc chan struct{}

	startTime int64
}

type SingleLeaderFSM interface {
	UniqueID() uint64

	// this is blocking until we have an init since
	// init is set ONLY once in the life of the FSM
	RaceForInit(timeout time.Duration) (bool, error)

	LeaderObserver() (LeaderObserver, error)
	RaceForLeader(leader Leader) error
	RefreshLeader() error
	ForceLeader(leader Leader) error
	DeleteLeader() error
	Leader(leader Leader) (bool, error)

	MemberObserver() (MemberObserver, error)
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

	f.Lock()
	f.gotInit = make(chan bool)
	f.Unlock()

	if err := f.proposeRaceForInit(); err != nil {
		return false, errors.Wrap(err, "Error proposing race for init")
	}

	select {
	// This can get dangerous. Find a better way
	case <-time.After(timeout):
		return false, ErrorRaceTimedOut
	case val := <-f.gotInit:
		f.Lock()
		f.gotInit = nil
		f.Unlock()
		return val, nil
	}
}

type Config struct {
	RaftPort       int      `yaml:"raft_port"`
	APIPort        int      `yaml:"api_port"`
	BootstrapPeers []string `yaml:"bootstrap_peers"`
	BootstrapNode  bool     `yaml:"is_bootstrap"`
	DataDir        string   `yaml:"data_dir"`
	ClusterID      uint64   `yaml:"cluster_id"`
	LeaderTTL      int      `yaml:"leader_ttl"`
	MemberTTL      int      `yaml:"member_ttl"`
}

func NewGovernorFSM(config *Config) (SingleLeaderFSM, error) {
	newFSM := &fsm{
		leaderTTL: time.Duration(time.Duration(config.LeaderTTL) * time.Millisecond).Nanoseconds(),
		memberTTL: time.Duration(time.Duration(config.MemberTTL) * time.Millisecond).Nanoseconds(),

		members: make(map[string]*memberBackend),

		memberChans: make(map[uint64]chan MemberUpdate),
		leaderChans: make(map[uint64]chan LeaderUpdate),

		stopc:    make(chan struct{}),
		stoppedc: make(chan struct{}),

		observationLock: sync.Mutex{},

		syncTicker: time.Tick(500 * time.Millisecond),
		startTime:  time.Now().UnixNano(),
	}

	raftConfig := &canoe.NodeConfig{
		FSM:            newFSM,
		ClusterID:      config.ClusterID,
		RaftPort:       config.RaftPort,
		APIPort:        config.APIPort,
		BootstrapPeers: config.BootstrapPeers,
		BootstrapNode:  config.BootstrapNode,
		DataDir:        config.DataDir,
		SnapshotConfig: &canoe.SnapshotConfig{
			Interval: 20 * time.Second,
		},
	}

	log.WithFields(log.Fields{
		"package": "fsm",
	}).Info("Creating canoe node")

	node, err := canoe.NewNode(raftConfig)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating new canoe node")
	}

	log.WithFields(log.Fields{
		"package": "fsm",
	}).Info("Successfully created canoe node")

	newFSM.raft = node

	log.WithFields(log.Fields{
		"package": "fsm",
	}).Info("Starting Governor FSM")
	if err := newFSM.start(); err != nil {
		return nil, errors.Wrap(err, "Error starting FSM")
	}
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Info("Successfully started Governor FSM")

	log.WithFields(log.Fields{
		"package": "fsm",
	}).Info("Waiting for Governor FSM to catch up on raft logs")
	// TODO: Have this come down a chan
	for !newFSM.CompletedRestore() {
		if err := newFSM.proposeNewNodeUpToDate(); err != nil {
			return nil, errors.Wrap(err, "Error proposing new node up to date")
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Info("Governor FSM up to date")

	return newFSM, nil
}

func (f *fsm) start() error {
	if err := f.raft.Start(); err != nil {
		return errors.Wrap(err, "Error starting raft")
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
		case <-f.stopc:
			return nil
		case <-ttlTicker.C:
			if err := f.proposeDeleteStaleLeader(); err != nil {
				return errors.Wrap(err, "Error proposing delete stale leader")
			}
			if err := f.proposeDeleteStaleMembers(); err != nil {
				return errors.Wrap(err, "Error proposing delete stale leader")
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
func (f *fsm) LeaderObserver() (LeaderObserver, error) {
	f.observationLock.Lock()
	defer f.observationLock.Unlock()
	ch := make(chan LeaderUpdate)
	observer := &leaderUpdateObserver{
		updateCh: ch,
		fsm:      f,
		id:       atomic.AddUint64(&f.leaderObserveID, 1),
	}
	f.leaderChans[observer.id] = ch
	return observer, nil
}

func (f *fsm) observeLeaderUpdate(lu *LeaderUpdate) error {
	f.observationLock.Lock()
	defer f.observationLock.Unlock()
	for _, val := range f.leaderChans {
		val <- *lu
	}
	return nil
}

type leaderUpdateObserver struct {
	updateCh <-chan LeaderUpdate
	fsm      *fsm
	id       uint64
}

func (l *leaderUpdateObserver) LeaderCh() <-chan LeaderUpdate {
	return l.updateCh
}

func (l *leaderUpdateObserver) Destroy() error {
	return l.fsm.unregisterLeaderObserver(l)
}

func (f *fsm) unregisterLeaderObserver(l *leaderUpdateObserver) error {
	f.observationLock.Lock()
	defer f.observationLock.Unlock()
	delete(f.leaderChans, l.id)
	return nil
}

func (f *fsm) RaceForLeader(leader Leader) error {
	return errors.Wrap(f.proposeRaceLeader(leader), "Error proposing race for leader")
}

func (f *fsm) RefreshLeader() error {
	return errors.Wrap(f.proposeRefreshLeader(), "Error proposing refresh leader")
}

func (f *fsm) ForceLeader(leader Leader) error {
	return errors.Wrap(f.proposeForceLeader(leader), "Error proposing force leader")
}

func (f *fsm) DeleteLeader() error {
	return errors.Wrap(f.proposeDeleteLeader(), "Error proposing delete leader")
}

func (f *fsm) Leader(leader Leader) (bool, error) {
	f.Lock()
	defer f.Unlock()
	if f.leader == nil {
		f.leader = nil
		return false, nil
	}

	if err := leader.UnmarshalFSM(f.leader.Data); err != nil {
		return false, errors.Wrap(err, "Error unmarshaling leader")
	}

	return true, nil
}

func (f *fsm) MemberObserver() (MemberObserver, error) {
	f.observationLock.Lock()
	defer f.observationLock.Unlock()
	ch := make(chan MemberUpdate)
	updateObserver := &memberUpdateObserver{
		updateCh: ch,
		fsm:      f,
		id:       atomic.AddUint64(&f.memberObserveID, 1),
	}
	f.memberChans[updateObserver.id] = ch
	return updateObserver, nil
}

func (f *fsm) observeMemberUpdate(mu *MemberUpdate) error {
	f.observationLock.Lock()
	defer f.observationLock.Unlock()
	for _, val := range f.memberChans {
		val <- *mu
	}
	return nil
}

type memberUpdateObserver struct {
	updateCh <-chan MemberUpdate
	fsm      *fsm
	id       uint64
}

func (m *memberUpdateObserver) MemberCh() <-chan MemberUpdate {
	return m.updateCh
}

func (m *memberUpdateObserver) Destroy() error {
	return m.fsm.unregisterMemberObserver(m)
}

func (f *fsm) unregisterMemberObserver(m *memberUpdateObserver) error {
	f.observationLock.Lock()
	defer f.observationLock.Unlock()
	delete(f.memberChans, m.id)
	return nil
}

func (f *fsm) SetMember(member Member) error {
	return errors.Wrap(f.proposeSetMember(member), "Error proposing set member")
}

func (f *fsm) RefreshMember(id string) error {
	return errors.Wrap(f.proposeRefreshMember(id), "Error proposing refresh member")
}

func (f *fsm) DeleteMember(id string) error {
	return errors.Wrap(f.proposeDeleteMember(id), "Error proposing delete member")
}

func (f *fsm) Member(id string, member Member) (bool, error) {
	f.Lock()
	defer f.Unlock()
	if data, ok := f.members[id]; ok {
		err := member.UnmarshalFSM(data.Data)
		if err != nil {
			return false, errors.Wrap(err, "Error unmarshaling member")
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
			return errors.Wrap(err, "Error unmarshaling member")
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
		return errors.Wrap(err, "Error stopping raft")
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
		return errors.Wrap(err, "Error destroying raft")
	}

	close(f.stopc)

	select {
	case <-f.stoppedc:
	case <-time.Tick(10 * time.Second):
		return ErrorTimedOutCleanup
	}

	return nil
}

type fsmSnapshot struct {
	Members map[string]*memberBackend `json:"members"`
	Leader  *leaderBackend            `json:"leader"`
}

func (f *fsm) Restore(data canoe.SnapshotData) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Info("Restoring from Snapshot")

	var fsmSnap fsmSnapshot

	if err := json.Unmarshal(data, &fsmSnap); err != nil {
		return errors.Wrap(err, "Error unmarshaling snapshot")
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
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Info("Creating Snapshot")
	f.Lock()
	defer f.Unlock()
	retData, err := json.Marshal(&fsmSnapshot{
		Members: f.members,
		Leader:  f.leader,
	})

	return retData, errors.Wrap(err, "Error marshalling fsm snapshot")
}

func (f *fsm) RegisterAPI(router *mux.Router) {
	return
}
