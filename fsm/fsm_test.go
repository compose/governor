package fsm

import (
	"encoding/json"
	"github.com/compose/canoe"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

var testDataDir = "./test_data"

// Cannot destroy raw tester. The stop channels won't be properly initialized
func newRawTestFSM() (*fsm, error) {
	newFSM := &fsm{}
	newFSM.observationLock = sync.Mutex{}
	// Try and clear values every half second
	newFSM.syncTicker = time.Tick(500 * time.Millisecond)
	newFSM.leaderTTL = time.Duration(3000 * time.Millisecond).Nanoseconds()
	newFSM.memberTTL = time.Duration(3000 * time.Millisecond).Nanoseconds()

	newFSM.leaderChans = make(map[uint64]chan LeaderUpdate)
	newFSM.memberChans = make(map[uint64]chan MemberUpdate)

	newFSM.stopc = make(chan struct{})
	newFSM.stoppedc = make(chan struct{})

	newFSM.members = make(map[string]*memberBackend)

	raftConfig := &canoe.NodeConfig{
		FSM:           newFSM,
		ClusterID:     0x1000,
		RaftPort:      1234,
		APIPort:       1235,
		BootstrapNode: true,
		DataDir:       "./fsm_test_data",
		SnapshotConfig: &canoe.SnapshotConfig{
			Interval: 20 * time.Second,
		},
	}

	node, err := canoe.NewNode(raftConfig)
	if err != nil {
		return nil, err
	}
	newFSM.raft = node

	return newFSM, nil
}

func newNonCurrentRunningTestFSM() (*fsm, error) {
	newFSM, err := newRawTestFSM()
	if err != nil {
		return nil, err
	}
	if err := newFSM.start(); err != nil {
		return nil, err
	}
	return newFSM, nil
}

func newCurrentRunningTestFSM() (*fsm, error) {
	newFSM, err := newNonCurrentRunningTestFSM()
	if err != nil {
		return nil, err
	}

	newNodeUpToDate := &newNodeUpToDateCmd{
		ID:        newFSM.UniqueID(),
		Timestamp: newFSM.startTime,
	}
	newNodeData, err := json.Marshal(newNodeUpToDate)
	if err != nil {
		return nil, err
	}

	cmd := &command{
		Op:   newNodeUpToDateOp,
		Data: newNodeData,
	}

	cmdData, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	if err := newFSM.raft.Propose(cmdData); err != nil {
		return nil, err
	}

	for !newFSM.current {
		time.Sleep(500 * time.Millisecond)
	}

	return newFSM, nil
}

func TestUniqueID(t *testing.T) {
	newFSM1, err := newRawTestFSM()
	assert.NoError(t, err, "Should be no error creating FSM")

	newFSM2, err := newRawTestFSM()
	assert.NoError(t, err, "Should be no error creating FSM")

	newFSM3, err := newRawTestFSM()
	assert.NoError(t, err, "Should be no error creating FSM")

	assert.NotEqual(t, newFSM1.UniqueID(), newFSM2.UniqueID(), "No unique ID should be the same")
	assert.NotEqual(t, newFSM1.UniqueID(), newFSM3.UniqueID(), "No unique ID should be the same")
	// Yes, transitive property, but tests are no place to be clever
	assert.NotEqual(t, newFSM2.UniqueID(), newFSM3.UniqueID(), "No unique ID should be the same")
}

func TestRaceForInit(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	// Give long timeout, as this is a non-loaded single node with this as it's only request
	hasInit, err := fsm.RaceForInit(5 * time.Second)
	assert.NoError(t, err, "Should be no error racing for init")
	assert.True(t, hasInit, "Should get init race")

	hasInit, err = fsm.RaceForInit(5 * time.Second)
	assert.NoError(t, err, "Should be no error racing for init")
	assert.False(t, hasInit, "Shouldn't get init race in second pass as it's already claimed")
}

func TestRaceForLeader(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new fsm")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	tl := &testLeader{UID: "1234"}

	err = fsm.RaceForLeader(tl)
	assert.NoError(t, err, "Should be no error racing for leader")

	// Let the leader race propogate before testing
	time.Sleep(time.Duration(fsm.leaderTTL / 2))

	tl = &testLeader{}
	leaderData, ok, err := fsm.Leader()
	assert.True(t, ok, "There should be a leader set at this point")
	assert.NoError(t, err, "There should be no error querying the leader id")

	tl.UnmarshalFSM(leaderData)
	assert.NoError(t, err, "There should be no error unmarshalling")
	assert.Equal(t, "1234", tl.UID)

	tl = &testLeader{UID: "5678"}
	err = fsm.RaceForLeader(tl)
	assert.NoError(t, err, "There should be no error racing for leader")

	// Let the leader race propogate before testing
	time.Sleep(time.Duration(fsm.leaderTTL / 2))

	tl = &testLeader{}
	leaderData, ok, err = fsm.Leader()
	assert.NoError(t, err, "There should be no error querying for leader")
	assert.True(t, ok, "There should still be a leader present")

	err = tl.UnmarshalFSM(leaderData)
	assert.NoError(t, err, "There should be no error unmarshalling")
	assert.Equal(t, "1234", tl.UID, "The leader should remain what it was before")
}

func TestLeaderTTL(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	tlb := &leaderBackend{
		ID:   "1234",
		Time: time.Now().UnixNano(),
		TTL:  fsm.leaderTTL,
	}
	fsm.Lock()
	fsm.leader = tlb
	fsm.Unlock()

	time.Sleep(time.Duration(time.Duration(tlb.TTL / 2)))
	assert.NotNil(t, fsm.leader, "Leader should not be nil by now")
	time.Sleep(time.Duration(500*time.Millisecond) + time.Duration(tlb.TTL/2))
	assert.Nil(t, fsm.leader, "Leader should be nil by now")
}

func TestMemberTTL(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	tmb := &memberBackend{
		ID:   "1234",
		Time: time.Now().UnixNano(),
		TTL:  fsm.memberTTL,
	}

	fsm.Lock()
	fsm.members[tmb.ID] = tmb
	fsm.Unlock()

	time.Sleep(time.Duration(time.Duration(tmb.TTL / 2)))
	_, ok := fsm.members[tmb.ID]
	assert.True(t, ok, "Member should not be nil by now")
	time.Sleep(time.Duration(500*time.Millisecond) + time.Duration(tmb.TTL/2))
	_, ok = fsm.members[tmb.ID]
	assert.False(t, ok, "Member should be nil by now")
}

func TestRefreshLeader(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	tlb := &leaderBackend{
		ID:   "1234",
		Time: time.Now().UnixNano(),
		TTL:  fsm.leaderTTL,
	}
	fsm.Lock()
	fsm.leader = tlb
	fsm.Unlock()

	time.Sleep(time.Duration(time.Duration(tlb.TTL / 2)))
	assert.NotNil(t, fsm.leader, "Leader should not be nil by now")

	err = fsm.RefreshLeader()
	assert.NoError(t, err, "Should have no error attempting to refresh leader")

	time.Sleep(time.Duration(time.Duration(tlb.TTL / 2)))
	assert.NotNil(t, fsm.leader, "Leader should not be nil by now")

	err = fsm.RefreshLeader()
	assert.NoError(t, err, "Should have no error attempting to refresh leader")

	time.Sleep(time.Duration(time.Duration(tlb.TTL / 2)))
	assert.NotNil(t, fsm.leader, "Leader should not be nil by now")
	time.Sleep(time.Duration(500*time.Millisecond) + time.Duration(tlb.TTL/2))
	assert.Nil(t, fsm.leader, "Leader should be nil by now")
}

func TestForceLeader(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	t.Run("Force Leader should work when leader is set", func(t *testing.T) {
		tlb := &leaderBackend{
			ID:   "1234",
			Time: time.Now().UnixNano(),
			TTL:  fsm.leaderTTL,
		}

		fsm.Lock()
		fsm.leader = tlb
		fsm.Unlock()

		tl := &testLeader{UID: "5678"}

		err = fsm.ForceLeader(tl)
		assert.NoError(t, err, "Should be no error proposing force leader")

		time.Sleep(time.Duration(time.Duration(tlb.TTL / 2)))
		tl = &testLeader{}
		leaderData, ok, err := fsm.Leader()
		assert.True(t, ok, "Should be a leader set right now")
		assert.NoError(t, err, "Should be no error fetching current leader")

		err = tl.UnmarshalFSM(leaderData)
		assert.NoError(t, err, "Should be no error unmarshalling leader")
		assert.Equal(t, "5678", tl.UID)
	})

	t.Run("Force Leader should work when leader is not set", func(t *testing.T) {
		tl := &testLeader{UID: "5678"}

		err = fsm.ForceLeader(tl)
		assert.NoError(t, err, "Should be no error proposing force leader")

		time.Sleep(500 * time.Millisecond)
		tl = &testLeader{}
		leaderData, ok, err := fsm.Leader()
		assert.True(t, ok, "Should be a leader set right now")
		assert.NoError(t, err, "Should be no error fetching current leader")

		err = tl.UnmarshalFSM(leaderData)
		assert.Equal(t, "5678", tl.UID)
	})
}

func TestDeleteLeader(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	t.Run("Delete leader should delete when IDs match", func(t *testing.T) {
		tlb := &leaderBackend{
			ID:   "1234",
			Time: time.Now().UnixNano(),
			// ensure it won't be deleted while testing
			TTL: time.Duration(40 * time.Second).Nanoseconds(),
		}

		fsm.Lock()
		fsm.leader = tlb
		fsm.Unlock()

		tl := &testLeader{UID: "1234"}

		err := fsm.DeleteLeader(tl)
		assert.NoError(t, err, "Should have no error when trying to delete leader")
		time.Sleep(500 * time.Millisecond)

		assert.Nil(t, fsm.leader, "Should have deleted leader")
	})
	t.Run("Delete leader should not delete when IDs match", func(t *testing.T) {
		tlb := &leaderBackend{
			ID:   "1234",
			Time: time.Now().UnixNano(),
			// ensure it won't be deleted while testing
			TTL: time.Duration(40 * time.Second).Nanoseconds(),
		}

		fsm.Lock()
		fsm.leader = tlb
		fsm.Unlock()

		tl := &testLeader{UID: "5678"}

		err := fsm.DeleteLeader(tl)
		assert.NoError(t, err, "Should have no error when trying to delete leader")
		time.Sleep(500 * time.Millisecond)

		assert.NotNil(t, fsm.leader, "Should have deleted leader")
	})
}

func TestLeader(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	t.Run("Should get leader back when set", func(t *testing.T) {
		tl := &testLeader{UID: "1234"}
		tlData, err := tl.MarshalFSM()
		assert.NoError(t, err, "Should be no error marshaling leader")
		tlb := &leaderBackend{
			ID:   "1234",
			Time: time.Now().UnixNano(),
			Data: tlData,
			// ensure it won't be deleted while testing
			TTL: time.Duration(40 * time.Second).Nanoseconds(),
		}

		fsm.Lock()
		fsm.leader = tlb
		fsm.Unlock()

		tl = &testLeader{}
		leaderData, ok, err := fsm.Leader()
		assert.True(t, ok, "There should be a leader since we just set it")
		assert.NoError(t, err, "There should be no error retreiving the leader")

		err = tl.UnmarshalFSM(leaderData)
		assert.NoError(t, err, "Should be no error unmarshaling leader data")
		assert.Equal(t, "1234", tl.UID)

		fsm.Lock()
		fsm.leader = nil
		fsm.Unlock()
	})

	t.Run("Should not get leader back when not set", func(t *testing.T) {
		_, ok, err := fsm.Leader()
		assert.False(t, ok, "There should not be a leader since we just set it")
		assert.NoError(t, err, "There should be no error retreiving the leader")

	})
}

func TestSetMember(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	t.Run("Should be able to set a member", func(t *testing.T) {
		tm := &testMember{UID: "1234"}
		err := fsm.SetMember(tm)
		assert.NoError(t, err, "There should be no error setting a member")

		time.Sleep(500 * time.Millisecond)

		val, ok := fsm.members["1234"]
		assert.True(t, ok, "The member should be set")

		tm = &testMember{}
		err = tm.UnmarshalFSM(val.Data)
		assert.NoError(t, err, "Should have no error unmarshaling member data")
		assert.Equal(t, "1234", tm.UID)

		delete(fsm.members, "1234")
	})
	t.Run("Should be able to set many members", func(t *testing.T) {
		tm1 := &testMember{UID: "1234"}
		tm2 := &testMember{UID: "5678"}

		err := fsm.SetMember(tm1)
		assert.NoError(t, err, "There should be no error setting a member")

		err = fsm.SetMember(tm2)
		assert.NoError(t, err, "There should be no error setting a member")

		time.Sleep(500 * time.Millisecond)

		val1, ok := fsm.members["1234"]
		assert.True(t, ok, "The member should be set")

		val2, ok := fsm.members["5678"]
		assert.True(t, ok, "The member should be set")

		tm1 = &testMember{}
		tm2 = &testMember{}

		err = tm1.UnmarshalFSM(val1.Data)
		assert.NoError(t, err, "Should be no error unmarshaling val")

		err = tm2.UnmarshalFSM(val2.Data)
		assert.NoError(t, err, "Should be no error unmarshaling val")

		assert.Equal(t, "1234", tm1.UID, "IDs should match for member 1234")
		assert.Equal(t, "5678", tm2.UID, "IDs should match for member 5678")

		delete(fsm.members, "1234")
		delete(fsm.members, "5678")
	})

	t.Run("Should be able to overwrite a member", func(t *testing.T) {
		tm := &testMember{
			UID:  "1234",
			Data: "0001",
		}
		tmData, err := tm.MarshalFSM()
		assert.NoError(t, err, "There should be no error marshaling data")

		tmb := &memberBackend{
			ID:   "1234",
			Time: time.Now().UnixNano(),
			Data: tmData,
			// Don't want this to expire soon
			TTL: time.Duration(50 * time.Second).Nanoseconds(),
		}

		fsm.Lock()
		fsm.members[tmb.ID] = tmb
		fsm.Unlock()

		tmOverwrite := &testMember{
			UID:  "1234",
			Data: "1110",
		}

		err = fsm.SetMember(tmOverwrite)
		assert.NoError(t, err, "Should have no error trying to set member")

		time.Sleep(500 * time.Millisecond)

		val, ok := fsm.members["1234"]
		assert.True(t, ok, "The member should be set")

		tm = &testMember{}
		err = tm.UnmarshalFSM(val.Data)
		assert.NoError(t, err, "Should have no error unmarshaling member data")
		assert.Equal(t, "1234", tm.UID, "Member should have same ID")
		assert.Equal(t, "1110", tm.Data, "Member should have the overwritten data")

		delete(fsm.members, "1234")
	})
}

func TestRefreshMember(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	tmb := &memberBackend{
		ID:   "1234",
		Time: time.Now().UnixNano(),
		TTL:  fsm.memberTTL,
	}
	fsm.Lock()
	fsm.members[tmb.ID] = tmb
	fsm.Unlock()

	time.Sleep(time.Duration(time.Duration(tmb.TTL / 2)))
	val, ok := fsm.members["1234"]
	assert.True(t, ok, "Member should not be nil by now")
	assert.Equal(t, "1234", val.ID, "Member should have correct ID")

	err = fsm.RefreshMember(tmb.ID)
	assert.NoError(t, err, "Should have no error attempting to refresh leader")

	time.Sleep(time.Duration(time.Duration(tmb.TTL / 2)))

	val, ok = fsm.members["1234"]
	assert.True(t, ok, "Member should not be nil by now")
	assert.Equal(t, "1234", val.ID, "Member should have correct ID")

	err = fsm.RefreshMember(tmb.ID)
	assert.NoError(t, err, "Should have no error attempting to refresh leader")

	time.Sleep(time.Duration(time.Duration(tmb.TTL / 2)))
	val, ok = fsm.members["1234"]
	assert.True(t, ok, "Member should not be nil by now")
	assert.Equal(t, "1234", val.ID, "Member should have correct ID")

	time.Sleep(time.Duration(500*time.Millisecond) + time.Duration(tmb.TTL/2))
	val, ok = fsm.members["1234"]
	assert.False(t, ok, "Member should be nil by now")
}

func TestDeleteMember(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	tmb := &memberBackend{
		ID:   "1234",
		Time: time.Now().UnixNano(),
		TTL:  fsm.memberTTL,
	}
	fsm.Lock()
	fsm.members[tmb.ID] = tmb
	fsm.Unlock()

	err = fsm.DeleteMember(tmb.ID)
	assert.NoError(t, err, "Should be no error proposing deletion of member")

	time.Sleep(500 * time.Millisecond)

	_, ok := fsm.members[tmb.ID]
	assert.False(t, ok, "Member should be deleted now")
}

func TestMember(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	t.Run("Should get member back when set", func(t *testing.T) {
		tm := &testMember{UID: "1234"}
		tmData, err := tm.MarshalFSM()
		assert.NoError(t, err, "Should be no error marshaling member")
		tmb := &memberBackend{
			ID:   "1234",
			Time: time.Now().UnixNano(),
			Data: tmData,
			// ensure it won't be deleted while testing
			TTL: time.Duration(40 * time.Second).Nanoseconds(),
		}

		fsm.Lock()
		fsm.members[tmb.ID] = tmb
		fsm.Unlock()

		tm = &testMember{}
		memberData, ok, err := fsm.Member(tmb.ID)
		assert.True(t, ok, "There should be a member since we just set it")
		assert.NoError(t, err, "There should be no error retreiving the leader")

		err = tm.UnmarshalFSM(memberData)
		assert.NoError(t, err, "Should be no error unmarshaling member")
		assert.Equal(t, "1234", tm.UID)

		fsm.Lock()
		delete(fsm.members, "1234")
		fsm.Unlock()
	})

	t.Run("Should not get member back when not set", func(t *testing.T) {
		_, ok, err := fsm.Member("1234")
		assert.False(t, ok, "There should not be a leader since we just set it")
		assert.NoError(t, err, "There should be no error retreiving the leader")
	})
}

func TestMembers(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	t.Run("Should get empty slice if not set", func(t *testing.T) {
		members, err := fsm.Members()
		assert.NoError(t, err, "There should be no error retreiving the leader")
		assert.Empty(t, members, "There should not be any members present")
	})

	t.Run("Should get all members when set", func(t *testing.T) {
		tm1 := &testMember{UID: "1234"}
		tm2 := &testMember{UID: "5678"}

		tm1Data, err := tm1.MarshalFSM()
		assert.NoError(t, err, "should have no error marshalling tm1")

		tm2Data, err := tm2.MarshalFSM()
		assert.NoError(t, err, "should have no error marshalling tm2")

		tmb1 := &memberBackend{
			ID:   "1234",
			Time: time.Now().UnixNano(),
			Data: tm1Data,
			// ensure it won't be deleted while testing
			TTL: time.Duration(40 * time.Second).Nanoseconds(),
		}
		tmb2 := &memberBackend{
			ID:   "5678",
			Time: time.Now().UnixNano(),
			Data: tm2Data,
			// ensure it won't be deleted while testing
			TTL: time.Duration(40 * time.Second).Nanoseconds(),
		}

		fsm.Lock()
		fsm.members[tmb1.ID] = tmb1
		fsm.members[tmb2.ID] = tmb2
		fsm.Unlock()

		members, err := fsm.Members()
		assert.NoError(t, err, "There should be no error retreiving the leader")
		assert.NotEmpty(t, members, "There should not be any members present")

		assert.Contains(t, members, tm1Data, "Members should have tm1")
		assert.Contains(t, members, tm2Data, "Members should have tm1")
	})
}

func TestCompletedRestore(t *testing.T) {
	nonRestoreFSM, err := newNonCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting non restored FSM")

	assert.False(t, nonRestoreFSM.CompletedRestore(), "Should not be restored")

	err = nonRestoreFSM.Destroy()
	assert.NoError(t, err, "Should be no error destroying FSM")

	restoredFSM, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error in getting restored FSM")

	assert.True(t, restoredFSM.CompletedRestore(), "This FSM should be restored and up to date")

	err = restoredFSM.Destroy()
	assert.NoError(t, err, "Should be no error destroying FSM")
}

func TestSnapshot(t *testing.T) {
	// Using raw FSM as the static state we set should be the only thing a snapshot cares about
	fsm, err := newRawTestFSM()
	assert.NoError(t, err, "Should be no error in getting raw fsm")

	// Seed data
	tl := &testLeader{UID: "1234"}
	tlData, err := tl.MarshalFSM()
	assert.NoError(t, err, "Should be no error marshaling leader")
	tlb := &leaderBackend{
		ID:   "1234",
		Time: time.Now().UnixNano(),
		Data: tlData,
		// ensure it won't be deleted while testing
		TTL: time.Duration(40 * time.Second).Nanoseconds(),
	}

	tm1 := &testMember{
		UID: "1234",
	}
	tm1Data, err := tm1.MarshalFSM()
	assert.NoError(t, err, "There should be no error marshaling data")

	tmb1 := &memberBackend{
		ID:   "1234",
		Time: time.Now().UnixNano(),
		Data: tm1Data,
		// Don't want this to expire soon
		TTL: time.Duration(50 * time.Second).Nanoseconds(),
	}

	tm2 := &testMember{
		UID: "5678",
	}
	tm2Data, err := tm2.MarshalFSM()
	assert.NoError(t, err, "There should be no error marshaling data")

	tmb2 := &memberBackend{
		ID:   "5678",
		Time: time.Now().UnixNano(),
		Data: tm2Data,
		// Don't want this to expire soon
		TTL: time.Duration(50 * time.Second).Nanoseconds(),
	}

	fsm.Lock()
	fsm.leader = tlb
	fsm.members[tmb1.ID] = tmb1
	fsm.members[tmb2.ID] = tmb2
	fsm.Unlock()

	expectedSnapStruct := &testSnapStruct{
		Members: map[string]*memberBackend{
			tmb1.ID: tmb1,
			tmb2.ID: tmb2,
		},
		Leader: tlb,
		InitID: fsm.initID,
	}
	expectedSnapData, err := json.Marshal(expectedSnapStruct)
	assert.NoError(t, err, "Should be no error marsaling snap data")

	actualSnapData, err := fsm.Snapshot()
	assert.NoError(t, err, "Should be no error taking snapshot")

	assert.EqualValues(t, expectedSnapData, actualSnapData)
}

func TestRestore(t *testing.T) {
	// Using raw FSM as the static state we set should be the only thing a snapshot cares about
	fsm, err := newRawTestFSM()
	assert.NoError(t, err, "Should be no error in getting raw fsm")

	tl := &testLeader{UID: "1234"}
	tlData, err := tl.MarshalFSM()
	assert.NoError(t, err, "Should be no error marshaling leader")
	tlb := &leaderBackend{
		ID:   "1234",
		Time: time.Now().UnixNano(),
		Data: tlData,
		// ensure it won't be deleted while testing
		TTL: time.Duration(40 * time.Second).Nanoseconds(),
	}

	tm1 := &testMember{
		UID: "1234",
	}
	tm1Data, err := tm1.MarshalFSM()
	assert.NoError(t, err, "There should be no error marshaling data")

	tmb1 := &memberBackend{
		ID:   "1234",
		Time: time.Now().UnixNano(),
		Data: tm1Data,
		// Don't want this to expire soon
		TTL: time.Duration(50 * time.Second).Nanoseconds(),
	}

	tm2 := &testMember{
		UID: "5678",
	}
	tm2Data, err := tm2.MarshalFSM()
	assert.NoError(t, err, "There should be no error marshaling data")

	tmb2 := &memberBackend{
		ID:   "5678",
		Time: time.Now().UnixNano(),
		Data: tm2Data,
		// Don't want this to expire soon
		TTL: time.Duration(50 * time.Second).Nanoseconds(),
	}

	members := map[string]*memberBackend{
		tmb1.ID: tmb1,
		tmb2.ID: tmb2,
	}

	initID := uint64(0xDEADBEEF)
	seedSnapStruct := &testSnapStruct{
		Members: members,
		Leader:  tlb,
		InitID:  &initID,
	}

	seedSnapData, err := json.Marshal(seedSnapStruct)
	assert.NoError(t, err, "There should be no error with marshaling seedSnapData")

	err = fsm.Restore(canoe.SnapshotData(seedSnapData))
	assert.NoError(t, err, "There should be no error restoring FSM")

	assert.EqualValues(t, initID, *fsm.initID, "Init ID should be restored")
	assert.EqualValues(t, members, fsm.members, "Should have same values for members")
	assert.EqualValues(t, tlb, fsm.leader, "Should have same values for leader")
}

func TestMemberObserver(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	observer, err := fsm.MemberObserver()
	assert.NoError(t, err, "Should get no error getting observer")

	memObserver := observer.(*memberUpdateObserver)
	_, ok := fsm.memberChans[memObserver.id]
	assert.True(t, ok, "Our observer should be registered")

	t.Run("Observer has chan set properly", func(t *testing.T) {
		memberCh := memObserver.MemberCh()

		memUpdate := MemberUpdate{
			Type:          MemberUpdateSetType,
			OldMember:     []byte("test1"),
			CurrentMember: []byte("test2"),
		}

		go func() {
			fsm.Lock()
			fsm.memberChans[memObserver.id] <- memUpdate
			fsm.Unlock()
		}()

		updateFromFSM := <-memberCh

		assert.Equal(t, MemberUpdateSetType, updateFromFSM.Type)
		assert.Equal(t, []byte("test1"), updateFromFSM.OldMember)
		assert.Equal(t, []byte("test2"), updateFromFSM.CurrentMember)
	})
	t.Run("Observer properly destroys itself", func(t *testing.T) {
		_, ok := fsm.memberChans[memObserver.id]
		assert.True(t, ok, "Our observer should be registered")

		err := memObserver.Destroy()
		assert.NoError(t, err, "Should be no error destroying observer")

		_, ok = fsm.memberChans[memObserver.id]
		assert.False(t, ok, "Our observer should be destroyed")
	})
}

func TestLeaderObserver(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new FSM")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	observer, err := fsm.LeaderObserver()
	assert.NoError(t, err, "Should get no error getting observer")

	leadObserver := observer.(*leaderUpdateObserver)
	_, ok := fsm.leaderChans[leadObserver.id]
	assert.True(t, ok, "Our observer should be registered")

	t.Run("Observer has chan set properly", func(t *testing.T) {
		leaderCh := leadObserver.LeaderCh()

		leadUpdate := LeaderUpdate{
			Type:          LeaderUpdateSetType,
			OldLeader:     []byte("test1"),
			CurrentLeader: []byte("test2"),
		}

		go func() {
			fsm.Lock()
			fsm.leaderChans[leadObserver.id] <- leadUpdate
			fsm.Unlock()
		}()

		updateFromFSM := <-leaderCh

		assert.Equal(t, LeaderUpdateSetType, updateFromFSM.Type)
		assert.Equal(t, []byte("test1"), updateFromFSM.OldLeader)
		assert.Equal(t, []byte("test2"), updateFromFSM.CurrentLeader)
	})
	t.Run("Observer properly destroys itself", func(t *testing.T) {
		_, ok := fsm.leaderChans[leadObserver.id]
		assert.True(t, ok, "Our observer should be registered")

		err := leadObserver.Destroy()
		assert.NoError(t, err, "Should be no error destroying observer")

		_, ok = fsm.leaderChans[leadObserver.id]
		assert.False(t, ok, "Our observer should be destroyed")
	})
}

type testSnapStruct struct {
	Members map[string]*memberBackend `json:"members"`
	Leader  *leaderBackend            `json:"leader"`
	InitID  *uint64                   `json:"init_id"`
}

type testLeader struct {
	UID string `json:"id"`
}

func (tl *testLeader) ID() string {
	return tl.UID
}

func (tl *testLeader) MarshalFSM() ([]byte, error) {
	return json.Marshal(tl)
}

func (tl *testLeader) UnmarshalFSM(data []byte) error {
	return json.Unmarshal(data, tl)
}

type testMember struct {
	UID  string `json:"id"`
	Data string `json:"data"`
}

func (tm *testMember) ID() string {
	return tm.UID
}

func (tm *testMember) MarshalFSM() ([]byte, error) {
	return json.Marshal(tm)
}

func (tm *testMember) UnmarshalFSM(data []byte) error {
	return json.Unmarshal(data, tm)
}
