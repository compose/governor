package fsm

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func marshalledTestCmdFromStructs(data interface{}, op string) ([]byte, error) {
	reqData, err := json.Marshal(data)
	if err != nil {
		return []byte{}, err
	}

	newCmd := &command{
		Op:   op,
		Data: reqData,
	}

	return json.Marshal(newCmd)
}

func TestApply(t *testing.T) {
	fsm, err := newCurrentRunningTestFSM()
	assert.NoError(t, err, "Should be no error getting new fsm")
	defer func() {
		err := fsm.Destroy()
		assert.NoError(t, err, "Should be no error destroying FSM")
	}()

	t.Run("Should apply force leader Op", func(t *testing.T) {
		t.Run("Apply force leader should work when no leader", func(t *testing.T) {
			assert.Nil(t, fsm.leader, "Leader should be nil at start of this test")

			forceLeader := &forceLeaderCmd{
				leaderBackend{
					ID: "1234",
				},
			}
			cmdData, err := marshalledTestCmdFromStructs(forceLeader, forceLeaderOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			assert.NotNil(t, fsm.leader, "Leader should no longer be nil")
			assert.Equal(t, "1234", fsm.leader.ID, "Leader ID should be 1234")

			// cleanup
			fsm.leader = nil
		})
		t.Run("Apply force leader should work when leader exists", func(t *testing.T) {
			assert.Nil(t, fsm.leader, "Leader should be nil at start of this test")

			// set initial value
			fsm.leader = &leaderBackend{
				ID: "1234",
			}

			forceLeader := &forceLeaderCmd{
				leaderBackend{
					ID: "5678",
				},
			}
			cmdData, err := marshalledTestCmdFromStructs(forceLeader, forceLeaderOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			assert.NotNil(t, fsm.leader, "Leader should no longer be nil")
			assert.Equal(t, "5678", fsm.leader.ID, "Leader ID should be overridden from force leader")

			// cleanup
			fsm.leader = nil
		})
	})
	t.Run("Should apply race leader op", func(t *testing.T) {
		t.Run("Apply race leader should work when no leader", func(t *testing.T) {
			assert.Nil(t, fsm.leader, "Leader should be nil at start of this test")

			raceLeader := &raceLeaderCmd{
				leaderBackend{
					ID: "1234",
				},
			}
			cmdData, err := marshalledTestCmdFromStructs(raceLeader, raceLeaderOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			assert.NotNil(t, fsm.leader, "Leader should no longer be nil")
			assert.Equal(t, "1234", fsm.leader.ID, "Leader ID should be 1234")

			// cleanup
			fsm.leader = nil
		})
		t.Run("Apply race leader should not alter an existing leader", func(t *testing.T) {
			assert.Nil(t, fsm.leader, "Leader should be nil at start of this test")

			fsm.leader = &leaderBackend{
				ID: "1234",
			}

			raceLeader := &raceLeaderCmd{
				leaderBackend{
					ID: "5678",
				},
			}
			cmdData, err := marshalledTestCmdFromStructs(raceLeader, raceLeaderOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			assert.NotNil(t, fsm.leader, "Leader should no longer be nil")
			assert.Equal(t, "1234", fsm.leader.ID, "Leader ID should be 1234")

			// cleanup
			fsm.leader = nil
		})
	})
	t.Run("Should apply refresh leader op", func(t *testing.T) {
		t.Run("Time should be set to the refresh time", func(t *testing.T) {
			fsm.leader = &leaderBackend{
				ID:   "1234",
				Time: time.Now().UnixNano(),
			}

			refreshLeader := &refreshLeaderCmd{
				Time: fsm.leader.Time + time.Duration(1+time.Second).Nanoseconds(),
			}
			cmdData, err := marshalledTestCmdFromStructs(refreshLeader, refreshLeaderOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			assert.Equal(t, refreshLeader.Time, fsm.leader.Time, "Time should be updated")
			fsm.leader = nil
		})
	})
	t.Run("Should apply delete leader op", func(t *testing.T) {
		t.Run("Leader should be deleted correct ID delete op applied", func(t *testing.T) {
			fsm.leader = &leaderBackend{
				ID: "1234",
			}

			deleteLeader := &deleteLeaderCmd{
				ID: "1234",
			}
			cmdData, err := marshalledTestCmdFromStructs(deleteLeader, deleteLeaderOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			assert.Nil(t, fsm.leader, "Leader should be deleted")
			fsm.leader = nil
		})
		t.Run("Leader should not be deleted after mismatch ID delete op applied", func(t *testing.T) {
			fsm.leader = &leaderBackend{
				ID: "1234",
			}

			deleteLeader := &deleteLeaderCmd{
				ID: "5678",
			}
			cmdData, err := marshalledTestCmdFromStructs(deleteLeader, deleteLeaderOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			assert.NotNil(t, fsm.leader, "Leader should be deleted")
			fsm.leader = nil
		})
	})
	t.Run("Should apply delete stale leader op", func(t *testing.T) {
		t.Run("Leader should be deleted when correct timeout delete stale leader op applied", func(t *testing.T) {
			fsm.leader = &leaderBackend{
				ID:  "1234",
				TTL: time.Duration(100 * time.Millisecond).Nanoseconds(),
				// Set time to be expired upon creation so we don't have to wait
				Time: time.Now().UnixNano() - time.Duration(time.Second*1).Nanoseconds(),
			}

			deleteStaleLeader := &deleteStaleLeaderCmd{
				Time: time.Now().UnixNano(),
			}
			cmdData, err := marshalledTestCmdFromStructs(deleteStaleLeader, deleteStaleLeaderOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			assert.Nil(t, fsm.leader, "Leader should be deleted")
			fsm.leader = nil
		})
		t.Run("Leader should not be deleted when incorrect timeout delete stale leader op applied", func(t *testing.T) {
			fsm.leader = &leaderBackend{
				ID:  "1234",
				TTL: time.Duration(1 * time.Second).Nanoseconds(),
				// Set time to be expired upon creation so we don't have to wait
				Time: time.Now().UnixNano(),
			}

			deleteStaleLeader := &deleteStaleLeaderCmd{
				Time: fsm.leader.Time,
			}
			cmdData, err := marshalledTestCmdFromStructs(deleteStaleLeader, deleteStaleLeaderOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			assert.NotNil(t, fsm.leader, "Leader should not be deleted")
			fsm.leader = nil
		})
	})
	t.Run("Should apply set member op", func(t *testing.T) {
		t.Run("Member should set when set member op is applied", func(t *testing.T) {
			setMember := &setMemberCmd{
				memberBackend{
					ID: "1234",
				},
			}
			cmdData, err := marshalledTestCmdFromStructs(setMember, setMemberOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			_, ok := fsm.members["1234"]
			assert.True(t, ok, "Member 1234 should exist in members")
			delete(fsm.members, "1234")
		})
		t.Run("Member should overwrite when set member op is applied to an existing member", func(t *testing.T) {
			fsm.members["1234"] = &memberBackend{
				ID:   "1234",
				Data: []byte("test_before"),
			}

			setMember := &setMemberCmd{
				memberBackend{
					ID:   "1234",
					Data: []byte("test_after"),
				},
			}
			cmdData, err := marshalledTestCmdFromStructs(setMember, setMemberOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			member, ok := fsm.members["1234"]
			assert.True(t, ok, "Member 1234 should exist in members")
			assert.Equal(t, "test_after", string(member.Data))
			delete(fsm.members, "1234")
		})
	})
	t.Run("Should apply refresh member op", func(t *testing.T) {
		t.Run("Member should be refreshed when a refresh cmd with it's ID is applied", func(t *testing.T) {
			fsm.members["1234"] = &memberBackend{
				ID:   "1234",
				Time: time.Now().UnixNano(),
			}

			refreshMember := &refreshMemberCmd{
				ID:   "1234",
				Time: 0,
			}
			cmdData, err := marshalledTestCmdFromStructs(refreshMember, refreshMemberOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			member, ok := fsm.members["1234"]
			assert.True(t, ok, "Member 1234 should exist in members")
			assert.Equal(t, refreshMember.Time, member.Time)
			delete(fsm.members, "1234")

		})
		t.Run("Member should not be refreshed when a refresh cmd with diff ID is applied", func(t *testing.T) {
			fsm.members["1234"] = &memberBackend{
				ID:   "1234",
				Time: time.Now().UnixNano(),
			}

			refreshMember := &refreshMemberCmd{
				ID:   "5678",
				Time: 0,
			}
			cmdData, err := marshalledTestCmdFromStructs(refreshMember, refreshMemberOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			member, ok := fsm.members["1234"]
			assert.True(t, ok, "Member 1234 should exist in members")
			assert.NotEqual(t, refreshMember.Time, member.Time)
			delete(fsm.members, "1234")

		})
	})
	t.Run("Should apply delete member op", func(t *testing.T) {
		t.Run("Member should be deleted when a delete cmd with it's ID is applied", func(t *testing.T) {
			fsm.members["1234"] = &memberBackend{
				ID:   "1234",
				Time: time.Now().UnixNano(),
			}

			deleteMember := &deleteMemberCmd{
				ID: "1234",
			}
			cmdData, err := marshalledTestCmdFromStructs(deleteMember, deleteMemberOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			_, ok := fsm.members["1234"]
			assert.False(t, ok, "Member 1234 should exist in members")
			delete(fsm.members, "1234")
		})
		t.Run("Member should not be deleted when a delete cmd with it's ID is applied", func(t *testing.T) {
			fsm.members["1234"] = &memberBackend{
				ID:   "1234",
				Time: time.Now().UnixNano(),
			}

			deleteMember := &deleteMemberCmd{
				ID: "5678",
			}
			cmdData, err := marshalledTestCmdFromStructs(deleteMember, deleteMemberOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			_, ok := fsm.members["1234"]
			assert.True(t, ok, "Member 1234 should exist in members")
			delete(fsm.members, "1234")
		})
	})
	t.Run("Should apply delete stale members op", func(t *testing.T) {
		t.Run("Member should be deleted if stale", func(t *testing.T) {
			fsm.members["1234"] = &memberBackend{
				ID:   "1234",
				TTL:  time.Duration(1 * time.Second).Nanoseconds(),
				Time: time.Now().UnixNano(),
			}

			deleteStaleMembers := &deleteStaleMembersCmd{
				Time: fsm.members["1234"].Time + time.Duration(2*time.Second).Nanoseconds(),
			}
			cmdData, err := marshalledTestCmdFromStructs(deleteStaleMembers, deleteStaleMembersOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			_, ok := fsm.members["1234"]
			assert.False(t, ok, "Member 1234 should not exist in members")
			delete(fsm.members, "1234")

		})
		t.Run("Member should not be deleted if a delete stale without proper time", func(t *testing.T) {
			fsm.members["1234"] = &memberBackend{
				ID:   "1234",
				TTL:  time.Duration(2 * time.Second).Nanoseconds(),
				Time: time.Now().UnixNano(),
			}

			deleteStaleMembers := &deleteStaleMembersCmd{
				Time: fsm.members["1234"].Time + time.Duration(1*time.Second).Nanoseconds(),
			}

			cmdData, err := marshalledTestCmdFromStructs(deleteStaleMembers, deleteStaleMembersOp)
			assert.NoError(t, err, "Should be no error getting raw command")

			err = fsm.Apply(cmdData)
			assert.NoError(t, err, "Should be no error applying command")

			_, ok := fsm.members["1234"]
			assert.True(t, ok, "Member 1234 should exist in members")
			delete(fsm.members, "1234")
		})
	})
}
