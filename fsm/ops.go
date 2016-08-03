package fsm

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/compose/canoe"
	"github.com/pkg/errors"
	"time"
)

var forceLeaderOp = "FORCE_LEADER"
var deleteLeaderOp = "DELETE_LEADER"
var raceLeaderOp = "RACE_LEADER"
var refreshLeaderOp = "REFRESH_LEADER"
var deleteStaleLeaderOp = "DELETE_STALE_LEADER"

var setMemberOp = "SET_MEMBER"
var refreshMemberOp = "REFRESH_MEMBER"
var deleteMemberOp = "DELETE_MEMBER"
var deleteStaleMembersOp = "DELETE_STALE_MEMBERS"

var newNodeUpToDateOp = "NEW_NODE_UP_TO_DATE"

var raceForInitOp = "RACE_FOR_INIT"

type command struct {
	Op   string `json:"op"`
	Data []byte `json:"data"`
}

func init() {
	log.SetLevel(log.DebugLevel)
}

// Apply completes the FSM requirement
// TODO: Add initialization Race?
func (f *fsm) Apply(log canoe.LogData) error {
	var cmd command
	if err := json.Unmarshal(log, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshaling command")
	}

	switch cmd.Op {
	case forceLeaderOp:
		if err := f.applyForceLeader(cmd.Data); err != nil {
			return errors.Wrap(err, "Error applying force leader op")
		}
	case raceLeaderOp:
		if err := f.applyRaceLeader(cmd.Data); err != nil {
			return errors.Wrap(err, "Error applying race leader op")
		}
	case refreshLeaderOp:
		if err := f.applyRefreshLeader(cmd.Data); err != nil {
			return errors.Wrap(err, "Error Applying refresh leader op")
		}
	case deleteLeaderOp:
		if err := f.applyDeleteLeader(); err != nil {
			return errors.Wrap(err, "Error applying delete leader op")
		}
	case deleteStaleLeaderOp:
		if err := f.applyDeleteStaleLeader(cmd.Data); err != nil {
			return errors.Wrap(err, "Error applying delete stale leader op")
		}
	case setMemberOp:
		if err := f.applySetMember(cmd.Data); err != nil {
			return errors.Wrap(err, "Error applying set member op")
		}
	case refreshMemberOp:
		if err := f.applyRefreshMember(cmd.Data); err != nil {
			return errors.Wrap(err, "Error applying refresh member op")
		}
	case deleteMemberOp:
		if err := f.applyDeleteMember(cmd.Data); err != nil {
			return errors.Wrap(err, "Error applying delete member op")
		}
	case deleteStaleMembersOp:
		if err := f.applyDeleteStaleMembers(cmd.Data); err != nil {
			return errors.Wrap(err, "Error applying delete stale members op")
		}
	case newNodeUpToDateOp:
		if err := f.applyNewNodeUpToDate(cmd.Data); err != nil {
			return errors.Wrap(err, "Error applying new node update to date op")
		}
	case raceForInitOp:
		if err := f.applyRaceForInit(cmd.Data); err != nil {
			return errors.Wrap(err, "Error applying apply race for init op")
		}
	default:
		return ErrorUnknownOperation
	}
	return nil
}

type deleteLeaderCmd struct {
}

func (f *fsm) applyDeleteLeader() error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying delete leader log")

	update := &LeaderUpdate{
		Type: LeaderUpdateDeletedType,
	}

	f.Lock()
	defer f.Unlock()

	if f.leader != nil {
		update.OldLeader = f.leader.Data
	}

	f.leader = nil

	if err := f.observeLeaderUpdate(update); err != nil {
		return errors.Wrap(err, "Error observing leader update")
	}

	return nil
}

func (f *fsm) proposeDeleteLeader() error {
	req := &deleteLeaderCmd{}

	return errors.Wrap(f.proposeCmd(deleteLeaderOp, req), "Error proposing delete cmd")
}

type deleteStaleLeaderCmd struct {
	Time int64 `json:"time"`
}

func (f *fsm) applyDeleteStaleLeader(cmdData []byte) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying delete stale leader log")

	var cmd deleteStaleLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshaling stale leader cmd")
	}

	update := &LeaderUpdate{
		Type: LeaderUpdateDeletedType,
	}

	f.Lock()
	defer f.Unlock()

	if f.leader != nil {
		update.OldLeader = f.leader.Data

		if cmd.Time >= f.leader.Time+f.leader.TTL {
			log.WithFields(log.Fields{
				"package": "fsm",
			}).Info("Deleting stale leader")
			f.leader = nil
			if err := f.observeLeaderUpdate(update); err != nil {
				return errors.Wrap(err, "Error observing leader update")
			}
		}
	}

	return nil
}

func (f *fsm) proposeDeleteStaleLeader() error {
	req := &deleteStaleLeaderCmd{
		Time: time.Now().UnixNano(),
	}

	return errors.Wrap(f.proposeCmd(deleteStaleLeaderOp, req), "Error proposing delete stale leader command")
}

type forceLeaderCmd struct {
	leaderBackend
}

func (f *fsm) applyForceLeader(cmdData []byte) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying force leader log")

	var cmd forceLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshaling force leader cmd")
	}

	update := &LeaderUpdate{
		Type: LeaderUpdateSetType,
	}

	f.Lock()
	defer f.Unlock()

	if f.leader != nil {
		update.OldLeader = f.leader.Data
	}

	f.leader = &cmd.leaderBackend

	update.CurrentLeader = f.leader.Data

	if err := f.observeLeaderUpdate(update); err != nil {
		return errors.Wrap(err, "Error observing leader update")
	}

	return nil
}

func (f *fsm) proposeForceLeader(leader Leader) error {
	data, err := leader.MarshalFSM()
	if err != nil {
		return errors.Wrap(err, "Error marshaling fsm leader")
	}

	req := &forceLeaderCmd{
		leaderBackend{
			ID:   leader.ID(),
			Data: data,
			Time: time.Now().UnixNano(),
			TTL:  f.leaderTTL,
		},
	}

	return errors.Wrap(f.proposeCmd(forceLeaderOp, req), "Error proposing force leader command")
}

type raceLeaderCmd struct {
	leaderBackend
}

func (f *fsm) applyRaceLeader(cmdData []byte) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying race leader log")

	var cmd raceLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshaling race leader command")
	}

	f.Lock()
	defer f.Unlock()

	if f.leader == nil {
		f.leader = &cmd.leaderBackend

		update := &LeaderUpdate{
			Type:          LeaderUpdateSetType,
			CurrentLeader: f.leader.Data,
		}

		if err := f.observeLeaderUpdate(update); err != nil {
			return errors.Wrap(err, "Error observing leader update")
		}
	}

	return nil
}

func (f *fsm) proposeRaceLeader(leader Leader) error {
	data, err := leader.MarshalFSM()
	if err != nil {
		return errors.Wrap(err, "Error unmarshalling leader")
	}

	req := &raceLeaderCmd{
		leaderBackend{
			ID:   leader.ID(),
			Data: data,
			Time: time.Now().UnixNano(),
			TTL:  f.leaderTTL,
		},
	}
	return errors.Wrap(f.proposeCmd(raceLeaderOp, req), "Error proposing leader race command")
}

type refreshLeaderCmd struct {
	Time int64 `json:"time"`
}

func (f *fsm) applyRefreshLeader(cmdData []byte) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying refresh leader log")

	var cmd refreshLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshaling refresh leader cmd")
	}

	f.Lock()
	defer f.Unlock()
	if f.leader != nil {
		f.leader.Time = cmd.Time
	}
	return nil
}

func (f *fsm) proposeRefreshLeader() error {
	req := &refreshLeaderCmd{
		Time: time.Now().UnixNano(),
	}

	return errors.Wrap(f.proposeCmd(refreshLeaderOp, req), "Error proposing refresh leader cmd")
}

type setMemberCmd struct {
	memberBackend
}

func (f *fsm) applySetMember(cmdData []byte) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying set member log")

	var cmd setMemberCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshaling set member command")
	}

	f.Lock()
	defer f.Unlock()
	update := &MemberUpdate{
		Type:      MemberUpdateSetType,
		OldMember: f.members[cmd.ID].Data,
	}
	f.members[cmd.ID] = &cmd.memberBackend

	update.CurrentMember = f.members[cmd.ID].Data

	if err := f.observeMemberUpdate(update); err != nil {
		return errors.Wrap(err, "Error observing member update")
	}

	return nil
}

func (f *fsm) proposeSetMember(member Member) error {
	data, err := member.MarshalFSM()
	if err != nil {
		return errors.Wrap(err, "Error marshaling set member command")
	}

	req := &setMemberCmd{
		memberBackend{
			ID:   member.ID(),
			Data: data,
			Time: time.Now().UnixNano(),
			TTL:  f.memberTTL,
		},
	}

	return errors.Wrap(f.proposeCmd(setMemberOp, req), "Error proposing set member command")
}

type refreshMemberCmd struct {
	ID   string `json:"id"`
	Time int64  `json:"time"`
}

func (f *fsm) applyRefreshMember(cmdData []byte) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying refresh member log")

	var cmd refreshMemberCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshalling refresh member command")
	}

	f.Lock()
	defer f.Unlock()

	if val, ok := f.members[cmd.ID]; ok {
		val.Time = cmd.Time
	}

	return nil
}

func (f *fsm) proposeRefreshMember(id string) error {
	req := &refreshMemberCmd{
		ID:   id,
		Time: time.Now().UnixNano(),
	}

	return errors.Wrap(f.proposeCmd(refreshMemberOp, req), "Error proposing refresh member cmd")
}

type deleteMemberCmd struct {
	ID string `json:"id"`
}

func (f *fsm) applyDeleteMember(cmdData []byte) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying delete member log")

	var cmd deleteMemberCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshalling delete member cmd")
	}

	f.Lock()
	defer f.Unlock()
	if _, ok := f.members[cmd.ID]; ok {
		update := &MemberUpdate{
			Type:      MemberUpdateDeletedType,
			OldMember: f.members[cmd.ID].Data,
		}

		delete(f.members, cmd.ID)
		if err := f.observeMemberUpdate(update); err != nil {
			return errors.Wrap(err, "Error observing member update")
		}
	}

	return nil
}

func (f *fsm) proposeDeleteMember(id string) error {
	req := &deleteMemberCmd{
		ID: id,
	}

	return errors.Wrap(f.proposeCmd(deleteMemberOp, req), "Error proposing delete member op")
}

type deleteStaleMembersCmd struct {
	Time int64 `json:"time"`
}

func (f *fsm) applyDeleteStaleMembers(cmdData []byte) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying delete stale members log")

	var cmd deleteStaleMembersCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshalling delete stale members command")
	}

	f.Lock()
	defer f.Unlock()
	for id, member := range f.members {
		if cmd.Time >= member.Time+member.TTL {
			update := &MemberUpdate{
				Type:      MemberUpdateDeletedType,
				OldMember: member.Data,
			}

			delete(f.members, id)

			if err := f.observeMemberUpdate(update); err != nil {
				return errors.Wrap(err, "Error observing member update")
			}

		}
	}

	return nil
}

func (f *fsm) proposeDeleteStaleMembers() error {
	req := &deleteStaleMembersCmd{
		Time: time.Now().UnixNano(),
	}

	return errors.Wrap(f.proposeCmd(deleteStaleMembersOp, req), "Error proposing delete stale members command")
}

type newNodeUpToDateCmd struct {
	ID        uint64 `json:"id"`
	Timestamp int64  `json:"timestamp"`
}

// TODO: Have timestamp here for confirmation
func (f *fsm) applyNewNodeUpToDate(cmdData []byte) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying new node up to date log")

	var cmd newNodeUpToDateCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshaling new node up to date cmd")
	}

	f.Lock()
	defer f.Unlock()

	if f.UniqueID() == cmd.ID && f.startTime == cmd.Timestamp {
		f.current = true
	}

	return nil
}

func (f *fsm) proposeNewNodeUpToDate() error {
	req := &newNodeUpToDateCmd{
		ID:        f.UniqueID(),
		Timestamp: f.startTime,
	}

	return errors.Wrap(f.proposeCmd(newNodeUpToDateOp, req), "Error proposing new node up to date cmd")
}

type raceForInitCmd struct {
	ID        uint64 `json:"id"`
	Timestamp int64  `json:"timestamp"`
}

func (f *fsm) applyRaceForInit(cmdData []byte) error {
	log.WithFields(log.Fields{
		"package": "fsm",
	}).Debug("Applying race for init log")

	var cmd raceForInitCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return errors.Wrap(err, "Error unmarshaling race for init command")
	}

	f.Lock()
	defer f.Unlock()

	log.Debug("Sending down chan")
	if f.initID != nil {
		if f.gotInit != nil {
			f.gotInit <- false
		}
	} else if f.UniqueID() == cmd.ID && cmd.Timestamp == f.startTime {
		f.initID = &cmd.ID
		if f.gotInit != nil {
			f.gotInit <- true
		}
	} else {
		f.initID = &cmd.ID
	}
	log.Debug("Done sending down chan")

	return nil
}

func (f *fsm) proposeRaceForInit() error {
	req := &raceForInitCmd{
		ID:        f.UniqueID(),
		Timestamp: f.startTime,
	}

	return errors.Wrap(f.proposeCmd(raceForInitOp, req), "Error proposing race for init cmd")
}

func (f *fsm) proposeCmd(op string, data interface{}) error {
	reqData, err := json.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "Error unmarshaling cmd data")
	}

	newCmd := &command{
		Op:   op,
		Data: reqData,
	}

	newCmdData, err := json.Marshal(newCmd)
	if err != nil {
		return errors.Wrap(err, "Error marshaling new final command")
	}

	return errors.Wrap(f.raft.Propose(newCmdData), "Error proposing to raft")
}
