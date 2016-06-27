package fsm

import (
	"encoding/json"
	"github.com/compose/canoe"
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
var deleteStaleMembersOp = "DELETE_STALE_MEMBER"

var newNodeUpToDateOp = "NEW_NODE_UP_TO_DATE"

var raceForInitOp = "RACE_FOR_INIT"

type command struct {
	Op   string `json:"op"`
	Data []byte `json:"data"`
}

// Apply completes the FSM requirement
// TODO: Add initialization Race?
func (f *fsm) Apply(log canoe.LogData) error {
	var cmd command
	if err := json.Unmarshal(log, &cmd); err != nil {
		return err
	}

	switch cmd.Op {
	case forceLeaderOp:
		if err := f.applyForceLeader(cmd.Data); err != nil {
			return err
		}
	case raceLeaderOp:
		if err := f.applyRaceLeader(cmd.Data); err != nil {
			return err
		}
	case refreshLeaderOp:
		if err := f.applyRefreshLeader(cmd.Data); err != nil {
			return err
		}
	case deleteLeaderOp:
		if err := f.applyDeleteLeader(); err != nil {
			return err
		}
	case deleteStaleLeaderOp:
		if err := f.applyDeleteStaleLeader(cmd.Data); err != nil {
			return err
		}
	case setMemberOp:
		if err := f.applySetMember(cmd.Data); err != nil {
			return err
		}
	case refreshMemberOp:
		if err := f.applyRefreshMember(cmd.Data); err != nil {
			return err
		}
	case deleteMemberOp:
		if err := f.applyDeleteMember(cmd.Data); err != nil {
			return err
		}
	case deleteStaleMembersOp:
		if err := f.applyDeleteStaleMembers(cmd.Data); err != nil {
			return err
		}
	case newNodeUpToDateOp:
		if err := f.applyNewNodeUpToDate(cmd.Data); err != nil {
			return err
		}
	case raceForInitOp:
		if err := f.applyRaceForInit(cmd.Data); err != nil {
			return err
		}
	default:
		return ErrorUnknownOperation
	}
	return nil
}

type deleteLeaderCmd struct {
}

func (f *fsm) applyDeleteLeader() error {
	update := &LeaderUpdate{
		Type: LeaderUpdateDeletedType,
	}

	f.Lock()
	defer f.Unlock()

	if f.leader != nil {
		update.OldLeader = f.leader.Data
	}

	f.leader = nil

	select {
	case f.leaderc <- update:
	default:
	}

	return nil
}

func (f *fsm) proposeDeleteLeader() error {
	req := &deleteLeaderCmd{}

	return f.proposeCmd(deleteLeaderOp, req)
}

type deleteStaleLeaderCmd struct {
	Time int64 `json:"time"`
}

func (f *fsm) applyDeleteStaleLeader(cmdData []byte) error {
	var cmd deleteStaleLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	update := &LeaderUpdate{
		Type: LeaderUpdateDeletedType,
	}

	f.Lock()
	defer f.Unlock()

	if f.leader != nil {
		update.OldLeader = f.leader.Data
	}

	if cmd.Time >= f.leader.Time+f.leader.TTL {
		f.leader = nil
		select {

		case f.leaderc <- update:
		default:
		}
	} else if cmd.Time < f.leader.Time {
		return ErrorBadTTLTimestamp
	}

	return nil
}

func (f *fsm) proposeDeleteStaleLeader() error {
	req := &deleteStaleLeaderCmd{
		Time: time.Now().UnixNano(),
	}

	return f.proposeCmd(deleteStaleLeaderOp, req)
}

type forceLeaderCmd struct {
	leaderBackend
}

func (f *fsm) applyForceLeader(cmdData []byte) error {
	var cmd forceLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
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

	select {
	case f.leaderc <- update:
	default:
	}

	return nil
}

func (f *fsm) proposeForceLeader(leader Leader) error {
	data, err := leader.MarshalFSM()
	if err != nil {
		return err
	}

	req := &forceLeaderCmd{
		leaderBackend{
			ID:   leader.ID(),
			Data: data,
			Time: time.Now().UnixNano(),
			TTL:  f.leaderTTL,
		},
	}

	return f.proposeCmd(forceLeaderOp, req)
}

type raceLeaderCmd struct {
	leaderBackend
}

func (f *fsm) applyRaceLeader(cmdData []byte) error {
	var cmd raceLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()
	if f.leader == nil {
		f.leader = &cmd.leaderBackend

		update := &LeaderUpdate{
			Type:          LeaderUpdateSetType,
			CurrentLeader: f.leader.Data,
		}

		select {
		case f.leaderc <- update:
		default:
		}
	}

	return nil
}

func (f *fsm) proposeRaceLeader(leader Leader) error {
	data, err := leader.MarshalFSM()
	if err != nil {
		return err
	}

	req := &raceLeaderCmd{
		leaderBackend{
			ID:   leader.ID(),
			Data: data,
			Time: time.Now().UnixNano(),
			TTL:  f.leaderTTL,
		},
	}
	return f.proposeCmd(raceLeaderOp, req)
}

type refreshLeaderCmd struct {
	Time int64 `json:"time"`
}

func (f *fsm) applyRefreshLeader(cmdData []byte) error {
	var cmd refreshLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
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

	return f.proposeCmd(refreshLeaderOp, req)
}

type setMemberCmd struct {
	memberBackend
}

func (f *fsm) applySetMember(cmdData []byte) error {
	var cmd setMemberCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()
	update := &MemberUpdate{
		Type:      MemberUpdateSetType,
		OldMember: f.members[cmd.ID].Data,
	}
	f.members[cmd.ID] = &cmd.memberBackend

	update.CurrentMember = f.members[cmd.ID].Data

	select {
	case f.memberc <- update:
	default:
	}

	return nil
}

func (f *fsm) proposeSetMember(member Member) error {
	data, err := member.MarshalFSM()
	if err != nil {
		return err
	}

	req := &setMemberCmd{
		memberBackend{
			ID:   member.ID(),
			Data: data,
			Time: time.Now().UnixNano(),
			TTL:  f.memberTTL,
		},
	}

	return f.proposeCmd(setMemberOp, req)
}

type refreshMemberCmd struct {
	ID   string `json:"id"`
	Time int64  `json:"time"`
}

func (f *fsm) applyRefreshMember(cmdData []byte) error {
	var cmd refreshMemberCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
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

	return f.proposeCmd(refreshMemberOp, req)
}

type deleteMemberCmd struct {
	ID string `json:"id"`
}

func (f *fsm) applyDeleteMember(cmdData []byte) error {
	var cmd deleteMemberCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()
	if _, ok := f.members[cmd.ID]; ok {
		update := &MemberUpdate{
			Type:      MemberUpdateDeletedType,
			OldMember: f.members[cmd.ID].Data,
		}

		select {
		case f.memberc <- update:
		default:
		}
		delete(f.members, cmd.ID)
	}

	return nil
}

func (f *fsm) proposeDeleteMember(id string) error {
	req := &deleteMemberCmd{
		ID: id,
	}

	return f.proposeCmd(deleteMemberOp, req)
}

type deleteStaleMembersCmd struct {
	Time int64 `json:"time"`
}

func (f *fsm) applyDeleteStaleMembers(cmdData []byte) error {
	var cmd deleteStaleMembersCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()
	for id, member := range f.members {
		if cmd.Time >= member.Time+member.TTL {
			update := &MemberUpdate{
				Type:      MemberUpdateDeletedType,
				OldMember: member.Data,
			}

			select {

			case f.memberc <- update:
			default:
			}

			delete(f.members, id)

		} else if cmd.Time < f.leader.Time {
			return ErrorBadTTLTimestamp
		}
	}

	return nil
}

func (f *fsm) proposeDeleteStaleMember() error {
	req := &deleteStaleMembersCmd{
		Time: time.Now().UnixNano(),
	}

	return f.proposeCmd(deleteStaleMembersOp, req)
}

type newNodeUpToDateCmd struct {
	ID uint64 `json:"id"`
}

func (f *fsm) applyNewNodeUpToDate(cmdData []byte) error {
	var cmd newNodeUpToDateCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()

	if f.UniqueID() == cmd.ID {
		f.current = true
	}

	return nil
}

func (f *fsm) proposeNewNodeUpToDate() error {
	req := &newNodeUpToDateCmd{
		ID: f.UniqueID(),
	}

	return f.proposeCmd(newNodeUpToDateOp, req)
}

type raceForInitCmd struct {
	ID uint64 `json:"id"`
}

func (f *fsm) applyRaceForInit(cmdData []byte) error {
	var cmd raceForInitCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()

	if f.initID != nil {
		f.gotInit <- false
	} else if f.UniqueID() == cmd.ID {
		f.initID = &cmd.ID
		f.gotInit <- true
	} else {
		f.initID = &cmd.ID
		f.gotInit <- false
	}

	return nil
}

func (f *fsm) proposeRaceForInit() error {
	req := &raceForInitCmd{
		ID: f.UniqueID(),
	}

	return f.proposeCmd(raceForInitOp, req)
}

func (f *fsm) proposeCmd(op string, data interface{}) error {
	reqData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	newCmd := &command{
		Op:   deleteStaleLeaderOp,
		Data: reqData,
	}

	newCmdData, err := json.Marshal(newCmd)
	if err != nil {
		return err
	}

	return f.raft.Propose(newCmdData)
}
