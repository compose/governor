package ha

import (
	"github.com/compose/governor/fsm"
	"github.com/compose/governor/service"
	"time"
)

type SingleLeaderHA struct {
	service    service.SingleLeaderService
	fsm        fsm.SingleLeaderFSM
	leaderc    <-chan *fsm.LeaderUpdate
	memberc    <-chan *fsm.MemberUpdate
	loopTicker <-chan Time
}

type SingleLeaderHAConfig struct {
	Service    service.SingleLeaderService
	FSM        fsm.SingleLeaderFSM
	UpdateWait time.Duration
}

func NewSingleLeaderHA(config *SingleLeaderHAConfig) *SingleLeaderHA {
	return &SingleLeaderHA{
		service:    config.Service,
		fsm:        config.FSM,
		leaderc:    config.FSM.LeaderCh(),
		memberc:    config.FSM.MemberCh(),
		loopTicker: time.Tick(config.UpdateWait),
	}
}

func (ha *SingleLeaderHA) Run() error {
	// Setup since we don't know how long the cluster we're joining has been active
	ha.init()

	//TODO: Add ready and updated field to Canoe so we know it is up to date.
	// 	For now probably a non-issue
	go scanChannels()

	for {
		select {
		case <-ha.loopTicker:
			if err := ha.RunCycle(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ha *SingleLeaderHA) RunCycle() error {
	// Do Checks and touch stuff

	return nil
}

// Spawning several goroutines as we need to VERY aggressively consume these chans
func (ha *SingleLeaderHA) scanChannels() {
	go scanMemberChan()
	go scanLeaderChan()
}

func (ha *SingleLeaderHA) scanMemberChan() {
	new
	for {
		go func(ha *SingleLeaderHA, mu fsm.MemberUpdate) {
			if err := ha.handleMemberUpdate(mu); err != nil {
				// TODO: add logging
				fmt.Println(err.Error())
			}
		}(ha, <-ha.memberc)
	}
}
func (ha *SingleLeaderHA) handleMemberUpdate(mu fsm.MemberUpdate) error {
	temp := ha.service.FSMMemberTemplate()

	switch mu.Type {
	case fsm.MemberUpdateSetType:
		if err := temp.Unmarshal(mu.CurrentMember); err != nil {
			return err
		}

		members := []fsm.Member{temp}
		return ha.service.AddMembers(members)
	case fsm.MemberUpdateDeleteType:
		if err := temp.Unmarshal(mu.OldMember); err != nil {
			return err
		}

		members := []fsm.Member{temp}
		return ha.service.RemoveMembers(members)
	default:
		return errors.New("Unknown update type")
	}

}

func (ha *SingleLeaderHA) scanLeaderChan() {
	for {
		go func(ha *SingleLeaderHA, member fsm.LeaderUpdate) {
			if err := ha.handleLeaderUpdate(member); err != nil {
				// TODO: add logging
				fmt.Println(err.Error())
			}
		}(ha, <-ha.leaderc)
	}
}

func (ha *SingleLeaderHA) handleLeaderUpdate(mu fsm.LeaderUpdate) error {
	temp := ha.service.FSMLeaderTemplate()

	switch mu.Type {
	case fsm.LeaderUpdateSetType:
		if err := temp.Unmarshal(mu.CurrentLeader); err != nil {
			return err
		}

		if ha.leaderIsMe(temp) {
			if !ha.fsm.RunningAsLeader() {
				return ha.fsm.Promote()
			}
		} else if ha.service.RunningAsLeader() {
			return ha.fsm.Demote(temp)
		} else {
			return ha.fsm.FollowTheLeader(temp)
		}

	case fsm.LeaderUpdateDeleteType:
		if err := ha.service.FollowNoLeader(); err != nil {
			return err
		}

		lead, err := ha.service.AsFSMLeader()
		if err != nil {
			return err
		}

		return ha.fsm.RaceForLeader(lead)

	default:
		return errors.New("Unknown update type")
	}

}

func (ha *SingleLeaderHA) leaderIsMe(leader fsm.Leader) (bool, error) {
	meAsLeader, err := ha.service.AsFSMLeader()
	if err != nil {
		return false, err
	}
	return meAsLeader.ID() == leader.ID(), nil

}

func (ha *SingleLeaderHA) isLeader() (bool, error) {
	curLeader := ha.service.FSMLeaderTemplate()
	exists, err := ha.fsm.Leader(curLeader)
	if err != nil {
		return false, err
	} else if !exists {
		return false, nil
	}

	meAsLeader, err := ha.service.AsFSMLeader()
	if err != nil {
		return false, err
	}

	return curLeader.ID() == meAsLeader.ID(), nil
}
