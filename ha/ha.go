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
			if err := RunCycle(); err != nil {
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
	for {
		//TODO: Will this block until something comes in?
		go func(ha *SingleLeaderHA, member fsm.MemberUpdate) {
			ha.handleMemberUpdate()
		}(ha, <-ha.memberc)
	}
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
