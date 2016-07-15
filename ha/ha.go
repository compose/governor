package ha

import (
	"errors"
	"fmt"
	"github.com/cenk/backoff"
	"github.com/compose/governor/fsm"
	"github.com/compose/governor/service"
	"sync"
	"time"
)

type SingleLeaderHA struct {
	service    service.SingleLeaderService
	fsm        fsm.SingleLeaderFSM
	leaderc    <-chan *fsm.LeaderUpdate
	memberc    <-chan *fsm.MemberUpdate
	loopTicker <-chan time.Time

	// we need to ensure we interact with the FSM and service in atomic units
	sync.Mutex
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

func (ha *SingleLeaderHA) init() error {

	// TODO: make this exp backoff
	fmt.Println("checking up to date")
	for !ha.fsm.CompletedRestore() {
		time.Sleep(500 * time.Millisecond)
		fmt.Println("checking up to date")
	}
	fmt.Println("is up to date")

	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = 2000 * time.Millisecond
	eb.MaxInterval = 10 * time.Second
	eb.MaxElapsedTime = 0 * time.Second
	// Only have 1 init ever in life of a cluster
	// otherwise may introduce split brain.
	// this if a node needs an init and can't find a leader.
	// then manual intervention MUST happen and the node should alert
	// like crazy
	if ha.service.NeedsInitialization() {

		hasInit, err := ha.raceRetryForInit(eb)
		if err != nil {
			return err
		}

		if hasInit {
			if err := ha.service.Initialize(); err != nil {
				fmt.Println("init err")
				return err
			}
			if err := ha.service.Start(); err != nil {
				fmt.Println("start err", err.Error())
				return err
			}

			fmt.Println("Getting leader")
			lead, err := ha.service.AsFSMLeader()
			if err != nil {
				return err
			}
			fmt.Println("forcing leader")

			if err := ha.fsm.ForceLeader(lead); err != nil {
				return err
			}
		} else {
			eb.Reset()
			leader, err := ha.waitForLeader(eb)
			if err != nil {
				return err
			}

			// Let the service determine if syncing should be seperate
			if err := ha.service.FollowTheLeader(leader); err != nil {
				return err
			}
		}

		// TODO: break the else logic into better bits
	} else {
		leader, err := ha.waitForLeader(eb)
		if err != nil {
			return err
		}
		if err := ha.service.FollowTheLeader(leader); err != nil {
			return err
		}
	}

	return nil
}

func (ha *SingleLeaderHA) raceRetryForInit(eb *backoff.ExponentialBackOff) (bool, error) {
	for {
		fmt.Println("isuing a race for init")
		hasInit, err := ha.fsm.RaceForInit(eb.NextBackOff())
		switch err {
		case nil:
			return hasInit, nil
		case fsm.ErrorRaceTimedOut:
			// TODO: Add real logging
			fmt.Printf("Timed out racing for init. Try again.\n")
			continue
		default:
			return false, err
		}
	}
}

func (ha *SingleLeaderHA) waitForLeader(eb *backoff.ExponentialBackOff) (fsm.Leader, error) {
	leader := ha.service.FSMLeaderTemplate()
	for {
		exists, err := ha.fsm.Leader(leader)
		if err != nil {
			return nil, err
		}

		if !exists {
			fmt.Printf("No leader found. Waiting to retry leader")
			time.Sleep(eb.NextBackOff())
			continue
		}

		return leader, nil
	}
}

func (ha *SingleLeaderHA) Run() error {
	// Setup since we don't know how long the cluster we're joining has been active
	fmt.Println("starting ha init")
	if err := ha.init(); err != nil {
		return err
	}
	fmt.Println("ended ha init")

	//TODO: Add ready and updated field to Canoe so we know it is up to date.
	// 	For now probably a non-issue
	go ha.scanChannels()

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

// Just refresh the TTL time
// leave the state updates to the channels.
// Maybe look at changing that though, as the chans must be
// heavily consumed to prevent errors
func (ha *SingleLeaderHA) RunCycle() error {
	ha.Lock()
	defer ha.Unlock()

	fmt.Println("in run cycle")
	if ha.service.IsHealthy() {
		fmt.Println("we are healthy")
		member, err := ha.service.AsFSMMember()
		if err != nil {
			fmt.Println("error in getting member")
			return err
		}

		if err := ha.fsm.RefreshMember(member.ID()); err != nil {
			fmt.Println("error refreshing member")
			return err
		}

		isLeader, err := ha.isLeader()
		if err != nil {
			fmt.Println("error getting leader")
			return err
		}
		if isLeader {
			if err := ha.fsm.RefreshLeader(); err != nil {
				fmt.Println("error refreshing leader")
				return err
			}
		}
	} else if !ha.service.IsRunning() {
		fmt.Println("we are not healthy and not running")
		if err := ha.service.Start(); err != nil {
			return err
		}
	} else {
		fmt.Println("we are not healthy running")
		if err := ha.service.Restart(); err != nil {
			return err
		}
	}
	return nil
}

// Spawning several goroutines as we need to VERY aggressively consume these chans
func (ha *SingleLeaderHA) scanChannels() {
	go ha.scanMemberChan()
	go ha.scanLeaderChan()
}

func (ha *SingleLeaderHA) scanMemberChan() {
	for {
		go func(ha *SingleLeaderHA, mu *fsm.MemberUpdate) {
			if err := ha.handleMemberUpdate(mu); err != nil {
				// TODO: add logging
				fmt.Println(err.Error())
			}
		}(ha, <-ha.memberc)
	}
}

func (ha *SingleLeaderHA) handleMemberUpdate(mu *fsm.MemberUpdate) error {
	ha.Lock()
	defer ha.Unlock()

	switch mu.Type {
	case fsm.MemberUpdateSetType:
		member, err := ha.service.FSMMemberFromBytes(mu.CurrentMember)
		if err != nil {
			return err
		}

		members := []fsm.Member{member}
		return ha.service.AddMembers(members)
	case fsm.MemberUpdateDeletedType:
		member, err := ha.service.FSMMemberFromBytes(mu.OldMember)
		if err != nil {
			return err
		}

		members := []fsm.Member{member}
		return ha.service.DeleteMembers(members)
	default:
		return errors.New("Unknown update type")
	}

}

func (ha *SingleLeaderHA) scanLeaderChan() {
	for {
		go func(ha *SingleLeaderHA, leader *fsm.LeaderUpdate) {
			if err := ha.handleLeaderUpdate(leader); err != nil {
				// TODO: add logging
				fmt.Println(err.Error())
			}
		}(ha, <-ha.leaderc)
	}
}

func (ha *SingleLeaderHA) handleLeaderUpdate(mu *fsm.LeaderUpdate) error {
	ha.Lock()
	defer ha.Unlock()

	switch mu.Type {
	case fsm.LeaderUpdateSetType:
		leader, err := ha.service.FSMLeaderFromBytes(mu.CurrentLeader)
		if err != nil {
			return err
		}

		leaderIsMe, err := ha.leaderIsMe(leader)
		if err != nil {
			return err
		}

		if leaderIsMe {
			if !ha.service.RunningAsLeader() {
				return ha.service.Promote()
			}
		} else if ha.service.RunningAsLeader() {
			return ha.service.Demote(leader)
		} else {
			return ha.service.FollowTheLeader(leader)
		}

	case fsm.LeaderUpdateDeletedType:
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

	return nil
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
