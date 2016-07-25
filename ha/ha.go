package ha

import (
	log "github.com/Sirupsen/logrus"
	"github.com/cenk/backoff"
	"github.com/compose/governor/fsm"
	"github.com/compose/governor/service"
	"github.com/pkg/errors"
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

	log.WithFields(log.Fields{
		"package": "ha",
	}).Info("Trying to ensure FSM up to date")

	for !ha.fsm.CompletedRestore() {
		time.Sleep(500 * time.Millisecond)
		log.WithFields(log.Fields{
			"package": "ha",
		}).Debug("FSM not up to date. Retrying")
	}

	log.WithFields(log.Fields{
		"package": "ha",
	}).Info("FSM is up to date")

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
				return err
			}
			if err := ha.service.Start(); err != nil {
				return err
			}

			lead, err := ha.service.AsFSMLeader()
			if err != nil {
				return err
			}

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
		hasInit, err := ha.fsm.RaceForInit(eb.NextBackOff())
		switch err {
		case nil:
			return hasInit, nil
		case fsm.ErrorRaceTimedOut:
			log.WithFields(log.Fields{
				"package": "ha",
			}).Warnf("Race timed out. Trying again.")
			continue
		default:
			return false, errors.Wrap(err, "Error racing for init")
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
			log.WithFields(log.Fields{
				"package": "ha",
			}).Warnf("No leader found. Waiting to retry leader")

			time.Sleep(eb.NextBackOff())
			continue
		}

		return leader, nil
	}
}

func (ha *SingleLeaderHA) Run() error {
	// Setup since we don't know how long the cluster we're joining has been active
	if err := ha.init(); err != nil {
		return err
	}

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

	if ha.service.IsHealthy() {
		member, err := ha.service.AsFSMMember()
		if err != nil {
			return err
		}

		if err := ha.fsm.RefreshMember(member.ID()); err != nil {
			return err
		}

		isLeader, err := ha.isLeader()
		if err != nil {
			return err
		}
		if isLeader {
			log.Infof("Running as leader")
			if err := ha.fsm.RefreshLeader(); err != nil {
				return err
			}
		} else {
			log.WithFields(log.Fields{
				"package": "ha",
			}).Infof("PG Running as follower")
		}
	} else if !ha.service.IsRunning() {
		if err := ha.service.Start(); err != nil {
			return err
		}
	} else {
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
				log.WithFields(log.Fields{
					"package": "ha",
				}).Errorf("Error handling member update %+v", err)
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
				log.WithFields(log.Fields{
					"package": "ha",
				}).Errorf("Error handling leader update %+v", err)
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
