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
	service        service.SingleLeaderService
	fsm            fsm.SingleLeaderFSM
	leaderObserver fsm.LeaderObserver
	memberObserver fsm.MemberObserver
	loopTicker     <-chan time.Time

	wg    sync.WaitGroup
	stopc chan interface{}

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
		loopTicker: time.Tick(config.UpdateWait),
		wg:         sync.WaitGroup{},
		stopc:      make(chan interface{}),
	}

}

func (ha *SingleLeaderHA) init() error {

	log.WithFields(log.Fields{
		"package": "ha",
	}).Info("Trying to ensure FSM up to date")

	// TODO: Turn this into chan
	for !ha.fsm.CompletedRestore() {
		time.Sleep(500 * time.Millisecond)
		log.WithFields(log.Fields{
			"package": "ha",
		}).Debug("FSM not up to date. Retrying")
	}

	leaderObserver, err := ha.fsm.LeaderObserver()
	if err != nil {
		return errors.Wrap(err, "Error getting fsm LeaderObserver")
	}
	ha.leaderObserver = leaderObserver

	memberObserver, err := ha.fsm.MemberObserver()
	if err != nil {
		return errors.Wrap(err, "Error getting fsm LeaderObserver")
	}
	ha.memberObserver = memberObserver

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
		log.WithFields(log.Fields{
			"package": "ha",
		}).Info("Service needs initialization")

		log.WithFields(log.Fields{
			"package": "ha",
		}).Info("Racing for initialization")
		hasInit, err := ha.raceRetryForInit(eb)
		if err != nil {
			return err
		}

		if hasInit {
			log.WithFields(log.Fields{
				"package": "ha",
			}).Info("Won initialization race")

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
			log.WithFields(log.Fields{
				"package": "ha",
			}).Info("Lost initialization race")

			eb.Reset()

			log.WithFields(log.Fields{
				"package": "ha",
			}).Info("Waiting for leader in FSM")
			leader, err := ha.waitForLeader(eb)
			if err != nil {
				return err
			}
			log.WithFields(log.Fields{
				"package": "ha",
			}).Info("Found leader in FSM")

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
		case <-ha.stopc:
			return nil
		case <-ha.loopTicker:
			if err := ha.RunCycle(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ha *SingleLeaderHA) Stop() error {
	close(ha.stopc)
	ha.wg.Wait()
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
	ha.wg.Add(1)
	go ha.scanMemberChan()

	ha.wg.Add(1)
	go ha.scanLeaderChan()
}

func (ha *SingleLeaderHA) scanMemberChan() {
	for {
		select {
		case <-ha.stopc:
			ha.memberObserver.Destroy()
			ha.wg.Done()
			return
		case mu := <-ha.memberObserver.MemberCh():
			log.WithFields(log.Fields{
				"package": "ha",
			}).Infof("Member Update occurred: %v", mu)
			if err := ha.handleMemberUpdate(&mu); err != nil {
				log.WithFields(log.Fields{
					"package": "ha",
				}).Errorf("Error handling member update %+v", err)
			}
		}
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
		select {
		case <-ha.stopc:
			ha.leaderObserver.Destroy()
			ha.wg.Done()
			return
		case lu := <-ha.leaderObserver.LeaderCh():
			log.WithFields(log.Fields{
				"package": "ha",
			}).Infof("Leader Update occurred: %v", lu)
			if err := ha.handleLeaderUpdate(&lu); err != nil {
				log.WithFields(log.Fields{
					"package": "ha",
				}).Errorf("Error handling leader update %+v", err)
			}
		}
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
