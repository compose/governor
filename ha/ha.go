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
		log.WithFields(log.Fields{
			"package": "ha",
		}).Info("Service is already initialized")

		// TODO: Become leader if none exists
		leader, err := ha.waitForLeader(eb)
		if err != nil {
			return err
		}
		if err := ha.service.FollowTheLeader(leader); err != nil {
			return err
		}
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

	// We can do this since we haven't started listening to channels
	// And will have little affect on performance
	if err := ha.addAllFSMMembersToService(); err != nil {
		return errors.Wrap(err, "Error adding members to service")
	}

	return nil
}

func (ha *SingleLeaderHA) addAllFSMMembersToService() error {
	members, err := ha.allFSMMembers()
	if err != nil {
		return err
	}

	if err := ha.service.AddMembers(members); err != nil {
		return errors.Wrap(err, "Error adding members to service")
	}

	return nil
}

func (ha *SingleLeaderHA) allFSMMembers() ([]fsm.Member, error) {
	membersData, err := ha.fsm.Members()
	if err != nil {
		return []fsm.Member{}, err
	}

	members := []fsm.Member{}
	for _, member := range membersData {
		fsmMember, err := ha.service.FSMMemberFromBytes(member)
		if err != nil {
			return members, err
		}
		members = append(members, fsmMember)
	}

	return members, nil
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
	for {
		leaderData, exists, err := ha.fsm.Leader()
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

		leader, err := ha.service.FSMLeaderFromBytes(leaderData)
		if err != nil {
			return nil, errors.Wrap(err, "Error converting bytes to leader")
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
			ha.wg.Wait()
			return nil
		case <-ha.loopTicker:
			if err := ha.RunCycle(); err != nil {
				return err
			}
		}
	}
}

func (ha *SingleLeaderHA) Stop() error {
	ha.wg.Add(1)
	defer ha.wg.Done()

	close(ha.stopc)
	// TODO: This may be racey against an existing health check op

	if err := ha.fsm.Cleanup(); err != nil {
		return err
	}

	if err := ha.service.Stop(); err != nil {
		return err
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

		_, exists, err := ha.fsm.Member(member.ID())
		if err != nil {
			return errors.Wrap(err, "Error checking for member existance")
		}
		if !exists {
			if err := ha.fsm.SetMember(member); err != nil {
				return errors.Wrap(err, "Error setting member")
			}
		}

		isLeader, err := ha.IsLeader()
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

	// TODO: Fix this to find healthiest node first
	case fsm.LeaderUpdateDeletedType:
		members, err := ha.allFSMMembers()
		if err != nil {
			return err
		}
		if ha.service.IsHealthiestOf(nil, members) {
			log.WithFields(log.Fields{
				"package": "ha",
			}).Info("We are the healthiest node - trying to get leader")

			lead, err := ha.service.AsFSMLeader()
			if err != nil {
				return err
			}
			return ha.fsm.RaceForLeader(lead)
		} else {
			log.WithFields(log.Fields{
				"package": "ha",
			}).Info("We are not the healthiest node - following no leader")
			return ha.service.FollowNoLeader()
		}
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

func (ha *SingleLeaderHA) IsLeader() (bool, error) {
	leaderData, exists, err := ha.fsm.Leader()
	if err != nil {
		return false, err
	} else if !exists {
		return false, nil
	}
	curLeader, err := ha.service.FSMLeaderFromBytes(leaderData)
	if err != nil {
		return false, errors.Wrap(err, "Error getting leader from bytes")
	}

	meAsLeader, err := ha.service.AsFSMLeader()
	if err != nil {
		return false, err
	}

	return curLeader.ID() == meAsLeader.ID(), nil
}
