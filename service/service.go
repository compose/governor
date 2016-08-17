package service

import (
	"fmt"
	"github.com/compose/governor/fsm"
)

// TODO: Maybe pass leader and member data as []byte to avoid hoop jumping?
type SingleLeaderService interface {
	Initialize() error         //Check
	NeedsInitialization() bool //Check

	// Will need to typecast to the struct type
	// Hopefully no reflection required
	FollowTheLeader(leader fsm.Leader) error //Check
	FollowNoLeader() error

	// A service should NEVER have knowledge of the FSM
	// However, it should implement it's own fsm.Member
	// and fsm.Leader so it can assert a type
	// The HA loop will get all the state store info and pass it to
	// the service
	//
	// NOTE: This means each Member must have health identifying information
	// 	 And have the service cast appropriately for that info
	// NOTE: Does not return error. If an error occurs, should log the error,
	// And return false
	IsHealthiestOf(leader fsm.Leader, members []fsm.Member) bool //Check

	// This could look different for different services
	// Eg: For PG if the node is a leader, then this would
	// add replication slots for each member
	//
	// NOTE: A leader should also be in the membership pool
	//       This means that a check to ensure that a leader
	//       Isn't adding itself as a member is needed
	AddMembers(members []fsm.Member) error    //check
	DeleteMembers(members []fsm.Member) error //check

	RunningAsLeader() bool //Check

	Promote() error                 //Check
	Demote(leader fsm.Leader) error //Check
	Start() error                   //Check
	Restart() error
	Stop() error //Check

	// Let's assume each SingleLeaderService is defined as identical accross
	// a cluster and can sufficiently typecast fsm.Member/Leader upon getting on back
	AsFSMMember() (fsm.Member, error) // Check
	AsFSMLeader() (fsm.Leader, error) // Check

	FSMMemberFromBytes(data []byte) (fsm.Member, error)
	FSMLeaderFromBytes(data []byte) (fsm.Leader, error)

	FSMMemberTemplate() fsm.Member
	FSMLeaderTemplate() fsm.Leader

	IsHealthy() bool
	IsRunning() bool
	Ping() error // Check
}

type ErrorAlreadyRunning struct {
	Service string
	Info    string
}

func (e ErrorAlreadyRunning) Error() string {
	return fmt.Sprintf("%s is already running - %s", e.Service, e.Info)
}

type ErrorNotRunning struct {
	Service string
	Info    string
}

func (e ErrorNotRunning) Error() string {
	return fmt.Sprintf("%s is already not running - %s", e.Service, e.Info)
}
