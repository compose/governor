package fsm

var LeaderUpdateSetType = "LEADER_SET"
var LeaderUpdateDeletedType = "LEADER_DELETED"

type Leader interface {
	Member
}

type leaderBackend struct {
	ID   string `json:"id"`
	Data []byte `json:"data"`
	Time int64  `json:"time"`
	TTL  int64  `json:"ttl"`
}

type LeaderUpdate struct {
	Type string

	// The Marshalled Leader structs
	CurrentLeader []byte
	OldLeader     []byte
}

type LeaderObserver interface {
	LeaderCh() <-chan LeaderUpdate

	// Destroy should unregister the chan from the FSM
	Destroy() error
}
