package fsm

var MemberUpdateDeletedType = "MEMBER_DELETED"
var MemberUpdateSetType = "MEMBER_SET"

type Member interface {
	// ID returns an unique identifier
	// For the member. This is used in
	// find and Delete operations
	ID() string
	MarshalFSM() ([]byte, error)
	UnmarshalFSM(data []byte) error
}

type memberBackend struct {
	ID   string `json:"id"`
	Data []byte `json:"data"`
	Time int64  `json:"time"`
	TTL  int64  `json:"ttl"`
}

type MemberUpdate struct {
	Type string
	// The Marshalled Member structs
	OldMember     []byte
	CurrentMember []byte
}

type MemberObserver interface {
	MemberCh() <-chan MemberUpdate

	// Destroy should unregister the chan from the FSM
	Destroy() error
}
