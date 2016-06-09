package fsm

import (
	"errors"
)

var ErrorTimedOutCleanup = errors.New("Timed out during cleanup")
var ErrorTimedOutDestroy = errors.New("Timed out during destroy")

var ErrorBadTTLTimestamp = errors.New("A TTL stamp is too far off for proper operation")

var ErrorUnknownOperation = errors.New("Unknown Op")
