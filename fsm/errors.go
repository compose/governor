package fsm

import (
	"errors"
)

var ErrorTimedOutCleanup = errors.New("Timed out during cleanup")
var ErrorTimedOutDestroy = errors.New("Timed out during destroy")

var ErrorUnknownOperation = errors.New("Unknown Op")
