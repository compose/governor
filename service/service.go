package service

import (
	"fmt"
)

type Service interface {
	Initialize() error
	NeedsInitilization() error
	Start() error
	Stop() error
	Name() error
	HealthValue() int
	Ping() error
}

type AlreadyRunningError struct {
	Service string
	Info    string
}

func (e AlreadyStartedError) Error() string {
	return fmt.Sprintf("%s is already running - %s", e.Service, e.Info)
}

type NotRunningError struct {
	Service string
	Info    string
}

func (e NotRunningError) Error() string {
	return fmt.Sprintf("%s is already not running - %s", e.Service, e.Info)
}
