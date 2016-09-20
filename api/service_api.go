package api

import (
	log "github.com/Sirupsen/logrus"
	"github.com/compose/governor/service"
	"github.com/gorilla/mux"
	"net/http"
)

func registerServiceRouter(singleService service.SingleLeaderService, r *mux.Router) error {
	r.HandleFunc("/running_as_leader", singleServiceRunningAsLeaderHandler(singleService)).Methods("GET")
	r.HandleFunc("/is_running", singleServiceIsRunningHandler(singleService)).Methods("GET")
	r.HandleFunc("/is_healthy", singleServiceIsHealthyHandler(singleService)).Methods("GET")
	return nil
}

func singleServiceRunningAsLeaderHandler(singleService service.SingleLeaderService) http.HandlerFunc {
	type isLeaderAPIResp struct {
		RunningAsLeader bool `json:"running_as_leader"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		runningAsLeader := singleService.RunningAsLeader()
		if err := sendResponse(200, isLeaderAPIResp{RunningAsLeader: runningAsLeader}, []error{}, w); err != nil {
			log.Error("Error sending response for ID request")
		}
	}
}

func singleServiceIsRunningHandler(singleService service.SingleLeaderService) http.HandlerFunc {
	type isRunningAPIResp struct {
		IsRunning bool `json:"is_running"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		running := singleService.IsRunning()
		if err := sendResponse(200, isRunningAPIResp{IsRunning: running}, []error{}, w); err != nil {
			log.Error("Error sending response for ID request")
		}
	}
}

func singleServiceIsHealthyHandler(singleService service.SingleLeaderService) http.HandlerFunc {
	type isHealthyAPIResp struct {
		IsHealthy bool `json:"is_healthy"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		healthy := singleService.IsHealthy()
		if err := sendResponse(200, isHealthyAPIResp{IsHealthy: healthy}, []error{}, w); err != nil {
			log.Error("Error sending response for ID request")
		}
	}
}
