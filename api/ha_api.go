package api

import (
	log "github.com/Sirupsen/logrus"
	"github.com/compose/governor/ha"
	"github.com/gorilla/mux"
	"net/http"
)

func registerHARouter(singleHA *ha.SingleLeaderHA, r *mux.Router) error {
	r.HandleFunc("/is_leader", singleHAIsLeaderHandler(singleHA)).Methods("GET")
	return nil
}

func singleHAIsLeaderHandler(singleHA *ha.SingleLeaderHA) http.HandlerFunc {
	type isLeaderAPIResp struct {
		IsLeader bool `json:"is_leader"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		isLeader, err := singleHA.IsLeader()
		if err != nil {
			sendResponse(500, nil, []error{err}, w)
		}
		if err := sendResponse(200, isLeaderAPIResp{IsLeader: isLeader}, []error{}, w); err != nil {
			log.Error("Error sending response for request")
		}
	}
}
