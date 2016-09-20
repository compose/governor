package api

import (
	log "github.com/Sirupsen/logrus"
	"github.com/compose/governor/fsm"
	"github.com/compose/governor/service"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"net/http"
)

// TODO: Add long listener for updates
func registerFSMRouter(singleFSM fsm.SingleLeaderFSM, singleService service.SingleLeaderService, r *mux.Router) error {
	r.HandleFunc("/id", singleFSMIDHandler(singleFSM)).Methods("GET")
	r.HandleFunc("/leader", singleFSMLeaderHandler(singleFSM, singleService)).Methods("GET")
	r.HandleFunc("/member/{id}", singleFSMMemberHandler(singleFSM, singleService)).Methods("GET")
	r.HandleFunc("/members", singleFSMMembersHandler(singleFSM, singleService)).Methods("GET")
	return nil
}

func singleFSMIDHandler(singleFSM fsm.SingleLeaderFSM) http.HandlerFunc {
	type idAPIResp struct {
		ID uint64 `json:"id"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		id := singleFSM.UniqueID()
		if err := sendResponse(200, idAPIResp{ID: id}, []error{}, w); err != nil {
			log.Error("Error sending response for ID request")
		}
	}
}

func singleFSMLeaderHandler(singleFSM fsm.SingleLeaderFSM, singleService service.SingleLeaderService) http.HandlerFunc {
	type leaderAPIResp struct {
		Leader fsm.Leader `json:"leader"`
		Exists bool       `json:"exists"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		leaderData, exists, err := singleFSM.Leader()
		if err != nil {
			if err := sendResponse(500, nil, []error{err}, w); err != nil {
				log.Error("Error sending error response")
			}
		}
		leader, err := singleService.FSMLeaderFromBytes(leaderData)
		if err != nil {
			if err := sendResponse(500, nil, []error{err}, w); err != nil {
				log.Error("Error sending error response")
			}
		}
		if err := sendResponse(200, leaderAPIResp{Leader: leader, Exists: exists}, []error{}, w); err != nil {
			log.Error("Error sending leader response")

		}
	}
}

func singleFSMMemberHandler(singleFSM fsm.SingleLeaderFSM, singleService service.SingleLeaderService) http.HandlerFunc {
	type memberAPIResp struct {
		Member fsm.Member `json:"member"`
		Exists bool       `json:"exists"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		id, ok := vars["id"]
		if !ok {
			if err := sendResponse(400, nil, []error{errors.New("ID Not provided in request for member")}, w); err != nil {
				log.Error("Error sending error response")
			}
		}

		memberData, exists, err := singleFSM.Member(id)
		if err != nil {
			if err := sendResponse(500, nil, []error{err}, w); err != nil {
				log.Error("Error sending error response")
			}
		}
		member, err := singleService.FSMMemberFromBytes(memberData)
		if err != nil {
			if err := sendResponse(500, nil, []error{err}, w); err != nil {
				log.Error("Error sending error response")
			}
		}
		if err := sendResponse(200, memberAPIResp{Member: member, Exists: exists}, []error{}, w); err != nil {
			log.Error("Error sending leader response")

		}
	}
}

func singleFSMMembersHandler(singleFSM fsm.SingleLeaderFSM, singleService service.SingleLeaderService) http.HandlerFunc {
	type membersAPIResp struct {
		Members []fsm.Member `json:"members"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		membersData, err := singleFSM.Members()
		members := []fsm.Member{}
		for _, memberData := range membersData {
			member, err := singleService.FSMMemberFromBytes(memberData)
			if err != nil {
				if err := sendResponse(500, nil, []error{err}, w); err != nil {
					log.Error("Error sending error response")
				}
			}
			members = append(members, member)
		}
		if err != nil {
			if err := sendResponse(500, nil, []error{err}, w); err != nil {
				log.Error("Error sending error response")
			}
		}
		if err := sendResponse(200, membersAPIResp{Members: members}, []error{}, w); err != nil {
			log.Error("Error sending leader response")

		}
	}
}
