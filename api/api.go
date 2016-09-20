package api

import (
	"encoding/json"
	"github.com/compose/governor/fsm"
	"github.com/compose/governor/ha"
	"github.com/compose/governor/service"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"net/http"
)

func Router(singleFSM fsm.SingleLeaderFSM, singleHA *ha.SingleLeaderHA, singleService service.SingleLeaderService) (*mux.Router, error) {
	r := mux.NewRouter()
	if err := registerFSMRouter(singleFSM, singleService, r.PathPrefix("/fsm").Subrouter()); err != nil {
		return nil, errors.Wrap(err, "Error registering FSM subrouter for API")
	}
	if err := registerServiceRouter(singleService, r.PathPrefix("/service").Subrouter()); err != nil {
		return nil, errors.Wrap(err, "Error registering Service subrouter for API")
	}
	if err := registerHARouter(singleHA, r.PathPrefix("/ha").Subrouter()); err != nil {
		return nil, errors.Wrap(err, "Error registering HA subrouter for API")
	}
	return r, nil
}

type apiSuccessResponse struct {
	Data interface{} `json:"data,omitempty"`
}
type apiErrorResponse struct {
	Errors []error `json:"errors"`
}

func sendResponse(code int, jsonData interface{}, errorMsgs []error, w http.ResponseWriter) error {
	var resp interface{}
	if len(errorMsgs) > 0 {
		resp = apiErrorResponse{Errors: errorMsgs}
	} else if jsonData != nil {
		resp = apiSuccessResponse{Data: jsonData}
	} else {
		return errors.New("Must supply either errorsMsgs or jsonDATA to response")
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	if resp == nil {
		return nil
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return errors.Wrap(err, "Error encoding JSON into response body")
	}
	return nil
}
