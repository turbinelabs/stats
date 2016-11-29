package client

//go:generate mockgen -source $GOFILE -destination mock_$GOFILE -package $GOPACKAGE

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	apihttp "github.com/turbinelabs/api/http"
	httperr "github.com/turbinelabs/api/http/error"
	statsapi "github.com/turbinelabs/api/service/stats"
	"github.com/turbinelabs/nonstdlib/executor"
)

const (
	clientID string = "tbn-stats-client (v0.1)"

	forwardPath = "/v1.0/stats/forward"
)

// internalStatsClient is an internal interface for issuing forwarding
// requests.
type internalStatsClient interface {
	// Issues a forwarding request for the given payload with the
	// given executor.CallbackFunc.
	IssueRequest(*statsapi.Payload, executor.CallbackFunc) error
}

type httpStatsV1 struct {
	dest           apihttp.Endpoint
	requestHandler apihttp.RequestHandler
	exec           executor.Executor
}

var _ statsapi.StatsService = &httpStatsV1{}
var _ internalStatsClient = &httpStatsV1{}

// NewStatsClient returns a blocking implementation of Stats. Each
// invocation of Forward accepts a single Payload, issues a forwarding
// request to a remote stats-server and awaits a response.
func NewStatsClient(
	dest apihttp.Endpoint,
	apiKey string,
	client *http.Client,
	exec executor.Executor,
) (statsapi.StatsService, error) {
	return newInternalStatsClient(dest, apiKey, client, exec)
}

func newInternalStatsClient(
	dest apihttp.Endpoint,
	apiKey string,
	client *http.Client,
	exec executor.Executor,
) (*httpStatsV1, error) {
	if client == nil {
		return nil, fmt.Errorf("Attempting to configure StatsClient with nil *http.Client")
	}

	return &httpStatsV1{dest, apihttp.NewRequestHandler(client, apiKey, clientID), exec}, nil
}

func encodePayload(payload *statsapi.Payload) (string, error) {
	if b, err := json.Marshal(payload); err == nil {
		return string(b), nil
	} else {
		msg := fmt.Sprintf("could not encode stats payload: %+v\n%+v", err, payload)
		return "", httperr.New400(msg, httperr.UnknownEncodingCode)
	}
}

func (hs *httpStatsV1) IssueRequest(payload *statsapi.Payload, cb executor.CallbackFunc) error {
	encoded, err := encodePayload(payload)
	if err != nil {
		return err
	}

	hs.exec.Exec(
		func(ctxt context.Context) (interface{}, error) {
			response := &statsapi.ForwardResult{}
			if err := hs.requestHandler.Do(
				func() (*http.Request, error) {
					rdr := strings.NewReader(encoded)
					req, err := http.NewRequest(
						"POST",
						hs.dest.Url(forwardPath, apihttp.Params{}),
						rdr)
					if err != nil {
						return nil, err
					}
					return req.WithContext(ctxt), nil
				},
				response,
			); err != nil {
				return nil, err
			}

			return response, nil
		},
		cb,
	)
	return nil
}

func (hs *httpStatsV1) Forward(payload *statsapi.Payload) (*statsapi.ForwardResult, error) {
	responseChan := make(chan executor.Try, 1)
	defer close(responseChan)

	err := hs.IssueRequest(
		payload,
		func(try executor.Try) {
			responseChan <- try
		},
	)
	if err != nil {
		return nil, err
	}

	try := <-responseChan
	if try.IsError() {
		return nil, try.Error()
	} else {
		return try.Get().(*statsapi.ForwardResult), nil
	}
}

func (hs *httpStatsV1) Close() error {
	return nil
}

func (hs *httpStatsV1) Query(*statsapi.Query) (*statsapi.QueryResult, error) {
	panic("NOT IMPLEMENTED YET")
}
